/*
 * Copyright 2013 Gregory Graham.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nz.co.gregs.dbvolution;

import nz.co.gregs.dbvolution.columns.QueryColumn;
import nz.co.gregs.dbvolution.internal.query.*;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import edu.uci.ics.jung.algorithms.layout.*;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.visualization.*;
import edu.uci.ics.jung.visualization.control.*;
import edu.uci.ics.jung.visualization.decorators.ToStringLabeller;
import edu.uci.ics.jung.visualization.renderers.DefaultEdgeLabelRenderer;
import java.awt.Color;
import java.awt.Dimension;
import java.io.PrintStream;
import java.io.Serializable;
import java.sql.*;
import java.util.*;
import javax.swing.JFrame;

import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.columns.AbstractColumn;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.expressions.*;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.exceptions.*;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import nz.co.gregs.dbvolution.internal.querygraph.*;
import nz.co.gregs.dbvolution.internal.properties.*;

/**
 * The Definition of a Query on a Database
 *
 * <p>
 * DBQuery brings together several DBRow classes into a single database query.
 *
 * <p>
 * Natural joins are created while protecting against accidental Cartesian Joins
 * and Blank Queries.
 *
 * <p>
 * A DBQuery is most easily created by calling
 * {@link DBDatabase#getDBQuery(nz.co.gregs.dbvolution.DBRow...) DBDatabase's getDBQuery method}.
 *
 * <p>
 * The foreign keys from the DBRow instances will be automatically aligned and
 * the criteria defined on the DBRows will be seamlessly added to the WHERE
 * clause.
 *
 * <p>
 * Outer joins are supported using
 * {@link #addOptional(nz.co.gregs.dbvolution.DBRow...) addOptional}, as well as
 * "all OR" queries with {@link #setToMatchAnyCondition()} ( all or is a query
 * like SELECT .. FROM ... WHERE a=b OR b=c OR c=d ...)
 *
 * <p>
 * more complicated conditions can be added to the query itself using the
 * {@link #addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) addCondition method}.
 *
 * <p>
 * DBQuery can even scan the Class path and find all related DBRow classes and
 * add them on request.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBQuery implements Serializable {

	private static final long serialVersionUID = 1l;

	/**
	 * The default timeout value used to prevent accidental long running queries
	 */
	private final DBDatabase database;
	private final QueryDetails details = new QueryDetails();
	private transient QueryGraph queryGraph;
	private transient JFrame queryGraphFrame = null;

	public QueryDetails getQueryDetails() {
		return details;
	}

	/**
	 * Use of this method is not recommended as it subverts database clustering.
	 *
	 * @return a ready database from the cluster, or this DBQuery's database if
	 * not using a cluster.
	 */
	private DBDatabase getReadyDatabase() {
		if (database instanceof DBDatabaseCluster) {
			return ((DBDatabaseCluster) database).getReadyDatabase();
		} else {
			return database;
		}
	}

	private DBQuery(DBDatabase database) {
		this.database = database;
//		this.details.setDefinition(database.getDefinition());
		blankResults();
	}

	/**
	 * Don't use this, it's for DBDatabase
	 *
	 * @param database
	 * @param examples
	 * @return
	 */
	public static DBQuery getInstance(DBDatabase database, DBRow... examples) {
		DBQuery dbQuery = new DBQuery(database);
		for (DBRow example : examples) {
			dbQuery.add(example);
		}
		return dbQuery;
	}

	/**
	 *
	 * Add a table to the query.
	 *
	 * <p>
	 * This method adds the DBRow to the list of required (INNER) tables.
	 *
	 * <p>
	 * Criteria (permitted and excluded values) from this instance will be
	 * automatically included in the query and an instance of this DBRow class
	 * will be created for each DBQueryRow returned.
	 *
	 * @param examples a list of DBRow objects that defines required tables and
	 * criteria
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBQuery instance
	 */
	public DBQuery add(DBRow... examples) {
		for (DBRow table : examples) {
			details.getRequiredQueryTables().add(table);
			details.getAllQueryTables().add(table);
			blankResults();
		}
		return this;
	}

	/**
	 *
	 * Add a List of tables to the query.
	 *
	 * <p>
	 * This method adds the DBRows to the list of required (INNER) tables.
	 *
	 * <p>
	 * Criteria (permitted and excluded values) from these instances will be
	 * automatically included in the query and an instance of this DBRow class
	 * will be created for each DBQueryRow returned.
	 *
	 * @param examples a list of DBRow objects that defines required tables and
	 * criteria
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBQuery instance
	 */
	public DBQuery add(List<DBRow> examples) {
		for (DBRow table : examples) {
			details.getRequiredQueryTables().add(table);
			details.getAllQueryTables().add(table);
			blankResults();
		}
		return this;
	}

	/**
	 * Add an optional table to this query
	 *
	 * <p>
	 * This method adds an optional (OUTER) table to the query.
	 *
	 * <p>
	 * The query will return an instance of this DBRow for each row found, though
	 * it may be a null instance as there was no matching row in the database.
	 *
	 * <p>
	 * Criteria (permitted and excluded values) specified in the supplied instance
	 * will be added to the query.
	 *
	 * @param examples a list of DBRow objects that defines optional tables and
	 * criteria
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBQuery instance
	 */
	public DBQuery addOptional(DBRow... examples) {
		for (DBRow table : examples) {
			details.getOptionalQueryTables().add(table);
			details.getAllQueryTables().add(table);
			blankResults();
		}
		return this;
	}

	/**
	 * Remove tables from the query
	 *
	 * <p>
	 * This method removes previously added tables from the query.
	 *
	 * <p>
	 * Previous results and SQL are discarded, and the query is set ready to be
	 * re-run.
	 *
	 * @param examples a list of DBRow instances to remove from the query
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBQuery instance
	 */
	public DBQuery remove(DBRow... examples) {
		for (DBRow table : examples) {
			Iterator<DBRow> iterator = details.getAllQueryTables().iterator();
			while (iterator.hasNext()) {
				DBRow qtab = iterator.next();
				if (qtab.isPeerOf(table)) {
					details.getRequiredQueryTables().remove(qtab);
					details.getOptionalQueryTables().remove(qtab);
					details.getAssumedQueryTables().remove(qtab);
					iterator.remove();
				}
			}
		}
		blankResults();
		return this;
	}

	/**
	 * Generates and returns the actual SQL to be used by this query.
	 *
	 * <p>
	 * Good for debugging and great for DBAs, this is how you find out what
	 * DBvolution is really doing.
	 *
	 * <p>
	 * Generates the SQL query for retrieving the objects but does not execute the
	 * SQL. Use {@link #getAllRows() the get*Rows methods} to retrieve the rows.
	 *
	 * <p>
	 * See also {@link DBQuery#getSQLForCount() getSQLForCount}
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a String of the SQL that will be used by this DBQuery.
	 */
	public String getSQLForQuery() {
		return details.getSQLForQuery(database, new QueryState(details), QueryType.SELECT, this.details.getOptions());
	}

	/**
	 * Prints the actual SQL to be used by this query.
	 *
	 * <p>
	 * Good for debugging and great for DBAs, this is how you find out what
	 * DBvolution is really doing.
	 *
	 * <p>
	 * Prints the SQL query for retrieving the objects but does not execute the
	 * SQL. Use {@link #getAllRows() the get*Rows methods} to retrieve the rows.
	 *
	 * <p>
	 * See also {@link DBQuery#getSQLForCount() getSQLForCount}
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 */
	public void printSQLForQuery() {
		System.out.println(getSQLForQuery());
	}

	/**
	 * Returns the SQL query that will used to count the rows
	 *
	 * <p>
	 * Use this method to check the SQL that will be executed during
	 * {@link DBQuery#count() the count() method}
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a String of the SQL query that will be used to count the rows
	 * returned by this query
	 */
	protected synchronized String getSQLForCount() {
		details.setQueryType(QueryType.COUNT);
		return database.getSQLForDBQuery(details);
	}

	/**
	 * Prints the actual SQL to be used for counting all the rows returned by this
	 * query.
	 *
	 * <p>
	 * Good for debugging and great for DBAs, this is how you find out what
	 * DBvolution is really doing.
	 *
	 * <p>
	 * Prints the SQL query for counting the objects but does not execute the SQL.
	 * Use {@link #getAllRows() the get*Rows methods} to retrieve the rows.
	 *
	 * <p>
	 * See also {@link DBQuery#getSQLForQuery() getSQLForQuery}
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 */
	public void printSQLForCount() {
		System.out.println(getSQLForCount());
	}

	/**
	 * Constructs the SQL for this DBQuery using the supplied DBRows as examples
	 * and executes it on the database, returning the rows found.
	 *
	 * <p>
	 * Adds all required DBRows as inner join tables and all optional DBRows as
	 * outer join tables. All criteria specified on the DBRows will be applied.
	 * <p>
	 * Uses the defined
	 * {@link nz.co.gregs.dbvolution.annotations.DBForeignKey foreign keys} on the
	 * DBRow to connect the tables. Foreign keys that have been
	 * {@link nz.co.gregs.dbvolution.DBRow#ignoreForeignKey(java.lang.Object) ignored}
	 * are not used.
	 * <p>
	 * Criteria such as
	 * {@link DBNumber#permittedValues(java.lang.Number...)  permitted values}
	 * defined on the fields of the DBRow examples are added as part of the WHERE
	 * clause.
	 *
	 * <p>
	 * Similarly conditions added to the DBQuery using
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) addCondition}
	 * are added.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A List of DBQueryRows containing all the DBRow instances aligned
	 * with their related instances. 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws java.sql.SQLTimeoutException
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 * @see DBRow
	 * @see DBForeignKey
	 * @see QueryableDatatype
	 * @see BooleanExpression
	 * @see DBDatabase
	 */
	public List<DBQueryRow> getAllRows() throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		final QueryOptions options = details.getOptions();
		if (this.needsResults(options)) {
			details.setQueryType(QueryType.SELECT);
			database.executeDBQuery(details);
		}
		if (options.getRowLimit() > 0 && details.getResults().size() > options.getRowLimit()) {
			final int firstItemOfPage = options.getPageIndex() * options.getRowLimit();
			final int firstItemOfNextPage = (options.getPageIndex() + 1) * options.getRowLimit();
			return details.getResults().subList(firstItemOfPage, firstItemOfNextPage);
		} else {
			return details.getResults();
		}
	}

	/**
	 * Sets all the expression columns using data from the current ResultSet row.
	 *
	 * Database exceptions may be thrown
	 *
	 * @param defn
	 * @param resultSet resultSet
	 * @param queryRow queryRow
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public void setExpressionColumns(DBDefinition defn, ResultSet resultSet, DBQueryRow queryRow) throws SQLException {
		for (Map.Entry<Object, QueryableDatatype<?>> entry : details.getExpressionColumns().entrySet()) {
			final Object key = entry.getKey();
			final QueryableDatatype<?> value = entry.getValue();
			String expressionAlias = defn.formatExpressionAlias(key);
			QueryableDatatype<?> expressionQDT = value.getQueryableDatatypeForExpressionValue();
			expressionQDT.setFromResultSet(defn, resultSet, expressionAlias);
			queryRow.addExpressionColumnValue(key, expressionQDT);
		}
	}

	/**
	 * Returns all the known instances of the exemplar.
	 *
	 * <p>
	 * Similar to
	 * {@link #getAllInstancesOf(nz.co.gregs.dbvolution.DBRow) getAllInstancesOf(DBRow)}
	 *
	 * <p>
	 * Expects there to be exactly one(1) object of the exemplar type.
	 *
	 * <p>
	 * An UnexpectedNumberOfRowsException is thrown if there is zero or more than
	 * one row.
	 *
	 * @param <R> a subclass of DBRow
	 * @param exemplar an instance of R
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the ONLY instance found using this query 1 Database exceptions may
	 * be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException
	 * nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 *
	 */
	public <R extends DBRow> R getOnlyInstanceOf(R exemplar) throws SQLException, UnexpectedNumberOfRowsException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		List<R> allInstancesFound = getAllInstancesOf(exemplar, 1);
		return allInstancesFound.get(0);
	}

	/**
	 * Returns all the known instances of the exemplar.
	 *
	 * <p>
	 * A simple means of ensuring that your query has retrieved the correct
	 * results. For instance if you are looking up 2 vehicles in the database and
	 * 3 are returned, this method will throw an exception stopping the DBScript
	 * or DBTransaction automatically.
	 *
	 * <p>
	 * Similar to
	 * {@link #getAllInstancesOf(nz.co.gregs.dbvolution.DBRow) getAllInstancesOf(DBRow)}
	 *
	 * <p>
	 * Expects there to be exactly as many objects of the exemplar type as
	 * specified
	 *
	 * <p>
	 * An UnexpectedNumberOfRowsException is thrown if there is less or more
	 * instances than than specified.
	 *
	 *
	 * @param <R> a class that extends DBRow
	 * @param exemplar The DBRow class that you would like returned.
	 * @param expected The expected number of rows, an exception will be thrown if
	 * this expectation is not met.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of all the instances of the exemplar found by this query.
	 *
	 * Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException
	 * nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 *
	 */
	public <R extends DBRow> List<R> getAllInstancesOf(R exemplar, long expected) throws SQLException, UnexpectedNumberOfRowsException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		List<R> allInstancesFound = getAllInstancesOf(exemplar);
		final int actual = allInstancesFound.size();
		if (actual > expected) {
			throw new UnexpectedNumberOfRowsException(expected, actual, "Too Many Results: expected " + expected + ", actually got " + actual);
		} else if (actual < expected) {
			throw new UnexpectedNumberOfRowsException(expected, actual, "Too Few Results: expected " + expected + ", actually got " + actual);
		} else {
			return allInstancesFound;
		}
	}

	private boolean needsResults(QueryOptions options) {
		return details.needsResults(options);
	}

	/**
	 * Finds all instances of the exemplar in the results and returns them.
	 *
	 * <p>
	 * Allows easy retrieval of all the examples of a DBRow class regardless of
	 * DBQueryRows they are in.
	 *
	 * <p>
	 * Facilitates processing of rows on a single table retrieved via a
	 * complicated series of tables.
	 *
	 * @param <R> a class that extends DBRow
	 * @param exemplar an instance of R that has been included in the query
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return A List of all the instances found of the exemplar.
	 *
	 * Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 */
	public <R extends DBRow> List<R> getAllInstancesOf(R exemplar) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		List<R> arrayList = new ArrayList<>();
		final QueryOptions options = details.getOptions();
		if (details.needsResults(options)) {
			details.setQueryType(QueryType.SELECT);
			database.executeDBQuery(details);
		}
		if (!details.getResults().isEmpty()) {
			for (DBQueryRow row : details.getResults()) {
				final R found = row.get(exemplar);
				if (found != null) { // in case there are no items of the exemplar
					if (!arrayList.contains(found)) {
						arrayList.add(found);
					}
				}
			}
		}
		return arrayList;
	}

	/**
	 * Convenience method to print all the rows in the current collection
	 * Equivalent to: printAll(System.out);
	 *
	 * @throws java.sql.SQLException database exception
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 */
	public void print() throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		print(System.out);
	}

	/**
	 * Fast way to print the results
	 *
	 * myTable.printRows(System.err);
	 *
	 * @param ps a printstream to print to.
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 *
	 */
	public void print(PrintStream ps) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		final QueryOptions options = details.getOptions();
		if (needsResults(options)) {
			details.setQueryType(QueryType.SELECT);
			database.executeDBQuery(details);
		}

		for (DBQueryRow row : details.getResults()) {
			String tableSeparator = "";
			for (DBRow tab : details.getAllQueryTables()) {
				ps.print(tableSeparator);
				DBRow rowPart = row.get(tab);
				if (rowPart != null) {
					String rowPartStr = rowPart.toString();
					ps.print(rowPartStr);
				}
				tableSeparator = " | ";
			}
			StringBuilder string = new StringBuilder();
			String separator = "";
			for (Map.Entry<Object, QueryableDatatype<?>> entry : row.getExpressionColumns().entrySet()) {
				Object key = entry.getKey();
				QueryableDatatype<?> qdt = entry.getValue();
				string.append(separator);
				string.append(" ");
				string.append(qdt.getColumnExpression()[0].toSQLString(database.getDefinition()));
				string.append(":");
				string.append(qdt.getValue().toString());
				separator = ",";
				ps.print(string.toString());
			}
			ps.println();
		}
	}

	/**
	 * Fast way to print the results.
	 *
	 * <p>
	 * Retrieves the rows if required and then prints all of the rows but only the
	 * fields that have non-null values.
	 *
	 * <p>
	 * Helps to trim a wide printout of columns down to only the data specified in
	 * the rows.
	 *
	 * <p>
	 * Example: myQuery.printAllDataColumns(System.err);
	 *
	 * @param printStream a printstream to print to
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 *
	 */
	public void printAllDataColumns(PrintStream printStream) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		final QueryOptions options = details.getOptions();
		if (needsResults(options)) {
			details.setQueryType(QueryType.SELECT);
			database.executeDBQuery(details);
		}

		for (DBQueryRow row : details.getResults()) {
			for (DBRow tab : this.details.getAllQueryTables()) {
				DBRow rowPart = row.get(tab);
				if (rowPart != null) {
					String rowPartStr = rowPart.toString();
					printStream.print(rowPartStr);
				}
			}
			printStream.println();
		}
	}

	/**
	 * Fast way to print the results.
	 *
	 * <p>
	 * Retrieves and prints all the rows but only prints the primary key columns.
	 *
	 * <p>
	 * Example: myQuery.printAllPrimaryKeys(System.err);
	 *
	 * @param ps a PrintStream to print to.
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 *
	 */
	public void printAllPrimaryKeys(PrintStream ps) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		final QueryOptions options = details.getOptions();
		if (needsResults(options)) {
			details.setQueryType(QueryType.SELECT);
			database.executeDBQuery(details);
		}

		for (DBQueryRow row : details.getResults()) {
			for (DBRow tab : this.details.getAllQueryTables()) {
				DBRow rowPart = row.get(tab);
				if (rowPart != null) {
					final List<QueryableDatatype<?>> primaryKeys = rowPart.getPrimaryKeys();
					for (QueryableDatatype<?> primaryKey : primaryKeys) {
						if (primaryKey != null) {
							String rowPartStr = primaryKey.toSQLString(getReadyDatabase().getDefinition());
							ps.print(" " + rowPart.getPrimaryKeyColumnNames() + ": " + rowPartStr);
						}
					}
				}
			}
			ps.println();
		}
	}

	/**
	 * Remove all tables from the query and discard any results or state.
	 *
	 * <p>
	 * Clears all the settings and collections within this instance and set it
	 * back to a blank state
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return this DBQuery instance.
	 */
	public DBQuery clear() {
		details.clear();
		return this;
	}

	/**
	 * Count the rows on the database without retrieving the rows.
	 *
	 * <p>
	 * Either: counts the results already retrieved, or creates a
	 * {@link #getSQLForCount() count query} for this instance and retrieves the
	 * number of rows that would have been returned had
	 * {@link #getAllRows()  getAllRows()} been called.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the number of rows that have or will be retrieved. Database
	 * exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 */
	public Long count() throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		if (needsResults(details.getOptions())) {
			this.details.setQueryType(QueryType.COUNT);
			database.executeDBQuery(details);
			return details.getCount();
		} else {
			return (long) details.getResults().size();
		}
	}

	/**
	 * Test whether this DBQuery will create a query without limitations.
	 *
	 * <p>
	 * Checks this instance for criteria and conditions and returns FALSE if at
	 * least one constraint has been placed on the query.
	 *
	 * <p>
	 * This helps avoid the common mistake of accidentally retrieving all the rows
	 * of the tables by forgetting to add criteria.
	 *
	 * <p>
	 * No attempt to compare the length of the query results with the length of
	 * the table is made: if your criteria selects all the row of the tables this
	 * method will still return FALSE.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if the DBQuery will retrieve all the rows of the tables, FALSE
	 * otherwise
	 */
	public boolean willCreateBlankQuery() {
		return details.willCreateBlankQuery(database);
	}

	/**
	 * Limit the query to only returning a certain number of rows.
	 *
	 * <p>
	 * Implements support of the LIMIT and TOP operators of many databases. Also
	 * sets the "page" length for retrieving rows by pages.
	 *
	 * <p>
	 * Only the specified number of rows will be returned from the database and
	 * DBvolution.
	 *
	 * <p>
	 * Only positive limits are permitted: negative numbers will be converted to
	 * zero(0). To remove the row limit use {@link #clearRowLimit() }.
	 *
	 * @param maximumNumberOfRowsReturned the require limit to the number of rows
	 * returned
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBQuery instance
	 * @see #clearRowLimit()
	 */
	public DBQuery setRowLimit(int maximumNumberOfRowsReturned) {
		int limit = maximumNumberOfRowsReturned;
		if (maximumNumberOfRowsReturned < 0) {
			limit = 0;
		}

		details.getOptions().setRowLimit(limit);
		blankResults();

		return this;
	}

	/**
	 * Clear the row limit on this DBQuery and return it to retrieving all rows.
	 *
	 * <p>
	 * Also resets the retrieved results so that the database will be re-queried.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return this DBQuery instance
	 * @see #setRowLimit(int)
	 */
	public DBQuery clearRowLimit() {
		details.getOptions().setRowLimit(-1);
		blankResults();

		return this;
	}

	/**
	 * Sets the sort order of properties (field and/or method) by the given
	 * property object references.
	 *
	 * <p>
	 * For example the following code snippet will sort by just the name column:
	 * <pre>
	 * Customer customer = ...;
	 * query.setSortOrder(customer.column(customer.name).getSortProvider());
	 * </pre>
	 * <p>
	 * The following code snippet will sort by just the length of the name column:
	 * <pre>
	 * Customer customer = ...;
	 * query.setSortOrder(customer.column(customer.name).length().ascending());
	 * </pre>
	 *
	 * <p>
	 * Where possible DBvolution sorts NULL values as the least significant value,
	 * for example "NULL, 1, 2, 3, 4..." not "... 4, 5, 6, NULL".
	 *
	 * @param sortColumns a list of sort providers to sort the query by.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBQuery instance
	 */
	public DBQuery setSortOrder(SortProvider... sortColumns) {
		blankResults();
		details.setSortOrder(sortColumns);
		return this;
	}

	/**
	 * Sets the sort order of properties (field and/or method) by the given
	 * property object references.
	 *
	 * <p>
	 * For example the following code snippet will sort by just the name column:
	 * <pre>
	 * Customer customer = ...;
	 * query.setSortOrder(customer.column(customer.name));
	 * </pre>
	 *
	 * <p>
	 * Where possible DBvolution sorts NULL values as the least significant value,
	 * for example "NULL, 1, 2, 3, 4..." not "... 4, 5, 6, NULL".
	 *
	 * @param columns a list of columns to sort the query by.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBQuery instance
	 */
	public DBQuery setSortOrder(ColumnProvider... columns) {
		List<SortProvider> results = new ArrayList<SortProvider>();
		for (ColumnProvider column : columns) {
			results.add(column.getSortProvider());
		}
		return setSortOrder(results.toArray(new SortProvider[]{}));
	}

	/**
	 * Adds the properties (field and/or method) to the end of the sort order.
	 *
	 * <p>
	 * For example the following code snippet will add the name column at the end
	 * of the sort order after district:
	 * <pre>
	 * Customer customer = ...;
	 * query.setSortOrder(customer.column(customer.district));
	 * query.addToSortOrder(customer.column(customer.name));
	 * </pre>
	 *
	 * <p>
	 * Note that the above example is equivalent to:
	 * <pre>
	 * Customer customer = ...;
	 * query.setSortOrder(customer.column(customer.district), customer.column(customer.name));
	 * </pre>
	 *
	 * @param sortColumns a list of columns to sort the query by.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBQuery instance
	 */
	public DBQuery addToSortOrder(SortProvider... sortColumns) {
		details.addToSortOrder(sortColumns);
		return this;
	}

	/**
	 * Remove all sorting that has been set on this DBQuery
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return this DBQuery instance
	 */
	public DBQuery clearSortOrder() {
		details.clearSortOrder();
		return this;
	}

	/**
	 * Change the Default Setting of Disallowing Blank Queries
	 *
	 * <p>
	 * A common mistake is creating a query without supplying criteria and
	 * accidently retrieving a huge number of rows.
	 *
	 * <p>
	 * DBvolution detects this situation and, by default, throws a
	 * {@link nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException AccidentalBlankQueryException}
	 * when it happens.
	 *
	 * <p>
	 * To change this behaviour, and allow blank queries, call
	 * {@code setBlankQueriesAllowed(true)}.
	 *
	 * @param allow - TRUE to allow blank queries, FALSE to return it to the
	 * default setting.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBQuery instance
	 */
	public DBQuery setBlankQueryAllowed(boolean allow) {
		this.details.getOptions().setBlankQueryAllowed(allow);

		return this;
	}

	/**
	 * Change the Default Setting of Disallowing Accidental Cartesian Joins
	 *
	 * <p>
	 * A common mistake is to create a query without connecting all the tables in
	 * the query and accident retrieve a huge number of rows.
	 *
	 * <p>
	 * DBvolution detects this situation and, by default, throws a
	 * {@link nz.co.gregs.dbvolution.exceptions.AccidentalCartesianJoinException AccidentalCartesianJoinException}
	 * when it happens.
	 *
	 * <p>
	 * To change this behaviour, and allow cartesian joins, call
	 * {@code setCartesianJoinsAllowed(true)}.
	 *
	 * @param allow - TRUE to allow cartesian joins, FALSE to return it to the
	 * default setting.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBQuery instance
	 */
	public DBQuery setCartesianJoinsAllowed(boolean allow) {
		this.details.getOptions().setCartesianJoinAllowed(allow);

		return this;
	}

	/**
	 * Constructs the SQL for this DBQuery and executes it on the database,
	 * returning the rows found.
	 *
	 * <p>
	 * Like {@link #getAllRows()  getAllRows()} this method retrieves all the rows
	 * for this DBQuery. However it checks the number of rows retrieved and throws
	 * a {@link UnexpectedNumberOfRowsException} if the number of rows retrieved
	 * differs from the expected number.
	 *
	 * <p>
	 * Adds all required DBRows as inner join tables and all optional DBRow as
	 * outer join tables.
	 * <p>
	 * Uses the defined
	 * {@link nz.co.gregs.dbvolution.annotations.DBForeignKey foreign keys} on the
	 * DBRow and multi-table conditions to connect the tables. Foreign keys that
	 * have been
	 * {@link nz.co.gregs.dbvolution.DBRow#ignoreForeignKey(java.lang.Object) ignored}
	 * are not used.
	 * <p>
	 * Criteria such as
	 * {@link DBNumber#permittedValues(java.lang.Number...)  permitted values}
	 * defined on the fields of the DBRow examples are added as part of the WHERE
	 * clause.
	 *
	 * <p>
	 * Similarly conditions added to the DBQuery using
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) addCondition}
	 * are added.
	 *
	 * @param expectedRows - the number of rows expected to be retrieved
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return A List of DBQueryRows containing all the DBRow instances aligned
	 * with their related instances.
	 *
	 * <p>
	 * Database exceptions may be thrown.
	 *
	 * @throws nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException
	 * @throws java.sql.SQLException
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 */
	public List<DBQueryRow> getAllRows(long expectedRows) throws UnexpectedNumberOfRowsException, SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		List<DBQueryRow> allRows = getAllRows();
		if (allRows.size() != expectedRows) {
			throw new UnexpectedNumberOfRowsException(expectedRows, allRows.size());
		} else {
			return allRows;
		}
	}

	/**
	 * Returns the current setting for ANSI join syntax.
	 *
	 * <p>
	 * Indicates whether or not this query will use JOIN in the FROM clause or
	 * treat foreign keys as a constraint in the WHERE clause.
	 *
	 * <p>
	 * N.B. Optional (outer) tables are only supported with ANSI syntax.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the useANSISyntax flag
	 */
	public boolean isUseANSISyntax() {
		return details.getOptions().isUseANSISyntax();
	}

	/**
	 * Sets whether this DBQuery will use ANSI syntax.
	 *
	 * <p>
	 * The default is to use ANSI syntax.
	 *
	 * <p>
	 * You should probably use ANSI syntax.
	 *
	 * <p>
	 * ANSI syntax has the foreign key and added relationships defined in the FROM
	 * clause with the JOIN operator. Pre-ANSI syntax treated the foreign keys and
	 * other relationships as part of the WHERE clause.
	 *
	 * <p>
	 * ANSI syntax supports OUTER joins with a standard syntax, and DBvolution
	 * supports OUTER thru the ANSI syntax.
	 *
	 * @param useANSISyntax the useANSISyntax flag to set
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBQuery instance
	 */
	public DBQuery setUseANSISyntax(boolean useANSISyntax) {
		this.details.getOptions().setUseANSISyntax(useANSISyntax);

		return this;
	}

	/**
	 * Creates a list of all DBRow subclasses that reference the DBRows within
	 * this query with foreign keys.
	 *
	 * <p>
	 * Similar to {@link #getReferencedTables() } but where this class is being
	 * referenced by the external DBRow subclass.
	 *
	 * <p>
	 * That is to say: where A is a DBRow in this query, returns a List of B such
	 * that B =&gt; A
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a list of classes that have a {@code @DBForeignKey} reference to
	 * this class
	 * @see #getReferencedTables()
	 * @see DBRow#getRelatedTables()
	 * @see DBRow#getReferencedTables()
	 */
	public SortedSet<DBRow> getRelatedTables() throws UnableToInstantiateDBRowSubclassException {
		SortedSet<Class<? extends DBRow>> resultClasses;
		resultClasses = new TreeSet<>(new DBRowClassNameComparator());

		SortedSet<DBRow> result = new TreeSet<>(new DBRowNameComparator());
		for (DBRow table : details.getAllQueryTables()) {
			SortedSet<Class<? extends DBRow>> allRelatedTables = table.getRelatedTables();
			for (Class<? extends DBRow> connectedTable : allRelatedTables) {
				try {
					if (resultClasses.add(connectedTable)) {
						result.add(connectedTable.newInstance());
					}
				} catch (IllegalAccessException | InstantiationException ex) {
					throw new UnableToInstantiateDBRowSubclassException(connectedTable, ex);
				}
			}
		}

		return result;
	}

	/**
	 * Returns all the DBRow subclasses referenced by the DBrows within this query
	 * with foreign keys
	 *
	 * <p>
	 * Similar to {@link #getAllConnectedTables() } but where this class directly
	 * references the external DBRow subclass with an {@code @DBForeignKey}
	 * annotation.
	 *
	 * <p>
	 * That is to say: where A is A DBRow in this class, returns a List of B such
	 * that A =&gt; B
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A list of DBRow subclasses referenced with {@code @DBForeignKey}
	 * @see #getRelatedTables()
	 * @see DBRow#getReferencedTables()
	 * @see DBRow#getRelatedTables()
	 *
	 */
	@SuppressWarnings("unchecked")
	public SortedSet<DBRow> getReferencedTables() {
		SortedSet<DBRow> result = new TreeSet<>(new DBRowNameComparator());
		for (DBRow table : details.getAllQueryTables()) {
			Set<Class<? extends DBRow>> allRelatedTables = table.getReferencedTables();
			for (Class<? extends DBRow> connectedTable : allRelatedTables) {
				try {
					result.add(connectedTable.newInstance());
				} catch (InstantiationException | IllegalAccessException ex) {
					throw new UnableToInstantiateDBRowSubclassException(connectedTable, ex);
				}
			}
		}

		return result;
	}

	/**
	 * Returns all the DBRow subclasses used in this query.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A list of DBRow subclasses included in this query.
	 * @see #getRelatedTables()
	 * @see #getReferencedTables()
	 * @see DBRow#getReferencedTables()
	 * @see DBRow#getRelatedTables()
	 *
	 */
	protected List<DBRow> getAllQueryTables() {
		return details.getAllQueryTables();
	}

	/**
	 * Creates a list of all DBRow subclasses that are connected to this query.
	 *
	 * <p>
	 * Uses {@link #getReferencedTables() } and {@link #getRelatedTables() } to
	 * produce a complete list of tables connected by foreign keys to the DBRow
	 * classes within this query.
	 *
	 * <p>
	 * That is to say: where A is a DBRow in this query, returns a List of B such
	 * that B =&gt; A or A =&gt; B
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a list of classes that have a {@code @DBForeignKey} reference to or
	 * from this class
	 * @see #getRelatedTables()
	 * @see #getReferencedTables()
	 * @see DBRow#getAllConnectedTables()
	 * @see DBRow#getReferencedTables()
	 * @see DBRow#getRelatedTables()
	 *
	 */
	public Set<DBRow> getAllConnectedTables() {
		final Set<DBRow> result = getReferencedTables();
		result.addAll(getRelatedTables());
		return result;
	}

	/**
	 * Search the classpath and add any DBRow classes that are connected to the
	 * DBRows within this DBQuery
	 *
	 * <p>
	 * This method automatically enlarges the query by finding all associated
	 * DBRow classes and adding them to the query.
	 *
	 * <p>
	 * In a sense this expands the query out by one level of indirection.
	 *
	 * <p>
	 * N.B. for any realistic database, repeatedly calling this method will
	 * quickly make the query impossibly large.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return this DBQuery instance
	 * @throws UnableToInstantiateDBRowSubclassException
	 */
	public DBQuery addAllConnectedTables() throws UnableToInstantiateDBRowSubclassException {
		List<DBRow> tablesToAdd = new ArrayList<>();
		for (DBRow table : details.getAllQueryTables()) {
			Set<Class<? extends DBRow>> allConnectedTables = table.getAllConnectedTables();
			for (Class<? extends DBRow> connectedTable : allConnectedTables) {
				try {
					tablesToAdd.add(connectedTable.newInstance());
				} catch (InstantiationException | IllegalAccessException ex) {
					throw new UnableToInstantiateDBRowSubclassException(connectedTable, ex);
				}
			}
		}
		add(tablesToAdd.toArray(new DBRow[]{}));

		return this;
	}

	/**
	 * Search the classpath and add any DBRow classes that are connected to the
	 * DBRows within this DBQuery
	 *
	 * <p>
	 * This method automatically enlarges the query by finding all associated
	 * DBRow classes and adding them to the query.
	 *
	 * <p>
	 * In a sense this expands the query out by one level of indirection.
	 *
	 * <p>
	 * N.B. for any realistic database, repeatedly calling this method will
	 * quickly make the query impossibly large.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return this DBQuery instance
	 * @throws UnableToInstantiateDBRowSubclassException
	 */
	public DBQuery addAllConnectedBaseTables() throws UnableToInstantiateDBRowSubclassException {
		List<DBRow> tablesToAdd = new ArrayList<>();
		for (DBRow table : details.getAllQueryTables()) {
			Set<Class<? extends DBRow>> allConnectedTables = table.getAllConnectedBaseTables();
			for (Class<? extends DBRow> connectedTable : allConnectedTables) {
				try {
					tablesToAdd.add(connectedTable.newInstance());
				} catch (InstantiationException | IllegalAccessException ex) {
					throw new UnableToInstantiateDBRowSubclassException(connectedTable, ex);
				}
			}
		}
		add(tablesToAdd.toArray(new DBRow[]{}));

		return this;
	}

	/**
	 * Search the classpath and add, as optional, any DBRow classes that reference
	 * the DBRows within this DBQuery
	 *
	 * <p>
	 * This method automatically enlarges the query by finding all associated
	 * DBRow classes and adding them to the query as optional tables.
	 *
	 * <p>
	 * In a sense this expands the query out by one level of indirection.
	 *
	 * <p>
	 * N.B. for any realistic database, repeatedly calling this method will
	 * quickly make the query impossibly large.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return this DBQuery instance
	 * @throws UnableToInstantiateDBRowSubclassException
	 */
	public DBQuery addAllConnectedTablesAsOptional() throws UnableToInstantiateDBRowSubclassException {
		Set<DBRow> tablesToAdd = new HashSet<>();
		List<Class<DBRow>> alreadyAddedClasses = new ArrayList<>();
		for (DBRow table : details.getAllQueryTables()) {
			@SuppressWarnings("unchecked")
			Class<DBRow> aClass = (Class<DBRow>) table.getClass();
			alreadyAddedClasses.add(aClass);
		}
		for (DBRow table : details.getAllQueryTables()) {
			Set<Class<? extends DBRow>> allRelatedTables = table.getAllConnectedTables();
			for (Class<? extends DBRow> relatedTable : allRelatedTables) {
				DBRow newInstance;
				try {
					newInstance = relatedTable.newInstance();
				} catch (InstantiationException | IllegalAccessException ex) {
					throw new UnableToInstantiateDBRowSubclassException(relatedTable, ex);
				}
				@SuppressWarnings("unchecked")
				final Class<DBRow> newInstanceClass = (Class<DBRow>) newInstance.getClass();
				if (!alreadyAddedClasses.contains(newInstanceClass)) {
					tablesToAdd.add(newInstance);
					alreadyAddedClasses.add(newInstanceClass);
				}
			}
		}
		addOptional(tablesToAdd.toArray(new DBRow[]{}));

		return this;
	}

	/**
	 * Search the classpath and add, as optional, any DBRow classes that reference
	 * the DBRows within this DBQuery
	 *
	 * <p>
	 * This method automatically enlarges the query by finding all associated
	 * DBRow classes and adding them to the query as optional tables.
	 *
	 * <p>
	 * In a sense this expands the query out by one level of indirection.
	 *
	 * <p>
	 * N.B. for any realistic database, repeatedly calling this method will
	 * quickly make the query impossibly large.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return this DBQuery instance
	 * @throws UnableToInstantiateDBRowSubclassException
	 */
	public DBQuery addAllConnectedBaseTablesAsOptional() throws UnableToInstantiateDBRowSubclassException {
		Set<DBRow> tablesToAdd = new HashSet<>();
		List<Class<DBRow>> alreadyAddedClasses = new ArrayList<>();
		for (DBRow table : details.getAllQueryTables()) {
			@SuppressWarnings("unchecked")
			Class<DBRow> aClass = (Class<DBRow>) table.getClass();
			alreadyAddedClasses.add(aClass);
		}
		for (DBRow table : details.getAllQueryTables()) {
			Set<Class<? extends DBRow>> allRelatedTables = table.getAllConnectedBaseTables();
			for (Class<? extends DBRow> relatedTable : allRelatedTables) {
//				DBRow newInstance = relatedTable.newInstance();
				DBRow newInstance;
				try {
					newInstance = relatedTable.newInstance();
				} catch (InstantiationException | IllegalAccessException ex) {
					throw new UnableToInstantiateDBRowSubclassException(relatedTable, ex);
				}
				@SuppressWarnings("unchecked")
				final Class<DBRow> newInstanceClass = (Class<DBRow>) newInstance.getClass();
				if (!alreadyAddedClasses.contains(newInstanceClass)) {
					tablesToAdd.add(newInstance);
					alreadyAddedClasses.add(newInstanceClass);
				}
			}
		}
		addOptional(tablesToAdd.toArray(new DBRow[]{}));

		return this;
	}

	/**
	 * Adds all the connected tables as branches, rather than a mesh.
	 *
	 * <p>
	 * Adding connected tables means adding their connections as well. However
	 * sometimes you just want the tables added without connecting them to all the
	 * other tables correctly.
	 *
	 * <p>
	 * This method adds all the connected tables as if they were only connected to
	 * the core tables and had no other relationships.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return this DBQuery instance
	 * @throws UnableToInstantiateDBRowSubclassException
	 */
	public DBQuery addAllConnectedTablesAsOptionalWithoutInternalRelations() throws UnableToInstantiateDBRowSubclassException {
		Set<DBRow> tablesToAdd = new HashSet<>();
		List<Class<DBRow>> alreadyAddedClasses = new ArrayList<>();
		final List<DBRow> allQueryTables = details.getAllQueryTables();
		DBRow[] originalTables = allQueryTables.toArray(new DBRow[]{});

		for (DBRow table : allQueryTables) {
			@SuppressWarnings("unchecked")
			Class<DBRow> aClass = (Class<DBRow>) table.getClass();
			alreadyAddedClasses.add(aClass);
		}
		for (DBRow table : allQueryTables) {
			Set<Class<? extends DBRow>> allRelatedTables = table.getAllConnectedTables();
			for (Class<? extends DBRow> relatedTable : allRelatedTables) {
//				DBRow newInstance = relatedTable.newInstance();
				DBRow newInstance;
				try {
					newInstance = relatedTable.newInstance();
				} catch (InstantiationException | IllegalAccessException ex) {
					throw new UnableToInstantiateDBRowSubclassException(relatedTable, ex);
				}
				@SuppressWarnings("unchecked")
				final Class<DBRow> newInstanceClass = (Class<DBRow>) newInstance.getClass();
				if (!alreadyAddedClasses.contains(newInstanceClass)) {
					newInstance.ignoreAllForeignKeysExceptFKsTo(originalTables);
					tablesToAdd.add(newInstance);
					alreadyAddedClasses.add(newInstanceClass);
				}
			}
		}
		addOptional(tablesToAdd.toArray(new DBRow[]{}));

		return this;
	}

	/**
	 * Provides all the DBQueryRows that the instance provided is part of.
	 *
	 * <p>
	 * This method returns the subset of this DBQuery's results that include the
	 * provided instance.
	 *
	 * <p>
	 * Slicing the results like this allows you to get a list of, for instance,
	 * status table DBRows and then process the DBQueryRows that have each status
	 * DBRow as a block.
	 *
	 * @param instance the DBRow instance you are interested in.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return A list of DBQueryRow instances that relate to the exemplar 1
	 * Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 */
	public List<DBQueryRow> getAllRowsContaining(DBRow instance) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		final QueryOptions options = details.getOptions();
		if (this.needsResults(options)) {
			details.setQueryType(QueryType.SELECT);
			database.executeDBQuery(details);
		}
		List<DBQueryRow> returnList = new ArrayList<>();
		for (DBQueryRow row : details.getResults()) {
			if (row.get(instance) == instance) {
				returnList.add(row);
			}
		}
		return returnList;
	}

	/**
	 * Retrieves that DBQueryRows for the page supplied.
	 *
	 * <p>
	 * DBvolution supports paging through this method. Use {@link #setRowLimit(int)
	 * } to set the page size and then call this method with the desired page
	 * number.
	 *
	 * <p>
	 * This method is zero-based so the first page is getPage(0).
	 *
	 * <p>
	 * Convenience method for {@link #getAllRowsForPage(java.lang.Integer) }.
	 *
	 * @param pageNumber	pageNumber
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of the DBQueryRows for the selected page. 1 Database
	 * exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 */
	public List<DBQueryRow> getPage(Integer pageNumber) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		return getAllRowsForPage(pageNumber);
	}

	/**
	 * Retrieves that DBQueryRows for the page supplied.
	 *
	 * <p>
	 * DBvolution supports paging through this method. Use {@link #setRowLimit(int)
	 * } to set the page size and then call this method with the desired page
	 * number.
	 *
	 * <p>
	 * This method is zero-based so the first page is getAllRowsForPage(0).
	 *
	 * @param pageNumber	pageNumber
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of the DBQueryRows for the selected page. 1 Database
	 * exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 */
	public List<DBQueryRow> getAllRowsForPage(Integer pageNumber) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		final QueryOptions options = details.getOptions();
		details.setQueryType(QueryType.ROWSFORPAGE);
		details.setResultsPageIndex(pageNumber);
		if (this.needsResults(options)) {
			database.executeDBQuery(details);
		}
		return details.getCurrentPage();
	}

	/**
	 * Use this method to add complex conditions to the DBQuery.
	 *
	 * <p>
	 * This method takes a BooleanExpression and adds it to the where clause of
	 * the Query
	 *
	 * <p>
	 * The easiest way to get a BooleanExpression is the DBRow.column() method and
	 * then apply the functions you require until you get a BooleanExpression
	 * back.
	 *
	 * <p>
	 * StringExpression, NumberExpression, DateExpression, and BooleanExpression
	 * all provide methods that will help. In particular they have the value()
	 * method to convert base Java types to expressions.
	 *
	 * <p>
	 * Standard uses of this method are:
	 * <pre>
	 * addCondition(myRow.column(myRow.myColumn).like("%THis%"));
	 * addCondition(myRow.column(myRow.myNumber).cos().greaterThan(0.5));
	 * addCondition(StringExpression.value("THis").like(myRwo.column(myRow.myColumn)));
	 * addCondition(BooleanExpression.anyOf(
	 * myRow.column(myRow.myColumn).between("That", "This"),
	 * myRow.column(myRow.myColumn).is("Something"))
	 * );
	 * </pre>
	 *
	 * @param condition a boolean expression that defines a require limit on the
	 * results of the query
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBQuery instance
	 */
	public DBQuery addCondition(BooleanExpression condition) {
		if (condition.isAggregator()) {
			details.setHavingColumns(condition);
			details.setGroupByRequiredByAggregator(true);
		} else {
			details.getConditions().add(condition);
		}
		blankResults();
		return this;
	}

	/**
	 * Use this method to add complex conditions to the DBQuery.
	 *
	 * <p>
	 * This method takes BooleanExpressions and adds them to the where clause of
	 * the Query
	 *
	 * <p>
	 * The easiest way to get a BooleanExpression is the DBRow.column() method and
	 * then apply the functions you require until you get a BooleanExpression
	 * back.
	 *
	 * <p>
	 * StringExpression, NumberExpression, DateExpression, and BooleanExpression
	 * all provide methods that will help. In particular they have the value()
	 * method to convert base Java types to expressions.
	 *
	 * <p>
	 * Standard uses of this method are:
	 * <pre>
	 * addConditions(myRow.column(myRow.myColumn).like("%THis%"));
	 * addConditions(myRow.column(myRow.myNumber).cos().greaterThan(0.5));
	 * addConditions(StringExpression.value("THis").like(myRwo.column(myRow.myColumn)));
	 * addConditions(BooleanExpression.anyOf(
	 * myRow.columns(myRow.myColumn).between("That", "This"),
	 * myRow.columns(myRow.myColumn).is("Something"))
	 * );
	 * </pre>
	 *
	 * @param conditions boolean expressions that define required limits on the
	 * results of the query
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBQuery instance
	 */
	public DBQuery addConditions(BooleanExpression... conditions) {
		for (BooleanExpression condition : conditions) {
			addCondition(condition);
		}
		return this;
	}

	/**
	 * Use this method to add complex conditions to the DBQuery.
	 *
	 * <p>
	 * This method takes BooleanExpressions and adds them to the where clause of
	 * the Query
	 *
	 * <p>
	 * The easiest way to get a BooleanExpression is the DBRow.column() method and
	 * then apply the functions you require until you get a BooleanExpression
	 * back.
	 *
	 * <p>
	 * StringExpression, NumberExpression, DateExpression, and BooleanExpression
	 * all provide methods that will help. In particular they have the value()
	 * method to convert base Java types to expressions.
	 *
	 * <p>
	 * Standard uses of this method are:
	 * <pre>
	 * addConditions(myRow.column(myRow.myColumn).like("%THis%"));
	 * addConditions(myRow.column(myRow.myNumber).cos().greaterThan(0.5));
	 * addConditions(StringExpression.value("THis").like(myRwo.column(myRow.myColumn)));
	 * addConditions(BooleanExpression.anyOf(
	 * myRow.columns(myRow.myColumn).between("That", "This"),
	 * myRow.columns(myRow.myColumn).is("Something"))
	 * );
	 * </pre>
	 *
	 * @param conditions boolean expressions that define required limits on the
	 * results of the query
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBQuery instance
	 */
	public DBQuery addConditions(Collection<BooleanExpression> conditions) {
		for (BooleanExpression condition : conditions) {
			addCondition(condition);
		}
		return this;
	}

	/**
	 * Remove all conditions from this query.
	 *
	 * @see #addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBQuery object
	 */
	public DBQuery clearConditions() {
		details.getConditions().clear();
		blankResults();
		return this;
	}

	/**
	 * Set the query to return rows that match any conditions
	 *
	 * <p>
	 * This means that all permitted*, excluded*, and comparisons are optional for
	 * any rows and rows will be returned if they match any of the conditions.
	 *
	 * <p>
	 * The conditions will be connected by OR in the SQL.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return this DBQuery instance
	 */
	public DBQuery setToMatchAnyCondition() {
		details.getOptions().setMatchAnyConditions();
		blankResults();
		return this;
	}

	/**
	 * Set the query to return rows that match any relationship.
	 *
	 * <p>
	 * This means that all foreign keys and ad hoc relationships are optional for
	 * all tables and rows will be returned if they match one of the
	 * relationships.
	 *
	 * <p>
	 * The relationships will be connected by OR in the SQL.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return this DBQuery instance
	 */
	public DBQuery setToMatchAnyRelationship() {
		details.getOptions().setMatchAnyRelationship();
		blankResults();
		return this;
	}

	/**
	 * Set the query to return rows that match all relationships.
	 *
	 * <p>
	 * This means that all foreign keys and ad hoc relationships are required for
	 * all tables and rows will be returned if they match all of the
	 * relationships.
	 *
	 * <p>
	 * The relationships will be connected by AND in the SQL.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return this DBQuery instance
	 */
	public DBQuery setToMatchAllRelationships() {
		details.getOptions().setMatchAllRelationships();
		blankResults();
		return this;
	}

	/**
	 * Set the query to only return rows that match all conditions
	 *
	 * <p>
	 * This is the default state
	 *
	 * <p>
	 * This means that all permitted*, excluded*, and comparisons are required for
	 * any rows and the conditions will be connected by AND.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return this DBQuery instance
	 */
	public DBQuery setToMatchAllConditions() {
		details.getOptions().setMatchAllConditions();
		blankResults();
		return this;
	}

	/**
	 * Automatically adds the example as a required table if it has criteria, or
	 * as an optional table otherwise.
	 *
	 * <p>
	 * Any DBRow example passed to this method that has criteria specified on it,
	 * however vague, will become a required table on the query.
	 *
	 * <p>
	 * Any DBRow example that has no criteria, i.e. where {@link DBRow#willCreateBlankQuery(nz.co.gregs.dbvolution.databases.definitions.DBDefinition)
	 * } is TRUE, will be added as an optional table.
	 *
	 * <p>
	 * Warning: not specifying a required table will result in a FULL OUTER join
	 * which some database don't handle. You may want to test that the query is
	 * not blank after adding all your tables.
	 *
	 * @param exampleWithOrWithoutCriteria an example DBRow that should be added
	 * to the query as a required or optional table as appropriate.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBQuery instance
	 */
	public DBQuery addOptionalIfNonspecific(DBRow exampleWithOrWithoutCriteria) {
		if (exampleWithOrWithoutCriteria.willCreateBlankQuery(getReadyDatabase().getDefinition())) {
			addOptional(exampleWithOrWithoutCriteria);
		} else {
			add(exampleWithOrWithoutCriteria);
		}
		return this;
	}

	/**
	 * Automatically adds the examples as required tables if they have criteria,
	 * or as an optional tables otherwise.
	 *
	 * <p>
	 * Any DBRow example passed to this method that has criteria specified on it,
	 * however vague, will become a required table on the query.
	 *
	 * <p>
	 * Any DBRow example that has no criteria, i.e. where {@link DBRow#willCreateBlankQuery(nz.co.gregs.dbvolution.databases.definitions.DBDefinition)
	 * } is TRUE, will be added as an optional table.
	 *
	 * <p>
	 * Warning: not specifying a required table will result in a FULL OUTER join
	 * which some database don't handle. You may want to test that the query is
	 * not blank after adding all your tables.
	 *
	 * @param examplesWithOrWithoutCriteria Example DBRow objects that should be
	 * added to the query as a optional or required table as appropriate.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBQuery instance
	 */
	public DBQuery addOptionalIfNonspecific(DBRow... examplesWithOrWithoutCriteria) {
		for (DBRow dBRow : examplesWithOrWithoutCriteria) {
			this.addOptionalIfNonspecific(dBRow);
		}
		return this;
	}

	/**
	 * Used by DBReport to add columns to the SELECT clause
	 *
	 * @param identifyingObject identifyingObject
	 * @param expressionToAdd expressionToAdd
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBQuery instance
	 */
	public DBQuery addExpressionColumn(Object identifyingObject, QueryableDatatype<?> expressionToAdd) {
		details.getExpressionColumns().put(identifyingObject, expressionToAdd);
		blankResults();
		return this;
	}

	/**
	 * Used by DBReport to add columns to the GROUP BY clause.
	 *
	 * @param identifyingObject identifyingObject
	 * @param expressionToAdd expressionToAdd
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBQuery instance
	 */
	protected DBQuery addGroupByColumn(Object identifyingObject, DBExpression expressionToAdd) {
		details.getDBReportGroupByColumns().put(identifyingObject, expressionToAdd);
		return this;
	}

	/**
	 * Clears the results and prepare the query to be re-run.
	 *
	 */
	protected void refreshQuery() {
		blankResults();
	}

	void setRawSQL(String rawQuery) {
		if (rawQuery == null) {
			details.setRawSQLClause("");
		} else {
			details.setRawSQLClause(" " + rawQuery + " ");
		}
	}

	/**
	 * Adds Extra Examples to the Query.
	 *
	 * <p>
	 * The included DBRow instances will be used to add extra criteria as though
	 * they were an added table.
	 *
	 * <p>
	 * Only useful for DBReports or queries that have been
	 * {@link DBQuery#setToMatchAnyCondition() set to match any of the conditions}.
	 *
	 * <p>
	 * They will NOT be added as tables however, for that use
	 * {@link #add(nz.co.gregs.dbvolution.DBRow...) add and related methods}.
	 *
	 * @param extraExamples
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBQuery with the extra examples added
	 */
	public DBQuery addExtraExamples(DBRow... extraExamples) {
		this.details.getExtraExamples().addAll(Arrays.asList(extraExamples));
		blankResults();
		return this;
	}

	private void blankResults() {
		details.setResults(null);
		details.setResultSQL(null);
		queryGraph = null;
	}

	/**
	 * Show the Graph window of the current QueryGraph.
	 *
	 * <p>
	 * A pictorial representation to help you with diagnosing the issues with
	 * queries and to visualize what is actually being used by DBvolution.
	 *
	 * <p>
	 * Internally DBvolution uses a graph to design the query that will be used.
	 * This graph is helpful for visualizing the underlying query, more so than an
	 * SQL query dump. So this method will display the query graph of this query
	 * at this time. The graph cannot be altered through the window but it can be
	 * moved to help show the parts of the graph. You can manipulate the query
	 * graph by
	 * {@link DBQuery#add(nz.co.gregs.dbvolution.DBRow[])  adding tables}, {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) using expressions that connect tables},
	 * or
	 * {@link DBRow#ignoreForeignKey(java.lang.Object) ignoring inappropriate foreign keys}.
	 *
	 */
	public void displayQueryGraph() {
		initialiseQueryGraph();

		Graph<QueryGraphNode, DBExpression> jungGraph = queryGraph.getJungGraph();

		FRLayout<QueryGraphNode, DBExpression> layout = new FRLayout<>(jungGraph);
		layout.setSize(new Dimension(550, 400));

		VisualizationViewer<QueryGraphNode, DBExpression> vv = new VisualizationViewer<>(layout);
		vv.setPreferredSize(new Dimension(600, 480));

		DefaultModalGraphMouse<QueryGraphNode, String> gm = new DefaultModalGraphMouse<>();
		gm.setMode(ModalGraphMouse.Mode.PICKING);
		vv.setGraphMouse(gm);

		RenderContext<QueryGraphNode, DBExpression> renderContext = vv.getRenderContext();
		renderContext.setEdgeLabelTransformer(new QueryGraphEdgeLabelTransformer(this));
		renderContext.setVertexLabelTransformer(new ToStringLabeller<QueryGraphNode>());
		renderContext.setEdgeLabelRenderer(new DefaultEdgeLabelRenderer(Color.BLUE, false));
		renderContext.setVertexFillPaintTransformer(new QueryGraphVertexFillPaintTransformer());
		renderContext.setEdgeStrokeTransformer(new QueryGraphEdgeStrokeTransformer(this));

		queryGraphFrame = new JFrame("DBQuery Graph");
		queryGraphFrame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		queryGraphFrame.setResizable(true);
		queryGraphFrame.getContentPane().add(vv);
		queryGraphFrame.pack();
		queryGraphFrame.setVisible(true);
	}

	private void initialiseQueryGraph() {
		if (queryGraph == null) {
			queryGraph = new QueryGraph(details.getRequiredQueryTables(), getConditions());
			queryGraph.addOptionalAndConnectToRelevant(details.getOptionalQueryTables(), getConditions());
		} else {
			queryGraph.clear();
			queryGraph.addAndConnectToRelevant(details.getRequiredQueryTables(), getConditions());
			queryGraph.addOptionalAndConnectToRelevant(details.getOptionalQueryTables(), getConditions());
		}
	}

	/**
	 * Hides and disposes of the QueryGraph window.
	 *
	 * <p>
	 * After calling {@link #displayQueryGraph() }, you should call this method to
	 * close the window automatically.
	 *
	 * <p>
	 * If the window has closed already, this method has no effect.
	 *
	 */
	public void closeQueryGraph() {
		if (queryGraphFrame != null) {
			queryGraphFrame.setVisible(false);
			queryGraphFrame.dispose();
		}
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the conditions
	 */
	private List<BooleanExpression> getConditions() {
		return details.getConditions();
	}

	/**
	 * Returns the unique values for the column in the database.
	 *
	 * <p>
	 * Creates a query that finds the distinct values that are used in the
	 * field/column supplied.
	 *
	 * <p>
	 * Some tables use repeated values instead of foreign keys or do not use all
	 * of the possible values of a foreign key. This method makes it easy to find
	 * the distinct or unique values that are used.
	 *
	 * @param fieldsOfProvidedRows - the field/column that you need data for.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of DBQQueryRows with distinct combinations of values used in
	 * the columns. 1 Database exceptions may be thrown
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@SuppressWarnings({"unchecked", "empty-statement"})
	public List<DBQueryRow> getDistinctCombinationsOfColumnValues(Object... fieldsOfProvidedRows) throws AccidentalBlankQueryException, SQLException {
		List<DBQueryRow> returnList = new ArrayList<>();

		DBQuery distinctQuery = database.getDBQuery();
		for (DBRow row : details.getRequiredQueryTables()) {
			final DBRow copyDBRow = DBRow.copyDBRow(row);
			copyDBRow.removeAllFieldsFromResults();
			distinctQuery.add(copyDBRow);
		}
		for (DBRow row : details.getOptionalQueryTables()) {
			final DBRow copyDBRow = DBRow.copyDBRow(row);
			copyDBRow.removeAllFieldsFromResults();
			distinctQuery.add(copyDBRow);
		}

		for (Object fieldOfProvidedRow : fieldsOfProvidedRows) {
			PropertyWrapper fieldProp = null;
			for (DBRow row : details.getAllQueryTables()) {
				fieldProp = row.getPropertyWrapperOf(fieldOfProvidedRow);
				if (fieldProp != null) {
					break;
				}
			}
			if (fieldProp == null) {
				throw new nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException();
			} else {
				final PropertyWrapperDefinition fieldDefn = fieldProp.getPropertyWrapperDefinition();
				DBRow fieldRow = null;
				Object thisQDT = null;
				for (DBRow row : distinctQuery.details.getAllQueryTables()) {
					try {
						thisQDT = fieldDefn.rawJavaValue(row);
					} catch (FailedToSetPropertyValueOnRowDefinition ex) {
						;// not worried about it
					}
					if (thisQDT != null) {
						fieldRow = row;
						break;
					}
				}
				if (thisQDT != null && fieldRow != null) {
					fieldRow.addReturnFields(thisQDT);
					distinctQuery.setBlankQueryAllowed(true);
					final ColumnProvider column = fieldRow.column(fieldDefn.getQueryableDatatype(fieldRow));
					distinctQuery.addToSortOrder(column.getSortProvider());
					distinctQuery.addGroupByColumn(fieldRow, column.getColumn().asExpression());
					returnList = distinctQuery.getAllRows();
				} else {
					throw new DBRuntimeException("Unable To Find Columns Specified");
				}
			}
		}
		return returnList;
	}

	/**
	 * Return a list of all tables, required or optional, used in this query.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return all DBRows used in this DBQuery
	 */
	public List<DBRow> getAllTables() {
		ArrayList<DBRow> arrayList = new ArrayList<>();
		arrayList.addAll(details.getAllQueryTables());
		return arrayList;
	}

	/**
	 * Return a list of all the required tables used in this query.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return all DBRows required by this DBQuery
	 */
	public List<DBRow> getRequiredTables() {
		ArrayList<DBRow> arrayList = new ArrayList<>();
		arrayList.addAll(details.getRequiredQueryTables());
		return arrayList;
	}

	/**
	 * Return a list of all the optional tables used in this query.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return all DBRows optionally returned by this DBQuery
	 */
	public List<DBRow> getOptionalTables() {
		ArrayList<DBRow> arrayList = new ArrayList<>();
		arrayList.addAll(details.getOptionalQueryTables());
		return arrayList;
	}

	/**
	 * DBQuery and DBtable are 2 of the few classes that rely on knowing the
	 * database they work on.
	 *
	 * <p>
	 * This method allows you to retrieve the database used when you execute this
	 * query.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the database used during execution of this query.
	 */
	public DBDatabase getDatabase() {
		return database;
	}

	/**
	 * DBQuery and DBtable are 2 of the few classes that rely on knowing the
	 * database they work on.
	 *
	 * <p>
	 * This method allows you to retrieve the database used when you execute this
	 * query.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the database used during execution of this query.
	 */
	public DBDefinition getDatabaseDefinition() {
		return database.getDefinition();
	}

	/**
	 * Add tables that will be used in the query but are already part of an outer
	 * query and need not be explicitly added to the SQL.
	 *
	 * <p>
	 * Used during recursive queries. If you are not manually constructing a
	 * recursive query do NOT use this method.
	 *
	 * <p>
	 * Also used by the {@link ExistsExpression}.
	 *
	 * @param tables	tables
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBQuery object.
	 */
	public DBQuery addAssumedTables(List<DBRow> tables) {
		return addAssumedTables(tables.toArray(new DBRow[]{}));
	}

	/**
	 * Add tables that will be used in the query but are already part of an outer
	 * query and need not be explicitly added to the SQL.
	 *
	 * <p>
	 * Used during recursive queries. If you are not manually constructing a
	 * recursive query do NOT use this method.
	 *
	 * <p>
	 * Also used by the {@link ExistsExpression}.
	 *
	 * @param tables	tables
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBQuery object.
	 */
	public DBQuery addAssumedTables(DBRow... tables) {
		for (DBRow table : tables) {
			details.getAssumedQueryTables().add(table);
			details.getAllQueryTables().add(table);
			blankResults();
		}
		return this;
	}

	/**
	 * Adds optional tables to this query
	 *
	 * <p>
	 * This method adds optional (OUTER) tables to the query.
	 *
	 * <p>
	 * The query will return an instance of these DBRows for each row found,
	 * though it may be a null instance as there was no matching row in the
	 * database.
	 *
	 * <p>
	 * Criteria (permitted and excluded values) specified in the supplied instance
	 * will be added to the query.
	 *
	 * @param optionalQueryTables a list of DBRow objects that defines optional
	 * tables and criteria
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBQuery instance
	 */
	public DBQuery addOptional(List<DBRow> optionalQueryTables) {
		for (DBRow optionalQueryTable : optionalQueryTables) {
			this.addOptional(optionalQueryTable);
		}
		return this;
	}

	/**
	 * Ignores the foreign key of the column provided.
	 * <p>
	 * Similar to {@link DBRow#ignoreForeignKey(java.lang.Object) } but uses a
	 * ColumnProvider which is portable between instances of DBRow.
	 * <p>
	 * For example the following code snippet will ignore the foreign key provided
	 * by a different instance of Customer:
	 * <pre>
	 * Customer customer = new Customer();
	 * IntegerColumn addressColumn = customer.column(customer.fkAddress);
	 * Customer cust2 = new Customer();
	 * cust2.ignoreForeignKey(addressColumn);
	 * </pre>
	 *
	 * @param foreignKeyToFollow the foreign key to ignore
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return This DBQuery object
	 */
	public DBQuery ignoreForeignKey(ColumnProvider foreignKeyToFollow) {
		Set<DBRow> tablesInvolved = foreignKeyToFollow.getColumn().getTablesInvolved();
		for (DBRow fkTable : tablesInvolved) {
			for (DBRow table : details.getAllQueryTables()) {
				if (fkTable.getClass().equals(table.getClass())) {
					table.ignoreForeignKey(foreignKeyToFollow);
				}
			}
		}
		return this;
	}

	/**
	 * Changes the default timeout for this query.
	 *
	 * <p>
	 * Use this method to set the exact timeout for the query.</p>
	 *
	 * <p>
	 * DBvolution defaults to a timeout of 10000milliseconds (10 seconds) to avoid
	 * eternal queries. The actual timeout is based on the performance of the
	 * application server.</p>
	 *
	 * <p>
	 * Use this method If you require a longer running query.
	 *
	 * @param milliseconds
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this query.
	 */
	public synchronized DBQuery setTimeoutInMilliseconds(Long milliseconds) {
		details.setTimeoutInMilliseconds(milliseconds);
		return this;
	}

	/**
	 * Changes the default timeout for this query.
	 *
	 * <p>
	 * Use this method to set the exact timeout for the query.
	 *
	 * <p>
	 * DBvolution defaults to a timeout of 10000milliseconds (10 seconds) to avoid
	 * eternal queries. The actual timeout is based on the performance of the
	 * application server.
	 *
	 * <p>
	 * Use this method If you require a longer running query.
	 *
	 * @param milliseconds
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this query.
	 */
	public synchronized DBQuery setTimeoutInMilliseconds(Integer milliseconds) {
		details.setTimeoutInMilliseconds(milliseconds);
		return this;
	}

	/**
	 * Returns the query to the default timeout.
	 *
	 * <p>
	 * DBvolution defaults to a timeout of approximately 10000milliseconds (10
	 * seconds) to avoid eternal queries. The actual timeout is based on the
	 * performance of the application server.
	 *
	 * <p>
	 * Use this method If you have an ordinary query.
	 *
	 * @return this query.
	 */
	public synchronized DBQuery setTimeoutToDefault() {
		details.setTimeoutToDefault();
		return this;
	}

	/**
	 * Changes the default timeout for this query.
	 *
	 * <p>
	 * Remove the automatic query timeout and allow the query to run forever if
	 * necessary.
	 *
	 * <p>
	 * Use this method If you require a longer running query.
	 *
	 * @return this query.
	 */
	public synchronized DBQuery setTimeoutToForever() {
		details.setTimeoutToForever();
		return this;
	}

	/**
	 * Completely removes the timeout from this query.
	 *
	 * <p>
	 * DBvolution defaults to a timeout of 10000milliseconds (10 seconds) to avoid
	 * eternal queries.
	 *
	 * <p>
	 * Use this method if you expect an extremely long query.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return this DBQuery object
	 */
	public synchronized DBQuery clearTimeout() {
		details.setTimeoutInMilliseconds((Long) null);
		return this;
	}

	/**
	 * Tags all the fields in the DBQuery so that they are not retrieved in the
	 * query.
	 *
	 * <p>
	 * All fields will be excluded from the SQL and the returned rows will be
	 * effectively a NULL row, however tables and fields will still be used in the
	 * query to set conditions.
	 *
	 * @return this query object
	 */
	public DBQuery setReturnFieldsToNone() {
		for (DBRow table : this.getAllTables()) {
			table.setReturnFieldsToNone();
		}
		return this;
	}

	public DBQuery setReturnFields(ColumnProvider... columns) {
		setReturnFieldsToNone();
		List<DBRow> allQueryTables = this.details.getAllQueryTables();
		for (ColumnProvider provider : columns) {
			if (provider instanceof QueryColumn) {
//				QueryColumn<?,?,?> qc = (QueryColumn)provider;
//				qc.setReturnField(true);
			} else {
				final AbstractColumn column = provider.getColumn();
				DBRow table = column.getInstanceOfRow();
				final DBRowClass tableClass = new DBRowClass(table);
				for (DBRow allQueryTable : allQueryTables) {
					final DBRowClass queryTableClass = new DBRowClass(allQueryTable);
					if (queryTableClass.equals(tableClass)) {
						Object appropriateFieldFromRow = column.getAppropriateFieldFromRow(allQueryTable);
						allQueryTable.addReturnFields(appropriateFieldFromRow);
					}
				}
			}
		}
		return this;
	}

	@SuppressWarnings("unchecked")
	public ColumnProvider column(QueryableDatatype<?> qdt) {
		List<DBRow> tables = getAllQueryTables();
		for (DBRow table : tables) {
			try {
				return table.column(qdt);
			} catch (IncorrectRowProviderInstanceSuppliedException exp) {;
			}
		}
		Map<Object, QueryableDatatype<?>> expressionColumns = details.getExpressionColumns();
		for (QueryableDatatype<?> entry : expressionColumns.values()) {
			if (entry.equals(qdt)) {
				return new QueryColumn<>(this, entry);
			}
		}
		throw new IncorrectRowProviderInstanceSuppliedException("the object provided could not be found in the table or expressions used in this query, please supply a QDT used by the tables or adde to the query as an expression column.");
	}

	/**
	 * Sets the query to retrieve that DBQueryRows for the page supplied.
	 *
	 * <p>
	 * DBvolution supports paging through this method. Use {@link #setRowLimit(int)
	 * } to set the page size and then call this method with the desired page
	 * number.
	 *
	 * <p>
	 * This method is zero-based so the first page is getAllRowsForPage(0).
	 *
	 * @param pageNumberZeroBased pageNumber
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of the DBQueryRows for the selected page. 1 Database
	 * exceptions may be thrown
	 */
	public DBQuery setPageRequired(int pageNumberZeroBased) {
		details.setQueryType(QueryType.ROWSFORPAGE);
		details.getOptions().setPageIndex(pageNumberZeroBased);
		return this;
	}

	public void printAllRows() throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		List<DBQueryRow> allRows = getAllRows();
		for (DBQueryRow row : allRows) {
			System.out.println(row);
		}
	}

}
