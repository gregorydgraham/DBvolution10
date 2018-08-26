/*
 * Copyright 2014 Gregory Graham.
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

import nz.co.gregs.dbvolution.databases.DBDatabase;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.actions.*;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.exceptions.AccidentalCartesianJoinException;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;
import nz.co.gregs.dbvolution.expressions.SortProvider;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapperDefinition;
import nz.co.gregs.dbvolution.internal.query.QueryOptions;

/**
 * DBTable provides features for making simple queries on the database.
 *
 * <p>
 * If your query only references one table, DBTable makes it easy to get the
 * rows from that table.
 *
 * <p>
 * Use
 * {@link DBDatabase#getDBTable(nz.co.gregs.dbvolution.DBRow) getDBTable from DBDatabase}
 * to retrieve an instance for particular DBRow subclass.
 *
 * <p>
 * DBTable and {@link DBQuery} are very similar but there are important
 * differences. In particular DBTable uses a simple
 * {@code List<<E extends DBRow>>} rather than {@code List<DBQueryRow>}.
 * Additionally DBTable results are always fresh: the internal query is rerun
 * each time a get* method is called.
 *
 * <p>
 * DBTable is a quick and easy API for targeted data retrieval; for more complex
 * needs, use {@link DBQuery}.
 *
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 * @param <E> DBRow type
 */
public class DBTable<E extends DBRow> {

	private E exemplar = null;
	private E original = null;
	private final DBDatabase database;
	private DBQuery query = null;
	private final QueryOptions options = new QueryOptions();

	/**
	 * Default constructor for DBTable, used by DBDatabase to create instances.
	 *
	 * @param database the database this DBTable instance is applicable too.
	 * @param exampleRow The row that this table is applicable too.
	 */
	protected DBTable(DBDatabase database, E exampleRow) {
		this.original = exampleRow;
		exemplar = DBRow.copyDBRow(exampleRow);
		this.database = database;
		this.query = database.getDBQuery(exemplar);
	}

	/**
	 * Factory method to create a DBTable.
	 *
	 * <p>
	 * The example will be copied to avoid unexpected changes of the results.
	 *
	 * <p>
	 * {@link DBDatabase#getDBTable(nz.co.gregs.dbvolution.DBRow) } is probably a
	 * better option.
	 *
	 * @param <E> DBRow type
	 * @param database database
	 * @param example example
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an instance of the supplied example
	 */
	public static <E extends DBRow> DBTable<E> getInstance(DBDatabase database, E example) {
		DBTable<E> dbTable = new DBTable<>(database, example);
		return dbTable;
	}

	/**
	 * Gets All Rows of the table from the database
	 *
	 * <p>
	 * Retrieves all rows that match the example set during creation or by
	 * subsequent {@link #getRowsByExample(nz.co.gregs.dbvolution.DBRow) } and
	 * similar methods.
	 *
	 * <p>
	 * If the example has no criteria specified and there is no
	 * {@link #setRawSQL(java.lang.String) raw SQL set} then all rows of the table
	 * will be returned.
	 *
	 * <p>
	 * Throws AccidentalBlankQueryException if you haven't specifically allowed
	 * blank queries with setBlankQueryAllowed(boolean)
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return all the appropriate rows of the table from the database;
	 * @throws SQLException database exceptions
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 */
	public List<E> getAllRows() throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		query.refreshQuery();
		applyConfigs();
		List<E> allInstancesOf = query.getAllInstancesOf(exemplar);
		if (options.getRowLimit() > 0 && allInstancesOf.size() > options.getRowLimit()) {
			final int firstItemOfPage = options.getPageIndex() * options.getRowLimit();
			final int firstItemOfNextPage = (options.getPageIndex() + 1) * options.getRowLimit();
			return allInstancesOf.subList(firstItemOfPage, firstItemOfNextPage);
		} else {
			return allInstancesOf;
		}
	}

	/**
	 * Synonym for {@link #getAllRows()}
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return all the appropriate rows 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public List<E> toList() throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		return getAllRows();
	}

	/**
	 * Sets the example and retrieves all the appropriate records.
	 *
	 * <p>
	 * The example is stored as the new exemplar and the query is rerun
	 *
	 * <p>
	 * The following will retrieve all records from the table where the Language
	 * column contains JAVA:<br>
	 * {@code DBTableOLD<MyRow> myTable = database.getDBTableOLD(new MyRow());}<br>
	 * {@code MyRow myExample = new MyRow();}<br>
     * {@code myExample.getLanguage.useLikeComparison("%JAVA%"); }<br>
     * {@code myTable.getByExample(myExample); }<br>
	 * {@code List<MyRow> myRows = myTable.toList();}
	 *
	 * @param example	example
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return All the rows that match the example 1 Database exceptions may be
	 * thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @see QueryableDatatype
	 * @see DBRow
	 */
	public List<E> getRowsByExample(E example) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		this.exemplar = DBRow.copyDBRow(example);
		this.query = database.getDBQuery(exemplar);
		return getAllRows();
	}

	/**
	 *
	 * Returns the first row of the table
	 *
	 * <p>
	 * Particularly helpful when you know there is only one row
	 *
	 * <p>
	 * Functionally equivalent to {@link #getAllRows()}.get(0).
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the first appropriate row in this DBTable
	 * @throws java.sql.SQLException java.sql.SQLException
	 *
	 */
	public E getFirstRow() throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		List<E> allRows = getAllRows();
		return allRows.get(0);
	}

	/**
	 *
	 * Returns the first row and only row of the table.
	 *
	 * <p>
	 * Similar to {@link #getFirstRow()} but throws an
	 * UnexpectedNumberOfRowsException if there is more than 1 row available
	 *
	 * <p>
	 * {@link #getAllRows() } with the initial exemplar will be run.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the first row in this DBTableOLD instance
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException
	 * nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException
	 *
	 *
	 */
	public E getOnlyRow() throws SQLException, UnexpectedNumberOfRowsException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		List<E> allRows = getAllRows();
		if (allRows.size() != 1) {
			throw new UnexpectedNumberOfRowsException(1, allRows.size());
		} else {
			return allRows.get(0);
		}
	}

	/**
	 * Sets the exemplar to the given example and retrieves the only appropriate
	 * record.
	 *
	 * <p>
	 * Throws an exception if there are no appropriate records, or several
	 * appropriate records.
	 *
	 * <p>
	 * The following will return the only record from the table where the Language
	 * column contains JAVA:<br>
	 * {@code MyTableRow myExample = new MyTableRow();}<br>
     * {@code myExample.getLanguage.useLikeComparison("%JAVA%"); }<br>
	 * {@code MyRow myRow = (new DBTable<MyTableRow>()).getOnlyRowByExample(myExample);}
	 *
	 * @param example	example
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return A list containing the rows that match the example 1 Database
	 * exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException
	 * nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException
	 *
	 *
	 * @see QueryableDatatype
	 * @see DBRow
	 */
	public E getOnlyRowByExample(E example) throws SQLException, UnexpectedNumberOfRowsException, AccidentalBlankQueryException {
		return getRowsByExample(example, 1L).get(0);
	}

	/**
	 * This method retrieves all the appropriate records, and throws an exception
	 * if the number of records differs from the required number.
	 *
	 * <p>
	 * The following will retrieve all 10 records from the table where the
	 * Language column contains JAVA, and throw an exception if anything other
	 * than 10 rows is returned.<br>
	 * {@code MyTableRow myExample = new MyTableRow();}<br>
     * {@code myExample.getLanguage.useLikeComparison("%JAVA%"); }<br>
	 * {@code List<MyTableRow> rows = (new DBTable<MyTableRow>()).getRowsByExample(myExample, 10L);}
	 *
	 * @param example example
	 * @param expectedNumberOfRows expectedNumberOfRows
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBTableOLD instance containing the rows that match the example 1
	 * Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException
	 * nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException
	 *
	 *
	 * @see QueryableDatatype
	 * @see DBRow
	 */
	public List<E> getRowsByExample(E example, long expectedNumberOfRows) throws SQLException, UnexpectedNumberOfRowsException, AccidentalBlankQueryException {
		List<E> rowsByExample = getRowsByExample(example);
		if (rowsByExample.size() == expectedNumberOfRows) {
			return rowsByExample;
		} else {
			throw new UnexpectedNumberOfRowsException(expectedNumberOfRows, rowsByExample.size());
		}
	}

	private List<E> getRowsByPrimaryKeyObject(Object pkValue) throws SQLException, ClassNotFoundException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		DBRow newInstance = DBRow.getDBRow(exemplar.getClass());
		final List<QueryableDatatype<?>> primaryKeys = newInstance.getPrimaryKeys();
		for (QueryableDatatype<?> primaryKey : primaryKeys) {
			if ((primaryKey instanceof DBString) && (pkValue instanceof String)) {
				((DBString) primaryKey).permittedValues((String) pkValue);
			} else if ((primaryKey instanceof DBInteger) && (pkValue instanceof Long)) {
				((DBInteger) primaryKey).permittedValues((Long) pkValue);
			} else if ((primaryKey instanceof DBInteger) && (pkValue instanceof Integer)) {
				((DBInteger) primaryKey).permittedValues((Integer) pkValue);
			} else if ((primaryKey instanceof DBNumber) && (pkValue instanceof Number)) {
				((DBNumber) primaryKey).permittedValues((Number) pkValue);
			} else if ((primaryKey instanceof DBDate) && (pkValue instanceof Date)) {
				((DBDate) primaryKey).permittedValues((Date) pkValue);
			} else if ((primaryKey instanceof DBBoolean) && (pkValue instanceof Boolean)) {
				((DBBoolean) primaryKey).permittedValues((Boolean) pkValue);
			} else {
				throw new ClassNotFoundException("The value supplied is not in a supported class or it does not match the primary key class.");
			}
		}
		this.query = database.getDBQuery(newInstance);
		return getAllRows();
	}

	/**
	 * Retrieves that DBRows for the page supplied.
	 *
	 * <p>
	 * DBvolution supports paging through this method. Use {@link #setRowLimit(int)
	 * } to set the page size and then call this method with the desired page
	 * number.
	 *
	 * <p>
	 * This method is zero-based so the first page is getAllRows(0).
	 *
	 * @param pageNumber	pageNumber
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of the DBRows for the selected page. 1 Database exceptions
	 * may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public List<E> getRowsForPage(Integer pageNumber) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		query.refreshQuery();
		applyConfigs();
		List<DBQueryRow> allRowsForPage = query.getAllRowsForPage(pageNumber);
		Set<E> set = new HashSet<>();
		for (DBQueryRow row : allRowsForPage) {
			set.add(row.get(exemplar));
		}
		return new ArrayList<>(set);
	}

	/**
	 * Retrieves the row (or rows in a bad database) that has the specified
	 * primary key.
	 *
	 * <p>
	 * The primary key column is identified by the {@code @DBPrimaryKey}
	 * annotation in the TableRow subclass.
	 *
	 * @param pkValue	pkValue
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a List containing the row(s) for the primary key 1 Database
	 * exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws java.lang.ClassNotFoundException java.lang.ClassNotFoundException
	 *
	 */
	public List<E> getRowsByPrimaryKey(Number pkValue) throws SQLException, ClassNotFoundException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		return getRowsByPrimaryKeyObject(pkValue);
	}

	/**
	 * Retrieves the row (or rows in a bad database) that has the specified
	 * primary key.
	 *
	 * <p>
	 * The primary key column is identified by the {@code @DBPrimaryKey}
	 * annotation in the TableRow subclass.
	 *
	 * @param pkValue	pkValue
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a List containing the row(s) for the primary key 1 Database
	 * exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws java.lang.ClassNotFoundException java.lang.ClassNotFoundException
	 *
	 */
	public List<E> getRowsByPrimaryKey(String pkValue) throws SQLException, ClassNotFoundException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		return getRowsByPrimaryKeyObject(pkValue);
	}

	/**
	 * Retrieves the row (or rows in a bad database) that has the specified
	 * primary key.
	 *
	 * <p>
	 * The primary key column is identified by the {@code @DBPrimaryKey}
	 * annotation in the TableRow subclass.
	 *
	 * @param pkValue	pkValue
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a List containing the row(s) for the primary key 1 Database
	 * exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws java.lang.ClassNotFoundException java.lang.ClassNotFoundException
	 *
	 */
	public List<E> getRowsByPrimaryKey(Date pkValue) throws SQLException, ClassNotFoundException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		return getRowsByPrimaryKeyObject(pkValue);
	}

	/**
	 * Generates and returns the actual SQL that will be used by {@link #getAllRows()
	 * } now.
	 *
	 * <p>
	 * Good for debugging and great for DBAs, this is how you find out what
	 * DBvolution is really doing.
	 *
	 * <p>
	 * Generates the SQL query for retrieving the objects but does not execute the
	 * SQL. Use {@link #getAllRows() the get* methods} to retrieve the rows.
	 *
	 * <p>
	 * See also {@link #getSQLForCount() getSQLForCount}
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a String of the SQL that will be used by {@link #getAllRows() }. 1
	 * Database exceptions may be thrown
	 */
	public String getSQLForQuery() {
		return query.getSQLForQuery();
	}

	/**
	 * Generates and returns the actual SQL that will be used by {@link #getRowsByExample(nz.co.gregs.dbvolution.DBRow)
	 * } now.
	 *
	 * <p>
	 * Good for debugging and great for DBAs, this is how you find out what
	 * DBvolution is really doing.
	 *
	 * <p>
	 * Generates the SQL query for retrieving the objects but does not execute the
	 * SQL. Use
	 * {@link #getRowsByExample(nz.co.gregs.dbvolution.DBRow) the get* methods} to
	 * retrieve the rows.
	 *
	 * <p>
	 * See also {@link #getSQLForCount() getSQLForCount} and {@link #getSQLForQuery()
	 * }
	 *
	 * @param exemplar
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a String of the SQL that will be used by {@link #getRowsByExample(nz.co.gregs.dbvolution.DBRow)
	 * }. 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public String getSQLForQuery(DBRow exemplar) throws SQLException {
		return database.getDBQuery(exemplar).getSQLForQuery();
	}

	/**
	 * Returns the SQL query that will used to count the rows
	 *
	 * <p>
	 * Use this method to check the SQL that will be executed during
	 * {@link #count() the count() method}
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a String of the SQL query that will be used to count the rows
	 * returned by this query 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public String getSQLForCount() throws SQLException {
		return query.getSQLForCount();
	}

	/**
	 * Count the rows on the database without retrieving the rows.
	 *
	 * <p>
	 * Either: counts the results already retrieved, or creates a
	 * {@link #getSQLForCount() count query} for this instance and retrieves the
	 * number of rows that would have been returned had
	 * {@link #getAllRows() getAllRows()} been called.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the number of rows that have or will be retrieved. 1 Database
	 * exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public Long count() throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		return query.count();
	}

	/**
	 * Convenience method to print all the rows in the current collection
	 * Equivalent to: print(System.out)
	 *
	 * @throws java.sql.SQLException SQLException
	 */
	public void print() throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		print(System.out);
	}

	/**
	 * The same as {@link #print()} but allows you to specify the PrintStream required.
	 *
	 * For example: myTable.printAllRows(System.err);
	 *
	 * @param stream stream
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public void print(PrintStream stream) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		List<E> allRows = getAllRows();
		for (E row : allRows) {
			stream.println(row);
		}
	}

	/**
	 * Inserts DBRows into the database.
	 *
	 * @param newRows	newRows
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBActionList of all the actions performed 1 Database exceptions
	 * may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@SafeVarargs
	public final DBActionList insert(E... newRows) throws SQLException {
		DBActionList actions = new DBActionList();
		for (E row : newRows) {
			actions.addAll(DBInsert.save(database, row));
		}
		query.refreshQuery();
		return actions;
	}

	/**
	 *
	 * Inserts DBRows into the database
	 *
	 * @param newRows	newRows
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBActionList of all the actions performed 1 Database exceptions
	 * may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public DBActionList insert(Collection<E> newRows) throws SQLException {
		DBActionList changes = new DBActionList();
		for (DBRow row : newRows) {
			changes.addAll(DBInsert.save(database, row));
		}
		query.refreshQuery();
		return changes;
	}

	/**
	 * Deletes the rows from the database permanently.
	 *
	 * @param oldRows	oldRows
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a {@link DBActionList} of the delete actions. 1 Database exceptions
	 * may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@SafeVarargs
	public final DBActionList delete(E... oldRows) throws SQLException {
		DBActionList actions = new DBActionList();
		actions.addAll(DBDelete.delete(database, oldRows));
		query.refreshQuery();
		return actions;
	}

	/**
	 * Deletes the rows from the database permanently.
	 *
	 * @param oldRows	oldRows
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a {@link DBActionList} of the delete actions. 1 Database exceptions
	 * may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public DBActionList delete(Collection<E> oldRows) throws SQLException {
		DBActionList actions = new DBActionList();
		actions.addAll(DBDelete.delete(database, oldRows));
		query.refreshQuery();
		return actions;
	}

	/**
	 *
	 * Updates the DBRow on the database.
	 *
	 * The row will be changed so that future updates will not include the current
	 * changes.
	 *
	 * @param oldRow	oldRow
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBActionList of the actions performed on the database 1 Database
	 * exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public DBActionList update(E oldRow) throws SQLException {
		query.refreshQuery();
		DBActionList updates = DBUpdate.update(database, oldRow);
		oldRow.setSimpleTypesToUnchanged();
		return updates;
	}

	/**
	 *
	 * Updates Lists of DBRows on the database
	 *
	 * @param oldRows	oldRows
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBActionList of the actions performed on the database 1 Database
	 * exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public DBActionList update(Collection<E> oldRows) throws SQLException {
		DBActionList changes = new DBActionList();
		for (E row : oldRows) {
			if (row.hasChangedSimpleTypes()) {
				changes.addAll(DBUpdate.update(database, row));
				row.setSimpleTypesToUnchanged();
			}
		}
		query.refreshQuery();
		return changes;
	}

	/**
	 * Retrieves the rows for this table and returns the primary keys of the rows
	 * as Longs.
	 *
	 * <p>
	 * Requires the primary key field to be a DBNumber of DBInteger
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a List of primary keys as Longs. 1 Database exceptions may be
	 * thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @see #getPrimaryKeysAsString()
	 * @see #getAllRows()
	 */
	public List<Long> getPrimaryKeysAsLong() throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		List<E> allRows = getAllRows();
		List<Long> longPKs = new ArrayList<>();
		for (E row : allRows) {
			List<QueryableDatatype<?>> primaryKeys = row.getPrimaryKeys();
			for (QueryableDatatype<?> primaryKey : primaryKeys) {
				if (DBNumber.class.isAssignableFrom(primaryKey.getClass())) {
					DBNumber num = (DBNumber) primaryKey;
					longPKs.add(num.longValue());
				}
			}
		}
		return longPKs;
	}

	/**
	 * Retrieves the rows for this table and returns the primary keys of the rows
	 * as Strings.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a List of primary keys as Longs. 1 Database exceptions may be
	 * thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @see #getPrimaryKeysAsString()
	 * @see #getAllRows()
	 */
	public List<String> getPrimaryKeysAsString() throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		List<E> allRows = getAllRows();
		List<String> stringPKs = new ArrayList<>();
		for (E row : allRows) {
			final List<QueryableDatatype<?>> primaryKeys = row.getPrimaryKeys();
			for (QueryableDatatype<?> primaryKey : primaryKeys) {
				stringPKs.add(primaryKey.stringValue());
			}
		}
		return stringPKs;
	}

	/**
	 * Compares 2 tables, presumably from different criteria or databases prints
	 * the differences to System.out
	 *
	 * Should be updated to return the varying rows somehow
	 *
	 * @param secondTable : a comparable table
	 * @throws java.sql.SQLException java.sql.SQLException
	 *
	 */
	public void compare(DBTable<E> secondTable) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		HashMap<String, E> secondMap = new HashMap<>();
		for (E row : secondTable.getAllRows()) {
			secondMap.put(row.getPrimaryKeys().toString(), row);
		}
		for (E row : this.getAllRows()) {
			E foundRow = secondMap.get(row.getPrimaryKeys().toString());
			if (foundRow == null) {
				System.out.println("NOT FOUND: " + row);
			} else if (!row.toString().equals(foundRow.toString())) {
				System.out.println("DIFFERENT: " + row);
				System.out.println("         : " + foundRow);
			}
		}
	}

	/**
	 * Limit the query to only returning a certain number of rows
	 *
	 * <p>
	 * Implements support of the LIMIT and TOP operators of many databases.
	 *
	 * <p>
	 * Only the specified number of rows will be returned from the database and
	 * DBvolution.
	 *
	 * @param rowLimit	rowLimit
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBTable instance
	 */
	public DBTable<E> setRowLimit(int rowLimit) {
		this.options.setRowLimit(rowLimit);
		return this;
	}

	private DBTable<E> applyRowLimit() {
		if (options.getRowLimit() > 0) {
			query.setRowLimit(options.getRowLimit());
		} else {
			query.clearRowLimit();
		}
		return this;
	}

	/**
	 * Removes the limit set with {@link #setRowLimit(int) }.
	 *
	 * <p>
	 * Al the rows will be returned from the database and DBvolution.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return this DBTable instance
	 */
	public DBTable<E> clearRowLimit() {
		this.options.setRowLimit(-1);
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
	 * customer.setSortOrder(customer, customer.name);
	 * </pre>
	 *
	 * <p>
	 * Requires that all {@literal orderColumns} be from the {@code baseRow}
	 * instance to work.
	 *
	 *
	 * @param sortColumns	sortColumns
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this
	 */
	public DBTable<E> setSortOrder(SortProvider... sortColumns) {
		this.options.setSortColumns(sortColumns);
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
	 * customer.setSortOrder(customer, customer.name);
	 * </pre>
	 *
	 * <p>
	 * Requires that all {@literal orderColumns} be from the {@code baseRow}
	 * instance to work.
	 *
	 *
	 * @param sortColumns	sortColumns
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this
	 */
	public DBTable<E> setSortOrder(ColumnProvider... sortColumns) {
		List<SortProvider> cols = new ArrayList<SortProvider>();
		for (ColumnProvider sortColumn : sortColumns) {
			cols.add(sortColumn.getSortProvider());
		}
		this.options.setSortColumns(cols.toArray(new SortProvider[]{}));
		return this;
	}

	/**
	 * Removes the sort order add with {@link #setSortOrder(nz.co.gregs.dbvolution.columns.ColumnProvider...)
	 * }.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return this DBTable instance
	 */
	public DBTable<E> clearSortOrder() {
		if (this.options.getSortColumns().length > 0) {
			this.options.setSortColumns(new SortProvider[]{});
		}
		return this;
	}

	private void applySortOrder() {
		if (options.getSortColumns().length > 0) {
			this.query.setSortOrder(options.getSortColumns());
		} else {
			query.clearSortOrder();
		}
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
	 * @return this DBTable instance
	 */
	public DBTable<E> setBlankQueryAllowed(boolean allow) {
		this.options.setBlankQueryAllowed(allow);
		return this;
	}

	private void applyBlankQueryAllowed() {
		this.query.setBlankQueryAllowed(options.isBlankQueryAllowed());
	}

	private void applyConfigs() {
		applyBlankQueryAllowed();
		applyRowLimit();
		applySortOrder();
		applyMatchAny();
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
	 */
	public void setToMatchAnyCondition() {
		this.options.setMatchAnyConditions();
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
	 */
	public void setToMatchAllConditions() {
		options.setMatchAllConditions();
	}

	private void applyMatchAny() {
		if (options.isMatchAny()) {
			query.setToMatchAnyCondition();
		} else if (options.isMatchAllConditions()) {
			query.setToMatchAllConditions();
		}
	}

	/**
	 * Adds the specified raw SQL to the DBTable query.
	 *
	 * <p>
	 * This method is for adding conditions that can not be created using the
	 * Expressions framework or the preferred/excluded methods of
	 * {@link QueryableDatatype}.
	 *
	 * <p>
	 * The raw SQL will be added as a condition to the where clause. It should and
	 * SQL excerpt that starts with AND (or if you are using Match Any Condition).
	 *
	 * <p>
	 * For instance {@code marque.name.permittedValues('peugeot','hummer')} could
	 * be implemented, rather more awkwardly, as
	 * {@code  table.setRawSQL("and lower(name) in ('peugeot','hummer')")}.
	 *
	 * @param rawQuery	rawQuery
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBtable instance. 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public DBTable<E> setRawSQL(String rawQuery) throws SQLException {
		query.setRawSQL(rawQuery);
		return this;
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
	 * @param <A>	DBRow type
	 * @param fieldOfProvidedRow - the field/column that you need data for. Must
	 * be from the exemplar
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of distinct values used in the column. 1 Database exceptions
	 * may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	@SuppressWarnings("unchecked")
	public <A> List<A> getDistinctValuesOfColumn(A fieldOfProvidedRow) throws IncorrectRowProviderInstanceSuppliedException, SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		List<A> returnList = new ArrayList<>();
		final PropertyWrapper fieldProp = original.getPropertyWrapperOf(fieldOfProvidedRow);
		if (fieldProp == null) {
			throw new IncorrectRowProviderInstanceSuppliedException();
		}
		final PropertyWrapperDefinition fieldDefn = fieldProp.getPropertyWrapperDefinition();
		QueryableDatatype<?> thisQDT = fieldDefn.getQueryableDatatype(exemplar);
		exemplar.setReturnFields(thisQDT);
		DBQuery distinctQuery = database.getDBQuery(exemplar);
		distinctQuery.setBlankQueryAllowed(true);
		final ColumnProvider column = exemplar.column(thisQDT);
		distinctQuery.setSortOrder(column.getSortProvider());
		distinctQuery.addGroupByColumn(exemplar, column.getColumn().asExpression());
		List<DBQueryRow> allRows = distinctQuery.getAllRows();
		for (DBQueryRow dBQueryRow : allRows) {
			E found = dBQueryRow.get(exemplar);
			returnList.add(found == null ? null : (A) fieldDefn.rawJavaValue(found));
		}
		return returnList;
	}

//	public void mustIntersectWith(DBQuery dbQuery) {
//		this.query.mustIntersectWith(dbQuery);
//	}

	public void printSQLForQuery() {
		System.out.println(this.getSQLForQuery());
	}

	public DBTable<E> setQueryTimeout(int i) {
		query.setTimeoutInMilliseconds(i);
		return this;
	}
	
	public DBTable<E> clearTimeout(){
		query.clearTimeout();
		return this;
	}
}
