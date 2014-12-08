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

import nz.co.gregs.dbvolution.internal.querygraph.*;
import edu.uci.ics.jung.algorithms.layout.*;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.visualization.*;
import edu.uci.ics.jung.visualization.control.*;
import edu.uci.ics.jung.visualization.decorators.ToStringLabeller;
import edu.uci.ics.jung.visualization.renderers.DefaultEdgeLabelRenderer;
import java.awt.Color;
import java.awt.Dimension;
import java.io.PrintStream;
import java.sql.*;
import java.util.*;
import javax.swing.JFrame;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;

import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.*;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapperDefinition;
import nz.co.gregs.dbvolution.query.QueryOptions;
import nz.co.gregs.dbvolution.internal.querygraph.QueryGraph;
import nz.co.gregs.dbvolution.internal.querygraph.QueryGraphNode;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 * The Definition of a Query on a Database
 *
 * <p>
 * DBvolution is available on <a
 * href="https://sourceforge.net/projects/dbvolution/">SourceForge</a> complete
 * with <a href="https://sourceforge.net/p/dbvolution/blog/">BLOG</a>
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
 * @author Gregory Graham
 */
public class DBQuery {

	private static final int COUNT_QUERY = 1;
	private static final int SELECT_QUERY = 0;

	private final DBDatabase database;
	private final List<DBRow> allQueryTables;
	private final List<DBRow> requiredQueryTables;
	private final List<DBRow> optionalQueryTables;
	private final QueryOptions options = new QueryOptions();
	private final List<DBRow> extraExamples = new ArrayList<DBRow>();
	private final List<BooleanExpression> conditions = new ArrayList<BooleanExpression>();
	private final Map<Object, DBExpression> expressionColumns = new LinkedHashMap<Object, DBExpression>();
	private final Map<Object, DBExpression> dbReportGroupByColumns = new LinkedHashMap<Object, DBExpression>();
	private final Map<Class<?>, Map<String, DBRow>> existingInstances = new HashMap<Class<?>, Map<String, DBRow>>();

	private String resultSQL;
	private QueryGraph queryGraph;
	private List<DBQueryRow> results;
	private String rawSQLClause = "";
	private Integer resultsRowLimit = -1;
	private Integer resultsPageIndex = 0;
	private JFrame queryGraphFrame = null;
	private ColumnProvider[] sortOrderColumns;
	private List<PropertyWrapper> sortOrder = null;

	private DBQuery(DBDatabase database) {
		this.requiredQueryTables = new ArrayList<DBRow>();
		this.optionalQueryTables = new ArrayList<DBRow>();
		this.allQueryTables = new ArrayList<DBRow>();
		this.database = database;
		blankResults();
	}

	static DBQuery getInstance(DBDatabase database, DBRow... examples) {
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
	 * @param tables a list of DBRow objects that defines required tables and
	 * criteria
	 * @return this DBQuery instance
	 */
	public DBQuery add(DBRow... tables) {
		for (DBRow table : tables) {
			requiredQueryTables.add(table);
			allQueryTables.add(table);
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
	 * @param tables a list of DBRow objects that defines required tables and
	 * criteria
	 * @return this DBQuery instance
	 */
	public DBQuery add(List<DBRow> tables) {
		for (DBRow table : tables) {
			requiredQueryTables.add(table);
			allQueryTables.add(table);
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
	 * The query will return an instance of this DBRow for each row found,
	 * though it may be a null instance as there was no matching row in the
	 * database.
	 *
	 * <p>
	 * Criteria (permitted and excluded values) specified in the supplied
	 * instance will be added to the query.
	 *
	 * @param tables a list of DBRow objects that defines optional tables and
	 * criteria
	 *
	 * @return this DBQuery instance
	 */
	public DBQuery addOptional(DBRow... tables) {
		for (DBRow table : tables) {
			optionalQueryTables.add(table);
			allQueryTables.add(table);
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
	 * @param tables a list of DBRow instances to remove from the query
	 * @return this DBQuery instance
	 */
	public DBQuery remove(DBRow... tables) {
		for (DBRow table : tables) {
			Iterator<DBRow> iterator = allQueryTables.iterator();
			while (iterator.hasNext()) {
				DBRow qtab = iterator.next();
				if (qtab.isPeerOf(table)) {
					requiredQueryTables.remove(qtab);
					optionalQueryTables.remove(qtab);
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
	 * Generates the SQL query for retrieving the objects but does not execute
	 * the SQL. Use {@link #getAllRows() the get*Rows methods} to retrieve the
	 * rows.
	 *
	 * <p>
	 * See also {@link DBQuery#getSQLForCount() getSQLForCount}
	 *
	 * @return a String of the SQL that will be used by this DBQuery.
	 */
	public String getSQLForQuery() {
		return getSQLForQuery(SELECT_QUERY);
	}

	String getANSIJoinClause(DBDatabase database, QueryState queryState, DBRow newTable, List<DBRow> previousTables) {
		List<String> joinClauses = new ArrayList<String>();
		List<String> conditionClauses = new ArrayList<String>();
		String lineSep = System.getProperty("line.separator");
		DBDefinition defn = database.getDefinition();
		boolean isLeftOuterJoin = false;
		boolean isFullOuterJoin = false;

		if (requiredQueryTables.isEmpty() && optionalQueryTables.size() == allQueryTables.size()) {
			isFullOuterJoin = true;
		} else if (optionalQueryTables.contains(newTable)) {
			isLeftOuterJoin = true;
		}

		//Store the expressions from the new table in the QueryState
		for (DBRow otherTable : previousTables) {
			queryState.remainingExpressions.addAll(newTable.getRelationshipsAsBooleanExpressions(database, otherTable, options));
		}

		// Add new table's conditions
		List<String> newTableConditions = newTable.getWhereClausesWithAliases(database);
		if (requiredQueryTables.contains(newTable)) {
			queryState.addRequiredConditions(newTableConditions);
		} else {
			conditionClauses.addAll(newTableConditions);
		}

		// Since the first table can not have a ON clause we need to add it's ON clause to the second table's.
		if (previousTables.size() == 1) {
			final DBRow firstTable = previousTables.get(0);
			if (!requiredQueryTables.contains(firstTable)) {
				List<String> firstTableConditions = firstTable.getWhereClausesWithAliases(database);
				conditionClauses.addAll(firstTableConditions);
			}
		}

		// Add all the expressions we can
		if (previousTables.size() > 0) {
			for (BooleanExpression expr : queryState.getRemainingExpressions()) {
				Set<DBRow> tablesInvolved = new HashSet<DBRow>(expr.getTablesInvolved());
				if (tablesInvolved.contains(newTable)) {
					tablesInvolved.remove(newTable);
				}
				if (tablesInvolved.size() <= previousTables.size()) {
					if (previousTables.containsAll(tablesInvolved)) {
						if (expr.isRelationship()) {
							joinClauses.add(expr.toSQLString(database));
						} else {
							if (requiredQueryTables.containsAll(tablesInvolved)) {
								queryState.addRequiredCondition(expr.toSQLString(database));
							} else {
								conditionClauses.add(expr.toSQLString(database));
							}
						}
						queryState.consumeExpression(expr);
					}
				}
			}
		}

		String sqlToReturn;
		if (previousTables.isEmpty()) {
			sqlToReturn = " " + defn.formatTableName(newTable) + defn.beginTableAlias() + defn.getTableAlias(newTable) + defn.endTableAlias();
		} else {
			if (isFullOuterJoin) {
				sqlToReturn = lineSep + defn.beginFullOuterJoin();
			} else if (isLeftOuterJoin) {
				sqlToReturn = lineSep + defn.beginLeftOuterJoin();
			} else {
				sqlToReturn = lineSep + defn.beginInnerJoin();
			}
			sqlToReturn += defn.formatTableName(newTable) + defn.beginTableAlias() + defn.getTableAlias(newTable) + defn.endTableAlias();
			sqlToReturn += defn.beginOnClause();
			if (!conditionClauses.isEmpty()) {
				if (!joinClauses.isEmpty()) {
					sqlToReturn += "(";
				}
				sqlToReturn += mergeConditionsIntoSQLClause(conditionClauses, defn);
			}
			if (!joinClauses.isEmpty()) {
				if (!conditionClauses.isEmpty()) {
					sqlToReturn += ")" + defn.beginAndLine() + "(";
				}
				String separator = "";
				for (String join : joinClauses) {
					sqlToReturn += separator + join;
					separator = defn.beginJoinClauseLine(options);
				}
				if (!conditionClauses.isEmpty()) {
					sqlToReturn += ")";
				}
			}
			if (conditionClauses.isEmpty() && joinClauses.isEmpty()) {
				sqlToReturn += defn.getWhereClauseBeginningCondition(options);
			}
			sqlToReturn += defn.endOnClause();
		}
		return sqlToReturn;
	}

	private String mergeConditionsIntoSQLClause(List<String> conditionClauses, DBDefinition defn) {
		String separator = "";
		String sqlToReturn = "";
		for (String cond : conditionClauses) {
			sqlToReturn += separator + cond;
			separator = defn.beginConditionClauseLine(options);
		}
		return sqlToReturn;
	}

	private String getSQLForQuery(int queryType) {
		String sqlString = "";

		if (allQueryTables.size() > 0) {
			QueryState queryState = new QueryState(this, getDatabase());

			initialiseQueryGraph();
			queryState.setGraph(this.queryGraph);

			DBDefinition defn = getDatabase().getDefinition();
			StringBuilder selectClause = new StringBuilder().append(defn.beginSelectStatement());
			int columnIndex = 1;
			String groupByColumnIndex = defn.beginGroupByClause();
			String groupByColumnIndexSeparator = "";
			HashMap<PropertyWrapperDefinition, Integer> indexesOfSelectedColumns = new HashMap<PropertyWrapperDefinition, Integer>();
			HashMap<DBExpression, Integer> indexesOfSelectedExpressions = new HashMap<DBExpression, Integer>();
			StringBuilder fromClause = new StringBuilder().append(defn.beginFromClause());
			List<DBRow> joinedTables = new ArrayList<DBRow>();
			final String initialWhereClause = new StringBuilder().append(defn.beginWhereClause()).append(defn.getWhereClauseBeginningCondition(options)).toString();
			StringBuilder whereClause = new StringBuilder(initialWhereClause);
			StringBuilder groupByClause = new StringBuilder().append(defn.beginGroupByClause());
			String lineSep = System.getProperty("line.separator");
//			DBRow startQueryFromTable = requiredQueryTables.isEmpty() ? allQueryTables.get(0) : requiredQueryTables.get(0);
			List<DBRow> sortedQueryTables = options.isCartesianJoinAllowed()
					? queryGraph.toListIncludingCartesian()
					: queryGraph.toList();

			if (this.options.getRowLimit() > 0) {
				selectClause.append(defn.getLimitRowsSubClauseDuringSelectClause(options));
			}

			String separator = "";
			String colSep = defn.getStartingSelectSubClauseSeparator();
			String groupByColSep = "";
			String tableName;

			for (DBRow tabRow : sortedQueryTables) {
				tableName = tabRow.getTableName();

				List<PropertyWrapper> tabProps = tabRow.getSelectedProperties();
				for (PropertyWrapper propWrapper : tabProps) {
					selectClause.append(colSep).append(propWrapper.getSelectableName(getDatabase())).append(" ").append(propWrapper.getColumnAlias(getDatabase()));
					colSep = defn.getSubsequentSelectSubClauseSeparator() + lineSep;

					// Now deal with the GROUP BY and ORDER BY clause requirements
					groupByColumnIndex += groupByColumnIndexSeparator + columnIndex;
					groupByColumnIndexSeparator = defn.getSubsequentGroupBySubClauseSeparator();
					indexesOfSelectedColumns.put(propWrapper.getDefinition(), columnIndex);
					columnIndex++;
				}
				if (!options.isUseANSISyntax()) {
					fromClause.append(separator).append(tableName);
				} else {
					fromClause.append(getANSIJoinClause(getDatabase(), queryState, tabRow, joinedTables));
				}
				joinedTables.add(tabRow);

				if (!options.isUseANSISyntax()) {
					List<String> tabRowCriteria = tabRow.getWhereClausesWithAliases(getDatabase());
					if (tabRowCriteria != null && !tabRowCriteria.isEmpty()) {
						for (String clause : tabRowCriteria) {
							whereClause.append(lineSep).append(defn.beginConditionClauseLine(options)).append(clause);
						}
					}
					getNonANSIJoin(tabRow, whereClause, defn, joinedTables, lineSep);
				}

				separator = ", " + lineSep;
			}

			//add conditions found during the ANSI Join creation
			final String conditionsAsSQLClause = mergeConditionsIntoSQLClause(queryState.getRequiredConditions(), defn);
			if (!conditionsAsSQLClause.isEmpty()) {
				whereClause.append(defn.beginConditionClauseLine(options)).append(conditionsAsSQLClause);
			}

			for (DBRow extra : extraExamples) {
				List<String> extraCriteria = extra.getWhereClausesWithAliases(getDatabase());
				if (extraCriteria != null && !extraCriteria.isEmpty()) {
					for (String clause : extraCriteria) {
						whereClause.append(lineSep).append(defn.beginConditionClauseLine(options)).append(clause);
					}
				}
			}

			for (BooleanExpression expression : queryState.getRemainingExpressions()) {
				whereClause.append(lineSep).append(defn.beginConditionClauseLine(options)).append("(").append(expression.toSQLString(getDatabase())).append(")");
				queryState.consumeExpression(expression);
			}

			for (Map.Entry<Object, DBExpression> entry : expressionColumns.entrySet()) {
				final Object key = entry.getKey();
				final DBExpression expression = entry.getValue();
				selectClause.append(colSep).append(expression.toSQLString(getDatabase())).append(" ").append(defn.formatExpressionAlias(key));
				colSep = defn.getSubsequentSelectSubClauseSeparator() + lineSep;
				if (!expression.isAggregator()) {
					groupByColumnIndex += groupByColumnIndexSeparator + columnIndex;
					groupByColumnIndexSeparator = defn.getSubsequentGroupBySubClauseSeparator();
				}
				indexesOfSelectedExpressions.put(expression, columnIndex);
				columnIndex++;
			}

			for (Map.Entry<Object, DBExpression> entry : dbReportGroupByColumns.entrySet()) {
				groupByClause.append(groupByColSep).append(entry.getValue().toSQLString(getDatabase()));
				groupByColSep = defn.getSubsequentGroupBySubClauseSeparator() + lineSep;
			}

			boolean useColumnIndexGroupBy = defn.prefersIndexBasedGroupByClause();

			// tidy up the raw SQL provided
			String rawSQLClauseFinal = (rawSQLClause.isEmpty() ? "" : rawSQLClause + lineSep);

			// Strip the unnecessary where clause if possible
			if (whereClause.toString().equals(initialWhereClause) && rawSQLClauseFinal.isEmpty()) {
				whereClause = new StringBuilder("");
			}

			if (queryType == SELECT_QUERY) {
				// Clean up the formatting of the optional clauses
				String groupByClauseFinal = (dbReportGroupByColumns.size() > 0 ? (useColumnIndexGroupBy ? groupByColumnIndex : groupByClause.toString()) + lineSep : "");
				String orderByClauseFinal = getOrderByClause(indexesOfSelectedColumns, indexesOfSelectedExpressions);
				if (!orderByClauseFinal.trim().isEmpty()) {
					orderByClauseFinal += lineSep;
				}
				sqlString = selectClause.append(lineSep)
						.append(fromClause).append(lineSep)
						.append(whereClause).append(lineSep)
						.append(rawSQLClauseFinal)
						.append(groupByClauseFinal)
						.append(orderByClauseFinal)
						.append(options.getRowLimit() > 0 ? defn.getLimitRowsSubClauseAfterWhereClause(options) : "")
						.append(defn.endSQLStatement())
						.toString();
			} else if (queryType == COUNT_QUERY) {
				sqlString = defn.beginSelectStatement() + defn.countStarClause() + lineSep + fromClause + lineSep + whereClause + lineSep + rawSQLClauseFinal + lineSep + defn.endSQLStatement();
			}
		}
		return sqlString;
	}

	private void getNonANSIJoin(DBRow tabRow, StringBuilder whereClause, DBDefinition defn, List<DBRow> otherTables, String lineSep) {
		for (DBExpression rel : tabRow.getAdHocRelationships()) {
			whereClause.append(defn.beginConditionClauseLine(options)).append("(").append(rel.toSQLString(getDatabase())).append(")");
		}

		for (DBRow otherTab : otherTables) {
			List<PropertyWrapper> otherTableFks = otherTab.getForeignKeyPropertyWrappers();
			for (PropertyWrapper otherTableFk : otherTableFks) {
				Class<? extends DBRow> fkReferencedClass = otherTableFk.referencedClass();

				if (fkReferencedClass.isAssignableFrom(tabRow.getClass())) {
					String formattedForeignKey = defn.formatTableAliasAndColumnName(
							otherTab, otherTableFk.columnName());

					String formattedReferencedColumn = defn.formatTableAliasAndColumnName(
							tabRow, otherTableFk.referencedColumnName());

					whereClause
							.append(lineSep)
							.append(defn.beginConditionClauseLine(options))
							.append("(")
							.append(formattedForeignKey)
							.append(defn.getEqualsComparator())
							.append(formattedReferencedColumn)
							.append(")");
				}
			}
		}
	}

	/**
	 * Returns the SQL query that will used to count the rows
	 *
	 * <p>
	 * Use this method to check the SQL that will be executed during
	 * {@link DBQuery#count() the count() method}
	 *
	 * @return a String of the SQL query that will be used to count the rows
	 * returned by this query
	 */
	public String getSQLForCount() {
		return getSQLForQuery(DBQuery.COUNT_QUERY);
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
	 * {@link nz.co.gregs.dbvolution.annotations.DBForeignKey foreign keys} on
	 * the DBRow and
	 * {@link nz.co.gregs.dbvolution.DBRow#addRelationship(nz.co.gregs.dbvolution.datatypes.DBNumber, nz.co.gregs.dbvolution.DBRow, nz.co.gregs.dbvolution.datatypes.DBNumber)  added relationships}
	 * to connect the tables. Foreign keys that have been
	 * {@link nz.co.gregs.dbvolution.DBRow#ignoreForeignKey(java.lang.Object) ignored}
	 * are not used.
	 * <p>
	 * Criteria such as
	 * {@link DBNumber#permittedValues(java.lang.Number...)  permitted values}
	 * defined on the fields of the DBRow examples are added as part of the
	 * WHERE clause.
	 *
	 * <p>
	 * Similarly conditions added to the DBQuery using
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) addCondition}
	 * are added.
	 *
	 * @return A List of DBQueryRows containing all the DBRow instances aligned
	 * with their related instances.
	 * @throws SQLException
	 * @see DBRow
	 * @see DBForeignKey
	 * @see QueryableDatatype
	 * @see BooleanExpression
	 * @see DBDatabase
	 */
	public List<DBQueryRow> getAllRows() throws SQLException, AccidentalBlankQueryException, AccidentalCartesianJoinException {
		prepareForQuery();

		if (requiredQueryTables.isEmpty() && optionalQueryTables.isEmpty()) {
			throw new AccidentalBlankQueryException();
		}

		if (!options.isBlankQueryAllowed() && willCreateBlankQuery() && rawSQLClause.isEmpty()) {
			throw new AccidentalBlankQueryException();
		}

		if (!options.isCartesianJoinAllowed() && (requiredQueryTables.size() + optionalQueryTables.size()) > 1 && queryGraph.willCreateCartesianJoin()) {
			throw new AccidentalCartesianJoinException(resultSQL);
		}

		DBQueryRow queryRow;

		DBStatement dbStatement = getDatabase().getDBStatement();
		try {
			ResultSet resultSet = getResultSetForSQL(dbStatement);
			try {
				while (resultSet.next()){
//						&& ((getDatabase().getDefinition().supportsPagingNatively(options) || options.getRowLimit() < 0) // No paging required or it is natively supported
//						|| (!database.getDefinition().supportsPagingNatively(options) && results.size() < options.getRowLimit()) // paging not supported and required so truncate it
//						)) {
					queryRow = new DBQueryRow(this);

					setExpressionColumns(resultSet, queryRow);

					setQueryRowFromResultSet(resultSet, queryRow, dbReportGroupByColumns.size() > 0);
					results.add(queryRow);
				}
			} finally {
				resultSet.close();
			}
		} finally {
			dbStatement.close();
		}
		return results;
	}

	/**
	 * Executes the query using the statement provided and returns the ResultSet
	 *
	 * @param dbStatement
	 * @return the ResultSet returned from the actual database.
	 * @throws SQLException
	 */
	protected ResultSet getResultSetForSQL(DBStatement dbStatement) throws SQLException {
		return dbStatement.executeQuery(resultSQL);
	}

	/**
	 * Using the current ResultSet row, set the values for the DBQueryRow
	 * provided.
	 *
	 * @param resultSet
	 * @param queryRow
	 * @param isGroupedQuery
	 * @throws SQLException
	 * @throws UnableToInstantiateDBRowSubclassException
	 */
	protected void setQueryRowFromResultSet(ResultSet resultSet, DBQueryRow queryRow, boolean isGroupedQuery) throws SQLException, UnableToInstantiateDBRowSubclassException {
		for (DBRow tableRow : allQueryTables) {
			DBRow newInstance = DBRow.getDBRow(tableRow.getClass());

			setFieldsFromColumns(tableRow, newInstance, resultSet);
			newInstance.setReturnFieldsBasedOn(tableRow);

			newInstance.setDefined(); // Actually came from the database so it is a defined row.

			Map<String, DBRow> existingInstancesOfThisTableRow = existingInstances.get(tableRow.getClass());
			existingInstancesOfThisTableRow = setExistingInstancesForTable(existingInstancesOfThisTableRow, newInstance);

			if (newInstance.isEmptyRow()) {
				queryRow.put(newInstance.getClass(), null);
			} else {
				if (isGroupedQuery) {
					queryRow.put(newInstance.getClass(), newInstance);
				} else {
					DBRow existingInstance = getOrSetExistingInstanceForRow(newInstance, existingInstancesOfThisTableRow);
					queryRow.put(existingInstance.getClass(), existingInstance);
				}
			}
		}
	}

	/**
	 * Retrieves or sets the existing instance of the DBRow provided.
	 *
	 * <p>
	 * Queries maintain a list of existing rows to avoid duplicating identical
	 * rows. This method checks to see if the supplied row already exists and
	 * returns the existing version.
	 *
	 * <p>
	 * If the row is new then this method stores it, and returns it as the
	 * existing instance.
	 *
	 * @param newInstance
	 * @param existingInstancesOfThisTableRow
	 * @return the exisinting instance of the provided row, or the row itself if
	 * none exists.
	 */
	protected DBRow getOrSetExistingInstanceForRow(DBRow newInstance, Map<String, DBRow> existingInstancesOfThisTableRow) {
		DBRow existingInstance = newInstance;
		final PropertyWrapper primaryKey = newInstance.getPrimaryKeyPropertyWrapper();
		if (primaryKey != null) {
			final QueryableDatatype qdt = primaryKey.getQueryableDatatype();
			if (qdt != null) {
				existingInstance = existingInstancesOfThisTableRow.get(qdt.toSQLString(this.getDatabase()));
				if (existingInstance == null) {
					existingInstance = newInstance;
					existingInstancesOfThisTableRow.put(qdt.toSQLString(this.getDatabase()), existingInstance);
				}
			}
		}
		return existingInstance;
	}

	/**
	 * Creates the list of already created rows for the DBRow class supplied.
	 *
	 * @param existingInstancesOfThisTableRow
	 * @param newInstance
	 * @return a list of existing rows.
	 */
	protected Map<String, DBRow> setExistingInstancesForTable(Map<String, DBRow> existingInstancesOfThisTableRow, DBRow newInstance) {
		Map<String, DBRow> hashMap = existingInstancesOfThisTableRow;
		if (hashMap == null) {
			hashMap = new HashMap<String, DBRow>();
		}
		existingInstances.put(newInstance.getClass(), hashMap);
		return hashMap;
	}

	/**
	 * Based on the template provided by oldInstance, fill all the fields of
	 * newInstance with data from the current row of the ResultSet.
	 *
	 * <p>
	 * OldInstance is used to find the selected properties, newInstance is the
	 * result, and restultSet contains the retrieved data.
	 *
	 * @param oldInstance
	 * @param newInstance
	 * @param resultSet
	 * @throws SQLException
	 */
	protected void setFieldsFromColumns(DBRow oldInstance, DBRow newInstance, ResultSet resultSet) throws SQLException {
		List<PropertyWrapper> selectedProperties = oldInstance.getSelectedProperties();
		List<PropertyWrapper> newProperties = newInstance.getPropertyWrappers();
		for (PropertyWrapper newProp : newProperties) {
			QueryableDatatype qdt = newProp.getQueryableDatatype();
			for (PropertyWrapper propertyWrapper : selectedProperties) {
				if (propertyWrapper.getDefinition().equals(newProp.getDefinition())) {

					String resultSetColumnName = newProp.getColumnAlias(getDatabase());

					qdt.setFromResultSet(getDatabase(), resultSet, resultSetColumnName);

					if (newInstance.isEmptyRow() && !qdt.isNull()) {
						newInstance.setEmptyRow(false);
					}
				}
			}

			// ensure field set when using type adaptors
			newProp.setQueryableDatatype(qdt);
		}
	}

	/**
	 * Sets all the expression columns using data from the current ResultSet
	 * row.
	 *
	 * @param resultSet
	 * @param queryRow
	 * @throws SQLException
	 */
	protected void setExpressionColumns(ResultSet resultSet, DBQueryRow queryRow) throws SQLException {
		for (Map.Entry<Object, DBExpression> entry : expressionColumns.entrySet()) {
			String expressionAlias = getDatabase().getDefinition().formatExpressionAlias(entry.getKey());
			QueryableDatatype expressionQDT = entry.getValue().getQueryableDatatypeForExpressionValue();
			expressionQDT.setFromResultSet(getDatabase(), resultSet, expressionAlias);
			queryRow.addExpressionColumnValue(entry.getKey(), expressionQDT);
		}
	}

	private void prepareForQuery() throws SQLException {
		results = new ArrayList<DBQueryRow>();
		resultsRowLimit = options.getRowLimit();
		resultsPageIndex = options.getPageIndex();
		resultSQL = this.getSQLForQuery();
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
	 * An UnexpectedNumberOfRowsException is thrown if there is zero or more
	 * than one row.
	 *
	 * @param <R> a subclass of DBRow
	 * @param exemplar an instance of R
	 * @return the ONLY instance found using this query
	 * @throws SQLException
	 * @throws UnexpectedNumberOfRowsException
	 */
	public <R extends DBRow> R getOnlyInstanceOf(R exemplar) throws SQLException, UnexpectedNumberOfRowsException {
		List<R> allInstancesFound = getAllInstancesOf(exemplar, 1);
		return allInstancesFound.get(0);
	}

	/**
	 * Returns all the known instances of the exemplar.
	 *
	 * <p>
	 * A simple means of ensuring that your query has retrieved the correct
	 * results. For instance if you are looking up 2 vehicles in the database
	 * and 3 are returned, this method will throw an exception stopping the
	 * DBScript or DBTransaction automatically.
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
	 * @param expected The expected number of rows, an exception will be thrown
	 * if this expectation is not met.
	 * @return a list of all the instances of the exemplar found by this query
	 * @throws SQLException
	 * @throws UnexpectedNumberOfRowsException
	 */
	public <R extends DBRow> List<R> getAllInstancesOf(R exemplar, long expected) throws SQLException, UnexpectedNumberOfRowsException {
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

	private boolean needsResults() {
		return results == null
				|| results.isEmpty()
				|| resultSQL == null
				|| !resultsPageIndex.equals(options.getPageIndex())
				|| !resultsRowLimit.equals(options.getRowLimit())
				|| !resultSQL.equals(getSQLForQuery());
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
	 * @return A List of all the instances found of the exemplar.
	 * @throws SQLException
	 */
	public <R extends DBRow> List<R> getAllInstancesOf(R exemplar) throws SQLException {
		List<R> arrayList = new ArrayList<R>();
		if (this.needsResults()) {
			getAllRows();
		}
		if (!results.isEmpty()) {
			for (DBQueryRow row : results) {
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
	 * @throws java.sql.SQLException
	 */
	public void print() throws SQLException {
		print(System.out);
	}

	/**
	 * Fast way to print the results
	 *
	 * myTable.printRows(System.err);
	 *
	 * @param ps a printstream to print to.
	 * @throws java.sql.SQLException
	 */
	public void print(PrintStream ps) throws SQLException {
		if (needsResults()) {
			this.getAllRows();
		}

		for (DBQueryRow row : this.results) {
			String tableSeparator = "";
			for (DBRow tab : this.allQueryTables) {
				ps.print(tableSeparator);
				DBRow rowPart = row.get(tab);
				if (rowPart != null) {
					String rowPartStr = rowPart.toString();
					ps.print(rowPartStr);
				}
				tableSeparator = " | ";
			}
			ps.println();
		}
	}

	/**
	 * Fast way to print the results.
	 *
	 * <p>
	 * Retrieves the rows if required and then prints all of the rows but only
	 * the fields that have non-null values.
	 *
	 * <p>
	 * Helps to trim a wide printout of columns down to only the data specified
	 * in the rows.
	 *
	 * <p>
	 * Example: myQuery.printAllDataColumns(System.err);
	 *
	 * @param printStream a printstream to print to
	 * @throws java.sql.SQLException
	 */
	public void printAllDataColumns(PrintStream printStream) throws SQLException {
		if (needsResults()) {
			this.getAllRows();
		}

		for (DBQueryRow row : this.results) {
			for (DBRow tab : this.allQueryTables) {
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
	 * Retrieves and prints all the rows but only prints the primary key
	 * columns.
	 *
	 * <p>
	 * Example: myQuery.printAllPrimaryKeys(System.err);
	 *
	 * @param ps a PrintStream to print to.
	 * @throws java.sql.SQLException
	 */
	public void printAllPrimaryKeys(PrintStream ps) throws SQLException {
		if (needsResults()) {
			this.getAllRows();
		}

		for (DBQueryRow row : this.results) {
			for (DBRow tab : this.allQueryTables) {
				DBRow rowPart = row.get(tab);
				if (rowPart != null) {
					final QueryableDatatype primaryKey = rowPart.getPrimaryKey();
					if (primaryKey != null) {
						String rowPartStr = primaryKey.toSQLString(this.getDatabase());
						ps.print(" " + rowPart.getPrimaryKeyColumnName() + ": " + rowPartStr);
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
	 * @return this DBQuery instance.
	 */
	public DBQuery clear() {
		this.requiredQueryTables.clear();
		this.optionalQueryTables.clear();
		this.allQueryTables.clear();
		this.conditions.clear();
		this.extraExamples.clear();
		blankResults();
		return this;
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
	 * @return the number of rows that have or will be retrieved.
	 * @throws SQLException
	 */
	public Long count() throws SQLException {
		if (results != null) {
			return (long) results.size();
		} else {
			Long result = 0L;

			DBStatement dbStatement = getDatabase().getDBStatement();
			try {
				final String sqlForCount = this.getSQLForCount();
				ResultSet resultSet = dbStatement.executeQuery(sqlForCount);
				try {
					while (resultSet.next()) {
						result = resultSet.getLong(1);
					}
				} finally {
					resultSet.close();
				}
			} finally {
				dbStatement.close();
			}
			return result;
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
	 * This helps avoid the common mistake of accidentally retrieving all the
	 * rows of the tables by forgetting to add criteria.
	 *
	 * <p>
	 * No attempt to compare the length of the query results with the length of
	 * the table is made: if your criteria selects all the row of the tables
	 * this method will still return FALSE.
	 *
	 * @return TRUE if the DBQuery will retrieve all the rows of the tables,
	 * FALSE otherwise
	 */
	public boolean willCreateBlankQuery() {
		boolean willCreateBlankQuery = true;
		for (DBRow table : allQueryTables) {
			willCreateBlankQuery = willCreateBlankQuery && table.willCreateBlankQuery(this.getDatabase());
		}
		for (DBRow table : extraExamples) {
			willCreateBlankQuery = willCreateBlankQuery && table.willCreateBlankQuery(this.getDatabase());
		}
		return willCreateBlankQuery && (conditions.isEmpty());
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
	 * @param maximumNumberOfRowsReturned the require limit to the number of
	 * rows returned
	 * @return this DBQuery instance
	 * @see #clearRowLimit()
	 */
	public DBQuery setRowLimit(int maximumNumberOfRowsReturned) {
		int limit = maximumNumberOfRowsReturned;
		if (maximumNumberOfRowsReturned < 0) {
			limit = 0;
		}

		options.setRowLimit(limit);
		blankResults();

		return this;
	}

	/**
	 * Clear the row limit on this DBQuery and return it to retrieving all rows.
	 *
	 * <p>
	 * Also resets the retrieved results so that the database will be
	 * re-queried.
	 *
	 * @return this DBQuery instance
	 * @see #setRowLimit(int)
	 */
	public DBQuery clearRowLimit() {
		options.setRowLimit(-1);
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
	 * query.setSortOrder(customer.column(customer.name));
	 * </pre>
	 *
	 * <p>
	 * Where possible DBvolution sorts NULL values as the least significant
	 * value, for example "NULL, 1, 2, 3, 4..." not "... 4, 5, 6, NULL".
	 *
	 * @param sortColumns a list of columns to sort the query by.
	 * @return this DBQuery instance
	 */
	public DBQuery setSortOrder(ColumnProvider... sortColumns) {
		blankResults();

		sortOrderColumns = Arrays.copyOf(sortColumns, sortColumns.length);

		sortOrder = new ArrayList<PropertyWrapper>();
		PropertyWrapper prop;
		for (ColumnProvider col : sortColumns) {
			prop = col.getColumn().getPropertyWrapper();
			if (prop != null) {
				sortOrder.add(prop);
			}
		}

		return this;
	}

	/**
	 * Adds the properties (field and/or method) to the end of the sort order.
	 *
	 * <p>
	 * For example the following code snippet will add the name column at the
	 * end of the sort order after district:
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
	 * @return this DBQuery instance
	 */
	public DBQuery addToSortOrder(ColumnProvider... sortColumns) {
		if (sortColumns != null) {
			blankResults();
			List<ColumnProvider> sortOrderColumnsList = new LinkedList<ColumnProvider>();
			if (sortOrderColumns != null) {
				sortOrderColumnsList.addAll(Arrays.asList(sortOrderColumns));
			}
			sortOrderColumnsList.addAll(Arrays.asList(sortColumns));

			return setSortOrder(sortOrderColumnsList.toArray(new ColumnProvider[]{}));
		}
		return this;
	}

	/**
	 * Adds the properties (field and/or method) to the end of the sort order.
	 *
	 * <p>
	 * For example the following code snippet will add the name column at the
	 * end of the sort order after district:
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
	 * @return this DBQuery instance
	 */
	public DBQuery addToSortOrder(DBExpression... sortColumns) {
		for (DBExpression dBExpression : sortColumns) {
			if (dBExpression instanceof ColumnProvider) {
				this.addToSortOrder((ColumnProvider) dBExpression);
			}
		}
		return this;
	}

	/**
	 * Remove all sorting that has been set on this DBQuery
	 *
	 * @return this DBQuery instance
	 */
	public DBQuery clearSortOrder() {
		sortOrder = null;
		sortOrderColumns = null;
		return this;
	}

	private String getOrderByClause(Map<PropertyWrapperDefinition, Integer> indexesOfSelectedProperties, Map<DBExpression, Integer> IndexesOfSelectedExpressions) {
		DBDefinition defn = getDatabase().getDefinition();
		final boolean prefersIndexBasedOrderByClause = defn.prefersIndexBasedOrderByClause();
		if (sortOrderColumns != null && sortOrderColumns.length > 0) {
			StringBuilder orderByClause = new StringBuilder(defn.beginOrderByClause());
			String sortSeparator = defn.getStartingOrderByClauseSeparator();
			for (ColumnProvider column : sortOrderColumns) {
				PropertyWrapper prop = column.getColumn().getPropertyWrapper();
				QueryableDatatype qdt = prop.getQueryableDatatype();
				PropertyWrapperDefinition propDefn = prop.getDefinition();
				if (prefersIndexBasedOrderByClause) {
					Integer columnIndex = indexesOfSelectedProperties.get(propDefn);
					if (columnIndex == null) {
						columnIndex = IndexesOfSelectedExpressions.get(qdt);
					}
					if (columnIndex == null) {
						final DBExpression columnExpression = qdt.getColumnExpression();
						columnIndex = IndexesOfSelectedExpressions.get(columnExpression);
					}
					orderByClause.append(sortSeparator).append(columnIndex).append(defn.getOrderByDirectionClause(qdt.getSortOrder()));
					sortSeparator = defn.getSubsequentOrderByClauseSeparator();
				} else {
					if (qdt.hasColumnExpression()) {
						final String dbColumnName = qdt.getColumnExpression().toSQLString(getDatabase());
						if (dbColumnName != null) {
							orderByClause.append(sortSeparator).append(dbColumnName).append(defn.getOrderByDirectionClause(qdt.getSortOrder()));
							sortSeparator = defn.getSubsequentOrderByClauseSeparator();
						}
					} else {
						final RowDefinition possibleDBRow = prop.getRowDefinitionInstanceWrapper().adapteeRowDefinition();

						if (possibleDBRow != null && DBRow.class.isAssignableFrom(possibleDBRow.getClass())) {
							final DBRow adapteeDBRow = (DBRow) possibleDBRow;
							final String dbColumnName = defn.formatTableAliasAndColumnName(adapteeDBRow, prop.columnName());
							if (dbColumnName
									!= null) {
								orderByClause.append(sortSeparator).append(dbColumnName).append(defn.getOrderByDirectionClause(qdt.getSortOrder()));
								sortSeparator = defn.getSubsequentOrderByClauseSeparator();
							}
						}
					}
				}
			}
			orderByClause.append(defn.endOrderByClause());
			return orderByClause.toString();
		}
		return "";
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
	 * @return this DBQuery instance
	 */
	public DBQuery setBlankQueryAllowed(boolean allow) {
		this.options.setBlankQueryAllowed(allow);

		return this;
	}

	/**
	 * Change the Default Setting of Disallowing Accidental Cartesian Joins
	 *
	 * <p>
	 * A common mistake is to create a query without connecting all the tables
	 * in the query and accident retrieve a huge number of rows.
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
	 * @return this DBQuery instance
	 */
	public DBQuery setCartesianJoinsAllowed(boolean allow) {
		this.options.setCartesianJoinAllowed(allow);

		return this;
	}

	/**
	 * Constructs the SQL for this DBQuery and executes it on the database,
	 * returning the rows found.
	 *
	 * <p>
	 * Like {@link #getAllRows() getAllRows()} this method retrieves all the
	 * rows for this DBQuery. However it checks the number of rows retrieved and
	 * throws a {@link UnexpectedNumberOfRowsException} if the number of rows
	 * retrieved differs from the expected number.
	 *
	 * <p>
	 * Adds all required DBRows as inner join tables and all optional DBRow as
	 * outer join tables.
	 * <p>
	 * Uses the defined
	 * {@link nz.co.gregs.dbvolution.annotations.DBForeignKey foreign keys} on
	 * the DBRow and
	 * {@link nz.co.gregs.dbvolution.DBRow#addRelationship(nz.co.gregs.dbvolution.datatypes.DBNumber, nz.co.gregs.dbvolution.DBRow, nz.co.gregs.dbvolution.datatypes.DBNumber)  added relationships}
	 * to connect the tables. Foreign keys that have been
	 * {@link nz.co.gregs.dbvolution.DBRow#ignoreForeignKey(java.lang.Object) ignored}
	 * are not used.
	 * <p>
	 * Criteria such as
	 * {@link DBNumber#permittedValues(java.lang.Number...)  permitted values}
	 * defined on the fields of the DBRow examples are added as part of the
	 * WHERE clause.
	 *
	 * <p>
	 * Similarly conditions added to the DBQuery using
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) addCondition}
	 * are added.
	 *
	 * @param expectedRows - the number of rows expected to be retrieved
	 * @return A List of DBQueryRows containing all the DBRow instances aligned
	 * with their related instances.
	 * @throws UnexpectedNumberOfRowsException
	 * @throws SQLException
	 * @see #getAllRows()
	 */
	public List<DBQueryRow> getAllRows(long expectedRows) throws UnexpectedNumberOfRowsException, SQLException {
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
	 * @return the useANSISyntax flag
	 */
	public boolean isUseANSISyntax() {
		return options.isUseANSISyntax();
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
	 * ANSI syntax has the foreign key and added relationships defined in the
	 * FROM clause with the JOIN operator. Pre-ANSI syntax treated the foreign
	 * keys and other relationships as part of the WHERE clause.
	 *
	 * <p>
	 * ANSI syntax supports OUTER joins with a standard syntax, and DBvolution
	 * supports OUTER thru the ANSI syntax.
	 *
	 * @param useANSISyntax the useANSISyntax flag to set
	 * @return this DBQuery instance
	 */
	public DBQuery setUseANSISyntax(boolean useANSISyntax) {
		this.options.setUseANSISyntax(useANSISyntax);

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
	 * That is to say: where A is a DBRow in this query, returns a List of B
	 * such that B => A
	 *
	 * @return a list of classes that have a {@code @DBForeignKey} reference to
	 * this class
	 * @see #getReferencedTables()
	 * @see DBRow#getRelatedTables()
	 * @see DBRow#getReferencedTables()
	 */
	public SortedSet<DBRow> getRelatedTables() throws UnableToInstantiateDBRowSubclassException {
		SortedSet<Class<? extends DBRow>> resultClasses;
		resultClasses = new TreeSet<Class<? extends DBRow>>(new DBRowClassNameComparator());

		SortedSet<DBRow> result = new TreeSet<DBRow>(new DBRowNameComparator());
		for (DBRow table : allQueryTables) {
			SortedSet<Class<? extends DBRow>> allRelatedTables = table.getRelatedTables();
			for (Class<? extends DBRow> connectedTable : allRelatedTables) {
				try {
					if (resultClasses.add(connectedTable)) {
						result.add(connectedTable.newInstance());
					}
				} catch (IllegalAccessException ex) {
					throw new UnableToInstantiateDBRowSubclassException(connectedTable, ex);
				} catch (InstantiationException ex) {
					throw new UnableToInstantiateDBRowSubclassException(connectedTable, ex);
				}
			}
		}

		return result;
	}

	/**
	 * Returns all the DBRow subclasses referenced by the DBrows within this
	 * query with foreign keys
	 *
	 * <p>
	 * Similar to {@link #getAllConnectedTables() } but where this class
	 * directly references the external DBRow subclass with an
	 * {@code @DBForeignKey} annotation.
	 *
	 * <p>
	 * That is to say: where A is A DBRow in this class, returns a List of B
	 * such that A => B
	 *
	 * @return A list of DBRow subclasses referenced with {@code @DBForeignKey}
	 * @see #getRelatedTables()
	 * @see DBRow#getReferencedTables()
	 * @see DBRow#getRelatedTables()
	 *
	 */
	@SuppressWarnings("unchecked")
	public SortedSet<DBRow> getReferencedTables() {
		SortedSet<DBRow> result = new TreeSet<DBRow>(new DBRowNameComparator());
		for (DBRow table : allQueryTables) {
			Set<Class<? extends DBRow>> allRelatedTables = table.getReferencedTables();
			for (Class<? extends DBRow> connectedTable : allRelatedTables) {
				try {
					result.add(connectedTable.newInstance());
				} catch (InstantiationException ex) {
					throw new UnableToInstantiateDBRowSubclassException(connectedTable, ex);
				} catch (IllegalAccessException ex) {
					throw new UnableToInstantiateDBRowSubclassException(connectedTable, ex);
				}
			}
		}

		return result;
	}

	/**
	 * Returns all the DBRow subclasses used in this query.
	 *
	 * @return A list of DBRow subclasses included in this query.
	 * @see #getRelatedTables()
	 * @see #getReferencedTables()
	 * @see DBRow#getReferencedTables()
	 * @see DBRow#getRelatedTables()
	 *
	 */
	protected List<DBRow> getAllQueryTables() {
		return allQueryTables;
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
	 * That is to say: where A is a DBRow in this query, returns a List of B
	 * such that B => A or A => B
	 *
	 * @return a list of classes that have a {@code @DBForeignKey} reference to
	 * or from this class
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
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @return this DBQuery instance
	 */
	public DBQuery addAllConnectedTables() throws InstantiationException, IllegalAccessException {
		List<DBRow> tablesToAdd = new ArrayList<DBRow>();
		for (DBRow table : allQueryTables) {
			Set<Class<? extends DBRow>> allConnectedTables = table.getAllConnectedTables();
			for (Class<? extends DBRow> ConnectedTable : allConnectedTables) {
				tablesToAdd.add(ConnectedTable.newInstance());
			}
		}
		add(tablesToAdd.toArray(new DBRow[]{}));

		return this;
	}

	/**
	 * Search the classpath and add, as optional, any DBRow classes that
	 * reference the DBRows within this DBQuery
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
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @return this DBQuery instance
	 */
	public DBQuery addAllConnectedTablesAsOptional() throws InstantiationException, IllegalAccessException {
		Set<DBRow> tablesToAdd = new HashSet<DBRow>();
		List<Class<DBRow>> alreadyAddedClasses = new ArrayList<Class<DBRow>>();
		for (DBRow table : allQueryTables) {
			@SuppressWarnings("unchecked")
			Class<DBRow> aClass = (Class<DBRow>) table.getClass();
			alreadyAddedClasses.add(aClass);
		}
		for (DBRow table : allQueryTables) {
			Set<Class<? extends DBRow>> allRelatedTables = table.getAllConnectedTables();
			for (Class<? extends DBRow> relatedTable : allRelatedTables) {
				DBRow newInstance = relatedTable.newInstance();
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
	 * sometimes you just want the tables added without connecting them to all
	 * the other tables correctly.
	 *
	 * <p>
	 * This method adds all the connected tables as if they were only connected
	 * to the core tables and had no other relationships.
	 *
	 * @return this DBQuery instance
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public DBQuery addAllConnectedTablesAsOptionalWithoutInternalRelations() throws InstantiationException, IllegalAccessException {
		Set<DBRow> tablesToAdd = new HashSet<DBRow>();
		List<Class<DBRow>> alreadyAddedClasses = new ArrayList<Class<DBRow>>();
		DBRow[] originalTables = allQueryTables.toArray(new DBRow[]{});

		for (DBRow table : allQueryTables) {
			@SuppressWarnings("unchecked")
			Class<DBRow> aClass = (Class<DBRow>) table.getClass();
			alreadyAddedClasses.add(aClass);
		}
		for (DBRow table : allQueryTables) {
			Set<Class<? extends DBRow>> allRelatedTables = table.getAllConnectedTables();
			for (Class<? extends DBRow> relatedTable : allRelatedTables) {
				DBRow newInstance = relatedTable.newInstance();
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
	 * Provides all the DBQueryRow that the instance provided is part of.
	 *
	 * <p>
	 * This method returns the subset of this DBQuery's results that include the
	 * provided instance.
	 *
	 * <p>
	 * Slicing the results like this allows you to get a list of, for instance,
	 * status table DBRows and then process the DBQueryRows that have each
	 * status DBRow as a block.
	 *
	 * @param instance the DBRow instance you are interested in.
	 * @return A list of DBQueryRow instances that relate to the exemplar
	 * @throws SQLException
	 */
	public List<DBQueryRow> getAllRowsContaining(DBRow instance) throws SQLException {
		if (this.needsResults()) {
			getAllRows();
		}
		List<DBQueryRow> returnList = new ArrayList<DBQueryRow>();
		for (DBQueryRow row : results) {
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
	 * @param pageNumber
	 * @return a list of the DBQueryRows for the selected page.
	 * @throws SQLException
	 */
	public List<DBQueryRow> getPage(Integer pageNumber) throws SQLException {
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
	 * @param pageNumber
	 * @return a list of the DBQueryRows for the selected page.
	 * @throws SQLException
	 */
	public List<DBQueryRow> getAllRowsForPage(Integer pageNumber) throws SQLException {

		if (database.supportsPaging(options)) {
			this.options.setPageIndex(pageNumber);
			if (this.needsResults()) {
				getAllRows();
			}
			return results;
		} else {
			if (this.needsResults()) {
				getAllRows();
			}
			int rowLimit = this.options.getRowLimit();
			int startIndex = rowLimit * pageNumber;
			startIndex = (startIndex < 0 ? 0 : startIndex);
			int stopIndex = rowLimit * (pageNumber + 1) - 1;
			stopIndex = (stopIndex >= results.size() ? results.size() - 1 : stopIndex);
			if (stopIndex - startIndex < 1) {
				return new ArrayList<DBQueryRow>();
			} else {
				return results.subList(startIndex, stopIndex);
			}
		}
	}

	/**
	 * Use this method to add complex conditions to the DBQuery.
	 *
	 * <p>
	 * This method takes a BooleanExpression and adds it to the where clause of
	 * the Query
	 *
	 * <p>
	 * The easiest way to get a BooleanExpression is the DBRow.column() method
	 * and then apply the functions you require until you get a
	 * BooleanExpression back.
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
	 * @return this DBQuery instance
	 */
	public DBQuery addCondition(BooleanExpression condition) {
		conditions.add(condition);
		blankResults();
		return this;
	}

	/**
	 * Remove all conditions from this query.
	 *
	 * @see #addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * @return this DBQuery object
	 */
	public DBQuery clearConditions() {
		conditions.clear();
		blankResults();
		return this;
	}

	/**
	 * Set the query to return rows that match any conditions
	 *
	 * <p>
	 * This means that all permitted*, excluded*, and comparisons are optional
	 * for any rows and rows will be returned if they match any of the
	 * conditions.
	 *
	 * <p>
	 * The conditions will be connected by OR in the SQL.
	 *
	 * @return this DBQuery instance
	 */
	public DBQuery setToMatchAnyCondition() {
		options.setMatchAnyConditions();
		blankResults();
		return this;
	}

	/**
	 * Set the query to return rows that match any relationship.
	 *
	 * <p>
	 * This means that all foreign keys and ad hoc relationships are optional
	 * for all tables and rows will be returned if they match one of the
	 * relationships.
	 *
	 * <p>
	 * The relationships will be connected by OR in the SQL.
	 *
	 * @return this DBQuery instance
	 */
	public DBQuery setToMatchAnyRelationship() {
		options.setMatchAnyRelationship();
		blankResults();
		return this;
	}

	/**
	 * Set the query to return rows that match all relationships.
	 *
	 * <p>
	 * This means that all foreign keys and ad hoc relationships are required
	 * for all tables and rows will be returned if they match all of the
	 * relationships.
	 *
	 * <p>
	 * The relationships will be connected by AND in the SQL.
	 *
	 * @return this DBQuery instance
	 */
	public DBQuery setToMatchAllRelationships() {
		options.setMatchAllRelationships();
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
	 * This means that all permitted*, excluded*, and comparisons are required
	 * for any rows and the conditions will be connected by AND.
	 *
	 * @return this DBQuery instance
	 */
	public DBQuery setToMatchAllConditions() {
		options.setMatchAllConditions();
		blankResults();
		return this;
	}

	/**
	 * Automatically adds the example as a required table if it has criteria, or
	 * as an optional table otherwise.
	 *
	 * <p>
	 * Any DBRow example passed to this method that has criteria specified on
	 * it, however vague, will become a required table on the query.
	 *
	 * <p>
	 * Any DBRow example that has no criteria, i.e. where {@link DBRow#willCreateBlankQuery(nz.co.gregs.dbvolution.DBDatabase)
	 * } is TRUE, will be added as an optional table.
	 *
	 * <p>
	 * Warning: not specifying a required table will result in a FULL OUTER join
	 * which some database don't handle. You may want to test that the query is
	 * not blank after adding all your tables.
	 *
	 * @param exampleWithOrWithoutCriteria an example DBRow that should be added
	 * to the query as a required or optional table as appropriate.
	 * @return this DBQuery instance
	 */
	public DBQuery addOptionalIfNonspecific(DBRow exampleWithOrWithoutCriteria) {
		if (exampleWithOrWithoutCriteria.willCreateBlankQuery(getDatabase())) {
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
	 * Any DBRow example passed to this method that has criteria specified on
	 * it, however vague, will become a required table on the query.
	 *
	 * <p>
	 * Any DBRow example that has no criteria, i.e. where {@link DBRow#willCreateBlankQuery(nz.co.gregs.dbvolution.DBDatabase)
	 * } is TRUE, will be added as an optional table.
	 *
	 * <p>
	 * Warning: not specifying a required table will result in a FULL OUTER join
	 * which some database don't handle. You may want to test that the query is
	 * not blank after adding all your tables.
	 *
	 * @param examplesWithOrWithoutCriteria Example DBRow objects that should be
	 * added to the query as a optional or required table as appropriate.
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
	 * @param identifyingObject
	 * @param expressionToAdd
	 * @return this DBQuery instance
	 */
	public DBQuery addExpressionColumn(Object identifyingObject, DBExpression expressionToAdd) {
		expressionColumns.put(identifyingObject, expressionToAdd);
		blankResults();
		return this;
	}

	/**
	 * Used by DBReport to add columns to the GROUP BY clause.
	 *
	 * @param identifyingObject
	 * @param expressionToAdd
	 * @return this DBQuery instance
	 */
	protected DBQuery addGroupByColumn(Object identifyingObject, DBExpression expressionToAdd) {
		dbReportGroupByColumns.put(identifyingObject, expressionToAdd);
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
			this.rawSQLClause = "";
		} else {
			this.rawSQLClause = " " + rawQuery + " ";
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
	 */
	void addExtraExamples(DBRow... extraExamples) {
		this.extraExamples.addAll(Arrays.asList(extraExamples));
		blankResults();
	}

	private void blankResults() {
		results = null;
		resultSQL = null;
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
	 * This graph is helpful for visualizing the underlying query, more so than
	 * an SQL query dump. So this method will display the query graph of this
	 * query at this time. The graph cannot be altered through the window but it
	 * can be moved to help show the parts of the graph. You can manipulate the
	 * query graph by
	 * {@link DBQuery#add(nz.co.gregs.dbvolution.DBRow[])  adding tables}, {@link DBRow#addRelationship(nz.co.gregs.dbvolution.datatypes.DBNumber, nz.co.gregs.dbvolution.DBRow, nz.co.gregs.dbvolution.datatypes.DBNumber) adding relationships to the DBRow}
	 * instances, or
	 * {@link DBRow#ignoreForeignKey(java.lang.Object) ignoring inappropriate foreign keys}.
	 *
	 */
	public void displayQueryGraph() {
		initialiseQueryGraph();

		Graph<QueryGraphNode, DBExpression> jungGraph = queryGraph.getJungGraph();

		FRLayout<QueryGraphNode, DBExpression> layout = new FRLayout<QueryGraphNode, DBExpression>(jungGraph);
		layout.setSize(new Dimension(550, 400));

		VisualizationViewer<QueryGraphNode, DBExpression> vv = new VisualizationViewer<QueryGraphNode, DBExpression>(layout);
		vv.setPreferredSize(new Dimension(600, 480));

		DefaultModalGraphMouse<QueryGraphNode, String> gm = new DefaultModalGraphMouse<QueryGraphNode, String>();
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
			queryGraph = new QueryGraph(getDatabase(), requiredQueryTables, getConditions(), options);
			queryGraph.addOptionalAndConnectToRelevant(getDatabase(), optionalQueryTables, getConditions(), options);
		} else {
			queryGraph.clear();
			queryGraph.addAndConnectToRelevant(getDatabase(), requiredQueryTables, getConditions(), options);
			queryGraph.addOptionalAndConnectToRelevant(getDatabase(), optionalQueryTables, getConditions(), options);
		}
	}

	/**
	 * Hides and disposes of the QueryGraph window.
	 *
	 * <p>
	 * After calling {@link #displayQueryGraph() }, you should call this method
	 * to close the window automatically.
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
	 * @return the conditions
	 */
	protected List<BooleanExpression> getConditions() {
		return conditions;
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
	 * of the possible values of a foreign key. This method makes it easy to
	 * find the distinct or unique values that are used.
	 *
	 * @param fieldsOfProvidedRows - the field/column that you need data for.
	 * @return a list of DBQQueryRows with distinct combinations of values used
	 * in the columns.
	 * @throws SQLException
	 */
	@SuppressWarnings({"unchecked", "empty-statement"})
	public List<DBQueryRow> getDistinctCombinationsOfColumnValues(Object... fieldsOfProvidedRows) throws AccidentalBlankQueryException, SQLException {
		List<DBQueryRow> returnList = new ArrayList<DBQueryRow>();

		DBQuery distinctQuery = getDatabase().getDBQuery();
		for (DBRow row : requiredQueryTables) {
			final DBRow copyDBRow = DBRow.copyDBRow(row);
			copyDBRow.removeAllFieldsFromResults();
			distinctQuery.add(copyDBRow);
		}
		for (DBRow row : optionalQueryTables) {
			final DBRow copyDBRow = DBRow.copyDBRow(row);
			copyDBRow.removeAllFieldsFromResults();
			distinctQuery.add(copyDBRow);
		}

		for (Object fieldOfProvidedRow : fieldsOfProvidedRows) {
			PropertyWrapper fieldProp = null;
			for (DBRow row : allQueryTables) {
				fieldProp = row.getPropertyWrapperOf(fieldOfProvidedRow);
				if (fieldProp != null) {
					break;
				}
			}
			if (fieldProp == null) {
				throw new nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException();
			} else {
				final PropertyWrapperDefinition fieldDefn = fieldProp.getDefinition();
				DBRow fieldRow = null;
				Object thisQDT = null;
				for (DBRow row : distinctQuery.allQueryTables) {
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
					distinctQuery.addToSortOrder(column);
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
	 * @return the database
	 */
	public DBDatabase getDatabase() {
		try {
			return database.clone();
		} catch (CloneNotSupportedException ex) {
			throw new RuntimeException(ex);
		}
	}

	public List<DBRow> getOptionalTables() {
		return new ArrayList<DBRow>(this.optionalQueryTables);
	}

	/**
	 * Helper class to store the progress of turning the DBQuery into an actual
	 * piece of SQL.
	 *
	 */
	protected static class QueryState {

		private final DBQuery query;
		private final DBDatabase database;
		private final DBDefinition defn;
		private QueryGraph graph;
		private final List<BooleanExpression> remainingExpressions;
		private final List<BooleanExpression> consumedExpressions = new ArrayList<BooleanExpression>();
		private final List<String> requiredConditions = new ArrayList<String>();
		private final List<String> optionalConditions = new ArrayList<String>();

		QueryState(DBQuery query, DBDatabase database) {
			this.query = query;
			this.database = database;
			this.defn = database.getDefinition();
			this.remainingExpressions = new ArrayList<BooleanExpression>(query.getConditions());
		}

		private Iterable<BooleanExpression> getRemainingExpressions() {
			return new ArrayList<BooleanExpression>(remainingExpressions);
		}

		private void consumeExpression(BooleanExpression expr) {
			remainingExpressions.remove(expr);
			consumedExpressions.add(expr);
		}

		private void setGraph(QueryGraph queryGraph) {
			this.graph = queryGraph;
		}

		/**
		 * Adds a condition that pertains to a required table.
		 *
		 * @param conditionClause
		 */
		protected void addRequiredCondition(String conditionClause) {
			requiredConditions.add(conditionClause);
		}

		private void addRequiredConditions(List<String> conditionClauses) {
			requiredConditions.addAll(conditionClauses);
		}

		/**
		 * Returns all the current conditions that pertain to required tables.
		 *
		 * @return a list of SQL snippets representing required conditions.
		 */
		protected List<String> getRequiredConditions() {
			return requiredConditions;
		}

		/**
		 * Add conditions that pertain to optional tables.
		 *
		 * @param conditionClauses
		 */
		protected void addOptionalConditions(List<String> conditionClauses) {
			optionalConditions.addAll(conditionClauses);
		}

		/**
		 * Returns all the current conditions that pertain to options tables.
		 *
		 * @return a list of SQL snippets representing conditions on optional
		 * tables.
		 */
		protected List<String> getOptionalConditions() {
			return optionalConditions;
		}
	}

	private static class DBRowClassNameComparator implements Comparator<Class<?>> {

		DBRowClassNameComparator() {
		}

		@Override
		public int compare(Class<?> first, Class<?> second) {
			String firstCanonicalName = first.getCanonicalName();
			String secondCanonicalName = second.getCanonicalName();
			if (firstCanonicalName != null && secondCanonicalName != null) {
				return firstCanonicalName.compareTo(secondCanonicalName);
			} else {
				return first.getSimpleName().compareTo(second.getSimpleName());
			}
		}
	}

	private static class DBRowNameComparator implements Comparator<DBRow> {

		DBRowNameComparator() {
		}

		@Override
		public int compare(DBRow first, DBRow second) {
			String firstCanonicalName = first.getClass().getCanonicalName();
			String secondCanonicalName = second.getClass().getCanonicalName();
			if (firstCanonicalName != null && secondCanonicalName != null) {
				return firstCanonicalName.compareTo(secondCanonicalName);
			} else {
				return first.getClass().getSimpleName().compareTo(second.getClass().getSimpleName());
			}
		}
	}
}
