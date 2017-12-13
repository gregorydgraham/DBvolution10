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
import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JFrame;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.actions.DBExecutable;
import nz.co.gregs.dbvolution.actions.DBQueryable;

import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.expressions.*;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.exceptions.*;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import nz.co.gregs.dbvolution.query.*;
import nz.co.gregs.dbvolution.internal.querygraph.*;
import nz.co.gregs.dbvolution.internal.properties.*;
import nz.co.gregs.dbvolution.internal.query.*;

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
public class DBQuery implements DBQueryable {

	/**
	 * The default timeout value used to prevent accidental long running queries
	 */
	private static final int DEFAULT_TIMEOUT_MILLISECONDS = 10000;
	private final DBDatabase database;
	private final QueryDetails details = new QueryDetails();
	private String resultSQL;
	private QueryGraph queryGraph;
	private List<DBQueryRow> results;
	private String rawSQLClause = "";
	private Integer resultsRowLimit = -1;
	private Integer resultsPageIndex = 0;
	private JFrame queryGraphFrame = null;
	private ColumnProvider[] sortOrderColumns;
	private List<PropertyWrapper> sortOrder = null;
	private Integer timeoutInMilliseconds = DEFAULT_TIMEOUT_MILLISECONDS;
	private QueryTimeout timeout;
	private final Map<Class<? extends DBRow>, DBRow> emptyRows = new HashMap<>();

	QueryDetails getQueryDetails() {
		return details;
	}

	private String getHavingClause(DBDatabase database, QueryOptions options) {
		BooleanExpression[] havingColumns = details.getHavingColumns();
		final DBDefinition defn = database.getDefinition();
		String havingClauseStart = defn.getHavingClauseStart();
		if (havingColumns.length == 1) {
			return havingClauseStart + havingColumns[0].toSQLString(defn);
		} else if (havingColumns.length > 1) {
			String sep = "";
			final String beginAndLine = defn.beginAndLine();
			StringBuilder returnStr = new StringBuilder(havingClauseStart);
			for (BooleanExpression havingColumn : havingColumns) {
				returnStr.append(sep).append(havingColumn.toSQLString(defn));
				sep = beginAndLine;
			}
			return returnStr.toString();
		} else {
			return "";
		}
	}

	DBDatabase getReadyDatabase() {
		if (database instanceof DBDatabaseCluster) {
			return ((DBDatabaseCluster) database).getReadyDatabase();
		} else {
			return database;
		}
	}

	private DBQuery(DBDatabase database) {
		this.database = database;
		this.details.setDatabase(database.getDefinition());
		blankResults();
	}

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
	 * SQL. Use
	 * {@link #getAllRowsInternal(nz.co.gregs.dbvolution.query.QueryOptions) the get*Rows methods}
	 * to retrieve the rows.
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
		return getSQLForQuery(getReadyDatabase(), new QueryState(this), QueryType.SELECT, this.details.getOptions());
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
	 * SQL. Use
	 * {@link #getAllRowsInternal(nz.co.gregs.dbvolution.query.QueryOptions) the get*Rows methods}
	 * to retrieve the rows.
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

	String getANSIJoinClause(DBDefinition defn, QueryState queryState, DBRow newTable, List<DBRow> previousTables, QueryOptions options) {
		List<String> joinClauses = new ArrayList<>();
		List<String> conditionClauses = new ArrayList<>();
		String lineSep = System.getProperty("line.separator");
		boolean isLeftOuterJoin = false;
		boolean isFullOuterJoin = false;

		final ArrayList<DBRow> preExistingTables = new ArrayList<>();
		preExistingTables.addAll(previousTables);
		preExistingTables.addAll(details.getAssumedQueryTables());

		List<DBRow> requiredQueryTables = details.getRequiredQueryTables();

		if (requiredQueryTables.isEmpty() && details.getOptionalQueryTables().size() == details.getAllQueryTables().size()) {
			isFullOuterJoin = true;
			queryState.addedFullOuterJoinToQuery();
		} else if (details.getOptionalQueryTables().contains(newTable)) {
			isLeftOuterJoin = true;
			queryState.addedLeftOuterJoinToQuery();
		} else {
			queryState.addedInnerJoinToQuery();
		}

		//Store the expressions from the new table in the QueryState
		for (DBRow otherTable : preExistingTables) {
			queryState.remainingExpressions.addAll(newTable.getRelationshipsAsBooleanExpressions(otherTable));
		}

		// Add new table's conditions
		List<String> newTableConditions = newTable.getWhereClausesWithAliases(defn);
		if (requiredQueryTables.contains(newTable)) {
			queryState.addRequiredConditions(newTableConditions);
		} else {
			conditionClauses.addAll(newTableConditions);
		}

		// Since the first table can not have a ON clause we need to add it's ON clause to the second table's.
		if (previousTables.size() == 1) {
			final DBRow firstTable = previousTables.get(0);
			if (!details.getRequiredQueryTables().contains(firstTable)) {
				List<String> firstTableConditions = firstTable.getWhereClausesWithAliases(defn);
				conditionClauses.addAll(firstTableConditions);
			}
		}

		// Add all the expressions we can
		if (previousTables.size() > 0) {
			for (BooleanExpression expr : queryState.getRemainingExpressions()) {
				Set<DBRow> tablesInvolved = new HashSet<>(expr.getTablesInvolved());
				if (tablesInvolved.contains(newTable)) {
					tablesInvolved.remove(newTable);
				}
				if (tablesInvolved.size() <= previousTables.size()) {
					if (previousTables.containsAll(tablesInvolved)) {
						if (expr.isRelationship()) {
							joinClauses.add(expr.toSQLString(defn));
						} else {
							if (requiredQueryTables.containsAll(tablesInvolved)) {
								queryState.addRequiredCondition(expr.toSQLString(defn));
							} else {
								conditionClauses.add(expr.toSQLString(defn));
							}
						}
						queryState.consumeExpression(expr);
					}
				}
			}
		}

		StringBuilder sqlToReturn = new StringBuilder();
		if (previousTables.isEmpty()) {
			sqlToReturn.append(" ").append(defn.getFromClause(newTable));
		} else {
			if (isFullOuterJoin) {
				sqlToReturn.append(lineSep).append(defn.beginFullOuterJoin());
			} else if (isLeftOuterJoin) {
				sqlToReturn.append(lineSep).append(defn.beginLeftOuterJoin());
			} else {
				sqlToReturn.append(lineSep).append(defn.beginInnerJoin());
			}
			sqlToReturn.append(defn.getFromClause(newTable));
			sqlToReturn.append(defn.beginOnClause());
			if (!conditionClauses.isEmpty()) {
				if (!joinClauses.isEmpty()) {
					sqlToReturn.append("(");
				}
				sqlToReturn.append(mergeConditionsIntoSQLClause(conditionClauses, defn, options));
			}
			if (!joinClauses.isEmpty()) {
				if (!conditionClauses.isEmpty()) {
					sqlToReturn.append(")").append(defn.beginAndLine()).append("(");
				}
				String separator = "";
				for (String join : joinClauses) {
					sqlToReturn.append(separator).append(join);
					separator = defn.beginJoinClauseLine(options);
				}
				if (!conditionClauses.isEmpty()) {
					sqlToReturn.append(")");
				}
			}
			if (conditionClauses.isEmpty() && joinClauses.isEmpty()) {
				sqlToReturn.append(defn.getWhereClauseBeginningCondition(options));
			}
			sqlToReturn.append(defn.endOnClause());
		}
		return sqlToReturn.toString();
	}

	private String mergeConditionsIntoSQLClause(List<String> conditionClauses, DBDefinition defn, QueryOptions options) {
		String separator = "";
		StringBuilder sqlToReturn = new StringBuilder();
		for (String cond : conditionClauses) {
			sqlToReturn.append(separator).append(cond);
			separator = defn.beginConditionClauseLine(options);
		}
		return sqlToReturn.toString();
	}

	private String getSQLForQuery(DBDatabase database, QueryState queryState, QueryType queryType, QueryOptions options) {
		String sqlString = "";

		if (details.getAllQueryTables().size() > 0) {

			initialiseQueryGraph();

			DBDefinition defn = database.getDefinition();
			StringBuilder selectClause = new StringBuilder().append(defn.beginSelectStatement());
			int columnIndex = 1;
			boolean groupByIsRequired = false;
			String groupByColumnIndex = defn.beginGroupByClause();
			String groupByColumnIndexSeparator = "";
			HashMap<PropertyWrapperDefinition, Integer> indexesOfSelectedColumns = new HashMap<>();
			HashMap<DBExpression, Integer> indexesOfSelectedExpressions = new HashMap<>();
			StringBuilder fromClause = new StringBuilder().append(defn.beginFromClause());
			List<DBRow> joinedTables = new ArrayList<>();
			final String initialWhereClause = new StringBuilder().append(defn.beginWhereClause()).append(defn.getWhereClauseBeginningCondition(options)).toString();
			StringBuilder whereClause = new StringBuilder(initialWhereClause);
			StringBuilder groupByClause = new StringBuilder().append(defn.beginGroupByClause());
			String havingClause;
			String lineSep = System.getProperty("line.separator");
//			DBRow startQueryFromTable = requiredQueryTables.isEmpty() ? allQueryTables.get(0) : requiredQueryTables.get(0);
			List<DBRow> sortedQueryTables = options.isCartesianJoinAllowed()
					? queryGraph.toListIncludingCartesianReversable(queryType == QueryType.REVERSESELECT)
					: queryGraph.toListReversable(queryType == QueryType.REVERSESELECT);

			if (options.getRowLimit() > 0) {
				selectClause.append(defn.getLimitRowsSubClauseDuringSelectClause(options));
			}

			String fromClauseTableSeparator = "";
			String colSep = defn.getStartingSelectSubClauseSeparator();
			String groupByColSep = "";
			String tableName;

			for (DBRow tabRow : sortedQueryTables) {
				tableName = tabRow.getTableName();

				List<PropertyWrapper> tabProps = tabRow.getSelectedProperties();
				for (PropertyWrapper propWrapper : tabProps) {
					final QueryableDatatype<?> qdt = propWrapper.getQueryableDatatype();
					final List<PropertyWrapperDefinition.ColumnAspects> columnAspectsList = propWrapper.getColumnAspects(defn);
					for (PropertyWrapperDefinition.ColumnAspects columnAspects : columnAspectsList) {
						String selectableName = columnAspects.selectableName;
						String columnAlias = columnAspects.columnAlias;
						String selectColumn = defn.doColumnTransformForSelect(qdt, selectableName);
						selectClause.append(colSep).append(selectColumn).append(" ").append(columnAlias);
						colSep = defn.getSubsequentSelectSubClauseSeparator() + lineSep;

						// Now deal with the GROUP BY and ORDER BY clause requirements
						DBExpression expression = columnAspects.expression;
						if (expression != null && expression.isAggregator()) {
							details.setGroupByRequiredByAggregator(true);
						}
						if (expression == null
								|| (!expression.isAggregator()
								&& (!expression.isPurelyFunctional() || defn.supportsPurelyFunctionalGroupByColumns()))) {
							groupByIsRequired = true;
							groupByColumnIndex += groupByColumnIndexSeparator + columnIndex;
							groupByColumnIndexSeparator = defn.getSubsequentGroupBySubClauseSeparator();
							if (expression != null) {
								groupByClause.append(groupByColSep).append(defn.transformToStorableType(expression).toSQLString(defn));
								groupByColSep = defn.getSubsequentGroupBySubClauseSeparator() + lineSep;
							} else {
								groupByClause.append(groupByColSep).append(selectColumn);
								groupByColSep = defn.getSubsequentGroupBySubClauseSeparator() + lineSep;
							}

							indexesOfSelectedColumns.put(propWrapper.getPropertyWrapperDefinition(), columnIndex);
						}

						columnIndex++;
					}
				}
				if (!options.isUseANSISyntax()) {
					fromClause.append(fromClauseTableSeparator).append(tableName);
					queryState.addedInnerJoinToQuery();
				} else {
					fromClause.append(getANSIJoinClause(defn, queryState, tabRow, joinedTables, options));
				}
				joinedTables.add(tabRow);

				if (!options.isUseANSISyntax()) {
					List<String> tabRowCriteria = tabRow.getWhereClausesWithAliases(defn);
					if (tabRowCriteria != null && !tabRowCriteria.isEmpty()) {
						for (String clause : tabRowCriteria) {
							whereClause.append(lineSep).append(defn.beginConditionClauseLine(options)).append(clause);
						}
					}
					getNonANSIJoin(tabRow, whereClause, defn, joinedTables, lineSep, options);
					queryState.addedInnerJoinToQuery();
				}

				fromClauseTableSeparator = ", " + lineSep;
			}

			//add conditions found during the ANSI Join creation
			final String conditionsAsSQLClause = mergeConditionsIntoSQLClause(queryState.getRequiredConditions(), defn, options);
			if (!conditionsAsSQLClause.isEmpty()) {
				whereClause.append(defn.beginConditionClauseLine(options)).append(conditionsAsSQLClause);
			}

			for (DBRow extra : details.getExtraExamples()) {
				List<String> extraCriteria = extra.getWhereClausesWithAliases(defn);
				if (extraCriteria != null && !extraCriteria.isEmpty()) {
					for (String clause : extraCriteria) {
						whereClause.append(lineSep).append(defn.beginConditionClauseLine(options)).append(clause);
					}
				}
			}

			for (BooleanExpression expression : queryState.getRemainingExpressions()) {
				whereClause.append(lineSep).append(defn.beginConditionClauseLine(options)).append("(").append(expression.toSQLString(defn)).append(")");
				queryState.consumeExpression(expression);
			}

			for (Map.Entry<Object, QueryableDatatype<?>> entry : details.getExpressionColumns().entrySet()) {
				final Object key = entry.getKey();
				final QueryableDatatype<?> qdt = entry.getValue();
				DBExpression[] expressions = qdt.getColumnExpression();
				for (DBExpression expression : expressions) {
					selectClause.append(colSep).append(defn.transformToStorableType(expression).toSQLString(defn)).append(" ").append(defn.formatExpressionAlias(key));
					colSep = defn.getSubsequentSelectSubClauseSeparator() + lineSep;
					if (expression.isAggregator()) {
						details.setGroupByRequiredByAggregator(true);
					}
					if (!expression.isAggregator()
							&& (!expression.isPurelyFunctional() || defn.supportsPurelyFunctionalGroupByColumns())) {
						groupByIsRequired = true;
						groupByColumnIndex += groupByColumnIndexSeparator + columnIndex;
						groupByColumnIndexSeparator = defn.getSubsequentGroupBySubClauseSeparator();
						groupByClause.append(groupByColSep).append(defn.transformToStorableType(expression).toSQLString(defn));
						groupByColSep = defn.getSubsequentGroupBySubClauseSeparator() + lineSep;

					}
					indexesOfSelectedExpressions.put(expression, columnIndex);
					columnIndex++;
				}
			}

			for (Map.Entry<Object, DBExpression> entry : details.getDBReportGroupByColumns().entrySet()) {
				final DBExpression expression = entry.getValue();
				if ((!expression.isPurelyFunctional() || defn.supportsPurelyFunctionalGroupByColumns())) {
					groupByClause.append(groupByColSep).append(defn.transformToStorableType(expression).toSQLString(defn));
					groupByColSep = defn.getSubsequentGroupBySubClauseSeparator() + lineSep;
				}
			}

			boolean useColumnIndexGroupBy = defn.prefersIndexBasedGroupByClause();

			// tidy up the raw SQL provided
			String rawSQLClauseFinal = (rawSQLClause.isEmpty() ? "" : rawSQLClause + lineSep);

			// Strip the unnecessary where clause if possible
			if (whereClause.toString().equals(initialWhereClause) && rawSQLClauseFinal.isEmpty()) {
				whereClause = new StringBuilder("");
			}

			if (queryType == QueryType.SELECT
					|| queryType == QueryType.REVERSESELECT) {
				if (details.getSelectClause() == null) {
					details.setSelectClause(selectClause.toString());
				}
				if (queryType == QueryType.REVERSESELECT) {
					selectClause = new StringBuilder(details.getSelectClause());
				}
				String groupByClauseFinal = "";
				if (details.isGroupedQuery() && groupByIsRequired) {
					if (useColumnIndexGroupBy) {
						groupByClauseFinal = groupByColumnIndex;
					} else {
						groupByClauseFinal = groupByClause.toString() + lineSep;
					}
				}
				String orderByClauseFinal = getOrderByClause(defn, indexesOfSelectedColumns, indexesOfSelectedExpressions);
				if (!orderByClauseFinal.trim().isEmpty()) {
					orderByClauseFinal += lineSep;
				}
				havingClause = getHavingClause(database, options);
				if (!havingClause.trim().isEmpty()) {
					havingClause += lineSep;
				}
				sqlString = defn.doWrapQueryForPaging(
						selectClause.append(lineSep)
								.append(fromClause).append(lineSep)
								.append(whereClause).append(lineSep)
								.append(rawSQLClauseFinal)
								.append(groupByClauseFinal)
								.append(havingClause)
								.append(orderByClauseFinal)
								.append(options.getRowLimit() > 0 ? defn.getLimitRowsSubClauseAfterWhereClause(options) : "")
								.append(defn.endSQLStatement())
								.toString(),
						options);
			} else if (queryType == QueryType.COUNT) {
				details.setSelectClause(defn.countStarClause());
				sqlString = defn.beginSelectStatement()
						+ defn.countStarClause() + lineSep
						+ fromClause + lineSep
						+ whereClause + lineSep
						+ rawSQLClauseFinal + lineSep
						+ defn.endSQLStatement();
			}
			if (options.creatingNativeQuery()
					&& queryState.isFullOuterJoin()
					&& !defn.supportsFullOuterJoinNatively()) {
				sqlString = getSQLForFakeFullOuterJoin(database, sqlString, queryState, details, options, queryType);
			}
		}
		return sqlString;
	}

	/**
	 * Adapts the query to work for a database that does not support full outer
	 * join queries.
	 *
	 * <p>
	 * Full outer join queries in this sense use a FULL OUTER join for ALL joins
	 * in the query.
	 *
	 * <p>
	 * The standard implementation replaces the query with a LEFT OUTER join query
	 * UNIONed with a RIGHT OUTER join query.
	 *
	 * @param querySQL
	 * @param options
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a fake full outer join query for databases that don't support FULL
	 * OUTER joins
	 */
	private String getSQLForFakeFullOuterJoin(DBDatabase database, String existingSQL, QueryState queryState, QueryDetails details, QueryOptions options, QueryType queryType) {
		String sqlForQuery;
		String unionOperator;
		DBDefinition defn = database.getDefinition();
		if (defn.supportsUnionDistinct()) {
			unionOperator = defn.getUnionDistinctOperator();
		} else {
			unionOperator = defn.getUnionOperator();
		}

		if (defn.supportsRightOuterJoinNatively()) {
			// Fake the outer join by revering the left outer joins to right outer joins

			sqlForQuery = existingSQL.replaceAll("; *$", " ").replaceAll(defn.beginFullOuterJoin(), defn.beginLeftOuterJoin());
			sqlForQuery += unionOperator + existingSQL.replaceAll(defn.beginFullOuterJoin(), defn.beginRightOuterJoin());
		} else {
			// Watch out for the infinite loop
			options.setCreatingNativeQuery(false);
			String reversedQuery = getSQLForQuery(database, queryState, QueryType.REVERSESELECT, options);
			options.setCreatingNativeQuery(true);

			sqlForQuery = existingSQL.replaceAll("; *$", " ").replaceAll(defn.beginFullOuterJoin(), defn.beginLeftOuterJoin());
			sqlForQuery += unionOperator;
			sqlForQuery += reversedQuery.replaceAll("; *$", " ").replaceAll(defn.beginFullOuterJoin(), defn.beginLeftOuterJoin());
		}
		return sqlForQuery;
	}

	private void getNonANSIJoin(DBRow tabRow, StringBuilder whereClause, DBDefinition defn, List<DBRow> otherTables, String lineSep, QueryOptions options) {

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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a String of the SQL query that will be used to count the rows
	 * returned by this query
	 */
	public String getSQLForCount() {
		DBDatabase database = getReadyDatabase();
		if (!database.getDefinition().supportsFullOuterJoinNatively()) {
			return "SELECT COUNT(*) FROM (" + getSQLForQuery(database, new QueryState(this), QueryType.SELECT, details.getOptions()).replaceAll("; *$", "") + ") A" + database.getDefinition().endSQLStatement();
		} else {
			return getSQLForQuery(database, new QueryState(this), QueryType.COUNT, details.getOptions());
		}
	}

	/**
	 * Prints the SQL query that will used to count the rows
	 *
	 * <p>
	 * Use this method to check the SQL that will be executed during
	 * {@link DBQuery#count() the count() method}
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
	 * @see DBRow
	 * @see DBForeignKey
	 * @see QueryableDatatype
	 * @see BooleanExpression
	 * @see DBDatabase
	 */
	public List<DBQueryRow> getAllRows() throws SQLException, SQLTimeoutException, AccidentalBlankQueryException, AccidentalCartesianJoinException {
		final QueryOptions options = details.getOptions();
		if (this.needsResults(options)) {
			database.executeDBQuery(this);
//			getAllRowsInternal(options);
		}
		if (options.getRowLimit() > 0 && results.size() > options.getRowLimit()) {
			final int firstItemOfPage = options.getPageIndex() * options.getRowLimit();
			final int firstItemOfNextPage = (options.getPageIndex() + 1) * options.getRowLimit();
			return results.subList(firstItemOfPage, firstItemOfNextPage);
		} else {
			return results;
		}
	}

	@Override
	public DBActionList query(DBDatabase db) throws SQLException {
		DBActionList actions = new DBActionList();
		details.getOptions().setQueryDatabase(db);
		fillResultSetInternal(db, details, this.details.getOptions());
		return actions;
	}

	private void fillResultSetInternal(DBDatabase db, QueryDetails details, QueryOptions options) throws SQLException, SQLTimeoutException, AccidentalBlankQueryException, AccidentalCartesianJoinException {
		prepareForQuery(db, options);

		final DBDefinition defn = db.getDefinition();

//		final QueryOptions options = details.getOptions();
		if (!options.isBlankQueryAllowed() && willCreateBlankQuery(db) && rawSQLClause.isEmpty()) {
			throw new AccidentalBlankQueryException();
		}

		if (!options.isCartesianJoinAllowed()
				&& (details.getRequiredQueryTables().size() + details.getOptionalQueryTables().size()) > 1
				&& queryGraph.willCreateCartesianJoin()) {
			throw new AccidentalCartesianJoinException(resultSQL);
		}

		DBQueryRow queryRow;

		try (DBStatement dbStatement = db.getDBStatement();
				ResultSet resultSet = getResultSetForSQL(dbStatement, resultSQL)) {
			while (resultSet.next()) {
				queryRow = new DBQueryRow(this);

				setExpressionColumns(defn, resultSet, queryRow);

				setQueryRowFromResultSet(defn, resultSet, details, queryRow, details.isGroupedQuery());
				results.add(queryRow);
			}
		}
		for (DBQueryRow result : results) {
			List<DBRow> rows = result.getAll();
			for (DBRow row : rows) {
				if (row != null) {
					row.setAutoFilledFields(this);
				}
			}
		}
	}

	/**
	 * Executes the query using the statement provided and returns the ResultSet
	 *
	 * @param statement dbStatement
	 * @param sql sql
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the ResultSet returned from the actual database. Database
	 * exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws java.sql.SQLTimeoutException
	 */
	protected synchronized ResultSet getResultSetForSQL(DBStatement statement, String sql) throws SQLException, SQLTimeoutException {
		if (this.timeoutInMilliseconds != null) {
			this.timeout = QueryTimeout.scheduleTimeout(statement, this.timeoutInMilliseconds);
		}
		final ResultSet queryResults = statement.executeQuery(sql);
		if (this.timeout != null) {
			this.timeout.cancel();
		}
		return queryResults;
	}

	/**
	 * Using the current ResultSet row, set the values for the DBQueryRow
	 * provided.
	 *
	 * Database exceptions may be thrown
	 *
	 * @param defn
	 * @param resultSet	resultSet
	 * @param details
	 * @param queryRow	queryRow
	 * @param isGroupedQuery	isGroupedQuery
	 * @throws java.sql.SQLException the database threw an exception
	 */
	protected void setQueryRowFromResultSet(DBDefinition defn, ResultSet resultSet, QueryDetails details, DBQueryRow queryRow, boolean isGroupedQuery) throws SQLException, UnableToInstantiateDBRowSubclassException {
		for (DBRow tableRow : details.getAllQueryTables()) {
			DBRow newInstance = DBRow.getDBRow(tableRow.getClass());

			setFieldsFromColumns(defn, tableRow, newInstance, resultSet);
			newInstance.setReturnFieldsBasedOn(tableRow);

			newInstance.setDefined(); // Actually came from the database so it is a defined row.

			Map<String, DBRow> existingInstancesOfThisTableRow = details.getExistingInstances().get(tableRow.getClass());
			existingInstancesOfThisTableRow = setExistingInstancesForTable(existingInstancesOfThisTableRow, newInstance);
			final Class<? extends DBRow> newInstanceClass = newInstance.getClass();

			if (newInstance.isEmptyRow()) {
				DBRow emptyRow = emptyRows.get(newInstanceClass);
				if (emptyRow != null) {
					queryRow.put(newInstanceClass, emptyRow);
				} else {
					emptyRows.put(newInstanceClass, newInstance);
					queryRow.put(newInstanceClass, newInstance);
				}
			} else {
				final List<QueryableDatatype<?>> primaryKeys = newInstance.getPrimaryKeys();
				boolean pksHaveBeenSet = true;
				for (QueryableDatatype<?> pk : primaryKeys) {
					pksHaveBeenSet = pksHaveBeenSet && pk.hasBeenSet();
				}
				if (isGroupedQuery || primaryKeys.isEmpty() || !pksHaveBeenSet) {
					queryRow.put(newInstanceClass, newInstance);
				} else {
					DBRow existingInstance = getOrSetExistingInstanceForRow(defn, newInstance, existingInstancesOfThisTableRow);
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
	 * @param newInstance newInstance
	 * @param existingInstancesOfThisTableRow existingInstancesOfThisTableRow
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the exisinting instance of the provided row, or the row itself if
	 * none exists.
	 */
	protected DBRow getOrSetExistingInstanceForRow(DBDefinition defn, DBRow newInstance, Map<String, DBRow> existingInstancesOfThisTableRow) {
		DBRow existingInstance = newInstance;
		final List<PropertyWrapper> primaryKeys = newInstance.getPrimaryKeyPropertyWrappers();
		for (PropertyWrapper primaryKey : primaryKeys) {
			if (primaryKey != null) {
				final QueryableDatatype<?> qdt = primaryKey.getQueryableDatatype();
				if (qdt != null) {
					existingInstance = existingInstancesOfThisTableRow.get(qdt.toSQLString(defn));
					if (existingInstance == null) {
						existingInstance = newInstance;
						existingInstancesOfThisTableRow.put(qdt.toSQLString(defn), existingInstance);
					}
				}
			}
		}
		return existingInstance;
	}

	/**
	 * Creates the list of already created rows for the DBRow class supplied.
	 *
	 * @param existingInstancesOfThisTableRow existingInstancesOfThisTableRow
	 * @param newInstance newInstance
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of existing rows.
	 */
	protected Map<String, DBRow> setExistingInstancesForTable(Map<String, DBRow> existingInstancesOfThisTableRow, DBRow newInstance) {
		Map<String, DBRow> hashMap = existingInstancesOfThisTableRow;
		if (hashMap == null) {
			hashMap = new HashMap<>();
		}
		details.getExistingInstances().put(newInstance.getClass(), hashMap);
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
	 * Database exceptions may be thrown
	 *
	 * @param defn
	 * @param oldInstance oldInstance
	 * @param newInstance newInstance
	 * @param resultSet resultSet
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	protected void setFieldsFromColumns(DBDefinition defn, DBRow oldInstance, DBRow newInstance, ResultSet resultSet) throws SQLException {
		List<PropertyWrapper> selectedProperties = oldInstance.getSelectedProperties();
		List<PropertyWrapper> newProperties = newInstance.getColumnPropertyWrappers();
		for (PropertyWrapper newProp : newProperties) {
			QueryableDatatype<?> qdt = newProp.getQueryableDatatype();
			for (PropertyWrapper propertyWrapper : selectedProperties) {
				if (propertyWrapper.getPropertyWrapperDefinition().equals(newProp.getPropertyWrapperDefinition())) {

					String resultSetColumnName = newProp.getColumnAlias(defn)[0];
					//for (String resultSetColumnName : resultSetColumnNames) {

					qdt.setFromResultSet(defn, resultSet, resultSetColumnName);

					if (newInstance.isEmptyRow() && !qdt.isNull()) {
						newInstance.setEmptyRow(false);
					}
					//}
				}
			}

			// ensure field set when using type adaptors
			newProp.setQueryableDatatype(qdt);
		}
	}

	/**
	 * Sets all the expression columns using data from the current ResultSet row.
	 *
	 * Database exceptions may be thrown
	 *
	 * @param resultSet resultSet
	 * @param queryRow queryRow
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	protected void setExpressionColumns(DBDefinition defn, ResultSet resultSet, DBQueryRow queryRow) throws SQLException {
		for (Map.Entry<Object, QueryableDatatype<?>> entry : details.getExpressionColumns().entrySet()) {
			final Object key = entry.getKey();
			final QueryableDatatype<?> value = entry.getValue();
			String expressionAlias = defn.formatExpressionAlias(key);
			QueryableDatatype<?> expressionQDT = value.getQueryableDatatypeForExpressionValue();
			expressionQDT.setFromResultSet(defn, resultSet, expressionAlias);
			queryRow.addExpressionColumnValue(key, expressionQDT);
		}
	}

	private void prepareForQuery(DBDatabase database, QueryOptions options) throws SQLException {
		results = new ArrayList<>();
//		final QueryOptions options = details.getOptions();
		resultsRowLimit = options.getRowLimit();
		resultsPageIndex = options.getPageIndex();
		resultSQL = this.getSQLForQuery(database, new QueryState(this), QueryType.SELECT, options);
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
	 *
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
	 *
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

	private boolean needsResults(QueryOptions options) {
		final DBDatabase queryDatabase = options.getQueryDatabase();
		return results == null
				|| results.isEmpty()
				|| resultSQL == null
				|| !resultsPageIndex.equals(options.getPageIndex())
				|| !resultsRowLimit.equals(options.getRowLimit())
				|| queryDatabase == null
				|| !resultSQL.equals(getSQLForQuery(queryDatabase, new QueryState(this), QueryType.SELECT, options));
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
	 */
	public <R extends DBRow> List<R> getAllInstancesOf(R exemplar) throws SQLException {
		List<R> arrayList = new ArrayList<>();
		final QueryOptions options = details.getOptions();
		if (this.needsResults(options)) {
			database.executeDBQuery(this);
//			getAllRowsInternal(options);
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
	 * @throws java.sql.SQLException database exception
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
	 * @throws java.sql.SQLException java.sql.SQLException
	 *
	 */
	public void print(PrintStream ps) throws SQLException {
		final QueryOptions options = details.getOptions();
		if (needsResults(options)) {
			database.executeDBQuery(this);
//			this.getAllRowsInternal(options);
		}

		for (DBQueryRow row : this.results) {
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
	 *
	 */
	public void printAllDataColumns(PrintStream printStream) throws SQLException {
		final QueryOptions options = details.getOptions();
		if (needsResults(options)) {
			database.executeDBQuery(this);
//			this.getAllRowsInternal(options);
		}

		for (DBQueryRow row : this.results) {
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
	 *
	 */
	public void printAllPrimaryKeys(PrintStream ps) throws SQLException {
		final QueryOptions options = details.getOptions();
		if (needsResults(options)) {
			database.executeDBQuery(this);
//			this.getAllRowsInternal(options);
		}

		for (DBQueryRow row : this.results) {
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
		this.details.getRequiredQueryTables().clear();
		this.details.getOptionalQueryTables().clear();
		this.details.getAllQueryTables().clear();
		this.details.getConditions().clear();
		this.details.getExtraExamples().clear();
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
	 * {@link #getAllRowsInternal(nz.co.gregs.dbvolution.query.QueryOptions)  getAllRows()}
	 * been called.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the number of rows that have or will be retrieved. Database
	 * exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public Long count() throws SQLException {
		if (results != null) {
			return (long) results.size();
		} else {
			Long result = 0L;

			try (DBStatement dbStatement = getReadyDatabase().getDBStatement()) {
				final String sqlForCount = this.getSQLForCount();
				try (ResultSet resultSet = dbStatement.executeQuery(sqlForCount)) {
					while (resultSet.next()) {
						result = resultSet.getLong(1);
					}
				}
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
		return willCreateBlankQuery(getReadyDatabase());
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
	protected boolean willCreateBlankQuery(DBDatabase db) {
		boolean willCreateBlankQuery = true;
		for (DBRow table : details.getAllQueryTables()) {
			willCreateBlankQuery = willCreateBlankQuery && table.willCreateBlankQuery(db.getDefinition());
		}
		for (DBRow table : details.getExtraExamples()) {
			willCreateBlankQuery = willCreateBlankQuery && table.willCreateBlankQuery(db.getDefinition());
		}
		willCreateBlankQuery = willCreateBlankQuery && details.getHavingColumns().length == 0;
		return willCreateBlankQuery && (details.getConditions().isEmpty());
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
	 * query.setSortOrder(customer.column(customer.name));
	 * </pre>
	 *
	 * <p>
	 * Where possible DBvolution sorts NULL values as the least significant value,
	 * for example "NULL, 1, 2, 3, 4..." not "... 4, 5, 6, NULL".
	 *
	 * @param sortColumns a list of columns to sort the query by.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBQuery instance
	 */
	public DBQuery setSortOrder(ColumnProvider... sortColumns) {
		blankResults();

		sortOrderColumns = Arrays.copyOf(sortColumns, sortColumns.length);

		sortOrder = new ArrayList<>();
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
	public DBQuery addToSortOrder(ColumnProvider... sortColumns) {
		if (sortColumns != null) {
			blankResults();
			List<ColumnProvider> sortOrderColumnsList = new LinkedList<>();
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return this DBQuery instance
	 */
	public DBQuery clearSortOrder() {
		sortOrder = null;
		sortOrderColumns = null;
		return this;
	}

	private String getOrderByClause(DBDefinition defn, Map<PropertyWrapperDefinition, Integer> indexesOfSelectedProperties, Map<DBExpression, Integer> IndexesOfSelectedExpressions) {
		final boolean prefersIndexBasedOrderByClause = defn.prefersIndexBasedOrderByClause();
		if (sortOrderColumns != null && sortOrderColumns.length > 0) {
			StringBuilder orderByClause = new StringBuilder(defn.beginOrderByClause());
			String sortSeparator = defn.getStartingOrderByClauseSeparator();
			for (ColumnProvider column : sortOrderColumns) {
				PropertyWrapper prop = column.getColumn().getPropertyWrapper();
				QueryableDatatype<?> qdt = prop.getQueryableDatatype();
				PropertyWrapperDefinition propDefn = prop.getPropertyWrapperDefinition();
				if (prefersIndexBasedOrderByClause) {
					Integer columnIndex = indexesOfSelectedProperties.get(propDefn);
					if (columnIndex == null) {
						columnIndex = IndexesOfSelectedExpressions.get(qdt);
					}
					if (columnIndex == null) {
						final DBExpression[] columnExpressions = qdt.getColumnExpression();
						for (DBExpression columnExpression : columnExpressions) {
							columnIndex = IndexesOfSelectedExpressions.get(columnExpression);
						}
					}
					orderByClause.append(sortSeparator).append(columnIndex).append(defn.getOrderByDirectionClause(qdt.getSortOrder()));
					sortSeparator = defn.getSubsequentOrderByClauseSeparator();
				} else {
					if (qdt.hasColumnExpression()) {
						final DBExpression[] columnExpressions = qdt.getColumnExpression();
						for (DBExpression columnExpression : columnExpressions) {
							final String dbColumnName = defn.transformToStorableType(columnExpression).toSQLString(defn);
							if (dbColumnName != null) {
								orderByClause.append(sortSeparator).append(dbColumnName).append(defn.getOrderByDirectionClause(qdt.getSortOrder()));
								sortSeparator = defn.getSubsequentOrderByClauseSeparator();
							}
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
	 * Like
	 * {@link #getAllRowsInternal(nz.co.gregs.dbvolution.query.QueryOptions)  getAllRows()}
	 * this method retrieves all the rows for this DBQuery. However it checks the
	 * number of rows retrieved and throws a
	 * {@link UnexpectedNumberOfRowsException} if the number of rows retrieved
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
	 * nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @see #getAllRowsInternal(nz.co.gregs.dbvolution.query.QueryOptions)
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
//			for (Class<? extends DBRow> ConnectedTable : allConnectedTables) {
//				tablesToAdd.add(ConnectedTable.newInstance());
			for (Class<? extends DBRow> connectedTable : allConnectedTables) {
				try {
					tablesToAdd.add(connectedTable.newInstance());
				} catch (InstantiationException ex) {
					throw new UnableToInstantiateDBRowSubclassException(connectedTable, ex);
				} catch (IllegalAccessException ex) {
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
//			for (Class<? extends DBRow> ConnectedTable : allConnectedTables) {
//				tablesToAdd.add(ConnectedTable.newInstance());
			for (Class<? extends DBRow> connectedTable : allConnectedTables) {
				try {
					tablesToAdd.add(connectedTable.newInstance());
				} catch (InstantiationException ex) {
					throw new UnableToInstantiateDBRowSubclassException(connectedTable, ex);
				} catch (IllegalAccessException ex) {
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
//				DBRow newInstance = relatedTable.newInstance();
				DBRow newInstance;
				try {
					newInstance = relatedTable.newInstance();
				} catch (InstantiationException ex) {
					throw new UnableToInstantiateDBRowSubclassException(relatedTable, ex);
				} catch (IllegalAccessException ex) {
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
				} catch (InstantiationException ex) {
					throw new UnableToInstantiateDBRowSubclassException(relatedTable, ex);
				} catch (IllegalAccessException ex) {
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
				} catch (InstantiationException ex) {
					throw new UnableToInstantiateDBRowSubclassException(relatedTable, ex);
				} catch (IllegalAccessException ex) {
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
	 */
	public List<DBQueryRow> getAllRowsContaining(DBRow instance) throws SQLException {
		final QueryOptions options = details.getOptions();
		if (this.needsResults(options)) {
			database.executeDBQuery(this);
//			getAllRowsInternal(options);
		}
		List<DBQueryRow> returnList = new ArrayList<>();
		for (DBQueryRow row : results) {
			if (row.get(instance) == instance) {
				returnList.add(row);
			}
		}
		return returnList;
	}

	/**
	 * Limits the query results by adding post query conditions, generally using a
	 * HAVING clause.
	 *
	 * <p>
	 * This method returns the subset of this DBQuery's results that match the
	 * post query conditions
	 *
	 * <p>
	 * The easiest way to get a list of duplicated identifiers, make a query that
	 * returns the identifier and a count of the rows, and then add a post
	 * condition that requires the count to be greater than 1.
	 *
	 * @param postQueryConditions all the post-query conditions that need to be
	 * matched
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return A list of DBQueryRow instances that fulfill the post-query
	 * conditions Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @deprecated Use {@link #addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * } to add all conditions to the query instead.
	 */
	@Deprecated
	public List<DBQueryRow> getAllRowsHaving(BooleanExpression... postQueryConditions) throws SQLException {
		final QueryOptions options = details.getOptions();
		details.setHavingColumns(postQueryConditions);
		if (this.needsResults(options)) {
			database.executeDBQuery(this);
//			getAllRowsInternal(options);
		}
		return results;
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
	 * @param pageNumber	pageNumber
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of the DBQueryRows for the selected page. 1 Database
	 * exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public List<DBQueryRow> getAllRowsForPage(Integer pageNumber) throws SQLException {
		final QueryOptions options = details.getOptions();
		DBDatabase database = getReadyDatabase();
		final DBDefinition defn = database.getDefinition();

		if (defn.supportsPagingNatively(options)) {
			options.setPageIndex(pageNumber);
			if (this.needsResults(options)) {
				database.executeDBQuery(this);
//				getAllRowsInternal(options);
			}
			return results;
		} else {
			if (defn.supportsRowLimitsNatively(options)) {
				QueryOptions tempOptions = options.copy();
				tempOptions.setRowLimit((pageNumber + 1) * options.getRowLimit());
				if (this.needsResults(tempOptions) || tempOptions.getRowLimit() > results.size()) {
					details.setOptions(tempOptions);
					database.executeDBQuery(this);
//					getAllRowsInternal(tempOptions);
				}
			} else {
				if (this.needsResults(options)) {
					int rowLimit = options.getRowLimit();
					options.setRowLimit(-1);
					database.executeDBQuery(this);
//					getAllRowsInternal(options);
					options.setRowLimit(rowLimit);
				}
			}
			int rowLimit = options.getRowLimit();
			int startIndex = rowLimit * pageNumber;
			startIndex = (startIndex < 0 ? 0 : startIndex);
			int stopIndex = rowLimit * (pageNumber + 1);
			stopIndex = (stopIndex >= results.size() ? results.size() : stopIndex);
			if (stopIndex - startIndex < 1) {
				return new ArrayList<>();
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
	protected List<BooleanExpression> getConditions() {
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
	 * DBvolution defaults to a timeout of 10000milliseconds (10 seconds) to avoid
	 * eternal queries.
	 *
	 * <p>
	 * Use this method If you require a longer running query.
	 *
	 * @param milliseconds
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this query.
	 */
	public synchronized DBQuery setTimeoutInMilliseconds(int milliseconds) {
		this.timeoutInMilliseconds = milliseconds;
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
		this.timeoutInMilliseconds = null;
		if (this.timeout != null) {
			this.timeout.cancel();
		}
		return this;
	}

	/**
	 * Helper class to store the progress of turning the DBQuery into an actual
	 * piece of SQL.
	 *
	 */
	protected static class QueryState {

//		private QueryGraph graph;
		private final List<BooleanExpression> remainingExpressions;
		private final List<BooleanExpression> consumedExpressions = new ArrayList<>();
		private final List<String> requiredConditions = new ArrayList<>();
		private final List<String> optionalConditions = new ArrayList<>();
		private boolean queryIsFullOuterJoin = true;
		private boolean queryIsLeftOuterJoin = true;
//		private final boolean queryIsNativeQuery = true;

		QueryState(DBQuery query) {
			this.remainingExpressions = new ArrayList<>(query.getConditions());
		}

		private Iterable<BooleanExpression> getRemainingExpressions() {
			return new ArrayList<>(remainingExpressions);
		}

		private void consumeExpression(BooleanExpression expr) {
			remainingExpressions.remove(expr);
			consumedExpressions.add(expr);
		}

//		private void setGraph(QueryGraph queryGraph) {
//			this.graph = queryGraph;
//		}
		/**
		 * Adds a condition that pertains to a required table.
		 *
		 * @param conditionClause	conditionClause
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
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return a list of SQL snippets representing required conditions.
		 */
		protected List<String> getRequiredConditions() {
			return requiredConditions;
		}

		/**
		 * Add conditions that pertain to optional tables.
		 *
		 * @param conditionClauses	conditionClauses
		 */
		protected void addOptionalConditions(List<String> conditionClauses) {
			optionalConditions.addAll(conditionClauses);
		}

		/**
		 * Returns all the current conditions that pertain to options tables.
		 *
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return a list of SQL snippets representing conditions on optional
		 * tables.
		 */
		protected List<String> getOptionalConditions() {
			return optionalConditions;
		}

		void addedFullOuterJoinToQuery() {
			queryIsFullOuterJoin = queryIsFullOuterJoin && true;
			queryIsLeftOuterJoin = false;
		}

		void addedLeftOuterJoinToQuery() {
			queryIsLeftOuterJoin = queryIsLeftOuterJoin && true;
			queryIsFullOuterJoin = false;
		}

		void addedInnerJoinToQuery() {
			queryIsLeftOuterJoin = false;
			queryIsFullOuterJoin = false;
		}

		boolean isFullOuterJoin() {
			return queryIsFullOuterJoin;
		}
	}

}
