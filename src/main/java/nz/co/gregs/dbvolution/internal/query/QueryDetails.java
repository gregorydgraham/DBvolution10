/*
 * Copyright 2014 gregorygraham.
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
package nz.co.gregs.dbvolution.internal.query;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.actions.DBQueryable;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.databases.QueryIntention;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.*;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.SortProvider;
import nz.co.gregs.dbvolution.internal.properties.ColumnAspects;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapperDefinition;
import nz.co.gregs.dbvolution.internal.querygraph.QueryGraph;
import nz.co.gregs.separatedstring.SeparatedString;
import nz.co.gregs.separatedstring.SeparatedStringBuilder;

/**
 *
 * @author gregorygraham
 */
public class QueryDetails implements DBQueryable, Serializable {

	private static final long serialVersionUID = 1l;

	private static final String LINE_SEP = System.getProperty("line.separator");

	private Long timeoutInMilliseconds = 0l;//DEFAULT_TIMEOUT_MILLISECONDS;

	private final Map<Class<? extends DBRow>, DBRow> emptyRows = new HashMap<>();

	private final List<DBRow> allQueryTables = new ArrayList<>();
	private final List<DBRow> requiredQueryTables = new ArrayList<>();
	private final List<DBRow> optionalQueryTables = new ArrayList<>();
	private final List<DBRow> assumedQueryTables = new ArrayList<>();

	private QueryOptions options = new QueryOptions();
	private final List<DBRow> extraExamples = new ArrayList<>();
	private final List<BooleanExpression> conditions = new ArrayList<>();
	private final Map<Object, QueryableDatatype<?>> expressionColumns = new LinkedHashMap<>();
	private final Map<Object, DBExpression> dbReportGroupByColumns = new LinkedHashMap<>();
	private final Map<Class<?>, Map<String, DBRow>> existingInstances = new HashMap<>();
	private boolean groupByRequiredByAggregator = false;
	private String selectSQLClause = null;
	private final ArrayList<BooleanExpression> havingColumns = new ArrayList<>();
	private String rawSQLClause = "";
	private List<DBQueryRow> results = new ArrayList<>();
	private final List<String> resultSQL = new ArrayList<>();
	private int resultsPageIndex = 0;
	private Integer resultsRowLimit = -1;
	private Long queryCount = null;
	private transient QueryGraph queryGraph;
	private SortProvider[] sortOrderColumns;
	private List<DBQueryRow> currentPage;
	private String label = "UNLABELLED";
	private boolean quietExceptions = false;
	private boolean databaseQuietExceptionsPreference = false;

	/**
	 *
	 * @return the allQueryTables
	 */
	public List<DBRow> getAllQueryTables() {
		return allQueryTables;
	}

	/**
	 *
	 * @return the requiredQueryTables
	 */
	public List<DBRow> getRequiredQueryTables() {
		return requiredQueryTables;
	}

	/**
	 *
	 *
	 *
	 * @return the optionalQueryTables
	 */
	public List<DBRow> getOptionalQueryTables() {
		return optionalQueryTables;
	}

	/**
	 *
	 *
	 *
	 * @return the assumedQueryTables
	 */
	public List<DBRow> getAssumedQueryTables() {
		return assumedQueryTables;
	}

	/**
	 *
	 *
	 *
	 * @return the options
	 */
	public synchronized QueryOptions getOptions() {
		return options;
	}

	/**
	 *
	 *
	 *
	 * @return the extraExamples
	 */
	public List<DBRow> getExtraExamples() {
		return extraExamples;
	}

	/**
	 * Get all conditions involved in this query.
	 *
	 * @param database the database
	 * @param options
	 * @return all conditions in the query
	 */
	private synchronized List<BooleanExpression> getAllConditions(QueryOptions options) {
		List<BooleanExpression> allConditions = new ArrayList<>();
		for (DBRow entry : allQueryTables) {
			allConditions.addAll(entry.getWhereClauseExpressions(options.getQueryDefinition(), true));
		}
		return allConditions;
	}

	/**
	 * Get all conditions involved in this query.
	 *
	 * @return all conditions in the query
	 */
	public List<BooleanExpression> getAllConditions() {
		return getAllConditions(getOptions());
	}

	/**
	 *
	 *
	 *
	 * @return the conditions
	 */
	public List<BooleanExpression> getConditions() {
		return conditions;
	}

	/**
	 *
	 *
	 *
	 * @return the expressionColumns
	 */
	public Map<Object, QueryableDatatype<?>> getExpressionColumns() {
		return expressionColumns;
	}

	/**
	 *
	 *
	 *
	 * @return the dbReportGroupByColumns
	 */
	public Map<Object, DBExpression> getDBReportGroupByColumns() {
		return dbReportGroupByColumns;
	}

	/**
	 *
	 *
	 *
	 * @return the existingInstances
	 */
	public Map<Class<?>, Map<String, DBRow>> getExistingInstances() {
		return existingInstances;
	}

	/**
	 * Set the requirement for a GROUP BY clause.
	 *
	 * @param b the setting required
	 */
	public synchronized void setGroupByRequiredByAggregator(boolean b) {
		this.groupByRequiredByAggregator = true;
	}

	private synchronized boolean getGroupByRequiredByAggregator() {
		return this.groupByRequiredByAggregator;
	}

	/**
	 * Return the requirement for a GROUP BY clause.
	 *
	 *
	 *
	 *
	 * @return TRUE if the GROUP BY clause is required, otherwise FALSE.
	 */
	public boolean isGroupedQuery() {
		return getDBReportGroupByColumns().size() > 0 || getGroupByRequiredByAggregator();
	}

	/**
	 * Define the SELECT clause used during the query.
	 *
	 * @param selectClause the select clause to set
	 */
	public synchronized void setSelectSQLClause(String selectClause) {
		this.selectSQLClause = selectClause;
	}

	/**
	 * Get the SELECT clause used during the query.
	 *
	 *
	 *
	 *
	 * @return the SELECT clause defined earlier
	 */
	public synchronized String getSelectSQLClause() {
		return selectSQLClause;
	}

	/**
	 *
	 *
	 *
	 * @return the havingColumns
	 */
	public synchronized BooleanExpression[] getHavingColumns() {
		return havingColumns.toArray(new BooleanExpression[]{});
	}

	/**
	 * @param havingColumns the havingColumns to set
	 */
	public synchronized void setHavingColumns(BooleanExpression... havingColumns) {
		Collections.addAll(this.havingColumns, havingColumns);
	}

	public void setQueryType(QueryType queryType) {
		this.options.setQueryType(queryType);
	}

	public synchronized void setOptions(QueryOptions tempOptions) {
		this.options = tempOptions;
	}

	/**
	 * @return the rawSQLClause
	 */
	public synchronized String getRawSQLClause() {
		return rawSQLClause;
	}

	/**
	 * @param rawSQLClause the rawSQLClause to set
	 */
	public synchronized void setRawSQLClause(String rawSQLClause) {
		this.rawSQLClause = rawSQLClause;
	}

	/**
	 * @return the results
	 */
	public synchronized List<DBQueryRow> getResults() {
		return results != null ? results.subList(0, results.size()) : null;
	}

	/**
	 * @param results the results to set
	 */
	public synchronized void setResults(List<DBQueryRow> results) {
		this.results = results;
	}

	/**
	 * @return the resultSQL
	 */
	public synchronized List<String> getSQLQueries() {
		return resultSQL;
	}

	/**
	 * @param resultSQL the resultSQL to set
	 */
	public synchronized void setResultSQL(List<String> resultSQL) {
		this.resultSQL.clear();
		if (resultSQL != null) {
			this.resultSQL.addAll(resultSQL);
		}
	}

	/**
	 * @return the resultsPageIndex
	 */
	public synchronized Integer getResultsPageIndex() {
		return resultsPageIndex;
	}

	/**
	 * @param resultsPageIndex the resultsPageIndex to set
	 */
	public synchronized void setResultsPageIndex(Integer resultsPageIndex) {
		this.resultsPageIndex = resultsPageIndex;
	}

	/**
	 * @return the resultsRowLimit
	 */
	public synchronized Integer getResultsRowLimit() {
		return resultsRowLimit;
	}

	/**
	 * @param resultsRowLimit the resultsRowLimit to set
	 */
	public synchronized void setResultsRowLimit(Integer resultsRowLimit) {
		this.resultsRowLimit = resultsRowLimit;
	}

	public synchronized void clearResults() {
		setResults(new ArrayList<DBQueryRow>());
		setResultSQL(null);
	}

	public synchronized Long getCount() {
		return queryCount;
	}

	private synchronized void getResultSetCount(DBDatabase db) throws SQLException {
		long result = 0L;
		try (DBStatement dbStatement = db.getDBStatement()) {
			final List<String> sqlForCount = getSQLForCountInternal(this, options);
			for (String sql : sqlForCount) {
				printSQLIfRequired(sql);
				var dets = new StatementDetails(getLabel(), QueryIntention.SIMPLE_SELECT_QUERY, sql);
				try (ResultSet resultSet = dbStatement.executeQuery(dets)) {
					while (resultSet.next()) {
						result = resultSet.getLong(1);
					}
					break;// we have successfully run the count so stop
				}
			}
		}
		queryCount = result;
	}

	private synchronized List<String> getSQLForCountInternal(QueryDetails details, QueryOptions options) {

		if (!options.getQueryDefinition().supportsFullOuterJoinNatively()) {
			final List<String> sqlForQueryInternal = getSQLForQueryInternal(new QueryState(details), QueryType.SELECT, options);
			String endStatement = options.getQueryDefinition().endSQLStatement();
			return sqlForQueryInternal
					.stream()
					.map((sql) -> "SELECT COUNT(*) FROM (" + sql.replaceAll("; *$", "") + ") A" + endStatement)
					.collect(Collectors.toList());
		} else {
			return getSQLForQueryInternal(new QueryState(details), QueryType.COUNT, options);
		}
	}

	protected synchronized List<String> getSQLForQueryInternal(QueryState queryState, QueryType queryType, QueryOptions options) {
		try {
			List<String> sqlList = new ArrayList<>();
			final List<DBRow> allQueryTablesList = getAllQueryTables();
			final int allQueryTablesListSize = allQueryTablesList.size();

			if (allQueryTablesListSize >= 0) {

				initialiseQueryGraph();

				DBDefinition defn = options.getQueryDefinition(); //getDatabaseDefinition(database);
				StringBuilder selectClause = new StringBuilder().append(defn.beginSelectStatement());
				int columnIndex = 1;
				boolean groupByIsRequired = false;
				var groupByColumnAlias = SeparatedStringBuilder
						.forSeparator(defn.getSubsequentGroupBySubClauseSeparator())
						.withPrefix(defn.beginGroupByClause());
				var groupByColumnIndex = SeparatedStringBuilder
						.forSeparator(defn.getSubsequentGroupBySubClauseSeparator())
						.withPrefix(defn.beginGroupByClause());
				var groupByColumnSelectExpression = SeparatedStringBuilder
						.forSeparator(defn.getSubsequentGroupBySubClauseSeparator())
						.withPrefix(defn.beginGroupByClause());
				var groupByClause = SeparatedStringBuilder
						.forSeparator(defn.getSubsequentGroupBySubClauseSeparator())
						.withPrefix(defn.beginGroupByClause());
				HashMap<PropertyWrapperDefinition<?, ?>, Integer> indexesOfSelectedColumns = new HashMap<>();
				HashMap<DBExpression, Integer> indexesOfSelectedExpressions = new HashMap<>();
				StringBuilder fromClause = new StringBuilder();
				if (allQueryTablesListSize == 0) {
					fromClause.append(defn.getFromDualEquivalent());
					queryState.setQueryOnDual(true);
				} else {
					fromClause.append(defn.beginFromClause());
				}
				final String initialWhereClause = new StringBuilder().append(defn.beginWhereClause()).append(defn.getWhereClauseBeginningCondition(options)).toString();
				StringBuilder whereClause = new StringBuilder(initialWhereClause);
				String havingClause;
				String fromClauseTableSeparator = "";
				String colSep = defn.getStartingSelectSubClauseSeparator();

				if (allQueryTablesListSize > 0) {
					List<DBRow> sortedQueryTables = options.isCartesianJoinAllowed()
							? queryGraph.toListIncludingCartesianReversable(queryType == QueryType.REVERSESELECT)
							: queryGraph.toListReversable(queryType == QueryType.REVERSESELECT);

					if (options.getRowLimit() > 0) {
						selectClause.append(defn.getLimitRowsSubClauseDuringSelectClause(options));
					}
					String tableName;

					for (DBRow tabRow : sortedQueryTables) {
						tableName = tabRow.getTableNameOrVariantIdentifier();

						var tabProps = tabRow.getSelectedProperties();
						for (var propWrapper : tabProps) {
							final var qdt = propWrapper.getQueryableDatatype();
							final List<ColumnAspects> columnAspectsList = propWrapper.getColumnAspects(defn);
							for (ColumnAspects columnAspects : columnAspectsList) {
								String selectableName = columnAspects.getSelectableName();
								String columnAlias = columnAspects.getColumnAlias();
								String selectColumn = defn.doColumnTransformForSelect(qdt, selectableName);
								selectClause.append(colSep).append(selectColumn).append(" ").append(columnAlias);
								colSep = defn.getSubsequentSelectSubClauseSeparator() + LINE_SEP;

								// Now deal with the GROUP BY and ORDER BY clause requirements
								DBExpression expression = columnAspects.getExpression();
								if (expression != null && expression.isAggregator()) {
									setGroupByRequiredByAggregator(true);
								}
								if (expression == null
										|| (!expression.isAggregator() && !expression.isWindowingFunction()
										&& (!expression.isPurelyFunctional() || defn.supportsPurelyFunctionalGroupByColumns()))) {
									groupByIsRequired = true;
									groupByColumnIndex.add("" + columnIndex);
									groupByColumnAlias.add(columnAlias);
									groupByColumnSelectExpression.add(selectColumn);
									if (expression != null) {
										groupByClause.add(defn.transformToGroupableType(expression).toSQLString(defn));
									} else {
										groupByClause.add(selectColumn);
									}

									indexesOfSelectedColumns.put(propWrapper.getPropertyWrapperDefinition(), columnIndex);
								}
								if (expression != null && expression.isComplexExpression()) {
									final boolean needsJoiner = queryState.hasHadATableAdded();
									String joiner = needsJoiner ? options.isUseANSISyntax() ? " join " : fromClauseTableSeparator : "";
									fromClause
											.append(joiner)
											.append(expression.createSQLForFromClause(options.getQueryDatabase()));
									fromClauseTableSeparator = ", " + LINE_SEP;
									if (options.isUseANSISyntax()
											&& defn.requiresOnClauseForAllJoins()
											&& queryState.hasHadATableAdded()) {
										fromClause
												.append(defn.beginOnClause())
												.append(BooleanExpression.trueExpression().toSQLString(defn))
												.append(defn.endOnClause());
									}
									if (!expression.isWindowingFunction()) {
										final String groupBySQL = expression.createSQLForGroupByClause(options.getQueryDatabase());
										if (groupBySQL != null && !groupBySQL.isEmpty() && !groupBySQL.trim().isEmpty()) {
											groupByClause.add(groupBySQL);
											groupByIsRequired = true;
										}
									}
									queryState.addJoinedExpression(expression);
								}

								columnIndex++;
							}
						}
						if (!options.isUseANSISyntax()) {
							fromClause.append(fromClauseTableSeparator).append(tableName);
							queryState.addedInnerJoinToQuery();
						} else {
							fromClause.append(getANSIJoinClause(defn, queryState, tabRow, options));
						}
						queryState.addJoinedTable(tabRow);

						if (!options.isUseANSISyntax()) {
							List<String> tabRowCriteria = tabRow.getWhereClausesWithAliases(defn);
							if (tabRowCriteria != null && !tabRowCriteria.isEmpty()) {
								for (String clause : tabRowCriteria) {
									whereClause.append(LINE_SEP).append(defn.beginConditionClauseLine(options)).append(clause);
								}
							}
							getNonANSIJoin(tabRow, whereClause, defn, queryState.getJoinedTables(), LINE_SEP, options);
							queryState.addedInnerJoinToQuery();
						}

						fromClauseTableSeparator = ", " + LINE_SEP;
					}
				}

				//add conditions found during the ANSI Join creation
				final String conditionsAsSQLClause = mergeConditionsIntoSQLClause(queryState.getRequiredConditions(), defn, options);
				if (!conditionsAsSQLClause.isEmpty()) {
					whereClause.append(defn.beginConditionClauseLine(options)).append(conditionsAsSQLClause);
				}

				for (DBRow extra : getExtraExamples()) {
					List<String> extraCriteria = extra.getWhereClausesWithAliases(defn);
					if (extraCriteria != null && !extraCriteria.isEmpty()) {
						for (String clause : extraCriteria) {
							whereClause.append(LINE_SEP).append(defn.beginConditionClauseLine(options)).append(clause);
						}
					}
				}

				for (BooleanExpression expression : queryState.getRemainingExpressions()) {
					whereClause.append(LINE_SEP).append(defn.beginConditionClauseLine(options)).append("(").append(expression.toSQLString(defn)).append(")");
					queryState.consumeExpression(expression);
				}

				for (Map.Entry<Object, QueryableDatatype<?>> entry : getExpressionColumns().entrySet()) {
					final Object key = entry.getKey();
					final QueryableDatatype<?> qdt = entry.getValue();
					DBExpression[] expressions = qdt.getColumnExpression();
					for (DBExpression expression : expressions) {
						final String columnAlias = defn.formatExpressionAlias(key);
						final String selectColumn = defn.transformToSelectableType(expression).toSQLString(defn);
						selectClause.append(colSep).append(selectColumn).append(" ").append(columnAlias);
						colSep = defn.getSubsequentSelectSubClauseSeparator() + LINE_SEP;
						if (expression.isAggregator()) {
							setGroupByRequiredByAggregator(true);
						}
						if (!expression.isAggregator() && !expression.isWindowingFunction()
								&& (!expression.isPurelyFunctional() || defn.supportsPurelyFunctionalGroupByColumns())) {
							groupByIsRequired = true;
							groupByColumnIndex.add("" + columnIndex);
							groupByColumnAlias.add(columnAlias);
							groupByColumnSelectExpression.add(selectColumn);
							groupByClause.add(defn.transformToGroupableType(expression).toSQLString(defn));

						}
						if (expression.isComplexExpression()) {
							fromClause
									.append(options.isUseANSISyntax() ? " join " : fromClauseTableSeparator)
									.append(expression.createSQLForFromClause(options.getQueryDatabase()));
							if (options.isUseANSISyntax()
									&& defn.requiresOnClauseForAllJoins()
									&& queryState.hasHadATableAdded()) {
								fromClause
										.append(defn.beginOnClause())
										.append(BooleanExpression.trueExpression().toSQLString(defn))
										.append(defn.endOnClause());
							}
							fromClauseTableSeparator = (options.isUseANSISyntax() ? " join " : ", ") + LINE_SEP;
							if (!expression.isWindowingFunction()) {
								final String groupBySQL = expression.createSQLForGroupByClause(options.getQueryDatabase());
								if (groupBySQL != null && !groupBySQL.isEmpty() && !groupBySQL.trim().isEmpty()) {
									groupByClause.add(groupBySQL);
									groupByIsRequired = true;
								}
							}
							queryState.addJoinedExpression(expression);
						}
						indexesOfSelectedExpressions.put(expression, columnIndex);
						columnIndex++;
					}
				}

				for (Map.Entry<Object, DBExpression> entry : getDBReportGroupByColumns().entrySet()) {
					final DBExpression expression = entry.getValue();
					if (!expression.isWindowingFunction()
							&& (!expression.isPurelyFunctional() || defn.supportsPurelyFunctionalGroupByColumns())) {
						groupByClause.add(defn.transformToGroupableType(expression).toSQLString(defn));
					}
				}

				// tidy up the raw SQL provided
				String rawSQLClauseFinal = (getRawSQLClause().isEmpty() ? "" : getRawSQLClause());

				// Strip the unnecessary where clause if possible
				if (whereClause.toString().equals(initialWhereClause) && rawSQLClauseFinal.isEmpty()) {
					whereClause = new StringBuilder("");
				}

				if (queryType == QueryType.SELECT
						|| queryType == QueryType.REVERSESELECT) {
					if (getSelectSQLClause() == null) {
						setSelectSQLClause(selectClause.toString());
					}
					if (queryType == QueryType.REVERSESELECT) {
						selectClause = new StringBuilder(getSelectSQLClause());
					}
					OrderByClause orderByClause = getOrderByClause(queryState, defn, indexesOfSelectedColumns, indexesOfSelectedExpressions);
					for (String str : orderByClause.getGroupByClauses()) {
						groupByClause.add(str);
					}
					String orderByClauseFinal = orderByClause.getOrderByClause();
					if (!orderByClauseFinal.trim().isEmpty()) {
						queryState.setHasBeenOrdered(true);
					} else if (options.getPageIndex() > 0 || options.getRowLimit() > 0) {
						orderByClauseFinal = defn.getDefaultOrderingClause();
					}
					havingClause = getHavingClause(options.getQueryDatabase(), options);
					List<String> groupByClauseFinal = new ArrayList<>();
					if (isGroupedQuery() && groupByIsRequired) {
						final DBDefinition.GroupByClauseMethod[] preferences = defn.preferredGroupByClauseMethod();
						for (DBDefinition.GroupByClauseMethod preference : preferences) {
							switch (preference) {
								case ALIAS:
									groupByClauseFinal.add(groupByColumnAlias.toString());
									break;
								case INDEX:
									groupByClauseFinal.add(groupByColumnIndex.toString());
									break;
								case SELECTEXPRESSION:
									groupByClauseFinal.add(groupByColumnSelectExpression.toString());
									break;
								case GROUPBYEXPRESSION:
									groupByClauseFinal.add(groupByClause.toString());
									break;
								default:
									// Default to the GROUPBYEXPRESSION method as it seems to the best supported
									groupByClauseFinal.add(groupByClause.toString());
									break;
							}
						}
					}
					if (groupByClauseFinal.size() > 0) {
						for (String groupByClauseSQL : groupByClauseFinal) {
							sqlList.add(assembleSQLQuery(defn, selectClause, fromClause, whereClause, rawSQLClauseFinal, groupByClauseSQL, havingClause, orderByClauseFinal, options, queryState));
						}
					} else {
						sqlList.add(assembleSQLQuery(defn, selectClause, fromClause, whereClause, rawSQLClauseFinal, "", havingClause, orderByClauseFinal, options, queryState));
					}

				} else if (queryType == QueryType.COUNT) {
					setSelectSQLClause(defn.countStarClause());
					sqlList.add(defn.beginSelectStatement()
							+ defn.countStarClause() + LINE_SEP
							+ fromClause + LINE_SEP
							+ whereClause + LINE_SEP
							+ rawSQLClauseFinal + LINE_SEP
							+ defn.endSQLStatement());
				}
				if (options.isCreatingNativeQuery()
						&& !queryState.isQueryOnDual()
						&& queryState.isFullOuterJoin()
						&& !defn.supportsFullOuterJoinNatively()) {
					List<String> collected = sqlList.stream().map((variant) -> getSQLForFakeFullOuterJoin(variant, this, options)).collect(Collectors.toList());
					sqlList.clear();
					sqlList.addAll(collected);
				}
			}
			return sqlList;
		} catch (Throwable e) {
			StackTraceElement[] trace = e.getStackTrace();
			System.out.println("" + trace[0]);
			System.out.println("" + trace[1]);
			System.out.println("" + trace[2]);
			System.out.println("" + trace[3]);
			System.out.println("" + trace[4]);
			throw e;
		}
	}

	protected String assembleSQLQuery(DBDefinition defn, StringBuilder selectClause, StringBuilder fromClause, StringBuilder whereClause, String rawSQLClauseFinal, String groupByClauseSQL, String havingClause, String orderByClauseFinal, QueryOptions options1, QueryState queryState) {
		return defn.doWrapQueryForPaging(
				SeparatedStringBuilder
						.lineSeparated()
						.trimBlanks()
						.add(selectClause.toString())
						.add(fromClause.toString())
						.add(whereClause.toString())
						.add(rawSQLClauseFinal)
						.add(groupByClauseSQL)
						.add(havingClause)
						.add(orderByClauseFinal)
						.add(options1.getRowLimit() > 0 ? defn.getLimitRowsSubClauseAfterWhereClause(queryState, options1) : "")
						.add(defn.endSQLStatement()).toString(),
				options1);
	}

	private synchronized void initialiseQueryGraph() {
		if (queryGraph == null) {
			queryGraph = new QueryGraph(getRequiredQueryTables(), getConditions());
			queryGraph.addOptionalAndConnectToRelevant(getOptionalQueryTables(), getConditions());
		} else {
			queryGraph.clear();
			queryGraph.addAndConnectToRelevant(getRequiredQueryTables(), getConditions());
			queryGraph.addOptionalAndConnectToRelevant(getOptionalQueryTables(), getConditions());
		}
	}

	public synchronized String getANSIJoinClause(DBDefinition defn, QueryState queryState, DBRow newTable, QueryOptions options) {
		List<String> joinClauses = new ArrayList<>();
		List<String> conditionClauses = new ArrayList<>();
		String lineSep = System.getProperty("line.separator");
		boolean isLeftOuterJoin = false;
		boolean isFullOuterJoin = false;

		List<DBRow> previousTables = queryState.getJoinedTables();
		final ArrayList<DBRow> preExistingTables = new ArrayList<>();
		preExistingTables.addAll(previousTables);
		preExistingTables.addAll(getAssumedQueryTables());

		List<DBRow> requiredTables = getRequiredQueryTables();

		if (requiredTables.isEmpty() && getOptionalQueryTables().size() == getAllQueryTables().size()) {
			isFullOuterJoin = true;
			queryState.addedFullOuterJoinToQuery();
		} else if (getOptionalQueryTables().contains(newTable)) {
			isLeftOuterJoin = true;
			queryState.addedLeftOuterJoinToQuery();
		} else {
			queryState.addedInnerJoinToQuery();
		}

		//Store the expressions from the new table in the QueryState
		for (DBRow otherTable : preExistingTables) {
			queryState.addAllToRemainingExpressions(newTable.getRelationshipsAsBooleanExpressions(otherTable));
		}

		// Add new table's conditions
		List<String> newTableConditions = newTable.getWhereClausesWithAliases(defn);
		if (requiredTables.contains(newTable)) {
			queryState.addRequiredConditions(newTableConditions);
		} else {
			conditionClauses.addAll(newTableConditions);
		}

		// Since the first table can not have a ON clause we need to add it's ON clause to the second table's.
		if (previousTables.size() == 1) {
			final DBRow firstTable = previousTables.get(0);
			if (!getRequiredQueryTables().contains(firstTable)) {
				List<String> firstTableConditions = firstTable.getWhereClausesWithAliases(defn);
				conditionClauses.addAll(firstTableConditions);
			}
		}

		// Add all the expressions we can
		if (previousTables.size() > 0 || conditionClauses.size() > 0) {
			for (BooleanExpression expr : queryState.getRemainingExpressions()) {
				Set<DBRow> tablesInvolved = new HashSet<>(expr.getTablesInvolved());
				if (tablesInvolved.contains(newTable)) {
					tablesInvolved.remove(newTable);
				}
				if (tablesInvolved.size() <= previousTables.size()) {
					if (previousTables.containsAll(tablesInvolved)) {
						if (expr.isWindowingFunction()) {
							if (defn.supportsWindowingFunctionsInTheHavingClause()) {
								havingColumns.add(expr);
							}
						} else if (expr.isRelationship()) {
							joinClauses.add(expr.toSQLString(defn));
						} else {
							if (requiredTables.containsAll(tablesInvolved)) {
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
		if (queryState.hasNotHadATableAddedYet()) {
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

	private synchronized void getNonANSIJoin(DBRow tabRow, StringBuilder whereClause, DBDefinition defn, List<DBRow> otherTables, String lineSep, QueryOptions options) {

		for (DBRow otherTab : otherTables) {
			var otherTableFks = otherTab.getForeignKeyPropertyWrappers();
			for (var otherTableFk : otherTableFks) {
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

	private synchronized String mergeConditionsIntoSQLClause(List<String> conditionClauses, DBDefinition defn, QueryOptions options) {
		String separator = "";
		StringBuilder sqlToReturn = new StringBuilder();
		for (String cond : conditionClauses) {
			sqlToReturn.append(separator).append(cond);
			separator = defn.beginConditionClauseLine(options);
		}
		return sqlToReturn.toString();
	}

	private synchronized OrderByClause getOrderByClause(QueryState state, DBDefinition defn, Map<PropertyWrapperDefinition<?, ?>, Integer> indexesOfSelectedProperties, Map<DBExpression, Integer> IndexesOfSelectedExpressions) {
		OrderByClause clause = new OrderByClause();
		final boolean prefersIndexBasedOrderByClause = defn.prefersIndexBasedOrderByClause();
		if (sortOrderColumns != null && sortOrderColumns.length > 0) {
			state.setHasBeenOrdered(true);
			SeparatedString orderByClause = SeparatedStringBuilder.byCommas();
			for (SortProvider sorter : sortOrderColumns) {
				if (!sorter.isWindowingFunction() || defn.supportsWindowingFunctionsInTheOrderByClause()) {
					clause.addGroupByClauses(sorter.getGroupByClauses(defn));
					if (sorter.hasQueryColumn()) {
						orderByClause.add(defn.transformToSortableType(sorter).toSQLString(defn));
					} else {
						if (prefersIndexBasedOrderByClause) {
							PropertyWrapperDefinition<?, ?> propDefn;
							QueryableDatatype<?> qdt;
							if (sorter instanceof SortProvider.Column) {
								var prop = ((SortProvider.Column) sorter).getPropertyWrapper();
								propDefn = prop.getPropertyWrapperDefinition();
								qdt = prop.getQueryableDatatype();
							} else {
								propDefn = null;
								qdt = sorter.asExpressionColumn();
							}

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
							orderByClause.add(columnIndex + sorter.getSortDirectionSQL(defn));
						} else {
							orderByClause.add(sorter.toSQLString(defn));
						}
					}
				}
			}
			orderByClause
					.withPrefix(defn.beginOrderByClause())
					.withSuffix(defn.endOrderByClause())
					.useWhenEmpty("");
			clause.setOrderByClause(orderByClause);
		}

		return clause;
	}

	private synchronized String getHavingClause(DBDatabase database, QueryOptions options) {
		BooleanExpression[] having = getHavingColumns();
		final DBDefinition defn = options.getQueryDefinition();
		String havingClauseStart = defn.getHavingClauseStart();
		if (having.length == 1) {
			return havingClauseStart + having[0].toSQLString(defn);
		} else if (having.length > 1) {
			String sep = "";
			final String beginAndLine = defn.beginAndLine();
			StringBuilder returnStr = new StringBuilder(havingClauseStart);
			for (BooleanExpression havingColumn : having) {
				returnStr.append(sep).append(havingColumn.toSQLString(defn));
				sep = beginAndLine;
			}
			return returnStr.toString();
		} else {
			return "";
		}
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
	 *
	 *
	 * @return a fake full outer join query for databases that don't support FULL
	 * OUTER joins
	 */
	private synchronized String getSQLForFakeFullOuterJoin(String existingSQL, QueryDetails details, QueryOptions options) {
		String sqlForQuery;
		String unionOperator;
		DBDefinition defn = options.getQueryDefinition();
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
			String reversedQuery = getSQLForQueryInternal(new QueryState(details), QueryType.REVERSESELECT, options).get(0);
			options.setCreatingNativeQuery(true);

			sqlForQuery = existingSQL.replaceAll("; *$", " ").replaceAll(defn.beginFullOuterJoin(), defn.beginLeftOuterJoin());
			sqlForQuery += unionOperator;
			sqlForQuery += reversedQuery.replaceAll("; *$", " ").replaceAll(defn.beginFullOuterJoin(), defn.beginLeftOuterJoin());
		}
		return sqlForQuery;
	}

	public synchronized void setSortOrder(SortProvider[] sortColumns) {
		blankResults();
		sortOrderColumns = Arrays.copyOf(sortColumns, sortColumns.length);
	}

	public synchronized void setSortOrder(ColumnProvider[] sortColumns) {
		List<SortProvider> sorters = new ArrayList<>();
		for (ColumnProvider col : sortColumns) {
			sorters.add(col.getSortProvider());
		}
		this.setSortOrder(sorters.toArray(new SortProvider[]{}));
	}

	public synchronized void blankResults() {
		setResults(null);
		setResultSQL(null);
		queryGraph = null;
	}

	public synchronized void addToSortOrder(SortProvider[] sortColumns) {
		if (sortColumns != null) {
			blankResults();
			List<SortProvider> sortOrderColumnsList = new LinkedList<>();
			if (sortOrderColumns != null) {
				sortOrderColumnsList.addAll(Arrays.asList(sortOrderColumns));
			}
			sortOrderColumnsList.addAll(Arrays.asList(sortColumns));

			setSortOrder(sortOrderColumnsList.toArray(new SortProvider[]{}));
		}
	}

	public synchronized void clearSortOrder() {
//		sortOrder = null;
		sortOrderColumns = null;
	}

	private synchronized void prepareForQuery(DBDatabase database, QueryOptions options) {
		clearResults();
		setResultSQL(null);
		options.setQueryDatabase(database);

		setReturnEmptyStringForNullString(
				getReturnEmptyStringForNullString()
				|| !database.supportsDifferenceBetweenNullAndEmptyString()
		);
	}

	public synchronized boolean needsResults(QueryOptions options) {
		final DBDatabase queryDatabase = options.getQueryDatabase();
		return getResults() == null
				|| queryDatabase == null
				|| getSQLQueries() == null
				|| getResults().isEmpty()
				|| !getResultsPageIndex().equals(options.getPageIndex())
				|| !getResultsRowLimit().equals(options.getRowLimit())
				|| !getSQLQueries().equals(getSQLForQueryInternal(new QueryState(this), QueryType.SELECT, options));
	}

	@Override
	public synchronized List<DBQueryRow> getAllRows() throws SQLException, SQLTimeoutException, AccidentalBlankQueryException, AccidentalCartesianJoinException {
		final QueryOptions opts = getOptions();
		if (this.needsResults(opts)) {
			try {
				getOptions().getQueryDatabase().executeDBQuery(this);
			} catch (LoopDetectedInRecursiveSQL ex) {
				/*This should never happen*/
				Logger.getLogger(QueryDetails.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
		if (opts.getRowLimit() > 0 && getResults().size() > opts.getRowLimit()) {
			final int firstItemOfPage = opts.getPageIndex() * opts.getRowLimit();
			final int firstItemOfNextPage = (opts.getPageIndex() + 1) * opts.getRowLimit();
			return getResults().subList(firstItemOfPage, firstItemOfNextPage);
		} else {
			return getResults();
		}
	}

	public synchronized String getSQLForQuery(DBDatabase db) {
		QueryType queryType = getOptions().getQueryType();
		getOptions().setQueryType(QueryType.GENERATESQLFORSELECT);
		prepareForQuery(db, options);
		String sql = getSQLForQueryInternal(new QueryState(this), QueryType.SELECT, getOptions()).get(0);
		getOptions().setQueryType(queryType);
		return sql;
	}

	public synchronized String getSQLForCount(DBDatabase db) {
		QueryType queryType = getOptions().getQueryType();
		getOptions().setQueryType(QueryType.GENERATESQLFORCOUNT);
		prepareForQuery(db, options);
		String sql = getSQLForQueryInternal(new QueryState(this), QueryType.SELECT, getOptions()).get(0);
		getOptions().setQueryType(queryType);
		return sql;
	}

	@Override
	public synchronized DBQueryable query(DBDatabase db) throws SQLException, AccidentalBlankQueryException, LoopDetectedInRecursiveSQL {
		final QueryOptions currentOptions = getOptions();
		prepareForQuery(db, currentOptions);
		final QueryType queryType = currentOptions.getQueryType();
		switch (queryType) {
			case COUNT:
				getResultSetCount(db);
				break;
			case ROWSFORPAGE:
				getAllRowsForPage(currentOptions);
				break;
			case GENERATESQLFORSELECT:
				this.setResultSQL(getSQLForQueryInternal(new QueryState(this), QueryType.SELECT, currentOptions));
				break;
			case GENERATESQLFORCOUNT:
				this.setResultSQL(QueryDetails.this.getSQLForCountInternal(this, currentOptions));
				break;
			case SELECT:
				fillResultSetInternal(currentOptions);
				break;
			default:
				throw new UnsupportedOperationException("Query Type Not Supported: " + queryType);
		}
		return this;
	}

	protected synchronized void getAllRowsForPage(QueryOptions opts) throws SQLException, AccidentalBlankQueryException, AccidentalCartesianJoinException, LoopDetectedInRecursiveSQL {
		int pageNumber = getResultsPageIndex();
		final DBDefinition defn = opts.getQueryDefinition();

		if (defn.supportsPagingNatively(opts)) {
			opts.setPageIndex(pageNumber);
			if (needsResults(opts)) {
				fillResultSetInternal(options);
			}
			setCurrentPage(getResults());
		} else {
			if (defn.supportsRowLimitsNatively(opts)) {
				QueryOptions tempOptions = new QueryOptions(opts);
				tempOptions.setQueryType(QueryType.SELECT);
				tempOptions.setRowLimit((pageNumber + 1) * opts.getRowLimit());
				if (needsResults(tempOptions) || tempOptions.getRowLimit() > getResults().size()) {
					setOptions(tempOptions);
					opts.getQueryDatabase().executeDBQuery(this);
					setOptions(opts);
				}
			} else {
				if (needsResults(opts)) {
					QueryOptions tempOptions = new QueryOptions(opts);
					tempOptions.setRowLimit(-1);
					tempOptions.setQueryType(QueryType.SELECT);
					setOptions(tempOptions);

					opts.getQueryDatabase().executeDBQuery(this);

					setOptions(opts);
				}
			}
			int rowLimit = opts.getRowLimit();
			int startIndex = rowLimit * pageNumber;
			startIndex = (startIndex < 0 ? 0 : startIndex);
			int stopIndex = rowLimit * (pageNumber + 1);
			stopIndex = (stopIndex >= getResults().size() ? getResults().size() : stopIndex);
			if (stopIndex - startIndex < 1) {
				setCurrentPage(new ArrayList<DBQueryRow>());
			} else {
				setCurrentPage(getResults().subList(startIndex, stopIndex));
			}
		}
	}

	protected synchronized void fillResultSetInternal(QueryOptions options) throws SQLException, AccidentalBlankQueryException, AccidentalCartesianJoinException, LoopDetectedInRecursiveSQL {
		final List<String> sqlForQuery = this.getSQLForQueryInternal(new QueryState(this), QueryType.SELECT, options);

		setResultSQL(sqlForQuery);

		final DBDefinition defn = options.getQueryDefinition();

		if (!options.isBlankQueryAllowed() && willCreateBlankQuery(options) && getRawSQLClause().isEmpty()) {
			throw new AccidentalBlankQueryException(options.isBlankQueryAllowed(), willCreateBlankQuery(options), getRawSQLClause().isEmpty());
		}

		if (!options.isCartesianJoinAllowed()
				&& (getRequiredQueryTables().size() + getOptionalQueryTables().size()) > 1
				&& queryGraph.willCreateCartesianJoin()) {
			throw new AccidentalCartesianJoinException(this);
		}
		// all set to execute the query
		fillResultSetFromSQL(options, defn);
	}

	protected synchronized void fillResultSetFromSQL(QueryOptions options, final DBDefinition defn) throws AccidentalCartesianJoinException, AccidentalBlankQueryException, LoopDetectedInRecursiveSQL, SQLTimeoutException, SQLException {
		ArrayList<DBQueryRow> foundRows = new ArrayList<DBQueryRow>();
		SQLException firstException = null;
		boolean successfulQuery = false;
		for (String sql : getSQLQueries()) {
			final DBDatabase queryDatabase = options.getQueryDatabase();
			try (DBStatement dbStatement = queryDatabase.getDBStatement()) {
				printSQLIfRequired(sql);
				try (ResultSet resultSet = getResultSetForSQL(dbStatement, sql)) {
					DBQueryRow queryRow;
					while (resultSet.next()) {
						queryRow = new DBQueryRow(this);

						setExpressionColumns(defn, resultSet, queryRow);

						setQueryRowFromResultSet(defn, resultSet, this, queryRow, isGroupedQuery());
						foundRows.add(queryRow);
					}
				}
				successfulQuery = true;
				break;// we've successfully run the sql so carry on
			} catch (SQLException e) {
				if (isQuietExceptions() == false) {
					StackTraceElement[] trace = e.getStackTrace();
					System.out.println("" + e.getMessage());
					System.out.println("" + e.getLocalizedMessage());
					for (int i = 0; i < 11 && i < trace.length; i++) {
						System.out.println("" + trace[i]);
					}
				}
				queryDatabase.handleErrorDuringExecutingSQL(queryDatabase, e, sql);
				if (firstException == null) {
					firstException = e;
				}
			}
		}
		if (successfulQuery) {
			for (DBQueryRow result : foundRows) {
				List<DBRow> rows = result.getAll();
				for (DBRow row : rows) {
					if (row != null) {
						setAutoFilledFields(foundRows, row);
					}
				}
			}
			setResults(foundRows);
		} else {
			throw firstException;
		}
	}

	private void printSQLIfRequired(String sql) {
		if (options.getPrintSQLBeforeExecution()) {
			System.out.println("/* SQL for " + this.label + " on " + options.getQueryDatabase().getLabel() + " */ " + sql);
		}
	}

	@SuppressWarnings("unchecked")
	synchronized void setAutoFilledFields(List<DBQueryRow> allRows, DBRow row) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		boolean arrayRequired = false;
		boolean listRequired = false;
		try {
			var fields = row.getAutoFillingPropertyWrappers();
			for (var field : fields) {
				if (field.isAutoFilling()) {
					Class<?> requiredClass = field.getRawJavaType();
					if (requiredClass.isArray()) {
						requiredClass = requiredClass.getComponentType();
						arrayRequired = true;

					} else if (Collection.class
							.isAssignableFrom(requiredClass)) {
						listRequired = true;
						requiredClass = field.getAutoFillingClass();

						if (requiredClass.isAssignableFrom(DBRow.class
						)) {
							throw new nz.co.gregs.dbvolution.exceptions.UnacceptableClassForAutoFillAnnotation(field, requiredClass);

						}
					}
					if (DBRow.class
							.isAssignableFrom(requiredClass)) {
						DBRow fieldInstance;
						try {
							fieldInstance = DBRow.getDBRow((Class<? extends DBRow>) requiredClass);
						} catch (IllegalArgumentException | SecurityException ex) {
							throw new UnableToInstantiateDBRowSubclassException((Class<? extends DBRow>) requiredClass, ex);
						}
						List<DBRow> relatedInstancesFromQuery = getRelatedInstancesFromQueryResults(allRows, row, fieldInstance);
						if (arrayRequired) {
							Object newInstance = Array.newInstance(requiredClass, relatedInstancesFromQuery.size());
							for (int index = 0; index < relatedInstancesFromQuery.size(); index++) {
								Array.set(newInstance, index, relatedInstancesFromQuery.get(index));
							}
							field.setRawJavaValue(newInstance);
						} else if (listRequired) {
							field.setRawJavaValue(relatedInstancesFromQuery);
						} else if (relatedInstancesFromQuery.isEmpty()) {
							field.setRawJavaValue(null);
						} else {
							field.setRawJavaValue(relatedInstancesFromQuery.get(0));
						}
					}
				}
			}
		} catch (UnacceptableClassForAutoFillAnnotation | UnableToInstantiateDBRowSubclassException | NegativeArraySizeException | IllegalArgumentException | ArrayIndexOutOfBoundsException ex) {
			throw new RuntimeException("Unable To AutoFill Field", ex);
		}
	}

	/**
	 * Finds all instances of {@code example} that share a {@link DBQueryRow} with
	 * this instance.
	 *
	 * @param <R> DBRow
	 * @param row the instance that the examples connect to.
	 * @param example example
	 *
	 *
	 * @return all instances of {@code example} that are connected to this
	 * instance in the {@code query} 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public <R extends DBRow> List<R> getRelatedInstancesFromQuery(DBRow row, R example) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		final List<DBQueryRow> allRows = getAllRows();
		return getRelatedInstancesFromQueryResults(allRows, row, example);
	}

	private <R extends DBRow> List<R> getRelatedInstancesFromQueryResults(final List<DBQueryRow> allRows, DBRow row, R example) {
		List<R> instances = new ArrayList<>();
		for (DBQueryRow qrow : allRows) {
			DBRow versionOfThis = qrow.get(row);
			R versionOfThat = qrow.get(example);
			if (versionOfThis.equals(row) && versionOfThat != null) {
				instances.add(versionOfThat);
			}
		}
		return instances;
	}

	public synchronized boolean willCreateBlankQuery(DBDatabase database) {
		prepareForQuery(database, options);
		return QueryDetails.this.willCreateBlankQuery(options);
	}

	protected synchronized boolean willCreateBlankQuery(QueryOptions options) {
		boolean willCreateBlankQuery = true;
		for (DBRow table : getAllQueryTables()) {
			willCreateBlankQuery = willCreateBlankQuery && table.willCreateBlankQuery(options.getQueryDefinition());
		}
		for (DBRow table : getExtraExamples()) {
			willCreateBlankQuery = willCreateBlankQuery && table.willCreateBlankQuery(options.getQueryDefinition());
		}
		willCreateBlankQuery = willCreateBlankQuery && getHavingColumns().length == 0;
		return willCreateBlankQuery && (getConditions().isEmpty());
	}

	/**
	 * Executes the query using the statement provided and returns the ResultSet
	 *
	 * @param statement dbStatement
	 * @param sql sql
	 *
	 *
	 * @return the ResultSet returned from the actual database. Database
	 * exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws java.sql.SQLTimeoutException time out exception
	 * @throws nz.co.gregs.dbvolution.exceptions.LoopDetectedInRecursiveSQL
	 */
	protected synchronized ResultSet getResultSetForSQL(final DBStatement statement, String sql) throws SQLException, SQLTimeoutException, LoopDetectedInRecursiveSQL {
		final Long timeoutTime = this.getTimeoutInMilliseconds();
		ScheduledFuture<?> cancelHandle = null;
		QueryCanceller canceller = null;
		if (timeoutTime > 0) {
			if (timeoutTime != null && timeoutTime > 0) {
				canceller = new QueryCanceller(statement, sql, this);
				cancelHandle = canceller.schedule(timeoutTime);
			}
		}
		final StatementDetails statementDetails = new StatementDetails(getLabel(), QueryIntention.SIMPLE_SELECT_QUERY, sql);
		statementDetails.setIgnoreExceptions(this.isQuietExceptions());
		final ResultSet queryResults = statement.executeQuery(statementDetails);

		if (cancelHandle != null) {
			cancelHandle.cancel(true);
		}
		if (canceller != null && canceller.queryWasCancelled()) {
			throw new SQLTimeoutException("Query Timed Out");
		}
		return queryResults;
	}

	private void setExpressionColumns(DBDefinition defn, ResultSet resultSet, DBQueryRow queryRow) throws SQLException {
		for (Map.Entry<Object, QueryableDatatype<?>> entry : getExpressionColumns().entrySet()) {
			final Object key = entry.getKey();
			final QueryableDatatype<?> value = entry.getValue();
			String expressionAlias = defn.formatExpressionAlias(key);
			QueryableDatatype<?> expressionQDT = value.getQueryableDatatypeForExpressionValue();
			expressionQDT.setFromResultSet(defn, resultSet, expressionAlias);
			queryRow.addExpressionColumnValue(key, expressionQDT);
		}
	}

	public synchronized void setQueryRowFromResultSet(DBDefinition defn, ResultSet resultSet, QueryDetails details, DBQueryRow queryRow, boolean isGroupedQuery) throws SQLException {
		for (DBRow tableRow : details.getAllQueryTables()) {
			DBRow newInstance = DBRow.getDBRow(tableRow.getClass());

			setFieldsFromColumns(defn, tableRow, newInstance, resultSet);
			newInstance.setReturnFieldsBasedOn(tableRow);

			newInstance.setDefined(); // Actually came from the database so it is a defined row.

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
					Map<String, DBRow> existingInstancesOfThisTableRow = details.getExistingInstances().get(tableRow.getClass());
					existingInstancesOfThisTableRow = setExistingInstancesForTable(existingInstancesOfThisTableRow, newInstance);
					DBRow existingInstance = getOrSetExistingInstanceForRow(defn, newInstance, existingInstancesOfThisTableRow);
					queryRow.put(existingInstance.getClass(), existingInstance);
				}
			}
		}
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
	 * @param defn the database definition
	 * @param oldInstance oldInstance
	 * @param newInstance newInstance
	 * @param resultSet resultSet
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	protected void setFieldsFromColumns(DBDefinition defn, DBRow oldInstance, DBRow newInstance, ResultSet resultSet) throws SQLException {
		var selectedProperties = oldInstance.getSelectedProperties();
		var newProperties = newInstance.getColumnPropertyWrappers();
		for (var newProp : newProperties) {
			QueryableDatatype<?> qdt = newProp.getQueryableDatatype();
			for (var propertyWrapper : selectedProperties) {
				if (propertyWrapper.getPropertyWrapperDefinition().equals(newProp.getPropertyWrapperDefinition())) {
					final String[] columnAliases = newProp.getColumnAlias(defn);
					if (columnAliases.length > 0) {
						String resultSetColumnName = columnAliases[0];
						//for (String resultSetColumnName : resultSetColumnNames) {

						qdt.setFromResultSet(defn, resultSet, resultSetColumnName);

						if (newInstance.isEmptyRow() && !qdt.isConsistentWithEmptyRow(defn)) {
							newInstance.setEmptyRow(false);
						}
					}
					//}
				}
			}

			// ensure field set when using type adaptors
			newProp.setQueryableDatatype(qdt);
		}
	}

	/**
	 * Creates the list of already created rows for the DBRow class supplied.
	 *
	 * @param existingInstancesOfThisTableRow existingInstancesOfThisTableRow
	 * @param newInstance newInstance
	 * @return a list of existing rows.
	 */
	protected Map<String, DBRow> setExistingInstancesForTable(Map<String, DBRow> existingInstancesOfThisTableRow, DBRow newInstance) {
		Map<String, DBRow> hashMap = existingInstancesOfThisTableRow;
		if (hashMap == null) {
			hashMap = new HashMap<>();
		}
		getExistingInstances().put(newInstance.getClass(), hashMap);
		return hashMap;
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
	 * @param defn the database definition
	 * @param newInstance newInstance
	 * @param existingInstancesOfThisTableRow existingInstancesOfThisTableRow
	 * @return the existing instance of the provided row, or the row itself if
	 * none exists.
	 */
	protected DBRow getOrSetExistingInstanceForRow(DBDefinition defn, DBRow newInstance, Map<String, DBRow> existingInstancesOfThisTableRow) {
		DBRow existingInstance = newInstance;
		String keyToSearchFor = "";
		final var primaryKeys = newInstance.getPrimaryKeyPropertyWrappers();
		for (var primaryKey : primaryKeys) {
			if (primaryKey != null) {
				final QueryableDatatype<?> qdt = primaryKey.getQueryableDatatype();
				if (qdt != null) {
					keyToSearchFor += "(" + qdt.toSQLString(defn) + ")";
				}
			}
		}
		if (!keyToSearchFor.isEmpty()) {
			existingInstance = existingInstancesOfThisTableRow.get(keyToSearchFor);
			if (existingInstance == null) {
				existingInstance = newInstance;
				existingInstancesOfThisTableRow.put(keyToSearchFor, existingInstance);
			}
		}
		return existingInstance;
	}

	protected synchronized void setCurrentPage(List<DBQueryRow> results) {
		currentPage = results;
	}

	public synchronized List<DBQueryRow> getCurrentPage() {
		return currentPage;
	}

	public synchronized void clear() {
		requiredQueryTables.clear();
		optionalQueryTables.clear();
		allQueryTables.clear();
		conditions.clear();
		extraExamples.clear();
		blankResults();
	}

	public synchronized void setTimeoutInMilliseconds(Long milliseconds) {
		this.timeoutInMilliseconds = milliseconds == null ? 0l : milliseconds;
	}

	public synchronized void setTimeoutInMilliseconds(Integer milliseconds) {
		setTimeoutInMilliseconds(milliseconds.longValue());
	}

	public synchronized void setTimeoutToDefault() {
		this.timeoutInMilliseconds = 0l;
	}

	public synchronized void setTimeoutToForever() {
		this.timeoutInMilliseconds = -1l;
	}

	/**
	 * @return the timeoutInMilliseconds
	 */
	public synchronized Long getTimeoutInMilliseconds() {
		if (timeoutInMilliseconds == null || timeoutInMilliseconds == 0) {
			return QueryCanceller.getStandardCancelOffset();
		} else {
			return timeoutInMilliseconds;
		}
	}

	@Override
	public synchronized String toSQLString(DBDatabase db) {
		prepareForQuery(db, options);
		switch (getOptions().getQueryType()) {
			case COUNT:
				return getSQLForCountInternal(this, options).get(0);
			default:
				return getSQLForQueryInternal(new QueryState(this), QueryType.SELECT, getOptions()).get(0);
		}
	}

	public void setLabel(String newLabel) {
		synchronized (this) {
			this.label = newLabel;
		}
	}

	public String getLabel() {
		return this.label;
	}

	@Override
	public void setReturnEmptyStringForNullString(boolean b) {
		getOptions().setRequireEmptyStringForNullString(b);
	}

	@Override
	public boolean getReturnEmptyStringForNullString() {
		return getOptions().getRequireEmptyStringForNullString();
	}

	public void setQuietExceptions(boolean b) {
		this.quietExceptions = b;
	}

	@Override
	public boolean isQuietExceptions() {
		return quietExceptions || databaseQuietExceptionsPreference;
	}

	@Override
	public void setDatabaseQuietExceptionsPreference(boolean b) {
		databaseQuietExceptionsPreference = b;
	}

	@Override
	public boolean getDatabaseQuietExceptionsPreference() {
		return databaseQuietExceptionsPreference;
	}

	private static class OrderByClause {

		String orderByClause = "";
		private final List<String> groupByClauses = new ArrayList<>();

		public OrderByClause() {
		}

		String getOrderByClause() {
			return orderByClause;
		}

		List<String> getGroupByClauses() {
			return groupByClauses;
		}

		void addGroupByClause(String clause) {
			groupByClauses.add(clause);
		}

		void addGroupByClauses(List<String> clauses) {
			groupByClauses.addAll(clauses);
		}

		void setOrderByClause(String clause) {
			orderByClause = clause;
		}

		void setOrderByClause(SeparatedString clause) {
			orderByClause = clause.toString();
		}
	}

}
