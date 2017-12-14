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
package nz.co.gregs.dbvolution.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class QueryDetails {

	private final List<DBRow> allQueryTables = new ArrayList<>();
	private final List<DBRow> requiredQueryTables = new ArrayList<>();
	private final List<DBRow> optionalQueryTables = new ArrayList<>();
	private final List<DBRow> assumedQueryTables = new ArrayList<>();
//	private final List<DBQuery> intersectingQueries;
	private QueryOptions options = new QueryOptions();
	private final List<DBRow> extraExamples = new ArrayList<>();
	private final List<BooleanExpression> conditions = new ArrayList<>();
	private final Map<Object, QueryableDatatype<?>> expressionColumns = new LinkedHashMap<>();
	private final Map<Object, DBExpression> dbReportGroupByColumns = new LinkedHashMap<>();
	private final Map<Class<?>, Map<String, DBRow>> existingInstances = new HashMap<>();
	private boolean groupByRequiredByAggregator = false;
	private DBDefinition databaseDefinition = null;
	private String selectClause = null;
	private final ArrayList<BooleanExpression> havingColumns = new ArrayList<>();
	private String rawSQLClause = "";
	private List<DBQueryRow> results;
	private String resultSQL;
	private Integer resultsPageIndex = 0;
	private Integer resultsRowLimit = -1;
	private Long queryCount = null;
	

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the allQueryTables
	 */
	public List<DBRow> getAllQueryTables() {
		return allQueryTables;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the requiredQueryTables
	 */
	public List<DBRow> getRequiredQueryTables() {
		return requiredQueryTables;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the optionalQueryTables
	 */
	public List<DBRow> getOptionalQueryTables() {
		return optionalQueryTables;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the assumedQueryTables
	 */
	public List<DBRow> getAssumedQueryTables() {
		return assumedQueryTables;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the options
	 */
	public QueryOptions getOptions() {
		return options;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the extraExamples
	 */
	public List<DBRow> getExtraExamples() {
		return extraExamples;
	}

	/**
	 * Get all conditions involved in this query.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return all conditions in the query
	 */
	public List<BooleanExpression> getAllConditions() {
		List<BooleanExpression> allConditions = new ArrayList<>();
		for (DBRow entry : allQueryTables) {
			allConditions.addAll(entry.getWhereClauseExpressions(databaseDefinition, true));
		}
		return allConditions;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the conditions
	 */
	public List<BooleanExpression> getConditions() {
		return conditions;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the expressionColumns
	 */
	public Map<Object, QueryableDatatype<?>> getExpressionColumns() {
		return expressionColumns;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the dbReportGroupByColumns
	 */
	public Map<Object, DBExpression> getDBReportGroupByColumns() {
		return dbReportGroupByColumns;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the existingInstances
	 */
	public Map<Class<?>, Map<String, DBRow>> getExistingInstances() {
		return existingInstances;
	}

	/**
	 * Set the requirement for a GROUP BY clause.
	 *
	 * @param b
	 */
	public void setGroupByRequiredByAggregator(boolean b) {
		this.groupByRequiredByAggregator = true;
	}

	private boolean getGroupByRequiredByAggregator() {
		return this.groupByRequiredByAggregator;
	}

	/**
	 * Return the requirement for a GROUP BY clause.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if the GROUP BY clause is required, otherwise FALSE.
	 */
	public boolean isGroupedQuery() {
		return getDBReportGroupByColumns().size() > 0 || getGroupByRequiredByAggregator();
	}

	/**
	 * Define the SELECT clause used during the query.
	 *
	 * @param selectClause
	 */
	public void setSelectClause(String selectClause) {
		this.selectClause = selectClause;
	}

	/**
	 * Get the SELECT clause used during the query.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the SELECT clause defined earlier
	 */
	public String getSelectClause() {
		return selectClause;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the havingColumns
	 */
	public BooleanExpression[] getHavingColumns() {
		return havingColumns.toArray(new BooleanExpression[]{});
	}

	/**
	 * @param havingColumns the havingColumns to set
	 */
	public void setHavingColumns(BooleanExpression... havingColumns) {
		Collections.addAll(this.havingColumns, havingColumns);
	}

	public void setDatabase(DBDefinition database) {
		this.databaseDefinition = database;
	}

	public void setQueryType(QueryType queryType) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	public void setOptions(QueryOptions tempOptions) {
		this.options = tempOptions;
	}

	/**
	 * @return the rawSQLClause
	 */
	public String getRawSQLClause() {
		return rawSQLClause;
	}

	/**
	 * @param rawSQLClause the rawSQLClause to set
	 */
	public void setRawSQLClause(String rawSQLClause) {
		this.rawSQLClause = rawSQLClause;
	}

	/**
	 * @return the results
	 */
	public List<DBQueryRow> getResults() {
		return results;
	}

	/**
	 * @param results the results to set
	 */
	public void setResults(List<DBQueryRow> results) {
		this.results = results;
	}

	/**
	 * @return the resultSQL
	 */
	public String getResultSQL() {
		return resultSQL;
	}

	/**
	 * @param resultSQL the resultSQL to set
	 */
	public void setResultSQL(String resultSQL) {
		this.resultSQL = resultSQL;
	}

	/**
	 * @return the resultsPageIndex
	 */
	public Integer getResultsPageIndex() {
		return resultsPageIndex;
	}

	/**
	 * @param resultsPageIndex the resultsPageIndex to set
	 */
	public void setResultsPageIndex(Integer resultsPageIndex) {
		this.resultsPageIndex = resultsPageIndex;
	}

	/**
	 * @return the resultsRowLimit
	 */
	public Integer getResultsRowLimit() {
		return resultsRowLimit;
	}

	/**
	 * @param resultsRowLimit the resultsRowLimit to set
	 */
	public void setResultsRowLimit(Integer resultsRowLimit) {
		this.resultsRowLimit = resultsRowLimit;
	}

	public void clearResults() {
		setResults(new ArrayList<DBQueryRow>());
		setResultsRowLimit(options.getRowLimit());
		setResultsPageIndex(options.getPageIndex());
		setResultSQL(null);
	}

	public void setCount(Long result) {
		queryCount = result;
	}
	public Long getCount() {
		return queryCount;
	}
}
