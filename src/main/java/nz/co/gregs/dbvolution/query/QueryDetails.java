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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;

/**
 *
 * @author gregorygraham
 */
public class QueryDetails {

	private final List<DBRow> allQueryTables = new ArrayList<DBRow>();
	private final List<DBRow> requiredQueryTables = new ArrayList<DBRow>();
	private final List<DBRow> optionalQueryTables = new ArrayList<DBRow>();
	private final List<DBRow> assumedQueryTables = new ArrayList<DBRow>();
//	private final List<DBQuery> intersectingQueries;
	private final QueryOptions options = new QueryOptions();
	private final List<DBRow> extraExamples = new ArrayList<DBRow>();
	private final List<BooleanExpression> conditions = new ArrayList<>();
	private final Map<Object, DBExpression> expressionColumns = new LinkedHashMap<Object, DBExpression>();
	private final Map<Object, DBExpression> dbReportGroupByColumns = new LinkedHashMap<Object, DBExpression>();
	private final Map<Class<?>, Map<String, DBRow>> existingInstances = new HashMap<Class<?>, Map<String, DBRow>>();
	private boolean groupByRequiredByAggregator = false;
	public DBDatabase database = null;
	private String selectClause;

	/**
	 * @return the allQueryTables
	 */
	public List<DBRow> getAllQueryTables() {
		return allQueryTables;
	}

	/**
	 * @return the requiredQueryTables
	 */
	public List<DBRow> getRequiredQueryTables() {
		return requiredQueryTables;
	}

	/**
	 * @return the optionalQueryTables
	 */
	public List<DBRow> getOptionalQueryTables() {
		return optionalQueryTables;
	}

	/**
	 * @return the assumedQueryTables
	 */
	public List<DBRow> getAssumedQueryTables() {
		return assumedQueryTables;
	}

	/**
	 * @return the options
	 */
	public QueryOptions getOptions() {
		return options;
	}

	/**
	 * @return the extraExamples
	 */
	public List<DBRow> getExtraExamples() {
		return extraExamples;
	}

	public List<BooleanExpression> getAllConditions() {
		List<BooleanExpression> allConditions = new ArrayList<>();
		for (DBRow entry : allQueryTables) {
			allConditions.addAll(entry.getWhereClauseExpressions(database, true));
		}
		return allConditions;
	}

	/**
	 * @return the conditions
	 */
	public List<BooleanExpression> getConditions() {
		return conditions;
	}

	/**
	 * @return the expressionColumns
	 */
	public Map<Object, DBExpression> getExpressionColumns() {
		return expressionColumns;
	}

	/**
	 * @return the dbReportGroupByColumns
	 */
	public Map<Object, DBExpression> getDBReportGroupByColumns() {
		return dbReportGroupByColumns;
	}

	/**
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
	 * @return TRUE if the GROUP BY clause is required, otherwise FALSE.
	 */
	public boolean isGroupedQuery() {
		return getDBReportGroupByColumns().size() > 0 || getGroupByRequiredByAggregator();
	}

	public void setSelectClause(String selectClause) {
		this.selectClause = selectClause;
	}

	public String getSelectClause() {
		return selectClause;
	}

}
