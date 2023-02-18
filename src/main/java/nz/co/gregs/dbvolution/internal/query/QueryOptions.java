/*
 * Copyright 2014 greg.
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
import java.util.Arrays;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.expressions.SortProvider;

/**
 *
 * @author Greogry Graham
 */
public class QueryOptions implements Serializable {

	private static final long serialVersionUID = 1l;

	private boolean matchAll = true;
	private int rowLimit = -1;
	private int pageIndex = 0;
	private SortProvider[] sortColumns = new SortProvider[]{};
	private boolean blankQueryAllowed = false;
	private boolean cartesianJoinAllowed = false;
	private boolean useANSISyntax = true;
	private boolean matchAnyRelationship = false;
	private boolean queryIsNativeQuery = true;
	private QueryType queryType = QueryType.SELECT;
	private boolean printSQLBeforeExecution = false;
	private boolean requireEmptyStringForNullString = false;
	private boolean useStarInsteadOfColumns = false;

	private String rawSQL = null;

	private final int DEFAULT_TIMEOUT_IN_MILLISECONDS = 10000;
	private int timeoutInMilliseconds = DEFAULT_TIMEOUT_IN_MILLISECONDS;
	private String label = "UNLABELLED QUERY";
	private DBDefinition workingDefinition;

	public QueryOptions() {
		super();
	}

	public QueryOptions(QueryOptions opts) {
		super();
		matchAll = opts.matchAll;
		rowLimit = opts.rowLimit;
		pageIndex = opts.pageIndex;
		sortColumns = new SortProvider[opts.sortColumns.length];
		System.arraycopy(opts.sortColumns, 0, sortColumns, 0, opts.sortColumns.length);
		blankQueryAllowed = opts.blankQueryAllowed;
		cartesianJoinAllowed = opts.cartesianJoinAllowed;
		useANSISyntax = opts.useANSISyntax;
		matchAnyRelationship = opts.matchAnyRelationship;
		queryIsNativeQuery = opts.queryIsNativeQuery;
		queryType = opts.queryType;
		printSQLBeforeExecution = opts.printSQLBeforeExecution;
		requireEmptyStringForNullString = opts.requireEmptyStringForNullString;
		useStarInsteadOfColumns = opts.useStarInsteadOfColumns;
		rawSQL = opts.rawSQL;
		timeoutInMilliseconds = opts.timeoutInMilliseconds;
		label = opts.label;
		workingDefinition = opts.workingDefinition;
	}

	/**
	 * Indicates whether this query will use AND rather than OR to add the
	 * conditions.
	 *
	 * @return TRUE if criteria should be collected using AND
	 */
	public boolean isMatchAllConditions() {
		return matchAll;
	}

	/**
	 * Indicates whether this query will use OR rather than AND to add the
	 * conditions.
	 *
	 * @return TRUE if criteria should be collected using OR
	 */
	public boolean isMatchAny() {
		return !matchAll;
	}

	/**
	 * Changes the DBQuery to using all ANDs to connect the criteria
	 *
	 */
	public void setMatchAllConditions() {
		this.matchAll = true;
	}

	/**
	 * Changes the DBQuery to using all ORs to connect the criteria
	 *
	 */
	public void setMatchAnyConditions() {
		this.matchAll = false;
	}

	/**
	 * Returns the current row limit.
	 *
	 * <p>
	 * The value will be -1 if no row limit is set.
	 *
	 * @return the rowLimit
	 */
	public int getRowLimit() {
		return rowLimit;
	}

	/**
	 * @param rowLimit the rowLimit to set
	 */
	public final void setRowLimit(int rowLimit) {
		this.rowLimit = rowLimit;
	}

	/**
	 *
	 * @return the sortColumns
	 */
	public SortProvider[] getSortColumns() {
		return Arrays.copyOf(sortColumns, sortColumns.length);
	}

	/**
	 * @param sortColumns the sortColumns to set
	 */
	public final void setSortColumns(SortProvider[] sortColumns) {
		this.sortColumns = Arrays.copyOf(sortColumns, sortColumns.length);
	}

	/**
	 *
	 * @return the blankQueryAllowed
	 */
	public boolean isBlankQueryAllowed() {
		return blankQueryAllowed;
	}

	/**
	 * @param blankQueryAllowed the blankQueryAllowed to set
	 */
	public final void setBlankQueryAllowed(boolean blankQueryAllowed) {
		this.blankQueryAllowed = blankQueryAllowed;
	}

	/**
	 *
	 * @return the useANSISyntax
	 */
	public boolean isUseANSISyntax() {
		return useANSISyntax;
	}

	/**
	 * @param useANSISyntax the useANSISyntax to set
	 */
	public final void setUseANSISyntax(boolean useANSISyntax) {
		this.useANSISyntax = useANSISyntax;
	}

	/**
	 *
	 * @return the cartesianJoinAllowed
	 */
	public boolean isCartesianJoinAllowed() {
		return cartesianJoinAllowed;
	}

	/**
	 * @param cartesianJoinAllowed the cartesianJoinAllowed to set
	 */
	public final void setCartesianJoinAllowed(boolean cartesianJoinAllowed) {
		this.cartesianJoinAllowed = cartesianJoinAllowed;
	}

	/**
	 * Defines which page of results the query is to retrieve.
	 *
	 * <p>
	 * {@link #getRowLimit() } defines the size of a page, and this method return
	 * which page is to be retrieved.
	 *
	 * <p>
	 * Be default the page index is zero.
	 *
	 * <p>
	 * The first item on the page will be (pageindex*rowlimit) , and the first
	 * item on the next page will be ((pageindex+1)*rowlimit).
	 *
	 * @return the pageIndex
	 */
	public int getPageIndex() {
		return pageIndex;
	}

	/**
	 * @param pageIndex the pageIndex to set
	 */
	public final void setPageIndex(int pageIndex) {
		this.pageIndex = pageIndex;
	}

	/**
	 * Controls how relationships, that is Foreign Keys, are compared.
	 *
	 * <p>
	 * If there are multiple FKs between 2 DBRows setMatchAnyRelationship() will
	 * switch the query to connecting the 2 rows if ANY of the FKs match, rather
	 * the normal case of ALL.
	 *
	 */
	public void setMatchAnyRelationship() {
		matchAnyRelationship = true;
	}

	/**
	 * Controls how relationships, that is Foreign Keys, are compared.
	 *
	 * <p>
	 * If there are multiple FKs between 2 DBRows setMatchAllRelationship() will
	 * switch the query to connecting the 2 rows if ALL of the FKs match.
	 *
	 * <p>
	 * This is the default option.
	 *
	 */
	public void setMatchAllRelationships() {
		matchAnyRelationship = false;
	}

	/**
	 *
	 * @return the matchAllRelationship
	 */
	public boolean isMatchAllRelationships() {
		return !matchAnyRelationship;
	}
	
	/**
	 * Used while simulating OUTER JOIN to indicate that the simulation is
	 * occurring.
	 *
	 * @return TRUE if the query is native, FALSE otherwise
	 */
	public boolean isCreatingNativeQuery() {
		return queryIsNativeQuery;
	}

	/**
	 * Used while simulating OUTER JOIN to indicate that the simulation is
	 * occurring.
	 *
	 * @param creatingNativeQuery the setting required
	 */
	public final void setCreatingNativeQuery(boolean creatingNativeQuery) {
		queryIsNativeQuery = creatingNativeQuery;
	}

	public synchronized QueryType getQueryType() {
		return queryType;
	}

	public synchronized final void setQueryType(QueryType queryType) {
		this.queryType = queryType;
	}

	private DBDatabase queryDatabase;

	public final void setQueryDatabase(DBDatabase db) {
		queryDatabase = db;
		workingDefinition = null;
		getQueryDefinition();
	}

	public DBDatabase getQueryDatabase() {
		return queryDatabase;
	}

	public DBDefinition getQueryDefinition() {
		if (workingDefinition == null) {
			workingDefinition = getQueryDatabase().getDefinition();
			if (getRequireEmptyStringForNullString()) {
				workingDefinition = workingDefinition.getOracleCompatibleVersion();
			}
		}
		return workingDefinition;
	}

	public void setPrintSQLBeforeExecution(boolean b) {
		printSQLBeforeExecution = b;
	}

	public boolean getPrintSQLBeforeExecution() {
		return printSQLBeforeExecution;
	}

	/**
	 * @return the requireEmptyStringForNullString
	 */
	public boolean getRequireEmptyStringForNullString() {
		return requireEmptyStringForNullString;
	}

	/**
	 * @param requireEmptyStringForNullString the requireEmptyStringForNullString
	 * to set
	 */
	public final void setRequireEmptyStringForNullString(boolean requireEmptyStringForNullString) {
		this.requireEmptyStringForNullString = requireEmptyStringForNullString;
	}

	public synchronized void setRawSQL(String rawQuery) {
		this.rawSQL = rawQuery;
	}

	public synchronized String getRawSQL() {
		return rawSQL;
	}

	public void setTimeoutInMilliseconds(int milliseconds) {
		this.timeoutInMilliseconds = milliseconds;
	}

	public void clearTimeout() {
		this.timeoutInMilliseconds = DEFAULT_TIMEOUT_IN_MILLISECONDS;
	}

	public void setTimeoutToForever() {
		this.timeoutInMilliseconds = -1;
	}

	public void setQueryLabel(String queryLabel) {
		this.label = queryLabel;
	}

	public String getQueryLabel() {
		return this.label;
	}

	public int getTimeoutInMilliseconds() {
		return this.timeoutInMilliseconds;
	}

	public boolean isUseStarInsteadOfColumns() {
		return useStarInsteadOfColumns;
	}

	public final void setUseStarInsteadOfColumns(boolean useStarInsteadOfColumns) {
		this.useStarInsteadOfColumns = useStarInsteadOfColumns;
	}

	public QueryOptions copy() {
		QueryOptions queryOptions = new QueryOptions(this);
		return queryOptions;
	}
}
