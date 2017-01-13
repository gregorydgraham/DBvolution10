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
package nz.co.gregs.dbvolution.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;

/**
 *
 * @author greg
 */
public class QueryOptions {

	private boolean matchAll = true;
	private int rowLimit = -1;
	private int pageIndex = 0;
	private ColumnProvider[] sortColumns = new ColumnProvider[]{};
	private boolean blankQueryAllowed = false;
	private boolean cartesianJoinAllowed = false;
	private boolean useANSISyntax = true;
	private boolean matchAnyRelationship = false;
	private boolean queryIsNativeQuery = true;

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
	public void setRowLimit(int rowLimit) {
		this.rowLimit = rowLimit;
	}

	/**
	 * @return the sortColumns
	 */
	public ColumnProvider[] getSortColumns() {
		return sortColumns;
	}

	/**
	 * @param sortColumns the sortColumns to set
	 */
	public void setSortColumns(ColumnProvider[] sortColumns) {
		this.sortColumns = sortColumns;
	}

	/**
	 * @return the blankQueryAllowed
	 */
	public boolean isBlankQueryAllowed() {
		return blankQueryAllowed;
	}

	/**
	 * @param blankQueryAllowed the blankQueryAllowed to set
	 */
	public void setBlankQueryAllowed(boolean blankQueryAllowed) {
		this.blankQueryAllowed = blankQueryAllowed;
	}

	/**
	 * @return the useANSISyntax
	 */
	public boolean isUseANSISyntax() {
		return useANSISyntax;
	}

	/**
	 * @param useANSISyntax the useANSISyntax to set
	 */
	public void setUseANSISyntax(boolean useANSISyntax) {
		this.useANSISyntax = useANSISyntax;
	}

	/**
	 * @return the cartesianJoinAllowed
	 */
	public boolean isCartesianJoinAllowed() {
		return cartesianJoinAllowed;
	}

	/**
	 * @param cartesianJoinAllowed the cartesianJoinAllowed to set
	 */
	public void setCartesianJoinAllowed(boolean cartesianJoinAllowed) {
		this.cartesianJoinAllowed = cartesianJoinAllowed;
	}

	/**
	 * Defines which page of results the query is to retrieve.
	 * 
	 * <p>
	 * {@link #getRowLimit() } defines the size of a page, and this method return which page is to be retrieved.
	 * 
	 * <p>
	 * Be default the page index is zero.
	 * 
	 * <p>
	 * The first item on the page will be (pageindex*rowlimit) , and the first item on the next page will be ((pageindex+1)*rowlimit). 
	 * 
	 * 
	 * @return the pageIndex
	 */
	public int getPageIndex() {
		return pageIndex;
	}

	/**
	 * @param pageIndex the pageIndex to set
	 */
	public void setPageIndex(int pageIndex) {
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
	 * @return the matchAllRelationship
	 */
	public boolean isMatchAllRelationships() {
		return !matchAnyRelationship;
	}

	/**
	 * Clones this QueryOptions
	 *
	 * @return very similar QueryOptions
	 */
	public QueryOptions copy() {
		return this.clone();
	}

	@Override
	protected QueryOptions clone() {
		QueryOptions opts = new QueryOptions();
		opts.matchAll = this.matchAll;
		opts.rowLimit = this.rowLimit;
		opts.sortColumns = Arrays.asList(this.sortColumns).toArray(sortColumns);
		opts.pageIndex = this.pageIndex;
		opts.blankQueryAllowed = this.blankQueryAllowed;
		opts.cartesianJoinAllowed = this.cartesianJoinAllowed;
		opts.useANSISyntax = this.useANSISyntax;
		opts.matchAnyRelationship = this.matchAnyRelationship;
		return opts;
	}


	/**
	 * Used while simulating OUTER JOIN to indicate that the simulation is occurring.
	 *
	 * @return TRUE if the query is native, FALSE otherwise
	 */
	public boolean creatingNativeQuery() {
		return queryIsNativeQuery;
	}

	/**
	 * Used while simulating OUTER JOIN to indicate that the simulation is occurring.
	 *
	 * @param creatingNativeQuery
	 */
	public void setCreatingNativeQuery(boolean creatingNativeQuery) {
		queryIsNativeQuery = creatingNativeQuery;
	}

}
