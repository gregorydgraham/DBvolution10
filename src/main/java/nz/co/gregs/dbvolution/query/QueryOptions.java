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

import nz.co.gregs.dbvolution.columns.ColumnProvider;

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
	private boolean matchAnyRelationship;

	/**
	 *
	 *
	 * @return the matchAll
	 */
	public boolean isMatchAllConditions() {
		return matchAll;
	}

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

	public void setMatchAnyRelationship() {
		matchAnyRelationship = true;
	}

	public void setMatchAllRelationships() {
		matchAnyRelationship = false;
	}

	/**
	 * @return the matchAnyRelationship
	 */
	public boolean isMatchAllRelationships() {
		return !matchAnyRelationship;
	}

}
