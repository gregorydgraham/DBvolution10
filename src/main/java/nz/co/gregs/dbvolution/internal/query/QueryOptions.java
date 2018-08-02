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
import nz.co.gregs.dbvolution.expressions.SortProvider;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author greg
 */
public class QueryOptions  implements Serializable{
	
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

	public QueryOptions() {
		super();
	}

	public QueryOptions(QueryOptions opts) {
		super();
		setBlankQueryAllowed(opts.isBlankQueryAllowed());
		setCartesianJoinAllowed(opts.isCartesianJoinAllowed());
		setCreatingNativeQuery(opts.isCreatingNativeQuery());
		setMatchAllConditions(opts.isMatchAllConditions());
		setMatchAllRelationships(opts.matchAnyRelationship);
		setPageIndex(opts.getPageIndex());
		setQueryDatabase(opts.getQueryDatabase());
		setQueryType(opts.getQueryType());
		setRowLimit(opts.getRowLimit());
		setSortColumns(opts.getSortColumns());
		setUseANSISyntax(opts.isUseANSISyntax());
	}

	/**
	 * Indicates whether this query will use AND rather than OR to add the
	 * conditions.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * {@link #getRowLimit() } defines the size of a page, and this method
	 * return which page is to be retrieved.
	 *
	 * <p>
	 * Be default the page index is zero.
	 *
	 * <p>
	 * The first item on the page will be (pageindex*rowlimit) , and the first
	 * item on the next page will be ((pageindex+1)*rowlimit).
	 *
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the matchAllRelationship
	 */
	public boolean isMatchAllRelationships() {
		return !matchAnyRelationship;
	}

//	/**
//	 * Clones this QueryOptions
//	 *
//	 * <p style="color: #F90;">Support DBvolution at
//	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
//	 *
//	 * @return very similar QueryOptions
//	 */
//	public QueryOptions copy() {
//		try {
//			return this.clone();
//		} catch (CloneNotSupportedException ex) {
//			Logger.getLogger(QueryOptions.class.getName()).log(Level.SEVERE, null, ex);
//		}
//		return new QueryOptions();
//	}
//
//	@Override
//	protected QueryOptions clone() throws CloneNotSupportedException {
//		super.clone();
//		QueryOptions opts = new QueryOptions();
//		opts.matchAll = this.matchAll;
//		opts.rowLimit = this.rowLimit;
//		opts.sortColumns = Arrays.asList(this.sortColumns).toArray(sortColumns);
//		opts.pageIndex = this.pageIndex;
//		opts.blankQueryAllowed = this.blankQueryAllowed;
//		opts.cartesianJoinAllowed = this.cartesianJoinAllowed;
//		opts.useANSISyntax = this.useANSISyntax;
//		opts.matchAnyRelationship = this.matchAnyRelationship;
//		return opts;
//	}
	/**
	 * Used while simulating OUTER JOIN to indicate that the simulation is
	 * occurring.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * @param creatingNativeQuery
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
	}

	public DBDatabase getQueryDatabase() {
		return queryDatabase;
	}

	private void setMatchAllConditions(boolean matchAllConditions) {
		this.matchAll = matchAllConditions;
	}

	private void setMatchAllRelationships(boolean matchAnyRelationship) {
		this.matchAnyRelationship = matchAnyRelationship;
	}
}
