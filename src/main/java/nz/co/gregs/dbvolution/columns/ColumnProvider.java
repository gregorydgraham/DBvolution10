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
package nz.co.gregs.dbvolution.columns;

import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.RangeExpression;
import nz.co.gregs.dbvolution.expressions.SortProvider;

/**
 * Interface to indicate that this object can provide a column.
 *
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public interface ColumnProvider extends DBExpression {

	/**
	 * Returns the AbstractColumn from this ColumnProvider.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the column that this provider supplies.
	 */
	public AbstractColumn getColumn();

	/**
	 * Indicates whether the ColumnProvider should use the table alias during
	 * query creation.
	 *
	 * <p>
	 * the standard implementation is
	 * {@code this.column.setUseTableAlias(useTableAlias);} and passes the boolean
	 * along to the underlying AbstractColumn
	 *
	 * @param useTableAlias true or false
	 */
	public void setUseTableAlias(boolean useTableAlias);

	/**
	 * Provides the sorting for this column as defined by the methods {@link QueryableDatatype#setSortOrder(java.lang.Boolean) }, {@link QueryableDatatype#setSortOrderAscending()
	 * }, and {@link QueryableDatatype#setSortOrderDescending() } on the QDT.
	 *
	 * <p>
	 * At the time of querying the sort order defined on the original QDT field
	 * provided is add to the query, with ASCENDING being used as the default
	 * ordering.</p>
	 * <p>
	 * To be certain of the ordering use the {@link RangeExpression#ascending() }
	 * and {@link RangeExpression#descending() } methods.</p>
	 *
	 * @return a SortProvider configured to use the sorting defined on original
	 * QDT or ASCENDING.
	 */
	public SortProvider getSortProvider();

	public SortProvider ascending();

	public SortProvider descending();

}
