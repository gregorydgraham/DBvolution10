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

/**
 * Interface to indicate that this object can provide a column.
 *
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public interface ColumnProvider {

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

}
