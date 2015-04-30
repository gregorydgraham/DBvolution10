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
package nz.co.gregs.dbvolution.operators;

import nz.co.gregs.dbvolution.expressions.ExistsExpression;
import java.util.*;
import nz.co.gregs.dbvolution.*;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer.DBSafeInternalQDTAdaptor;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
import nz.co.gregs.dbvolution.expressions.*;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 * This operator is DEPRECATED, change to {@link ExistsExpression} immediately.
 *
 * @author Gregory Graham
 */
@Deprecated
public class DBExistsOperator extends DBOperator {

	private static final long serialVersionUID = 1L;

	List<DBRow> innerTables = new ArrayList<DBRow>();

	/**
	 * Creates an exists operator on the given table row instance and column
	 * identified by the given property's object reference (field or method).
	 *
	 * <p>
	 * For example the following code snippet will create an exists operator on
	 * the uid column:
	 * <pre>
	 * Customer customer = ...;
	 * new DBExistsOperator(customer, customer.uid);
	 * </pre>
	 *
	 * <p>
	 * Requires that {@literal qdtOfTheRow} is from the innerTable} instance for
	 * this to work.
	 *
	 * @param tableRows	 tableRows	
	 * @throws IncorrectRowProviderInstanceSuppliedException if the
	 * {@code qdtOfTheRow} is not from the {@code innerTable} instance
	 */
	public DBExistsOperator(DBRow... tableRows) throws IncorrectRowProviderInstanceSuppliedException {
		this.innerTables.addAll(Arrays.asList(tableRows));
	}

	@Override
	public DBExistsOperator copyAndAdapt(DBSafeInternalQDTAdaptor typeAdaptor) {
		return this;
	}

	@Override
	public BooleanExpression generateWhereExpression(DBDatabase db, DBExpression column) {
		if (column instanceof ColumnProvider) {
			ColumnProvider columnProvider = (ColumnProvider) column;
			PropertyWrapper outerProp = columnProvider.getColumn().getPropertyWrapper();
			RowDefinition outerTable = outerProp.getRowDefinitionInstanceWrapper().adapteeRowDefinition();
			if (outerTable instanceof DBRow) {
				DBRow outerRow = (DBRow) outerTable;
				final ExistsExpression existsExpression = new ExistsExpression(outerRow, innerTables);
				if (this.invertOperator) {
					return existsExpression.not();
				} else {
					return existsExpression;
				}
			}
		}
		return BooleanExpression.trueExpression();
	}

}
