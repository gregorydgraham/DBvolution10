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

import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer.DBSafeInternalQDTAdaptor;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 * This operator exists but is currently unsupported.
 *
 * @author Gregory Graham
 */
public class DBExistsOperator extends DBOperator {

	public static final long serialVersionUID = 1L;

	DBRow innerTable;
	Object innerField;

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
	 * Requires that {@literal qdtOfTheRow} is from theliteralode innerTable}
	 * instance for this to work.
	 *
	 * @param tableRow
	 * @param qdtOfTheRow
	 * @throws IncorrectRowProviderInstanceSuppliedException if the
	 * {@code qdtOfTheRow} is not from the {@code innerTable} instance
	 */
	public DBExistsOperator(DBRow tableRow, Object qdtOfTheRow) throws IncorrectRowProviderInstanceSuppliedException {
		this.innerTable = tableRow;
		innerField = qdtOfTheRow;
		if (innerField == null) {
			throw new IncorrectRowProviderInstanceSuppliedException(tableRow, qdtOfTheRow);
		}
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
				DBRow innerRow = (DBRow) outerTable;
				Object outerQDT = outerProp.getQueryableDatatype();
				return new ExistsExpression(innerRow, outerQDT, innerTable, innerField);
			}
		}
		return BooleanExpression.trueExpression();
	}

	public class ExistsExpression extends BooleanExpression {

		private DBRow outerTable = null;
		private DBRow innerTable = null;

		protected ExistsExpression() {
		}

		public ExistsExpression(DBRow outerTable, Object outerQDT, DBRow innerTable, Object innerQDT) {
			this.outerTable = outerTable;
			this.innerTable = DBRow.copyDBRow(innerTable);
		}

		@Override
		public String toSQLString(DBDatabase db) {
			DBRow outerCopy = DBRow.copyDBRow(outerTable);
			DBRow innerCopy = DBRow.copyDBRow(innerTable);
			outerCopy.removeExistsOperators();
			innerCopy.removeExistsOperators();
			outerCopy.setReturnFields(outerCopy.getPrimaryKey());
			innerCopy.setReturnFieldsToNone();
			DBQuery dbQuery = db.getDBQuery(outerCopy, innerCopy);
			String sql = dbQuery.getSQLForQuery().replaceAll(";", "");
			return " EXISTS (" + sql + ")";
		}

		@Override
		@SuppressWarnings("unchecked")
		public ExistsExpression copy() {
			ExistsExpression clone;
			try {
				clone = (ExistsExpression) this.clone();
			} catch (CloneNotSupportedException ex) {
				throw new RuntimeException(ex);
			}
			return clone;
		}

		@Override
		public boolean isAggregator() {
			return false;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			Set<DBRow> returnSet = new HashSet<DBRow>();
			returnSet.add(outerTable);
			returnSet.add(innerTable);
			return returnSet;
		}
	}
}
