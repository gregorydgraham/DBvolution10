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
package nz.co.gregs.dbvolution.datatypes;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.columns.UntypedColumn;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
import nz.co.gregs.dbvolution.expressions.AnyExpression;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 *
 * Class to allow some functionality for data types that DBvolution does not yet
 * support
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBUnknownDatatype extends QueryableDatatype<Object> {

	private static final long serialVersionUID = 1L;

	/**
	 * The default constructor for a DBUnknownDatatype.
	 *
	 * <p>
	 * Literally does nothing.
	 */
	public DBUnknownDatatype() {
		super();
	}

	@Override
	public String getSQLDatatype() {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String formatValueForSQLStatement(DBDefinition db) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	void setValue(Object newLiteralValue) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public DBUnknownDatatype getQueryableDatatypeForExpressionValue() {
		return new DBUnknownDatatype();
	}

	@Override
	public boolean isAggregator() {
		return false;
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		return new HashSet<>();
	}

	@Override
	protected Object getFromResultSet(DBDefinition defn, ResultSet resultSet, String fullColumnName) throws SQLException {
		return resultSet.getString(fullColumnName);
	}

	@Override
	protected void setValueFromStandardStringEncoding(String encodedValue) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public ColumnProvider getColumn(RowDefinition row) throws IncorrectRowProviderInstanceSuppliedException {
		return new UntypedColumn(row, this);
	}
}
