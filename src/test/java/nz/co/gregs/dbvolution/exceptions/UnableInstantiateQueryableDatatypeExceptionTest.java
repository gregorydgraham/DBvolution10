/*
 * Copyright 2014 gregory.graham.
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
package nz.co.gregs.dbvolution.exceptions;

import java.sql.ResultSet;
import java.sql.SQLException;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.query.RowDefinition;
import org.junit.Test;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregory.graham
 */
public class UnableInstantiateQueryableDatatypeExceptionTest {

	public UnableInstantiateQueryableDatatypeExceptionTest() {
	}

	@Test(expected = UnableInstantiateQueryableDatatypeException.class)
	public void testSomeMethod() {
		AQDT aqdt = new AQDT();
		QueryableDatatype<Object> copy = aqdt.copy();
	}

	public class AQDT extends QueryableDatatype<Object> {

		private static final long serialVersionUID = 1L;
		private final Object undefined = null;

		@Override
		public String getSQLDatatype() {
			throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
		}

		@Override
		protected String formatValueForSQLStatement(DBDefinition db) {
			throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
		}

		@Override
		protected Object getFromResultSet(DBDefinition database, ResultSet resultSet, String fullColumnName) throws SQLException {
			throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
		}

		@Override
		public boolean isAggregator() {
			throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
		}

		@Override
		protected void setValueFromStandardStringEncoding(String encodedValue) {
			throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
		}

		@Override
		public ColumnProvider getColumn(RowDefinition row) throws nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException {
			throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
		}
	}

}
