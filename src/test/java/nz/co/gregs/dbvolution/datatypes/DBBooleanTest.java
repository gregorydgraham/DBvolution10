/*
 * Copyright 2015 gregorygraham.
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

import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.Matchers.is;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class DBBooleanTest extends AbstractTest {

	public DBBooleanTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	/**
	 * Test of getSQLDatatype method, of class DBDateOnly.
	 *
	 * @throws java.sql.SQLException
	 */
	@Test
	public void testGetValue() throws SQLException {
		BooleanTest boolTest = new BooleanTest();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(boolTest);
		database.createTable(boolTest);

		boolTest.boolColumn.setValue(true);
		database.insert(boolTest);
		List<BooleanTest> allRows = database.getDBTable(new BooleanTest()).setBlankQueryAllowed(true).getAllRows();
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).boolColumn.getValue(), is(true));

		boolTest = new BooleanTest();
		boolTest.boolColumn.setValue(false);
		database.insert(boolTest);
		allRows = database.getDBTable(new BooleanTest()).setBlankQueryAllowed(true).getAllRows();
		Assert.assertThat(allRows.size(), is(2));

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(boolTest);
	}

	@Test
	public void testPermittedAndExcludedValues() throws SQLException {
		BooleanTest boolTest = new BooleanTest();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(boolTest);
		database.createTable(boolTest);

		boolTest.boolColumn.setValue(true);
		database.insert(boolTest);
		boolTest = new BooleanTest();
		boolTest.boolColumn.setValue(false);
		database.insert(boolTest);

		boolTest = new BooleanTest();
		boolTest.boolColumn.excludedValues(true);
		List<BooleanTest> allRows = database.getDBTable(boolTest).getAllRows();
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).boolColumn.booleanValue(), is(false));

		boolTest = new BooleanTest();
		boolTest.boolColumn.permittedValues(true);
		allRows = database.getDBTable(boolTest).getAllRows();
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).boolColumn.booleanValue(), is(true));

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(boolTest);
	}

	public static class BooleanTest extends DBRow {

		private static final long serialVersionUID = 1L;
		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		DBInteger ddateonlypk = new DBInteger();

		@DBColumn
		DBBoolean boolColumn = new DBBoolean();
	}

}
