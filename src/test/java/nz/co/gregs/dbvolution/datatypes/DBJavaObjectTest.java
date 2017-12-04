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
package nz.co.gregs.dbvolution.datatypes;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBJavaObjectTest extends AbstractTest {

	@BeforeClass
	public static void setUpClass() {
	}

	@AfterClass
	public static void tearDownClass() {
	}

	public DBJavaObjectTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testTableCreation() throws SQLException {
		final DBJavaObjectTable row = new DBJavaObjectTable();

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(row);

		database.createTable(row);

		row.colInt.setValue(3);
		row.javaInteger.setValue(3);
		row.javaString.setValue("Thisland");
		row.someRandomClass.setValue(new SomeClass(3, "Thisland"));
		DBActionList insert = database.insert(row);

		final DBTable<DBJavaObjectTable> tableQuery = database.getDBTable(new DBJavaObjectTable()).setBlankQueryAllowed(true);

		List<DBJavaObjectTable> allRows = tableQuery.getAllRows();
		Assert.assertThat(allRows.size(), is(1));
		final DBJavaObjectTable foundRow = allRows.get(0);
		Assert.assertThat(foundRow.javaInteger.getSize(), is(81));
		Assert.assertThat(foundRow.javaInteger.getValue(), is(3));
		Assert.assertThat(foundRow.javaInteger.stringValue(), is("3"));
		Assert.assertThat(foundRow.javaString.getSize(), is(15));
		Assert.assertThat(foundRow.javaString.getValue(), is("Thisland"));
		Assert.assertThat(foundRow.someRandomClass.getValue().str, is("Thisland"));
		Assert.assertThat(foundRow.someRandomClass.getValue().integer, is(3));

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(foundRow);
	}

	public static class DBJavaObjectTable extends DBRow {

		private static final long serialVersionUID = 1L;
		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		DBInteger pkcolumn = new DBInteger();

		@DBColumn
		DBInteger colInt = new DBInteger();

		@DBColumn
		DBJavaObject<Integer> javaInteger = new DBJavaObject<Integer>();

		@DBColumn
		DBJavaObject<String> javaString = new DBJavaObject<String>();

		@DBColumn
		DBJavaObject<SomeClass> someRandomClass = new DBJavaObject<SomeClass>();
	}

	public static class SomeClass implements Serializable {

		private static final long serialVersionUID = 1L;
		public String str;
		public int integer;

		public SomeClass(int integer, String str) {
			this.str = str;
			this.integer = integer;
		}
	}

}
