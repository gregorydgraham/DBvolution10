/*
 * Copyright 2014 gregorygraham.
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
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author gregorygraham
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
		database.preventDroppingOfTables(true);

		database.createTable(row);

		row.colInt.setValue(3);
		row.javaInteger.setValue(3);
		row.javaString.setValue("Thisland");
		DBActionList insert = database.insert(row);

		List<DBJavaObjectTable> allRows = database.getDBTable(new DBJavaObjectTable()).setBlankQueryAllowed(true).getAllRows();
		Assert.assertThat(allRows.size(), is(1));
		final DBJavaObjectTable foundRow = allRows.get(0);
		Assert.assertThat(foundRow.javaInteger.getValue(), is(3));
		Assert.assertThat(foundRow.javaString.getValue(), is("Thisland"));
		System.out.println(foundRow);

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(foundRow);
		database.preventDroppingOfTables(true);
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
	}

}
