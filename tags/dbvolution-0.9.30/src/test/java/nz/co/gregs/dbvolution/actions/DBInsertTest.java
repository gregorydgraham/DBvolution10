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
package nz.co.gregs.dbvolution.actions;

import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.*;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.exceptions.AutoIncrementFieldClassAndDatatypeMismatch;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Test;
import org.junit.Assert;

/**
 *
 * @author gregorygraham
 */
public class DBInsertTest extends AbstractTest {

	public DBInsertTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	/**
	 * Test of save method, of class DBInsert.
	 *
	 * @throws java.lang.Exception
	 */
	@Test
	public void testSave() throws Exception {
		DBRow row = new CarCompany("TATA", 123);
		DBActionList result = DBInsert.save(database, row);
		Assert.assertThat(result.size(), is(1));
		DBAction act = result.get(0);
		if (act instanceof DBInsert) {
			DBInsert insert = (DBInsert) act;
			final List<Long> generatedKeys = insert.getGeneratedPrimaryKeys();
			if (generatedKeys.size() > 0) {
				System.out.println("Primary Key Returned: " + generatedKeys.get(0));
			}
		}
	}

	@Test
	public void testSaveWithDefaultValues() throws Exception {
		TestDefaultValueRetrieval row = new TestDefaultValueRetrieval();
		TestDefaultValueRetrieval row2 = new TestDefaultValueRetrieval();

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(row);
		database.preventDroppingOfTables(true);

		database.createTable(row);

		row.name.setValue("First Row");
		row2.name.setValue("Second Row");
		database.insert(row);
		Assert.assertThat(row.pk_uid.getValue(), is(1L));
		database.insert(row2);
		Assert.assertThat(row2.pk_uid.getValue(), is(2L));

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(row);
		database.preventDroppingOfTables(true);
	}

	@Test(expected = AutoIncrementFieldClassAndDatatypeMismatch.class)
	public void testSaveWithDefaultValuesAndIncorrectDatatype() throws Exception {
		TestDefaultValueIncorrectDatatype row = new TestDefaultValueIncorrectDatatype();

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(row);
		database.preventDroppingOfTables(true);

		database.createTable(row);

		try {
			row.name.setValue("First Row");
			database.insert(row);
		} finally {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(row);
			database.preventDroppingOfTables(true);
		}
	}

	public static class TestDefaultValueRetrieval extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBPrimaryKey
		@DBColumn
		@DBAutoIncrement
		public DBInteger pk_uid = new DBInteger();

		@DBColumn
		public DBString name = new DBString();

	}

	public static class TestDefaultValueIncorrectDatatype extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBPrimaryKey
		@DBColumn(value = "pkuid", comments = "thi is the pk")
		@DBAutoIncrement
		public DBString pk_uid = new DBString();

		@DBColumn
		public DBString name = new DBString();

	}
}
