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
package nz.co.gregs.dbvolution.actions;

import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.*;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Gregory Graham
 */
public class DBDeleteTest extends AbstractTest {

	public DBDeleteTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testDeleteWithNoValuesThrowsBlankQueryException() throws Exception {
		TestDeleteThrowsExceptionOnBlankRow row = new TestDeleteThrowsExceptionOnBlankRow();
		TestDeleteThrowsExceptionOnBlankRow row2 = new TestDeleteThrowsExceptionOnBlankRow();

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(row);

		database.createTable(row);

		row.pk_uid.setValue(1);
		row.name.setValue("First Row");
		database.insert(row);
		assertThat(row.pk_uid.getValue(), is(1L));

		row2.pk_uid.setValue(2);
		row2.name.setValue("Second Row");
		database.insert(row2);
		assertThat(row2.pk_uid.getValue(), is(2L));

		final Long pkValue = row2.pk_uid.getValue();
		TestDeleteThrowsExceptionOnBlankRow gotRow2 = database.getDBTable(row2).getRowsByPrimaryKey(pkValue).get(0);
		assertThat(gotRow2.pk_uid.getValue(), is(2L));

		assertThat(database.getCount(new TestDeleteThrowsExceptionOnBlankRow()), is(2l));
		try {
			database.delete(new TestDeleteThrowsExceptionOnBlankRow());
			Assert.fail("Should have thrown an AccidentalBlankQueryException");
		} catch (AccidentalBlankQueryException abq) {
			// successfully threw an exception
		} catch (Exception exp) {
			Assert.fail("Should have thrown an AccidentalBlankQueryException");
		} finally {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(row);
		}

	}

	@Test
	public void testDeleteAllWithNoValues() throws Exception {
		TestDeleteAll row = new TestDeleteAll();
		TestDeleteAll row2 = new TestDeleteAll();

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(row);

		database.createTable(row);

		row.name.setValue("First Row");
		row2.name.setValue("Second Row");
		database.insert(row);
		assertThat(row.pk_uid.getValue(), is(1L));
		database.insert(row2);
		assertThat(row2.pk_uid.getValue(), is(2L));
		final Long pkValue = row2.pk_uid.getValue();
		TestDeleteAll gotRow2 = database.getDBTable(row2).getRowsByPrimaryKey(pkValue).get(0);
		assertThat(gotRow2.pk_uid.getValue(), is(2L));

		assertThat(database.getCount(new TestDeleteAll()), is(2l));
		database.setPreventAccidentalDeletingAllRowsFromTable(false);
		database.deleteAllRowsFromTable(new TestDeleteAll());
		assertThat(database.getCount(new TestDeleteAll()), is(0l));

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(row);
	}

	public static class TestDeleteThrowsExceptionOnBlankRow extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBPrimaryKey
		@DBColumn
		public DBInteger pk_uid = new DBInteger();

		@DBColumn
		public DBString name = new DBString();

	}

	public static class TestDeleteAll extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBPrimaryKey
		@DBColumn
		@DBAutoIncrement
		public DBInteger pk_uid = new DBInteger();

		@DBColumn
		public DBString name = new DBString();

	}
}
