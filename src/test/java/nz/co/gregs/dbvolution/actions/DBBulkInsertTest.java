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

import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.*;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Test;

/**
 *
 *
 * @author Gregory Graham
 */
public class DBBulkInsertTest extends AbstractTest {
	
	public DBBulkInsertTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}
	
	@Test
	public void testSaveWithMultipleRows() throws Exception {
		DBRow row = new CarCompany("Lada", 124);
		DBRow row2 = new CarCompany("Saab", 125);

		DBActionList result = database.insert(row, row2);

		CarCompany example = new CarCompany();
		example.uidCarCompany.permittedValues(124, 125);
		final List<CarCompany> allRows = database.getDBTable(example).getAllRows();
//		database.print(allRows);
		assertThat(result.size(), is(2));
	}
	
	@Test
	public void testSaveWithAutoIncrement() throws Exception {
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new BulkInsertTestTable());
		database.createTableNoExceptions(new BulkInsertTestTable());
		final BulkInsertTestTable row = new BulkInsertTestTable("Lada");
		final BulkInsertTestTable row2 = new BulkInsertTestTable("Saab");

		DBActionList result = database.insert(row, row2);
		assertThat(result.size(), is(2));

		assertThat(row.pk.isDefined(), is(true));
		assertThat(row.pk.getValue(), is(1l));
		assertThat(row2.pk.isDefined(), is(true));
		assertThat(row2.pk.getValue(), is(2l));
	}
	
	public static class BulkInsertTestTable extends DBRow {

	private static final long serialVersionUID = 1L;
		
		@DBPrimaryKey
		@DBAutoIncrement
		@DBColumn
		DBInteger pk = new DBInteger();
		
		@DBColumn
		DBString string = new DBString();
		
		public BulkInsertTestTable() {
		}
		
		public BulkInsertTestTable(String str) {
			string.setValue(str);
		}
	}
	
}
