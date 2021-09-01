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
package nz.co.gregs.dbvolution.datatypes;

import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Test;

/**
 *
 * @author gregory.graham
 */
public class DBLocalDateTest extends AbstractTest {

	public DBLocalDateTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	/**
	 * Test of getSQLDatatype method, of class DBLocalDate.
	 *
	 * @throws java.sql.SQLException database errors
	 */
	@Test
	public void testGetSQLDatatype() throws SQLException {		
		DBLocalDateTable dateOnlyTest = new DBLocalDateTable();
		LocalDate then = LocalDate.now();
		dateOnlyTest.dateOnly.setValue(then);
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(dateOnlyTest);
		database.createTable(dateOnlyTest);
		database.insert(dateOnlyTest);
		List<DBLocalDateTable> allRows = database.getDBTable(new DBLocalDateTable()).setBlankQueryAllowed(true).getAllRows();
		assertThat(allRows.size(), is(1));
		assertThat(allRows.get(0).dateOnly.getValue(), is(then));
		assertThat(allRows.get(0).dateOnly.localDateValue(), is(then));
		assertThat(allRows.get(0).dateOnly.getValue().compareTo(LocalDate.now()), isOneOf(-1, 0));
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(dateOnlyTest);
	}

	public static class DBLocalDateTable extends DBRow {

		private static final long serialVersionUID = 1L;
		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		DBInteger ddateonlypk = new DBInteger();

		@DBColumn
		DBLocalDate dateOnly = new DBLocalDate();
	}

}
