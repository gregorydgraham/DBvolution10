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
import java.time.LocalDateTime;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregory.graham
 */
public class DBLocalDateTimeTest extends AbstractTest {

	public DBLocalDateTimeTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	/**
	 * Test of getSQLDatatype method, of class DBLocalDate.
	 *
	 * @throws java.sql.SQLException
	 */
	@Test
	@SuppressWarnings("deprecation")
	public void testGetSQLDatatype() throws SQLException {
database.setPrintSQLBeforeExecuting(true);

		DBLocalDateTimeTable dateOnlyTest = new DBLocalDateTimeTable();
		final LocalDateTime then = LocalDateTime.now();
		dateOnlyTest.dateOnly.setValue(then);

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(dateOnlyTest);
		database.createTable(dateOnlyTest);
		database.insert(dateOnlyTest);
		List<DBLocalDateTimeTable> allRows = database.getDBTable(new DBLocalDateTimeTable()).setBlankQueryAllowed(true).getAllRows();
		
database.setPrintSQLBeforeExecuting(false);
		Assert.assertThat(allRows.size(), is(1));

		Assert.assertThat(then.plusMonths(1).isAfter(allRows.get(0).dateOnly.getValue()), is(true));
		Assert.assertThat(then.plusDays(1).isAfter(allRows.get(0).dateOnly.getValue()), is(true));
		Assert.assertThat(then.plusHours(1).isAfter(allRows.get(0).dateOnly.getValue()), is(true));
		Assert.assertThat(then.plusMinutes(1).isAfter(allRows.get(0).dateOnly.getValue()), is(true));
		Assert.assertThat(then.plusSeconds(1).isAfter(allRows.get(0).dateOnly.getValue()), is(true));
		
		Assert.assertThat(then.minusMonths(1).isBefore((allRows.get(0).dateOnly.getValue())), is(true));
		Assert.assertThat(then.minusDays(1).isBefore((allRows.get(0).dateOnly.getValue())), is(true));
		Assert.assertThat(then.minusHours(1).isBefore((allRows.get(0).dateOnly.getValue())), is(true));
		Assert.assertThat(then.minusMinutes(1).isBefore((allRows.get(0).dateOnly.getValue())), is(true));
		Assert.assertThat(then.minusSeconds(1).isBefore((allRows.get(0).dateOnly.getValue())), is(true));
	}

	public static class DBLocalDateTimeTable extends DBRow {

		private static final long serialVersionUID = 1L;
		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		DBInteger ddateonlypk = new DBInteger();

		@DBColumn
		DBLocalDateTime dateOnly = new DBLocalDateTime();
	}

}
