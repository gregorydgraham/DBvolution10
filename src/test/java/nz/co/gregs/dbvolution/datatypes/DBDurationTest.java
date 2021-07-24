/*
 * Copyright 2015 gregory.graham.
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
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Test;
import org.junit.Assert;
import static org.hamcrest.Matchers.*;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBDurationTest extends AbstractTest {
	
	public static LocalDateTime march23rd2013LocalDateTime = LocalDateTime.of(2013, 2, 23, 12, 34, 56);
	public static LocalDateTime april2nd2011LocalDateTime = LocalDateTime.of(2011, 3, 2, 1, 2, 3);

	public DBDurationTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void basicTest() throws SQLException {
		final DurationTable durationTable = new DurationTable();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(durationTable);
		database.createTable(durationTable);
		final Duration testPeriod = Duration.ofSeconds(2).plusMinutes(3).plusHours(4).plusDays(5);
		durationTable.durationCol.setValue(testPeriod);
		database.insert(durationTable);
		DBTable<DurationTable> tab = database.getDBTable(new DurationTable()).setBlankQueryAllowed(true);
		List<DurationTable> allRows = tab.getAllRows();

		Assert.assertThat(allRows.size(), is(1));

		Assert.assertThat(allRows.get(0).durationCol.durationValue().toString(), is(testPeriod.toString()));
	}

	@Test
	public void testNegativeSeconds() throws SQLException {
		final DurationTable durationTable = new DurationTable();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(durationTable);
		database.createTable(durationTable);
		final Duration testPeriod = Duration.ofSeconds(-2);
		durationTable.durationCol.setValue(testPeriod);
		database.insert(durationTable);
		DBTable<DurationTable> tab = database.getDBTable(new DurationTable()).setBlankQueryAllowed(true);
		List<DurationTable> allRows = tab.getAllRows();

		Assert.assertThat(allRows.size(), is(1));

		Assert.assertThat(allRows.get(0).durationCol.durationValue().toString(), is(testPeriod.toString()));
	}

	@Test
	public void testNegativeDays() throws SQLException {
		final DurationTable durationTable = new DurationTable();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(durationTable);
		database.createTable(durationTable);
		final Duration testPeriod = Duration.ofDays(-2);
		durationTable.durationCol.setValue(testPeriod);
		database.insert(durationTable);
		DBTable<DurationTable> tab = database.getDBTable(new DurationTable()).setBlankQueryAllowed(true);
		List<DurationTable> allRows = tab.getAllRows();

		Assert.assertThat(allRows.size(), is(1));

		Assert.assertThat(allRows.get(0).durationCol.durationValue().toString(), is(testPeriod.toString()));
	}

	@Test
	public void testNegativeHours() throws SQLException {
		final DurationTable durationTable = new DurationTable();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(durationTable);
		database.createTable(durationTable);
		final Duration testPeriod = Duration.ofHours(-2);
		durationTable.durationCol.setValue(testPeriod);
		database.insert(durationTable);
		DBTable<DurationTable> tab = database.getDBTable(new DurationTable()).setBlankQueryAllowed(true);
		List<DurationTable> allRows = tab.getAllRows();

		Assert.assertThat(allRows.size(), is(1));

		Assert.assertThat(allRows.get(0).durationCol.durationValue().toString(), is(testPeriod.toString()));
	}

	@Test
	public void testNegativeMinutes() throws SQLException {
		final DurationTable durationTable = new DurationTable();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(durationTable);
		database.createTable(durationTable);
		final Duration testPeriod = Duration.ofMinutes(-2);
		durationTable.durationCol.setValue(testPeriod);
		database.insert(durationTable);
		DBTable<DurationTable> tab = database.getDBTable(new DurationTable()).setBlankQueryAllowed(true);
		List<DurationTable> allRows = tab.getAllRows();
		
		Assert.assertThat(allRows.size(), is(1));

		Assert.assertThat(allRows.get(0).durationCol.durationValue().toString(), is(testPeriod.toString()));
	}

	@Test
	public void testNegativeNanos() throws SQLException {
		final DurationTable durationTable = new DurationTable();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(durationTable);
		database.createTable(durationTable);
		// Postgres only supports microsecond precision so make sure it's thousands of nanos
		final Duration testPeriod = Duration.ofNanos(-2*1000);
		durationTable.durationCol.setValue(testPeriod);
		database.insert(durationTable);
		DBTable<DurationTable> tab = database.getDBTable(new DurationTable()).setBlankQueryAllowed(true);
		List<DurationTable> allRows = tab.getAllRows();

		Assert.assertThat(allRows.size(), is(1));

		Assert.assertThat(allRows.get(0).durationCol.durationValue().toString(), is(testPeriod.toString()));
	}

	public static class DurationTable extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		DBInteger pkid = new DBInteger();

		@DBColumn
		DBDuration durationCol = new DBDuration();
	}
}
