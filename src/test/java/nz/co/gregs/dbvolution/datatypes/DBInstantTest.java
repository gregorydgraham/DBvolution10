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
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.hamcrest.Matcher;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Test;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregory.graham
 */
public class DBInstantTest extends AbstractTest {

	public DBInstantTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	/**
	 * Test of getSQLDatatype method, of class DBLocalDate.
	 *
	 * @throws java.sql.SQLException database errors
	 */
	@Test
	@SuppressWarnings("deprecation")
	public void testGetSQLDatatype() throws SQLException {
		DBInstantTable dateOnlyTest = new DBInstantTable();
		var then = Instant.now();
		dateOnlyTest.instantField.setValue(then);

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(dateOnlyTest);
		database.createTable(dateOnlyTest);
		database.insert(dateOnlyTest);

		// Protect against SQLite's low precision problems
		Matcher<Instant> matchExactValue = is(then);
		if (!database.supportsNanosecondPrecision() || !database.supportsMicrosecondPrecision()) {
			matchExactValue = isOneOf(
					then,
					then.truncatedTo(ChronoUnit.MILLIS),
					then.truncatedTo(ChronoUnit.MICROS));
		}

		List<DBInstantTable> allRows = database.getDBTable(new DBInstantTable())
				.setQueryLabel("CHECK INSTANT INSERT ROUND TRIP")
				.setBlankQueryAllowed(true)
				.getAllRows();

		assertThat(allRows.size(), is(1));
		final OffsetDateTime gotValue = allRows.get(0).instantField.getValue().atOffset(ZoneOffset.UTC);

		assertThat(then.atOffset(ZoneOffset.UTC).plusMonths(1).isAfter(gotValue), is(true));
		assertThat(then.atOffset(ZoneOffset.UTC).plusDays(1).isAfter(gotValue), is(true));
		assertThat(then.atOffset(ZoneOffset.UTC).plusHours(1).isAfter(gotValue), is(true));
		assertThat(then.atOffset(ZoneOffset.UTC).plusMinutes(1).isAfter(gotValue), is(true));
		assertThat(then.atOffset(ZoneOffset.UTC).plusSeconds(1).isAfter(gotValue), is(true));

		assertThat(then.atOffset(ZoneOffset.UTC).minusMonths(1).isBefore((gotValue)), is(true));
		assertThat(then.atOffset(ZoneOffset.UTC).minusDays(1).isBefore((gotValue)), is(true));
		assertThat(then.atOffset(ZoneOffset.UTC).minusHours(1).isBefore((gotValue)), is(true));
		assertThat(then.atOffset(ZoneOffset.UTC).minusMinutes(1).isBefore((gotValue)), is(true));
		assertThat(then.atOffset(ZoneOffset.UTC).minusSeconds(1).isBefore((gotValue)), is(true));
		assertThat(allRows.get(0).instantField.instantValue(), matchExactValue);
		assertThat(allRows.get(0).instantField.getValue(), matchExactValue);
	}

	public static class DBInstantTable extends DBRow {

		private static final long serialVersionUID = 1L;
		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		DBInteger ddateonlypk = new DBInteger();

		@DBColumn
		DBInstant instantField = new DBInstant();
	}

}
