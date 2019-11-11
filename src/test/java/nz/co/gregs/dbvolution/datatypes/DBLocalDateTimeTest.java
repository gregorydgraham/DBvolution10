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
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.hamcrest.Matcher;
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
	 * @throws java.sql.SQLException database errors
	 */
	@Test
	@SuppressWarnings("deprecation")
	public void testGetSQLDatatype() throws SQLException {

		DBLocalDateTimeTable dateOnlyTest = new DBLocalDateTimeTable();
		LocalDateTime then = LocalDateTime.now();
		System.out.println("THEN: " + then);
		dateOnlyTest.dateOnly.setValue(then);

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(dateOnlyTest);
		database.createTable(dateOnlyTest);
		database.insert(dateOnlyTest);
		List<DBLocalDateTimeTable> allRows = database.getDBTable(new DBLocalDateTimeTable()).setBlankQueryAllowed(true).getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));

		// Protect against SQLite's low precision problems
		Matcher<LocalDateTime> matchExactValue = is(then);
		Matcher<LocalTime> matchExactTime = is(then.toLocalTime());
		if (!database.supportsNanosecondPrecision() || !database.supportsMicrosecondPrecision()) {
			matchExactValue = isOneOf(
					then,
					then.truncatedTo(ChronoUnit.MILLIS),
					then.truncatedTo(ChronoUnit.MICROS)
			);
			matchExactTime = isOneOf(
					then.toLocalTime(),
					then.truncatedTo(ChronoUnit.MILLIS).toLocalTime(),
					then.truncatedTo(ChronoUnit.MICROS).toLocalTime()
			);
		}

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
		Assert.assertThat(allRows.get(0).dateOnly.getValue(), matchExactValue);
		Assert.assertThat(allRows.get(0).dateOnly.localDateTimeValue(), matchExactValue);
		Assert.assertThat(allRows.get(0).dateOnly.localDateValue(), is(then.toLocalDate()));
		Assert.assertThat(allRows.get(0).dateOnly.localTimeValue(), matchExactTime);
	}

	public static class DBLocalDateTimeTable extends DBRow {

		private static final long serialVersionUID = 1L;
		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		DBInteger ddateonlypk = new DBInteger();

		@DBColumn
		DBLocalDateTime dateOnly = new DBLocalDateTime();

//		@DBColumn
//		DBInteger year = this.column(this.dateOnly).year().asExpressionColumn();
//		
//		@DBColumn
//		DBInteger month = this.column(this.dateOnly).month().asExpressionColumn();
//		
//		@DBColumn
//		DBInteger day = this.column(this.dateOnly).day().asExpressionColumn();
//		
//		@DBColumn
//		DBInteger hour = this.column(this.dateOnly).hour().asExpressionColumn();
//		
//		@DBColumn
//		DBInteger minute = this.column(this.dateOnly).minute().asExpressionColumn();
//		
//		@DBColumn
//		DBInteger second = this.column(this.dateOnly).second().asExpressionColumn();
//		
//		@DBColumn
//		DBNumber subsecond = this.column(this.dateOnly).subsecond().asExpressionColumn();
	}

}
