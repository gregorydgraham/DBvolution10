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
package nz.co.gregs.dbvolution.expressions;

import java.sql.SQLException;
import java.text.ParseException;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBDateOnly;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

public class DateExpressionTest extends AbstractTest {

	public DateExpressionTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	public static class CurrentDateReport extends DBReport {

		private static final long serialVersionUID = 1L;
		public Marque marque = new Marque();
		@DBColumn
		public DBString name = new DBString(marque.column(marque.name));
		@DBColumn
		public DBDate savedJavaDate = new DBDate(marque.column(marque.creationDate));
		@DBColumn
		public DBDate actualJavaDate = new DBDate(new Date());
		@DBColumn
		public DBDate currentDate = new DBDate(DateExpression.currentDateOnly());
		@DBColumn
		public DBDate currentDateTime = new DBDate(DateExpression.currentDate());
		@DBColumn
		public DBDate currentDateTimeMinus10Seconds = new DBDate(DateExpression.currentDate().addSeconds(-10));
		@DBColumn
		public DBDate currentDateTimePlus10Seconds = new DBDate(DateExpression.currentDate().addSeconds(10));
	}

	@Test
	public void testOverlaps() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(DateExpression.overlaps(
				marq.column(marq.creationDate), marq.column(marq.creationDate).addDays(-5),
				DateExpression.value(march23rd2013).addWeeks(-5), DateExpression.value(march23rd2013).addDays(-2))
		);
		List<DBQueryRow> allRows = query.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(18));
	}

	@Test
	public void testCurrentDateTimeAndAddSecondsFunctions() throws SQLException {
//        database.setPrintSQLBeforeExecuting(true);
		Marque marq = new Marque();
		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate(), null);
		List<Marque> got = database.get(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(0));

		database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));

		Marque reportLimitingMarque = new Marque();
		reportLimitingMarque.name.permittedPatternIgnoreCase("% HUMMER %");
		CurrentDateReport currentDateReport = new CurrentDateReport();
		final List<CurrentDateReport> reportRows = DBReport.getRows(database, currentDateReport, reportLimitingMarque);
		database.print(reportRows);
		Assert.assertThat(reportRows.size(), is(1));

		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate().addSeconds(-10), null);
		got = database.get(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(1));

		marq.creationDate.permittedRangeInclusive(null, DateExpression.currentDate().addSeconds(-10));
		got = database.get(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(21));

		marq.creationDate.permittedRangeInclusive(
				DateExpression.currentDate().addMinutes(-5), null);
		got = database.getDBTable(marq).getAllRows();
		database.print(got);
		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testCurrentDateAndAddDaysFunctions() throws SQLException {
//        database.setPrintSQLBeforeExecuting(true);
		Marque marq = new Marque();
		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate(), null);
		List<Marque> got = database.get(marq);
//        database.print(got);
		Assert.assertThat(got.size(), is(0));

		database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));
		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate().addDays(-1), null);
		got = database.get(marq);
//        database.print(got);
		Assert.assertThat(got.size(), is(1));

		marq.creationDate.permittedRangeInclusive(null, DateExpression.currentDate().addDays(-1));
		got = database.get(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(21));

		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate().addDays(-1), DateExpression.currentDate().addDays(+1));
		got = database.get(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testCurrentDateAndAddHoursFunctions() throws SQLException {
//        database.setPrintSQLBeforeExecuting(true);
		Marque marq = new Marque();
		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate(), null);
		List<Marque> got = database.get(marq);
//        database.print(got);
		Assert.assertThat(got.size(), is(0));

		database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));
		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate().addHours(-1), null);
		got = database.get(marq);
//        database.print(got);
		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testCurrentDateAndAddWeeksFunctions() throws SQLException {
//        database.setPrintSQLBeforeExecuting(true);
		Marque marq = new Marque();
		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate(), null);
		List<Marque> got = database.get(marq);
//        database.print(got);
		Assert.assertThat(got.size(), is(0));

		database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));
		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate().addWeeks(-1), null);
		got = database.get(marq);
//        database.print(got);
		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testCurrentDateAndAddMinutesFunctions() throws SQLException {
//        database.setPrintSQLBeforeExecuting(true);
		Marque marq = new Marque();
		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate(), null);
		List<Marque> got = database.get(marq);
//        database.print(got);
		Assert.assertThat(got.size(), is(0));

		database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));
		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate().addMinutes(-1), null);
		got = database.get(marq);
//        database.print(got);
		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testCurrentDateAndAddMonthsFunctions() throws SQLException {
//        database.setPrintSQLBeforeExecuting(true);
		Marque marq = new Marque();
		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate(), null);
		List<Marque> got = database.get(marq);
//        database.print(got);
		Assert.assertThat(got.size(), is(0));

		database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));
		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate().addMonths(-1), null);
		got = database.get(marq);
//        database.print(got);
		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testCurrentDateAndAddYearsFunctions() throws SQLException {
//        database.setPrintSQLBeforeExecuting(true);
		Marque marq = new Marque();
		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate(), null);
		List<Marque> got = database.get(marq);
//        database.print(got);
		Assert.assertThat(got.size(), is(0));

		database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));
		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate().addYears(-1), null);
		got = database.get(marq);
//        database.print(got);
		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testYearFunction() throws SQLException {
//        database.setPrintSQLBeforeExecuting(true);
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).year().is(2014));
		List<Marque> got = query.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(0));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).year().is(2013));
		got = query.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(18));

	}

	@Test
	public void testMonthFunction() throws SQLException {
//        database.setPrintSQLBeforeExecuting(true);
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).month().is(3));
		List<Marque> got = query.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).month().is(4));
		got = query.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(3));

	}

	@Test
	public void testEndOfMonthFunction() throws SQLException {
//        database.setPrintSQLBeforeExecuting(true);
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).endOfMonth().day().is(31));
		List<Marque> got = query.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).endOfMonth().day().is(30));
		got = query.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).endOfMonth().day().isNull());
		got = query.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(1));

	}

	@Test
	public void testFirstOfMonthFunction() throws SQLException {
		final Date march1st2013 = (new GregorianCalendar(2013, 2, 1)).getTime();
		final Date march2nd2013 = (new GregorianCalendar(2013, 2, 2)).getTime();
		final Date april1st2011 = (new GregorianCalendar(2011, 3, 1)).getTime();
		final Date april2nd2011 = (new GregorianCalendar(2011, 3, 2)).getTime();
		final Date nullDate = null;
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).firstOfMonth().isBetween(march1st2013, march2nd2013));
		List<Marque> got = query.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).firstOfMonth().isBetween(april1st2011, april2nd2011));
		got = query.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).firstOfMonth().isNull());
		got = query.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(1));

	}

	@Test
	public void testDayOfWeekFunction() throws SQLException {
//        database.setPrintSQLBeforeExecuting(true);
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).addMonths(1).dayOfWeek().is(DateExpression.TUESDAY));
		List<Marque> got = query.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).addMonths(1).dayOfWeek().is(DateExpression.MONDAY));
		got = query.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).addMonths(1).dayOfWeek().isNull());
		got = query.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(1));

	}

	@Test
	public void testDayFunction() throws SQLException {
//        database.setPrintSQLBeforeExecuting(true);
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).day().is(23));
		List<Marque> got = query.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).day().is(2));
		got = query.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(3));

	}

	@Test
	public void testHourFunction() throws SQLException {
//        database.setPrintSQLBeforeExecuting(true);
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).hour().is(12));
		List<Marque> got = query.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).hour().is(1));
		got = query.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(3));

	}

	@Test
	public void testMinuteFunction() throws SQLException {
//        database.setPrintSQLBeforeExecuting(true);
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).minute().is(34));
		List<Marque> got = query.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).minute().is(2));
		got = query.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(3));

	}

	@Test
	public void testSecondFunction() throws SQLException {
//        database.setPrintSQLBeforeExecuting(true);
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).second().is(56));
		List<Marque> got = query.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).second().is(3));
		got = query.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(3));

	}

//	@Test
//	public void testMillisecondFunction() throws SQLException {
////        database.setPrintSQLBeforeExecuting(true);
//		Marque marq = new Marque();
//		DBQuery query = database.getDBQuery(marq);
//		query.addCondition(
//				marq.column(marq.creationDate).addMilliseconds(5).milliseconds().is(5));
//		List<Marque> got = query.getAllInstancesOf(marq);
//		database.print(got);
//		Assert.assertThat(got.size(), is(21));
//	}
	@Test
	public void testIsInWithNulls() throws SQLException, ParseException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationDate).isIn((Date) null, datetimeFormat.parse(firstDateStr))
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(19));

		Marque newMarque = new Marque(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", null, 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();
		database.print(allRows);

		dbQuery.addCondition(
				marque.column(marque.creationDate).isIn((Date) null, datetimeFormat.parse(firstDateStr))
		);

		allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(20));
	}

	@Test
	public void testIsWithNulls() throws SQLException, ParseException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationDate).is((Date) null)
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));

		Marque newMarque = new Marque(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", null, 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();
		database.print(allRows);

		dbQuery.addCondition(
				marque.column(marque.creationDate).is((Date) null)
		);

		allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(2));
	}

	@Test
	public void testIsNull() throws SQLException, ParseException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationDate).isNull()
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));

		Marque newMarque = new Marque(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", null, 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();
		database.print(allRows);

		dbQuery.addCondition(
				marque.column(marque.creationDate).isNull()
		);

		allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(2));
	}

	public static class DiffTestReport extends DBReport {

		private static final long serialVersionUID = 1L;

		public Marque marq = new Marque();
		public DBDate dayNormal = new DBDate(
				marq.column(marq.creationDate));
		public DBDate dayAdds = new DBDate(
				marq.column(marq.creationDate).addDays(2));
		public DBNumber dayDiff = new DBNumber(marq.column(marq.creationDate).daysFrom(
				marq.column(marq.creationDate).addDays(2)));
		public DBNumber dayDiffAsHours = new DBNumber(marq.column(marq.creationDate).hoursFrom(
				marq.column(marq.creationDate).addDays(2)));
//		public DBNumber monthDiff = new DBNumber(marq.column(marq.creationDate).monthsFrom(
//				marq.column(marq.creationDate).addMonths(2)));
	}

	@Test
//	@Ignore
	public void testDayDifferenceFunction() throws SQLException, ParseException {
		database.print(DBReport.getAllRows(database, new DiffTestReport()));

		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate)
				.daysFrom(
						marq.column(marq.creationDate).addDays(2))
				.is(2));
		List<Marque> got = query.getAllInstancesOf(marq);
		database.print(got);
		Marque nonNullMarque = new Marque();
		nonNullMarque.creationDate.excludedValues((Date) null);
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		marq = new Marque();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate)
				.hoursFrom(
						marq.column(marq.creationDate).addDays(2))
				.isIn(48, 49)); //Interestingly  one of my examples is near DST transition and NuoDB gives 49 hours
		got = query.getAllInstancesOf(marq);
		database.print(got);
		nonNullMarque = new Marque();
		nonNullMarque.creationDate.excludedValues((Date) null);
		numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		Date secondDate = AbstractTest.datetimeFormat.parse(AbstractTest.secondDateStr);
		marq = new Marque();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate)
				.daysFrom(secondDate)
				.is(0));
		got = query.getAllInstancesOf(marq);
		database.print(got);
		Marque secondDateMarques = new Marque();
		secondDateMarques.creationDate.permittedValues(secondDate);
		int numberOfSecondDateRows = database.getDBTable(secondDateMarques).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfSecondDateRows));
	}

	@Test
//	@Ignore
	public void testWeekDifferenceFunction() throws SQLException, ParseException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate)
				.weeksFrom(
						marq.column(marq.creationDate).addWeeks(2))
				.is(2));
		List<Marque> got = query.getAllInstancesOf(marq);
		database.print(got);
		Marque nonNullMarque = new Marque();
		nonNullMarque.creationDate.excludedValues((Date) null);
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		Date secondDate = AbstractTest.datetimeFormat.parse(AbstractTest.secondDateStr);
		marq = new Marque();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate)
				.weeksFrom(secondDate)
				.is(0));
		got = query.getAllInstancesOf(marq);
		database.print(got);
		Marque secondDateMarques = new Marque();
		secondDateMarques.creationDate.permittedValues(secondDate);
		int numberOfSecondDateRows = database.getDBTable(secondDateMarques).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfSecondDateRows));
	}

	@Test
//	@Ignore
	public void testMonthDifferenceFunction() throws SQLException, ParseException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate)
				.monthsFrom(
						marq.column(marq.creationDate).addMonths(2))
				.is(2));
		List<Marque> got = query.getAllInstancesOf(marq);
		database.print(got);
		Marque nonNullMarque = new Marque();
		nonNullMarque.creationDate.excludedValues((Date) null);
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		Date secondDate = AbstractTest.datetimeFormat.parse(AbstractTest.secondDateStr);
		marq = new Marque();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate)
				.monthsFrom(secondDate)
				.is(0));
		got = query.getAllInstancesOf(marq);
		database.print(got);
		Marque secondDateMarques = new Marque();
		secondDateMarques.creationDate.permittedValues(secondDate);
		int numberOfSecondDateRows = database.getDBTable(secondDateMarques).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfSecondDateRows));
	}

	@Test
//	@Ignore
	public void testYearDifferenceFunction() throws SQLException, ParseException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate)
				.yearsFrom(
						marq.column(marq.creationDate).addYears(2))
				.is(2));
		List<Marque> got = query.getAllInstancesOf(marq);
		database.print(got);
		Marque nonNullMarque = new Marque();
		nonNullMarque.creationDate.excludedValues((Date) null);
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		Date secondDate = AbstractTest.datetimeFormat.parse(AbstractTest.secondDateStr);
		marq = new Marque();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate)
				.yearsFrom(secondDate)
				.is(0));
		got = query.getAllInstancesOf(marq);
		database.print(got);
		Marque secondDateMarques = new Marque();
		secondDateMarques.creationDate.permittedValues(secondDate);
		int numberOfSecondDateRows = database.getDBTable(secondDateMarques).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfSecondDateRows));
	}

	@Test
//	@Ignore
	public void testHourDifferenceFunction() throws SQLException, ParseException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate)
				.hoursFrom(
						marq.column(marq.creationDate).addHours(2))
				.is(2));
		List<Marque> got = query.getAllInstancesOf(marq);
		database.print(got);
		Marque nonNullMarque = new Marque();
		nonNullMarque.creationDate.excludedValues((Date) null);
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		Date secondDate = AbstractTest.datetimeFormat.parse(AbstractTest.secondDateStr);
		marq = new Marque();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate)
				.hoursFrom(secondDate)
				.is(0));
		got = query.getAllInstancesOf(marq);
		database.print(got);
		Marque secondDateMarques = new Marque();
		secondDateMarques.creationDate.permittedValues(secondDate);
		int numberOfSecondDateRows = database.getDBTable(secondDateMarques).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfSecondDateRows));
	}

	@Test
//	@Ignore
	public void testMinutesDifferenceFunction() throws SQLException, ParseException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate)
				.minutesFrom(
						marq.column(marq.creationDate).addMinutes(2))
				.is(2));
		List<Marque> got = query.getAllInstancesOf(marq);
		database.print(got);
		Marque nonNullMarque = new Marque();
		nonNullMarque.creationDate.excludedValues((Date) null);
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		Date secondDate = AbstractTest.datetimeFormat.parse(AbstractTest.secondDateStr);
		marq = new Marque();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate)
				.minutesFrom(secondDate)
				.is(0));
		got = query.getAllInstancesOf(marq);
		database.print(got);
		Marque secondDateMarques = new Marque();
		secondDateMarques.creationDate.permittedValues(secondDate);
		int numberOfSecondDateRows = database.getDBTable(secondDateMarques).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfSecondDateRows));
	}

	@Test
//	@Ignore
	public void testSecondsDifferenceFunction() throws SQLException, ParseException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate)
				.secondsFrom(
						marq.column(marq.creationDate).addSeconds(2))
				.is(2));
		List<Marque> got = query.getAllInstancesOf(marq);
		database.print(got);
		Marque nonNullMarque = new Marque();
		nonNullMarque.creationDate.excludedValues((Date) null);
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		Date secondDate = AbstractTest.datetimeFormat.parse(AbstractTest.secondDateStr);
		marq = new Marque();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate)
				.secondsFrom(secondDate)
				.is(0));
		got = query.getAllInstancesOf(marq);
		database.print(got);
		Marque secondDateMarques = new Marque();
		secondDateMarques.creationDate.permittedValues(secondDate);
		int numberOfSecondDateRows = database.getDBTable(secondDateMarques).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfSecondDateRows));
	}

	public static class MarqueWithSecondsFromDate extends Marque {

		Date date = new Date(10l);
		@DBColumn
		DBNumber subseconds = new DBNumber(DateExpression.value(date).subsecond());
	}

	@Test
	public void testSecondsFromReturnsDecimal() throws SQLException {
		final List<MarqueWithSecondsFromDate> allRows = database.getDBTable(new MarqueWithSecondsFromDate()).setBlankQueryAllowed(true).getAllRows();
		database.print(allRows);
		for (MarqueWithSecondsFromDate row : allRows) {
				Assert.assertThat(row.subseconds.doubleValue(), is(0.01));
		}
	}

//	@Test
//	public void testMillisecondsDifferenceFunction() throws SQLException, ParseException {
//		Marque marq = new Marque();
//		DBQuery query = database.getDBQuery(marq);
//		query.addCondition(
//				marq.column(marq.creationDate)
//				.millisecondsFrom(
//						marq.column(marq.creationDate).addSeconds(2))
//				.isBetween(1999,3000));
//		List<Marque> got = query.getAllInstancesOf(marq);
//		database.print(got);
//		Marque nonNullMarque = new Marque();
//		nonNullMarque.creationDate.excludedValues((Date) null);
//		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
//		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));
//
//		Date secondDate = AbstractTest.datetimeFormat.parse(AbstractTest.secondDateStr);
//		marq = new Marque();
//		query = database.getDBQuery(marq);
//		query.addCondition(
//				marq.column(marq.creationDate)
//				.millisecondsFrom(secondDate)
//				.isBetween(-1000,1000));
//		got = query.getAllInstancesOf(marq);
//		database.print(got);
//		Marque secondDateMarques = new Marque();
//		secondDateMarques.creationDate.permittedValues(secondDate);
//		int numberOfSecondDateRows = database.getDBTable(secondDateMarques).setBlankQueryAllowed(true).count().intValue();
//		Assert.assertThat(got.size(), is(numberOfSecondDateRows));
//	}
	@Test
	public void testEndOfMonthCalculation() throws SQLException {
		MarqueWithEndOfMonthColumn marq = new MarqueWithEndOfMonthColumn();
		DBTable<MarqueWithEndOfMonthColumn> table = database.getDBTable(marq);
		List<MarqueWithEndOfMonthColumn> allRows = table.setBlankQueryAllowed(true).getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(22));
		final Date march31st2013 = (new GregorianCalendar(2013, 2, 31)).getTime();
		final Date april30th2011 = (new GregorianCalendar(2011, 3, 30)).getTime();
		final Date march1st2013 = (new GregorianCalendar(2013, 2, 1)).getTime();
		final Date april1st2011 = (new GregorianCalendar(2011, 3, 1)).getTime();
		final Date nullDate = null;
		for (MarqueWithEndOfMonthColumn allRow : allRows) {
			Assert.assertThat(allRow.endOfMonth.dateValue(),
					anyOf(
							is(nullDate),
							is(march31st2013),
							is(april30th2011)
					));
			Assert.assertThat(allRow.firstOfMonth.dateValue(),
					anyOf(
							is(nullDate),
							is(march1st2013),
							is(april1st2011)
					));
		}

		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.endOfMonth).dayIs(31));
		List<DBQueryRow> allRows1 = dbQuery.getAllRows();
		for (DBQueryRow row : allRows1) {
			Assert.assertThat(
					row.get(marq).endOfMonth.dateValue(),
					is(march31st2013));
			Assert.assertThat(
					row.get(marq).firstOfMonth.dateValue(),
					is(march1st2013)
			);
		}
	}

	public static class MarqueWithEndOfMonthColumn extends Marque {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBDateOnly firstOfMonth = new DBDateOnly(this.column(this.creationDate).addDays(this.column(this.creationDate).day().minus(1).bracket().times(-1)));
		@DBColumn
		DBDateOnly endOfMonth = new DBDateOnly(this.column(this.creationDate).addDays(this.column(this.creationDate).day().minus(1).bracket().times(-1)).addMonths(1).addDays(-1));
		@DBColumn
		DBDateOnly shouldBreakPostgreSQL = new DBDateOnly(this.column(this.creationDate).addDays(this.column(this.creationDate).day().minus(1).times(-1)).addMonths(1).addDays(-1));
	}
}
