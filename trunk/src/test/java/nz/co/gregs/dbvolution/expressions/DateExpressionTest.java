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
import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static nz.co.gregs.dbvolution.generic.AbstractTest.tedhiFormat;
import static org.hamcrest.Matchers.is;
import org.junit.Assert;
import org.junit.Ignore;
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
		public DBDate currentDate = new DBDate(DateExpression.currentDate());
		@DBColumn
		public DBDate currentDateTime = new DBDate(DateExpression.currentDate());
		@DBColumn
		public DBDate currentDateTimeMinus10Seconds = new DBDate(DateExpression.currentDate().addSeconds(-10));
		@DBColumn
		public DBDate currentDateTimePlus10Seconds = new DBDate(DateExpression.currentDate().addSeconds(10));
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
		database.print(DBReport.getRows(database, currentDateReport, reportLimitingMarque));

		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate().addSeconds(-10), null);
		got = database.get(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(1));

		marq.creationDate.permittedRangeInclusive(null, DateExpression.currentDate().addSeconds(-10));
		got = database.get(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(21));

		marq.creationDate.permittedRangeInclusive(
				DateExpression.currentDate().addSeconds(-20),
				DateExpression.currentDate().addSeconds(+20));
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
		public Marque marq = new Marque();
		public DBNumber dayDiff = new DBNumber(marq.column(marq.creationDate).daysFrom(
				marq.column(marq.creationDate).addDays(2)));
		public DBNumber hourDiff = new DBNumber(marq.column(marq.creationDate).hoursFrom(
				marq.column(marq.creationDate).addHours(2)));
		public DBNumber monthDiff = new DBNumber(marq.column(marq.creationDate).monthsFrom(
				marq.column(marq.creationDate).addMonths(2)));
	}

	@Test
//	@Ignore
	public void testDayDifferenceFunction() throws SQLException, ParseException {
//        database.setPrintSQLBeforeExecuting(true);
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

		Date secondDate = AbstractTest.datetimeFormat.parse(super.secondDateStr);
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
//        database.setPrintSQLBeforeExecuting(true);
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

		Date secondDate = AbstractTest.datetimeFormat.parse(super.secondDateStr);
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
//        database.setPrintSQLBeforeExecuting(true);
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

		Date secondDate = AbstractTest.datetimeFormat.parse(super.secondDateStr);
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
//        database.setPrintSQLBeforeExecuting(true);
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

		Date secondDate = AbstractTest.datetimeFormat.parse(super.secondDateStr);
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
//        database.setPrintSQLBeforeExecuting(true);
		database.print(DBReport.getAllRows(database, new DiffTestReport()));
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

		Date secondDate = AbstractTest.datetimeFormat.parse(super.secondDateStr);
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
//        database.setPrintSQLBeforeExecuting(true);
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

		Date secondDate = AbstractTest.datetimeFormat.parse(super.secondDateStr);
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
//        database.setPrintSQLBeforeExecuting(true);
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

		Date secondDate = AbstractTest.datetimeFormat.parse(super.secondDateStr);
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
}
