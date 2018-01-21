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
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.columns.DateColumn;
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
		public DBNumber dayFromCurrentDate = new DBNumber(DateExpression.currentDateOnly().day());
		@DBColumn
		public DBDate currentDateTime = new DBDate(DateExpression.currentDate());
		@DBColumn
		public DBDate currentDateTimeMinus10Seconds = new DBDate(DateExpression.currentDate().addSeconds(-10));
		@DBColumn
		public DBDate currentDateTimePlus10Seconds = new DBDate(DateExpression.currentDate().addSeconds(10));
		@DBColumn
		public DBDate created = marque.column(marque.creationDate).asExpressionColumn();
		@DBColumn
		public DBDate currentTime = DateExpression.currentTime().asExpressionColumn();
	}

	@Test
	public void testIsNotDateExpression() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		final DateExpression fiveDaysPriorToCreation = marq.column(marq.creationDate).addDays(-5);
		query.addCondition(
				DateExpression.leastOf(
						marq.column(marq.creationDate),
						fiveDaysPriorToCreation,
						DateExpression.value(march23rd2013).addWeeks(-5),
						DateExpression.value(march23rd2013).addDays(-2))
						.isNot(fiveDaysPriorToCreation)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(18));
	}

	@Test
	public void testIsNotDate() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);

		query.addCondition(
				marq.column(marq.creationDate).isNot(april2nd2011)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(18));
	}

	@Test
	public void testLeastOf() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		final DateExpression fiveDaysPriorToCreation = marq.column(marq.creationDate).addDays(-5);
		query.addCondition(
				DateExpression.leastOf(
						marq.column(marq.creationDate),
						fiveDaysPriorToCreation,
						DateExpression.value(march23rd2013).addWeeks(-5), DateExpression.value(march23rd2013).addDays(-2))
						.is(fiveDaysPriorToCreation)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testLeastOfWithList() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		final DateColumn creationDate = marq.column(marq.creationDate);
		final DateExpression fiveDaysPriorToCreation = creationDate.addDays(-5);
		List<DateExpression> poss = new ArrayList<>();
		poss.add(creationDate);
		poss.add(fiveDaysPriorToCreation);
		poss.add(DateExpression.value(march23rd2013).addWeeks(-5));
		poss.add(DateExpression.value(march23rd2013).addDays(-2));
		query.addCondition(
				DateExpression.leastOf(
						poss)
						.is(fiveDaysPriorToCreation)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testLeastOfWithDates() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);

		query.addCondition(
				DateExpression.leastOf(
						march23rd2013,
						april2nd2011)
						.is(april2nd2011)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(22));
	}

	@Test
	public void testGreatestOf() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		final DateColumn creationDate = marq.column(marq.creationDate);
		final DateExpression fiveDaysPriorToCreation = creationDate.addDays(-5);
		query.addCondition(
				DateExpression.greatestOf(
						creationDate,
						fiveDaysPriorToCreation,
						DateExpression.value(march23rd2013).addWeeks(-5), DateExpression.value(march23rd2013).addDays(-2))
						.is(creationDate)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(18));
	}

	@Test
	public void testGreatestOfWithList() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		final DateColumn creationDate = marq.column(marq.creationDate);
		final DateExpression fiveDaysPriorToCreation = creationDate.addDays(-5);
		List<DateExpression> poss = new ArrayList<>();
		poss.add(creationDate);
		poss.add(fiveDaysPriorToCreation);
		poss.add(DateExpression.value(march23rd2013).addWeeks(-5));
		poss.add(DateExpression.value(march23rd2013).addDays(-2));
		query.addCondition(
				DateExpression.greatestOf(
						poss)
						.is(creationDate)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(18));
	}

	@Test
	public void testGreatestOfWithDates() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				DateExpression.greatestOf(
						march23rd2013,
						april2nd2011)
						.is(march23rd2013)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(22));
	}

	@Test
	public void testOverlapsDateExpressionDateResult() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(DateExpression.overlaps(
				marq.column(marq.creationDate), marq.column(marq.creationDate).addDays(-5),
				DateExpression.value(march23rd2013).addWeeks(-5), DateExpression.value(march23rd2013).addDays(-2))
		);
		List<DBQueryRow> allRows = query.getAllRows();
		Assert.assertThat(allRows.size(), is(18));
	}

	@Test
	public void testOverlapsAllDateResults() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(DateExpression.overlaps(
				marq.creationDate, marq.column(marq.creationDate).addDays(-5),
				DateExpression.value(march23rd2013).addWeeks(-5), DateExpression.value(march23rd2013).addDays(-2))
		);
		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(18));
	}

	@Test
	public void testAggregators() throws SQLException {
		MarqueWithDateAggregators marq = new MarqueWithDateAggregators();
		DBQuery query = database.getDBQuery(marq).setBlankQueryAllowed(true);
		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		MarqueWithDateAggregators got = allRows.get(0).get(marq);
		Assert.assertThat(got.countOfDates.intValue(), is(21));
		Assert.assertThat(got.maxOfDates.dateValue(), is(march23rd2013));
		Assert.assertThat(got.minOfDates.dateValue(), is(april2nd2011));

	}

	public static class MarqueWithDateAggregators extends Marque {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBNumber countOfDates = new DBNumber(this.column(this.creationDate).count());
		@DBColumn
		DBDate maxOfDates = new DBDate(this.column(this.creationDate).max());
		@DBColumn
		DBDate minOfDates = new DBDate(this.column(this.creationDate).min());

		{
			this.setReturnFields(this.countOfDates, this.maxOfDates, this.minOfDates);
		}
	}

	@Test
	public void testCurrentTime() throws SQLException {

		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addConditions(marq.column(marq.creationDate).isBetween(DateExpression.currentDate(), DateExpression.nullDate()));

		List<DBQueryRow> got = query.getAllRows();

		Assert.assertThat(got.size(), is(0));

		database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));

		Marque reportLimitingMarque = new Marque();
		reportLimitingMarque.name.permittedPatternIgnoreCase("% HUMMER %");
		CurrentDateReport currentDateReport = new CurrentDateReport();

		final List<CurrentDateReport> reportRows = DBReport.getRows(database, currentDateReport, reportLimitingMarque);

		Assert.assertThat(reportRows.size(), is(1));

		query = database.getDBQuery(marq);
		query.addConditions(
				marq.column(marq.creationDate)
						.hour().isIn(
								DateExpression.currentDate().addSeconds(-100).hour(),
								DateExpression.currentDate().addSeconds(100).hour()),
				marq.column(marq.creationDate).isGreaterThan(march23rd2013)
		);

		got = query.getAllRows();

		Assert.assertThat(got.size(), is(1));

	}

	@Test
	public void testCurrentDateTimeAndAddSecondsFunctions() throws SQLException {

		Marque marq = new Marque();
		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate(), null);
		List<Marque> got = database.get(marq);

		Assert.assertThat(got.size(), is(0));

		database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));

		Marque reportLimitingMarque = new Marque();
		reportLimitingMarque.name.permittedPatternIgnoreCase("% HUMMER %");
		CurrentDateReport currentDateReport = new CurrentDateReport();
		final List<CurrentDateReport> reportRows = DBReport.getRows(database, currentDateReport, reportLimitingMarque);

		Assert.assertThat(reportRows.size(), is(1));

		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate().addSeconds(-10), null);
		got = database.get(marq);

		Assert.assertThat(got.size(), is(1));

		marq.creationDate.permittedRangeInclusive(null, DateExpression.currentDate().addSeconds(-10));
		got = database.get(marq);

		Assert.assertThat(got.size(), is(21));

		marq.creationDate.permittedRangeInclusive(
				DateExpression.currentDate().addMinutes(-5), null);
		got = database.getDBTable(marq).getAllRows();

		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testCurrentDateAndAddDaysFunctions() throws SQLException {
		Marque marq = new Marque();
		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate(), null);
		List<Marque> got = database.get(marq);

		Assert.assertThat(got.size(), is(0));

		database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));
		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate().addDays(-1), null);
		got = database.get(marq);

		Assert.assertThat(got.size(), is(1));

		marq.creationDate.permittedRangeInclusive(null, DateExpression.currentDate().addDays(-1));
		got = database.get(marq);

		Assert.assertThat(got.size(), is(21));

		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate().addDays(-1), DateExpression.currentDate().addDays(+1));
		got = database.get(marq);

		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testCurrentDateAndAddHoursFunctions() throws SQLException {
		Marque marq = new Marque();
		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate(), null);
		List<Marque> got = database.get(marq);

		Assert.assertThat(got.size(), is(0));

		database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));
		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate().addHours(-1), null);
		got = database.get(marq);

		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testCurrentDateAndAddWeeksFunctions() throws SQLException {
		Marque marq = new Marque();
		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate(), null);
		List<Marque> got = database.get(marq);

		Assert.assertThat(got.size(), is(0));

		database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));
		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate().addWeeks(-1), null);
		got = database.get(marq);

		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testCurrentDateAndAddMinutesFunctions() throws SQLException {
		Marque marq = new Marque();
		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate(), null);
		List<Marque> got = database.get(marq);

		Assert.assertThat(got.size(), is(0));

		database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));
		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate().addMinutes(-1), null);
		got = database.get(marq);

		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testCurrentDateAndAddMonthsFunctions() throws SQLException {
		Marque marq = new Marque();
		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate(), null);
		List<Marque> got = database.get(marq);

		Assert.assertThat(got.size(), is(0));

		database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));
		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate().addMonths(-1), null);
		got = database.get(marq);

		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testCurrentDateAndAddYearsFunctions() throws SQLException {
		Marque marq = new Marque();
		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate(), null);
		List<Marque> got = database.get(marq);

		Assert.assertThat(got.size(), is(0));

		database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));
		marq.creationDate.permittedRangeInclusive(DateExpression.currentDate().addYears(-1), null);
		got = database.get(marq);

		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testYearFunction() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).year().is(2014));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(0));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).year().is(2013));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));
	}

	@Test
	public void testYearIsFunction() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).yearIs(2014));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(0));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).yearIs(2013));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).yearIs(NumberExpression.value(2014).numberResult()));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(0));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).yearIs(NumberExpression.value(2013)));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));
	}

	@Test
	public void testMonthFunction() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).monthIs(3));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).monthIs(4));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).monthIs(NumberExpression.value(3).numberResult()));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).monthIs(NumberExpression.value(4)));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));
	}

	@Test
	public void testEndOfMonthFunction() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).endOfMonth().day().is(31));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).endOfMonth().day().is(30));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).endOfMonth().day().isNull());
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testFirstOfMonthFunction() throws SQLException {
		final Date march1st2013 = (new GregorianCalendar(2013, 2, 1)).getTime();
		final Date march2nd2013 = (new GregorianCalendar(2013, 2, 2)).getTime();
		final Date april1st2011 = (new GregorianCalendar(2011, 3, 1)).getTime();

		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).firstOfMonth().isBetween(march1st2013, march2nd2013));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).firstOfMonth().isBetween(april1st2011, april2nd2011));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).firstOfMonth().isBetween(DateExpression.value(april1st2011), april2nd2011));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).firstOfMonth().isBetween(april1st2011, DateExpression.value(april2nd2011)));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).firstOfMonth().isNull());
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testIsNotNull() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).firstOfMonth().isNotNull());
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(21));
	}

	@Test
	public void testBetweenInclusiveFunction() throws SQLException {
		final Date march1st2013 = (new GregorianCalendar(2013, 2, 1)).getTime();
		final Date march2nd2013 = (new GregorianCalendar(2013, 2, 2)).getTime();
		final Date april1st2011 = (new GregorianCalendar(2011, 3, 1)).getTime();

		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).firstOfMonth().isBetweenInclusive(march1st2013, march2nd2013));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).firstOfMonth().isBetweenInclusive(april1st2011, april2nd2011));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).firstOfMonth().isBetweenInclusive(DateExpression.value(april1st2011), april2nd2011));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).firstOfMonth().isBetweenInclusive(april1st2011, DateExpression.value(april2nd2011)));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).firstOfMonth().isNull());
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testisGreaterThan() throws SQLException {
		final Date march1st2013 = (new GregorianCalendar(2013, 2, 1)).getTime();
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).firstOfMonth().isGreaterThan(march1st2013));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));
	}

	@Test
	public void testisGreaterThanOrEqual() throws SQLException {
		final Date march1st2013 = (new GregorianCalendar(2013, 2, 1)).getTime();
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).firstOfMonth().isGreaterThanOrEqual(march1st2013));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));
	}

	@Test
	public void testisGreaterThanWithFallback() throws SQLException {
		final Date march1st2013 = (new GregorianCalendar(2013, 2, 1)).getTime();
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).firstOfMonth().isGreaterThan(march1st2013, marq.column(marq.name).isGreaterThan("T")));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));
	}

	@Test
	public void testisLessThan() throws SQLException {
		final Date march1st2013 = (new GregorianCalendar(2013, 2, 1)).getTime();
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).firstOfMonth().isLessThan(march1st2013));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));
	}

	@Test
	public void testisLessThanWithFallback() throws SQLException {
		final Date march1st2013 = (new GregorianCalendar(2013, 2, 1)).getTime();
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).firstOfMonth().isLessThan(march1st2013, marq.column(marq.name).isGreaterThan("T")));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));
	}

	@Test
	public void testBetweenExclusiveFunction() throws SQLException {
		final Date march1st2013 = (new GregorianCalendar(2013, 2, 1)).getTime();
		final Date march2nd2013 = (new GregorianCalendar(2013, 2, 2)).getTime();
		final Date april1st2011 = (new GregorianCalendar(2011, 3, 1)).getTime();

		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).firstOfMonth().isBetweenExclusive(march1st2013, march2nd2013));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).firstOfMonth().isBetweenExclusive(april1st2011, april2nd2011));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).firstOfMonth().isBetweenExclusive(DateExpression.value(april1st2011), april2nd2011));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).firstOfMonth().isBetweenExclusive(april1st2011, DateExpression.value(april2nd2011)));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).firstOfMonth().isNull());
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(1));

	}

	@Test
	public void testDayOfWeekFunction() throws SQLException {

		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).addMonths(1).dayOfWeek().is(DateExpression.TUESDAY));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).addMonths(1).dayOfWeek().is(DateExpression.MONDAY));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).addMonths(1).dayOfWeek().isNull());
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(1));

	}

	@Test
	public void testDayFunction() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).day().is(23));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).day().is(2));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));
	}

	@Test
	public void testDayIsFunction() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).dayIs(23));
		List<Marque> got = query.getAllInstancesOf(marq);
		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).dayIs(2));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).dayIs(NumberExpression.value(2)));
		got = query.getAllInstancesOf(marq);
		
		Assert.assertThat(got.size(), is(3));
		
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).dayIs(NumberExpression.value(2).numberResult()));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));
	}

	@Test
	public void testHourFunction() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).hour().is(12));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).hour().is(1));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));
	}

	@Test
	public void testHourIsFunction() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).hourIs(12));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).hourIs(1));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).hourIs(NumberExpression.value(1).numberResult()));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).hourIs(NumberExpression.value(1)));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));
	}

	@Test
	public void testMinuteFunction() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).minute().is(34));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).minute().is(2));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));
	}

	@Test
	public void testMinuteIsFunction() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).minuteIs(34));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).minuteIs(2));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).minuteIs(NumberExpression.value(2).numberResult()));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).minuteIs(NumberExpression.value(2)));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));
	}

	@Test
	public void testSecondFunction() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).second().is(56));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).second().is(3));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));
	}

	@Test
	public void testSecondIsFunction() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).secondIs(56));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).secondIs(3));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).secondIs(NumberExpression.value(3)));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));
	}

	@Test
	public void testIsInWithNulls() throws SQLException, ParseException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationDate).isIn((Date) null, DATETIME_FORMAT.parse(firstDateStr))
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		Assert.assertThat(allRows.size(), is(19));

		Marque newMarque = new Marque(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", null, 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();
		
		Assert.assertThat(allRows.size(), is(23));

		dbQuery.addCondition(
				marque.column(marque.creationDate).isIn((Date) null, DATETIME_FORMAT.parse(firstDateStr))
		);

		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(20));
	}

	@Test
	public void testIsIn() throws SQLException, ParseException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationDate).isIn((Date) null, DATETIME_FORMAT.parse(firstDateStr))
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(19));

		Marque newMarque = new Marque(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", null, 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();
		
		Assert.assertThat(allRows.size(), is(23));

		dbQuery.addCondition(
				marque.column(marque.creationDate).isIn(april2nd2011, march23rd2013)
		);

		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(21));
	}

	@Test
	public void testIsInWithList() throws SQLException, ParseException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationDate).isIn((Date) null, DATETIME_FORMAT.parse(firstDateStr))
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(19));

		Marque newMarque = new Marque(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", null, 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();
		
		Assert.assertThat(allRows.size(), is(23));

		List<DateExpression> dates = new ArrayList<DateExpression>();
		dates.add(DateExpression.value(march23rd2013));
		dates.add(DateExpression.value(april2nd2011));

		dbQuery.addCondition(
				marque.column(marque.creationDate).isIn(dates)
		);

		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(21));
	}

	@Test
	public void testIsWithNulls() throws SQLException, ParseException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationDate).is((Date) null)
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));

		Marque newMarque = new Marque(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", null, 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();
		
		Assert.assertThat(allRows.size(), is(23));

		dbQuery.addCondition(
				marque.column(marque.creationDate).is((Date) null)
		);

		allRows = dbQuery.getAllRows();

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

		Assert.assertThat(allRows.size(), is(1));

		Marque newMarque = new Marque(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", null, 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();
		
		Assert.assertThat(allRows.size(), is(23));

		dbQuery.addCondition(
				marque.column(marque.creationDate).isNull()
		);

		allRows = dbQuery.getAllRows();

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
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate)
						.daysFrom(
								marq.column(marq.creationDate).addDays(2))
						.is(2));
		List<Marque> got = query.getAllInstancesOf(marq);

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

		nonNullMarque = new Marque();
		nonNullMarque.creationDate.excludedValues((Date) null);
		numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		Date secondDate = AbstractTest.DATETIME_FORMAT.parse(AbstractTest.secondDateStr);
		marq = new Marque();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate)
						.daysFrom(secondDate)
						.is(0));
		got = query.getAllInstancesOf(marq);

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

		Marque nonNullMarque = new Marque();
		nonNullMarque.creationDate.excludedValues((Date) null);
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		Date secondDate = AbstractTest.DATETIME_FORMAT.parse(AbstractTest.secondDateStr);
		marq = new Marque();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate)
						.weeksFrom(secondDate)
						.is(0));
		got = query.getAllInstancesOf(marq);

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

		Marque nonNullMarque = new Marque();
		nonNullMarque.creationDate.excludedValues((Date) null);
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		Date secondDate = AbstractTest.DATETIME_FORMAT.parse(AbstractTest.secondDateStr);
		marq = new Marque();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate)
						.monthsFrom(secondDate)
						.is(0));
		got = query.getAllInstancesOf(marq);

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

		Marque nonNullMarque = new Marque();
		nonNullMarque.creationDate.excludedValues((Date) null);
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		Date secondDate = AbstractTest.DATETIME_FORMAT.parse(AbstractTest.secondDateStr);
		marq = new Marque();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate)
						.yearsFrom(secondDate)
						.is(0));
		got = query.getAllInstancesOf(marq);

		Marque secondDateMarques = new Marque();
		secondDateMarques.creationDate.permittedValues(secondDate);
		int numberOfSecondDateRows = 
				database
				.getDBTable(secondDateMarques)
				.setBlankQueryAllowed(true)
				.count()
				.intValue();
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

		Marque nonNullMarque = new Marque();
		nonNullMarque.creationDate.excludedValues((Date) null);
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		Date secondDate = AbstractTest.DATETIME_FORMAT.parse(AbstractTest.secondDateStr);
		marq = new Marque();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate)
						.hoursFrom(secondDate)
						.is(0));
		got = query.getAllInstancesOf(marq);

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

		Marque nonNullMarque = new Marque();
		nonNullMarque.creationDate.excludedValues((Date) null);
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		Date secondDate = AbstractTest.DATETIME_FORMAT.parse(AbstractTest.secondDateStr);
		marq = new Marque();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate)
						.minutesFrom(secondDate)
						.is(0));
		got = query.getAllInstancesOf(marq);

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

		Marque nonNullMarque = new Marque();
		nonNullMarque.creationDate.excludedValues((Date) null);
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		Date secondDate = AbstractTest.DATETIME_FORMAT.parse(AbstractTest.secondDateStr);
		marq = new Marque();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate)
						.secondsFrom(secondDate)
						.is(0));
		got = query.getAllInstancesOf(marq);

		Marque secondDateMarques = new Marque();
		secondDateMarques.creationDate.permittedValues(secondDate);
		int numberOfSecondDateRows = database.getDBTable(secondDateMarques).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfSecondDateRows));
	}

	public static class MarqueWithSecondsFromDate extends Marque {

		private static final long serialVersionUID = 1L;

		Date date = new Date(10l);
		@DBColumn
		DBNumber subseconds = new DBNumber(DateExpression.value(date).subsecond());
	}

	@Test
	public void testSecondsFromReturnsDecimal() throws SQLException {
		final List<MarqueWithSecondsFromDate> allRows = database.getDBTable(new MarqueWithSecondsFromDate()).setBlankQueryAllowed(true).getAllRows();

		for (MarqueWithSecondsFromDate row : allRows) {
			Assert.assertThat(row.subseconds.doubleValue(), is(0.01));
		}
	}

	@Test
	public void testEndOfMonthCalculation() throws SQLException {
		MarqueWithEndOfMonthColumn marq = new MarqueWithEndOfMonthColumn();
		DBTable<MarqueWithEndOfMonthColumn> table = database.getDBTable(marq);
		List<MarqueWithEndOfMonthColumn> allRows = table.setBlankQueryAllowed(true).getAllRows();

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
		DBDateOnly endOfMonth = new DBDateOnly(this.column(this.creationDate).endOfMonth());
		@DBColumn
		DBDateOnly shouldBreakPostgreSQL = new DBDateOnly(this.column(this.creationDate).addDays(this.column(this.creationDate).day().minus(1).times(-1)).addMonths(1).addDays(-1));
	}
}
