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
import java.time.LocalDate;
import java.time.Month;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.annotations.DBColumn;
//import nz.co.gregs.dbvolution.datatypes.DBLocalDate;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBLocalDate;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

public class LocalDateExpressionTest extends AbstractTest {

	public LocalDateExpressionTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	public static class CurrentDateReport extends DBReport {

		private static final long serialVersionUID = 1L;
		public Marque marque = new Marque();
		@DBColumn
		public DBString name = new DBString(marque.column(marque.name));
		@DBColumn
		public DBLocalDate savedJavaDate = new DBLocalDate(marque.column(marque.creationDate).toLocalDate());
		@DBColumn
		public DBLocalDate actualJavaDate = new DBLocalDate(LocalDate.now());
		@DBColumn
		public DBLocalDate currentDate = new DBLocalDate(LocalDateExpression.today());
		@DBColumn
		public DBNumber dayFromCurrentDate = new DBNumber(LocalDateExpression.now().day());
		@DBColumn
		public DBLocalDate currentDateTime = new DBLocalDate(LocalDateExpression.currentDate());
		@DBColumn
		public DBLocalDate created = marque.column(marque.creationDate).toLocalDate().asExpressionColumn();
		@DBColumn
		public DBLocalDate currentTime = LocalDateExpression.currentLocalDate().asExpressionColumn();
	}

	LocalDate march23rd2013LocalDate = (new GregorianCalendar(2013, 2, 23, 12, 34, 56)).toZonedDateTime().toLocalDate();
	LocalDate april2nd2011LocalDate = (new GregorianCalendar(2011, 3, 2, 1, 2, 3)).toZonedDateTime().toLocalDate();

	@Test
	public void testIsNotDateExpression() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		final LocalDateExpression fiveDaysPriorToCreation = marq.column(marq.creationDate).toLocalDate().addDays(-5);
		query.addCondition(LocalDateExpression.leastOf(marq.column(marq.creationDate).toLocalDate(),
				fiveDaysPriorToCreation,
				LocalDateExpression.value(march23rd2013LocalDate).addWeeks(-5),
				LocalDateExpression.value(march23rd2013LocalDate).addDays(-2))
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
				marq.column(marq.creationDate).toLocalDate().isNot(april2nd2011LocalDate)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(18));
	}

	@Test
	public void testLeastOf() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		final LocalDateExpression fiveDaysPriorToCreation = marq.column(marq.creationDate).toLocalDate().addDays(-5);
		query.addCondition(LocalDateExpression.leastOf(marq.column(marq.creationDate).toLocalDate(),
				fiveDaysPriorToCreation,
				LocalDateExpression.value(march23rd2013LocalDate).addWeeks(-5),
				LocalDateExpression.value(march23rd2013LocalDate).addDays(-2))
				.is(fiveDaysPriorToCreation)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testLeastOfWithList() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		final LocalDateExpression creationDate = marq.column(marq.creationDate).toLocalDate();
		final LocalDateExpression fiveDaysPriorToCreation = creationDate.addDays(-5);
		List<LocalDateExpression> poss = new ArrayList<>();
		poss.add(creationDate);
		poss.add(fiveDaysPriorToCreation);
		poss.add(LocalDateExpression.value(march23rd2013LocalDate).addWeeks(-5));
		poss.add(LocalDateExpression.value(march23rd2013LocalDate).addDays(-2));
		query.addCondition(
				LocalDateExpression.leastOf(
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

		query.addCondition(LocalDateExpression
				.leastOf(march23rd2013LocalDate,
						april2nd2011LocalDate)
				.is(april2nd2011LocalDate)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(22));
	}

	@Test
	public void testGreatestOf() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		final LocalDateExpression creationDate = marq.column(marq.creationDate).toLocalDate();
		final LocalDateExpression fiveDaysPriorToCreation = creationDate.addDays(-5);
		query.addCondition(LocalDateExpression.greatestOf(creationDate,
				fiveDaysPriorToCreation,
				LocalDateExpression.value(march23rd2013LocalDate).addWeeks(-5),
				LocalDateExpression.value(march23rd2013LocalDate).addDays(-2))
				.is(creationDate)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(18));
	}

	@Test
	public void testGreatestOfWithList() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		final LocalDateExpression creationDate = marq.column(marq.creationDate).toLocalDate();
		final LocalDateExpression fiveDaysPriorToCreation = creationDate.addDays(-5);
		List<LocalDateExpression> poss = new ArrayList<>();
		poss.add(creationDate);
		poss.add(fiveDaysPriorToCreation);
		poss.add(LocalDateExpression.value(march23rd2013LocalDate).addWeeks(-5));
		poss.add(LocalDateExpression.value(march23rd2013LocalDate).addDays(-2));
		query.addCondition(
				LocalDateExpression.greatestOf(
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
				LocalDateExpression
						.greatestOf(
								march23rd2013LocalDate,
								april2nd2011LocalDate
						).is(march23rd2013LocalDate)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(22));
	}

	@Test
	public void testOverlapsDateExpressionDateResult() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(LocalDateExpression.overlaps(marq.column(marq.creationDate).toLocalDate(),
				marq.column(marq.creationDate).toLocalDate().addDays(-5),
				LocalDateExpression.value(march23rd2013LocalDate).addWeeks(-5),
				LocalDateExpression.value(march23rd2013LocalDate).addDays(-2))
		);
		List<DBQueryRow> allRows = query.getAllRows();
		Assert.assertThat(allRows.size(), is(18));
	}

	@Test
	public void testOverlapsAllDateResults() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(LocalDateExpression.overlaps(marq.column(marq.creationDate).toLocalDate(),
				marq.column(marq.creationDate).toLocalDate().addDays(-5),
				LocalDateExpression.value(march23rd2013LocalDate).addWeeks(-5),
				LocalDateExpression.value(march23rd2013LocalDate).addDays(-2))
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
		Assert.assertThat(got.maxOfDates.localDateValue(), is(march23rd2013LocalDate));
		Assert.assertThat(got.minOfDates.getValue(), is(april2nd2011LocalDate));

	}

	public static class MarqueWithDateAggregators extends Marque {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBNumber countOfDates = new DBNumber(this.column(this.creationDate).count());
		@DBColumn
		DBLocalDate maxOfDates = new DBLocalDate(this.column(this.creationDate).toLocalDate().max());
		@DBColumn
		DBLocalDate minOfDates = new DBLocalDate(this.column(this.creationDate).toLocalDate().min());

		{
			this.setReturnFields(this.countOfDates, this.maxOfDates, this.minOfDates);
		}
	}

	@Test
	public void testWindowingFunctions() throws SQLException {
		MarqueWithDateWindowingFunctions marq = new MarqueWithDateWindowingFunctions();

		DBQuery query = database.getDBQuery(marq)
				.setBlankQueryAllowed(true)
				.setSortOrder(marq.column(marq.carCompany));

		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(22));

		MarqueWithDateWindowingFunctions got;// = allRows.get(0).get(marq);
		ArrayList<Object[]> expectedValues = new ArrayList<>();
		expectedValues.add(new Object[]{2, march23rd2013LocalDate, march23rd2013LocalDate});
		expectedValues.add(new Object[]{2, march23rd2013LocalDate, march23rd2013LocalDate});
		expectedValues.add(new Object[]{1, march23rd2013LocalDate, march23rd2013LocalDate});
		expectedValues.add(new Object[]{3, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{3, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{3, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		for (int i = 0; i < allRows.size(); i++) {
			got = allRows.get(i).get(marq);
			Object[] expect = expectedValues.get(i);
			Assert.assertThat(got.countOfDates.intValue(), is((Integer) expect[0]));
			Assert.assertThat(got.maxOfAll.getValue(), is((LocalDate) expect[1]));
			Assert.assertThat(got.maxOfDates.getValue(), is((LocalDate) expect[1]));
			Assert.assertThat(got.minOfDates.getValue(), is((LocalDate) expect[2]));
		}
	}

	public static class MarqueWithDateWindowingFunctions extends Marque {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBNumber countOfAllRows = new DBNumber(this.column(this.creationDate).toLocalDate().count().over().allRows());
		@DBColumn
		DBNumber rowNumber = new DBNumber(this.column(this.creationDate).toLocalDate().count().over().AllRowsAndOrderBy(this.column(this.carCompany).ascending()));
		@DBColumn
		DBNumber countOfDates = new DBNumber(this.column(this.creationDate).toLocalDate().count().over().partition(this.column(this.carCompany)).unordered());
		@DBColumn
		DBNumber rowWithinCarCo = new DBNumber(this.column(this.creationDate).toLocalDate().count()
				.over()
				.partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).ascending())
				.defaultFrame());
		@DBColumn
		DBLocalDate maxOfAll = new DBLocalDate(this.column(this.creationDate).toLocalDate().max().over().allRows());
		@DBColumn
		DBLocalDate maxOfDates = new DBLocalDate(this.column(this.creationDate).toLocalDate().max().over().partition(this.column(this.carCompany)).unordered());
		@DBColumn
		DBLocalDate minOfDates = new DBLocalDate(this.column(this.creationDate).toLocalDate().min().over().partition(this.column(this.carCompany)).unordered());
		@DBColumn
		DBLocalDate test1 = new DBLocalDate(this.column(this.creationDate)
				.toLocalDate()
				.min()
				.over().partition(this.column(this.carCompany))
				.unsorted());
		@DBColumn
		DBLocalDate test2 = new DBLocalDate(this.column(this.creationDate).toLocalDate().min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().unboundedPreceding().currentRow());
		@DBColumn
		DBLocalDate test3 = new DBLocalDate(this.column(this.creationDate).toLocalDate().min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().unboundedPreceding().currentRow());
		@DBColumn
		DBLocalDate test4 = new DBLocalDate(this.column(this.creationDate).toLocalDate().min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().currentRow().currentRow());
		@DBColumn
		DBLocalDate test5 = new DBLocalDate(this.column(this.creationDate).toLocalDate().min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().offsetPrecedingAndCurrentRow(5));
		@DBColumn
		DBLocalDate test6 = new DBLocalDate(this.column(this.creationDate).toLocalDate().min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().currentRow().unboundedFollowing());
		@DBColumn
		DBLocalDate test7 = new DBLocalDate(this.column(this.creationDate).toLocalDate().min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().currentRow().following(3));
		@DBColumn
		DBLocalDate test8 = new DBLocalDate(this.column(this.creationDate).toLocalDate().min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().unboundedPreceding().unboundedFollowing());
		@DBColumn
		DBLocalDate test9 = new DBLocalDate(this.column(this.creationDate).toLocalDate().min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().unboundedPreceding().following(1));
	}

	@Test
	public void testAggregatorWithWindowingFunctions() throws SQLException {
		MarqueWithAggregatorAndDateWindowingFunctions marq = new MarqueWithAggregatorAndDateWindowingFunctions();

		DBQuery query = database.getDBQuery(marq)
				.setBlankQueryAllowed(true)
				.setSortOrder(marq.column(marq.carCompany));

		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(22));

		MarqueWithAggregatorAndDateWindowingFunctions got;// = allRows.get(0).get(marq);
		ArrayList<Object[]> expectedValues = new ArrayList<>();
		expectedValues.add(new Object[]{2, march23rd2013LocalDate, march23rd2013LocalDate});
		expectedValues.add(new Object[]{2, march23rd2013LocalDate, march23rd2013LocalDate});
		expectedValues.add(new Object[]{1, march23rd2013LocalDate, march23rd2013LocalDate});
		expectedValues.add(new Object[]{3, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{3, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{3, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011LocalDate});
		for (int i = 0; i < allRows.size(); i++) {
			got = allRows.get(i).get(marq);
			Object[] expect = expectedValues.get(i);
			Assert.assertThat(got.countOfDates.intValue(), is((Integer) expect[0]));
			Assert.assertThat(got.maxOfDates.getValue(), is((LocalDate) expect[1]));
			Assert.assertThat(got.minOfDates.getValue(), is((LocalDate) expect[2]));
		}
	}

	public static class MarqueWithAggregatorAndDateWindowingFunctions extends Marque {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBNumber countAggregator = new DBNumber(this.column(this.creationDate).toLocalDate().count());
		@DBColumn
		DBNumber countOfAllRows = new DBNumber(this.column(this.creationDate).toLocalDate().count().over().allRows());
		@DBColumn
		DBNumber countOfDates = new DBNumber(this.column(this.creationDate).toLocalDate().count()
				.over()
				.partition(this.column(this.carCompany)).unordered());
		@DBColumn
		DBLocalDate maxOfAll = new DBLocalDate(this.column(this.creationDate)
				.toLocalDate().max().over().allRows());
		@DBColumn
		DBLocalDate maxOfDates = new DBLocalDate(this.column(this.creationDate)
				.toLocalDate().max().over()
				.partition(this.column(this.carCompany)).unordered());
		@DBColumn
		DBLocalDate minOfDates = new DBLocalDate(this.column(this.creationDate)
				.toLocalDate().min()
				.over()
				.partition(this.column(this.carCompany)).unordered());
		@DBColumn
		DBInteger rowNumber = new DBInteger(AnyExpression.rowNumber().AllRowsAndOrderBy(column(carCompany).ascending()));
		@DBColumn
		DBInteger rank = new DBInteger(AnyExpression.rank().AllRowsAndOrderBy(column(carCompany).ascending()));
		@DBColumn
		DBInteger denseRank = new DBInteger(AnyExpression.denseRank().AllRowsAndOrderBy(column(carCompany).ascending()));
		@DBColumn
		DBInteger nTile = new DBInteger(IntegerExpression.nTile(3).AllRowsAndOrderBy(column(carCompany).ascending()));
	}

	@Test
	public void testComplexWindowingFunctions() throws SQLException {
		MarqueWithComplexWindowingFunction marq = new MarqueWithComplexWindowingFunction();

		DBQuery query = database.getDBQuery(marq)
				.setBlankQueryAllowed(true)
				.setSortOrder(marq.column(marq.carCompany));

		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(22));

		MarqueWithComplexWindowingFunction got;// = allRows.get(0).get(marq);
		ArrayList<Object[]> expectedValues = new ArrayList<>();
		expectedValues.add(new Object[]{2, march23rd2013LocalDate, march23rd2013LocalDate});
		expectedValues.add(new Object[]{2, march23rd2013LocalDate, march23rd2013LocalDate});
		expectedValues.add(new Object[]{1, march23rd2013LocalDate, march23rd2013LocalDate});
		expectedValues.add(new Object[]{3, march23rd2013LocalDate, april2nd2011});
		expectedValues.add(new Object[]{3, march23rd2013LocalDate, april2nd2011});
		expectedValues.add(new Object[]{3, march23rd2013LocalDate, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDate, april2nd2011});
		for (int i = 0; i < allRows.size(); i++) {
			got = allRows.get(i).get(marq);
			Object[] expect = expectedValues.get(i);
		}
	}

	public static class MarqueWithComplexWindowingFunction extends Marque {

		private static final long serialVersionUID = 1L;

		@DBColumn //(rank - 1) / (total partition rows - 1)
		DBNumber percentileRank = AnyExpression.percentageRank().AllRowsAndOrderBy(column(carCompany).ascending()).asExpressionColumn();

		// (0.0+( ROW_NUMBER() OVER (partition by *PARTITION_FIELDS* order by *PARTITION_FIELDS*, *PK_FIELDS* ) - 1)) 
		//  / greatest(1,(COUNT(*) OVER (partition by *PARTITION_FIELDS* ORDER BY  (1=1)  ASC  ) - 1))
//		@DBColumn //(rank - 1) / (total partition rows - 1)
//		DBNumber fakePercentileRank = new DBNumber(AnyExpression.fakePercentageRank());
	}

	@Test
	public void testCurrentTime() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addConditions(marq.column(marq.creationDate).toLocalDate().isBetween(LocalDateExpression.currentLocalDate(), LocalDateExpression.nullLocalDate()));

		List<DBQueryRow> got = query.getAllRows();

		Assert.assertThat(got.size(), is(0));

		database.insert(new Marque(3, "False", 1246974, "", 0, "", "     LADA               ", "", "Y", new Date(), 3, null));

		Marque reportLimitingMarque = new Marque();
		reportLimitingMarque.name.permittedPatternIgnoreCase("% LADA %");
		CurrentDateReport currentDateReport = new CurrentDateReport();
		
		final List<CurrentDateReport> reportRows = DBReport.getRows(database, currentDateReport, reportLimitingMarque);

		Assert.assertThat(reportRows.size(), is(1));

		query = database.getDBQuery(marq);
		final LocalDateExpression now = LocalDateExpression.now();
		query.addConditions(
				marq.column(marq.creationDate).toLocalDate().day()
						.isIn(
								now.day(),
								now.addDays(-1).day(),
								now.addDays(1).day()),
				marq.column(marq.creationDate).toLocalDate().isGreaterThan(march23rd2013LocalDate)
		);

		got = query.getAllRows();

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
				marq.column(marq.creationDate).toLocalDate().year().is(2014));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(0));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().year().is(2013));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));
	}

	@Test
	public void testYearIsFunction() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().yearIs(2014));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(0));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().yearIs(2013));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().yearIs(NumberExpression.value(2014).numberResult()));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(0));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().yearIs(NumberExpression.value(2013)));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));
	}

	@Test
	public void testMonthFunction() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().monthIs(3));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().monthIs(4));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().monthIs(NumberExpression.value(3).numberResult()));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().monthIs(NumberExpression.value(4)));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));
	}

	@Test
	public void testEndOfMonthFunction() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().endOfMonth().day().is(31));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().endOfMonth().day().is(30));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().endOfMonth().day().isNull());
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testFirstOfMonthFunction() throws SQLException {
		LocalDate february28th2013 = LocalDate.of(2013, Month.FEBRUARY, 28);
		LocalDate march2nd2013 = LocalDate.of(2013, Month.MARCH, 2);
		LocalDate march31st2011 = LocalDate.of(2011, Month.MARCH, 31);

		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().firstOfMonth().isBetween(february28th2013, march2nd2013));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(marq.column(marq.creationDate).toLocalDate().firstOfMonth().isBetween(march31st2011, april2nd2011LocalDate));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(marq.column(marq.creationDate).toLocalDate().firstOfMonth().isBetween(LocalDateExpression.value(march31st2011), april2nd2011LocalDate));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(marq.column(marq.creationDate).toLocalDate().firstOfMonth().isBetween(march31st2011, LocalDateExpression.value(april2nd2011LocalDate)));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().firstOfMonth().isNull());
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testIsNotNull() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().firstOfMonth().isNotNull());
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(21));
	}

	@Test
	public void testBetweenInclusiveFunction() throws SQLException {
		LocalDate march1st2013 = LocalDate.of(2013, Month.MARCH, 1);
		LocalDate march2nd2013 = LocalDate.of(2013, Month.MARCH, 2);
		LocalDate april1st2011 = LocalDate.of(2011, Month.APRIL, 1);

		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().firstOfMonth().isBetweenInclusive(march1st2013, march2nd2013));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(marq.column(marq.creationDate).toLocalDate().firstOfMonth().isBetweenInclusive(april1st2011, april2nd2011LocalDate));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(marq.column(marq.creationDate).toLocalDate().firstOfMonth().isBetweenInclusive(LocalDateExpression.value(april1st2011), april2nd2011LocalDate));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(marq.column(marq.creationDate).toLocalDate().firstOfMonth().isBetweenInclusive(april1st2011, LocalDateExpression.value(april2nd2011LocalDate)));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().firstOfMonth().isNull());
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testisGreaterThan() throws SQLException {
		LocalDate february28th2013 = LocalDate.of(2013, Month.FEBRUARY, 28);

		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().firstOfMonth().isGreaterThan(february28th2013));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));
	}

	@Test
	public void testisGreaterThanOrEqual() throws SQLException {
		LocalDate march1st2013 = LocalDate.of(2013, Month.MARCH, 1);

		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().firstOfMonth().isGreaterThanOrEqual(march1st2013));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));
	}

	@Test
	public void testisGreaterThanWithFallback() throws SQLException {
		LocalDate february28th2013 = LocalDate.of(2013, Month.FEBRUARY, 28);

		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().firstOfMonth().isGreaterThan(february28th2013, marq.column(marq.name).isGreaterThan("T")));

		List<Marque> got = query.getAllInstancesOf(marq);
		
		Assert.assertThat(got.size(), is(18));
	}

	@Test
	public void testisLessThan() throws SQLException {
		LocalDate march1st2013 = LocalDate.of(2013, Month.MARCH, 1);

		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().firstOfMonth().isLessThan(march1st2013));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));
	}

	@Test
	public void testisLessThanWithFallback() throws SQLException {
		LocalDate february28th2013 = LocalDate.of(2013, Month.FEBRUARY, 28);

		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().firstOfMonth().isLessThan(february28th2013, marq.column(marq.name).isGreaterThan("T")));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));
	}

	@Test
	public void testBetweenExclusiveFunction() throws SQLException {
		LocalDate february28th2013 = LocalDate.of(2013, Month.FEBRUARY, 28);
		LocalDate march2nd2013 = LocalDate.of(2013, Month.MARCH, 2);
		LocalDate march31st2011 = LocalDate.of(2011, Month.MARCH, 31);

		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().firstOfMonth().isBetweenExclusive(february28th2013, march2nd2013));

		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(marq.column(marq.creationDate).toLocalDate().firstOfMonth().isBetweenExclusive(march31st2011, april2nd2011LocalDate));
		got = query.getAllInstancesOf(marq);
		
		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(marq.column(marq.creationDate).toLocalDate().firstOfMonth().isBetweenExclusive(LocalDateExpression.value(march31st2011), april2nd2011LocalDate));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(marq.column(marq.creationDate).toLocalDate().firstOfMonth().isBetweenExclusive(march31st2011, LocalDateExpression.value(april2nd2011LocalDate)));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().firstOfMonth().isNull());
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(1));

	}

	@Test
	public void testDayOfWeekFunction() throws SQLException {

		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().addMonths(1).dayOfWeek().is(LocalDateExpression.TUESDAY));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().addMonths(1).dayOfWeek().is(LocalDateExpression.MONDAY));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().addMonths(1).dayOfWeek().isNull());
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(1));

	}

	@Test
	public void testDayFunction() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().day().is(23));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().day().is(2));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));
	}

	@Test
	public void testDayIsFunction() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().dayIs(23));
//		query.printSQLForQuery();
		List<Marque> got = query.getAllInstancesOf(marq);
		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().dayIs(2));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().dayIs(NumberExpression.value(2)));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate().dayIs(NumberExpression.value(2).numberResult()));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));
	}

	@Test
	public void testIsInWithNulls() throws SQLException, ParseException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationDate).toLocalDate()
						.isIn((LocalDate) null,
								LocalDate.parse(
										firstDateStr.subSequence(0, firstDateStr.length()),
										LOCALDATETIME_FORMAT
								)
						)
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
				marque.column(marque.creationDate).toLocalDate()
						.isIn(
								(LocalDate) null,
								LocalDate.parse(
										firstDateStr.subSequence(0, firstDateStr.length()),
										LOCALDATETIME_FORMAT
								)
						)
		);

		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(20));
	}

	@Test
	public void testIsIn() throws SQLException, ParseException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationDate).toLocalDate().isIn((LocalDate) null, LocalDate.parse(firstDateStr.subSequence(0, firstDateStr.length()), LOCALDATETIME_FORMAT))
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(19));

		Marque newMarque = new Marque(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", null, 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(23));

		dbQuery.addCondition(marque.column(marque.creationDate).toLocalDate().isIn(april2nd2011LocalDate, march23rd2013LocalDate)
		);

		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(21));
	}

	@Test
	public void testIsInWithList() throws SQLException, ParseException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationDate).toLocalDate()
						.isIn(
								(LocalDate) null,
								LocalDate.parse(firstDateStr.subSequence(0, firstDateStr.length()), LOCALDATETIME_FORMAT))
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(19));

		Marque newMarque = new Marque(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", null, 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(23));

		List<LocalDateExpression> dates = new ArrayList<LocalDateExpression>();
		dates.add(LocalDateExpression.value(march23rd2013LocalDate));
		dates.add(LocalDateExpression.value(april2nd2011LocalDate));

		dbQuery.addCondition(
				marque.column(marque.creationDate).toLocalDate().isIn(dates)
		);

		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(21));
	}

	@Test
	public void testIsWithNulls() throws SQLException, ParseException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationDate).toLocalDate().is((LocalDate) null)
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
				marque.column(marque.creationDate).toLocalDate().is((LocalDate) null)
		);

		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(2));
	}

	@Test
	public void testIsNull() throws SQLException, ParseException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationDate).toLocalDate().isNull()
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
				marque.column(marque.creationDate).toLocalDate().isNull()
		);

		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(2));
	}

	@Test
//	@Ignore
	public void testDayDifferenceFunction() throws SQLException, ParseException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate()
						.daysFrom(
								marq.column(marq.creationDate).toLocalDate().addDays(2))
						.is(2));
		List<Marque> got = query.getAllInstancesOf(marq);

		Marque nonNullMarque = new Marque();
		nonNullMarque.creationDate.excludeNull();
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		LocalDate secondLocalDate = LocalDate.of(2011, Month.APRIL, 2);
//		LocalDate secondLocalDate = LocalDate.parse(AbstractTest.secondDateStr.subSequence(0, secondDateStr.length()), LOCALDATETIME_FORMAT);
		marq = new Marque();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate()
						.daysFrom(secondLocalDate)
						.is(0));
		got = query.getAllInstancesOf(marq);

		Marque secondDateMarques = new Marque();
		Date secondDate = AbstractTest.DATETIME_FORMAT.parse(AbstractTest.secondDateStr);
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
				marq.column(marq.creationDate).toLocalDate()
						.weeksFrom(
								marq.column(marq.creationDate).toLocalDate().addWeeks(2))
						.is(2));
		List<Marque> got = query.getAllInstancesOf(marq);

		Marque nonNullMarque = new Marque();
		nonNullMarque.creationDate.excludeNull();
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		LocalDate secondLocalDate = LocalDate.of(2011, Month.APRIL, 2);
//		LocalDate secondLocalDate = LocalDate.parse(AbstractTest.secondDateStr.subSequence(0, secondDateStr.length()), LOCALDATETIME_FORMAT);
		Date secondDate = AbstractTest.DATETIME_FORMAT.parse(AbstractTest.secondDateStr);
		marq = new Marque();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate()
						.weeksFrom(secondLocalDate)
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
				marq.column(marq.creationDate).toLocalDate()
						.monthsFrom(
								marq.column(marq.creationDate).toLocalDate().addMonths(2))
						.is(2));
		List<Marque> got = query.getAllInstancesOf(marq);

		Marque nonNullMarque = new Marque();
		nonNullMarque.creationDate.excludeNull();
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		LocalDate secondLocalDate = LocalDate.of(2011, Month.APRIL, 2);
//		LocalDate secondLocalDate = LocalDate.parse(AbstractTest.secondDateStr.subSequence(0, secondDateStr.length()), LOCALDATETIME_FORMAT);
		Date secondDate = AbstractTest.DATETIME_FORMAT.parse(AbstractTest.secondDateStr);
		marq = new Marque();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate()
						.monthsFrom(secondLocalDate)
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
				marq.column(marq.creationDate).toLocalDate()
						.yearsFrom(
								marq.column(marq.creationDate).toLocalDate().addYears(2))
						.is(2));
		List<Marque> got = query.getAllInstancesOf(marq);

		Marque nonNullMarque = new Marque();
		nonNullMarque.creationDate.excludeNull();
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		LocalDate secondLocalDate = LocalDate.of(2011, Month.APRIL, 2);
//		LocalDate secondLocalDate = LocalDate.parse(AbstractTest.secondDateStr.subSequence(0, secondDateStr.length()), LOCALDATETIME_FORMAT);
		Date secondDate = AbstractTest.DATETIME_FORMAT.parse(AbstractTest.secondDateStr);
		marq = new Marque();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDate()
						.yearsFrom(secondLocalDate)
						.is(0));
		got = query.getAllInstancesOf(marq);

		Marque secondDateMarques = new Marque();
		secondDateMarques.creationDate.permittedValues(secondDate);
		int numberOfSecondDateRows
				= database
						.getDBTable(secondDateMarques)
						.setBlankQueryAllowed(true)
						.count()
						.intValue();
		Assert.assertThat(got.size(), is(numberOfSecondDateRows));
	}

	@Test
	public void testEndOfMonthCalculation() throws SQLException {

		MarqueWithEndOfMonthColumn marq = new MarqueWithEndOfMonthColumn();
		DBTable<MarqueWithEndOfMonthColumn> table = database.getDBTable(marq);
		List<MarqueWithEndOfMonthColumn> allRows = table.setBlankQueryAllowed(true).getAllRows();

		Assert.assertThat(allRows.size(), is(22));
		final LocalDate march31st2013 = LocalDate.of(2013, Month.MARCH, 31);
		final LocalDate april30th2011 = LocalDate.of(2011, Month.APRIL, 30);
		final LocalDate march1st2013 = LocalDate.of(2013, Month.MARCH, 1);
		final LocalDate april1st2011 = LocalDate.of(2011, Month.APRIL, 1);
		final LocalDate nullDate = null;
		for (MarqueWithEndOfMonthColumn allRow : allRows) {
			Assert.assertThat(allRow.endOfMonth.localDateValue(),
					anyOf(
							is(nullDate),
							is(march31st2013),
							is(april30th2011)
					));
			Assert.assertThat(allRow.firstOfMonth.getValue(),
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
					row.get(marq).endOfMonth.getValue(),
					is(march31st2013));
			Assert.assertThat(
					row.get(marq).firstOfMonth.getValue(),
					is(march1st2013)
			);
		}
	}

	public static class MarqueWithEndOfMonthColumn extends Marque {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBLocalDate firstOfMonth = new DBLocalDate(this.column(this.creationDate).toLocalDate().addDays(this.column(this.creationDate).toLocalDate().day().minus(1).bracket().times(-1)));
		@DBColumn
		DBLocalDate endOfMonth = this.column(this.creationDate).toLocalDate().endOfMonth().asExpressionColumn();
		@DBColumn
		DBLocalDate shouldBreakPostgreSQL = new DBLocalDate(this.column(this.creationDate).toLocalDate().addDays(this.column(this.creationDate).day().minus(1).times(-1)).addMonths(1).addDays(-1));
	}
}
