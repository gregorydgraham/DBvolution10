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
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.datatypes.DBLocalDateTime;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBLocalDate;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

public class LocalDateTimeExpressionTest extends AbstractTest {

	public LocalDateTimeExpressionTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	public static class CurrentDateReport extends DBReport {

		private static final long serialVersionUID = 1L;
		public Marque marque = new Marque();
		@DBColumn
		public DBString name = new DBString(marque.column(marque.name));
		@DBColumn
		public DBLocalDateTime savedJavaDate = new DBLocalDateTime(marque.column(marque.creationDate).toLocalDateTime());
		@DBColumn
		public DBLocalDateTime actualJavaDate = new DBLocalDateTime(LocalDateTime.now());
		@DBColumn
		public DBLocalDateTime currentDate = new DBLocalDateTime(LocalDateTimeExpression.now());
		@DBColumn
		public DBNumber dayFromCurrentDate = new DBNumber(LocalDateTimeExpression.now().day());
		@DBColumn
		public DBLocalDateTime currentDateTime = new DBLocalDateTime(LocalDateTimeExpression.currentTime());
		@DBColumn
		public DBLocalDateTime currentDateTimeMinus10Seconds = new DBLocalDateTime(LocalDateTimeExpression.now().addSeconds(-10));
		@DBColumn
		public DBLocalDateTime currentDateTimePlus10Seconds = new DBLocalDateTime(LocalDateTimeExpression.now().addSeconds(10));
		@DBColumn
		public DBLocalDateTime created = marque.column(marque.creationDate).toLocalDateTime().asExpressionColumn();
		@DBColumn
		public DBLocalDateTime currentTime = LocalDateTimeExpression.currentTime().asExpressionColumn();
	}

	LocalDateTime march23rd2013LocalDateTime = (new GregorianCalendar(2013, 2, 23, 12, 34, 56)).toZonedDateTime().toLocalDateTime();
	LocalDateTime april2nd2011LocalDateTime = (new GregorianCalendar(2011, 3, 2, 1, 2, 3)).toZonedDateTime().toLocalDateTime();

	@Test
	public void testIsNotDateExpression() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		final LocalDateTimeExpression fiveDaysPriorToCreation = marq.column(marq.creationDate).toLocalDateTime().addDays(-5);
		query.addCondition(
				LocalDateTimeExpression.leastOf(
						marq.column(marq.creationDate).toLocalDateTime(),
						fiveDaysPriorToCreation,
						LocalDateTimeExpression.value(march23rd2013LocalDateTime).addWeeks(-5),
						LocalDateTimeExpression.value(march23rd2013LocalDateTime).addDays(-2))
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
		final LocalDateTimeExpression fiveDaysPriorToCreation = marq.column(marq.creationDate).toLocalDateTime().addDays(-5);
		query.addCondition(
				LocalDateTimeExpression.leastOf(
						marq.column(marq.creationDate).toLocalDateTime(),
						fiveDaysPriorToCreation,
						LocalDateTimeExpression.value(march23rd2013LocalDateTime).addWeeks(-5),
						LocalDateTimeExpression.value(march23rd2013LocalDateTime).addDays(-2))
						.is(fiveDaysPriorToCreation)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testLeastOfWithList() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		
		final LocalDateTimeExpression creationDate = marq.column(marq.creationDate).toLocalDateTime();
		final LocalDateTimeExpression fiveDaysPriorToCreation = creationDate.addDays(-5);
		
		List<LocalDateTimeExpression> poss = new ArrayList<>();
		poss.add(creationDate);
		poss.add(fiveDaysPriorToCreation);
		poss.add(LocalDateTimeExpression.value(march23rd2013LocalDateTime).addWeeks(-5));
		poss.add(LocalDateTimeExpression.value(march23rd2013LocalDateTime).addDays(-2));
		
		query.addCondition(
				LocalDateTimeExpression.leastOf(
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
				LocalDateTimeExpression.leastOf(
						march23rd2013LocalDateTime,
						april2nd2011LocalDateTime)
						.is(april2nd2011LocalDateTime)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(22));
	}

	@Test
	public void testGreatestOf() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		final LocalDateTimeExpression creationDate = marq.column(marq.creationDate).toLocalDateTime();
		final LocalDateTimeExpression fiveDaysPriorToCreation = creationDate.addDays(-5);
		query.addCondition(
				LocalDateTimeExpression.greatestOf(
						creationDate,
						fiveDaysPriorToCreation,
						LocalDateTimeExpression.value(march23rd2013LocalDateTime).addWeeks(-5),
						LocalDateTimeExpression.value(march23rd2013LocalDateTime).addDays(-2))
						.is(creationDate)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(18));
	}

	@Test
	public void testGreatestOfWithList() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		final LocalDateTimeExpression creationDate = marq.column(marq.creationDate).toLocalDateTime();
		final LocalDateTimeExpression fiveDaysPriorToCreation = creationDate.addDays(-5);
		List<LocalDateTimeExpression> poss = new ArrayList<>();
		poss.add(creationDate);
		poss.add(fiveDaysPriorToCreation);
		poss.add(LocalDateTimeExpression.value(march23rd2013LocalDateTime).addWeeks(-5));
		poss.add(LocalDateTimeExpression.value(march23rd2013LocalDateTime).addDays(-2));
		query.addCondition(
				LocalDateTimeExpression.greatestOf(
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
				LocalDateTimeExpression.greatestOf(
						march23rd2013LocalDateTime,
						april2nd2011LocalDateTime)
						.is(march23rd2013LocalDateTime)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(22));
	}

	@Test
	public void testOverlapsDateExpressionDateResult() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(LocalDateTimeExpression.overlaps(
				marq.column(marq.creationDate).toLocalDateTime(),
				marq.column(marq.creationDate).toLocalDateTime().addDays(-5),
				LocalDateTimeExpression.value(march23rd2013LocalDateTime).addWeeks(-5),
				LocalDateTimeExpression.value(march23rd2013LocalDateTime).addDays(-2))
		);
		List<DBQueryRow> allRows = query.getAllRows();
		Assert.assertThat(allRows.size(), is(18));
	}

	@Test
	public void testOverlapsAllDateResults() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(LocalDateTimeExpression.overlaps(
				marq.column(marq.creationDate).toLocalDateTime(),
				marq.column(marq.creationDate).toLocalDateTime().addDays(-5),
				LocalDateTimeExpression.value(march23rd2013LocalDateTime).addWeeks(-5),
				LocalDateTimeExpression.value(march23rd2013LocalDateTime).addDays(-2))
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
		Assert.assertThat(got.maxOfDates.localDateTimeValue(), is(march23rd2013LocalDateTime));
		Assert.assertThat(got.minOfDates.getValue(), is(april2nd2011LocalDateTime));

	}

	public static class MarqueWithDateAggregators extends Marque {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBNumber countOfDates = new DBNumber(this.column(this.creationDate).count());
		@DBColumn
		DBLocalDateTime maxOfDates = new DBLocalDateTime(this.column(this.creationDate).toLocalDateTime().max());
		@DBColumn
		DBLocalDateTime minOfDates = new DBLocalDateTime(this.column(this.creationDate).toLocalDateTime().min());

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
		expectedValues.add(new Object[]{2, march23rd2013LocalDateTime, march23rd2013LocalDateTime});
		expectedValues.add(new Object[]{2, march23rd2013LocalDateTime, march23rd2013LocalDateTime});
		expectedValues.add(new Object[]{1, march23rd2013LocalDateTime, march23rd2013LocalDateTime});
		expectedValues.add(new Object[]{3, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{3, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{3, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		for (int i = 0; i < allRows.size(); i++) {
			got = allRows.get(i).get(marq);
			Object[] expect = expectedValues.get(i);
			Assert.assertThat(got.countOfDates.intValue(), is((Integer) expect[0]));
			Assert.assertThat(got.maxOfAll.getValue(), is((LocalDateTime) expect[1]));
			Assert.assertThat(got.maxOfDates.getValue(), is((LocalDateTime) expect[1]));
			Assert.assertThat(got.minOfDates.getValue(), is((LocalDateTime) expect[2]));
		}
	}

	public static class MarqueWithDateWindowingFunctions extends Marque {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBNumber countOfAllRows = new DBNumber(this.column(this.creationDate).count().over().allRows());
		@DBColumn
		DBNumber rowNumber = new DBNumber(this.column(this.creationDate).count().over().AllRowsAndOrderBy(this.column(this.carCompany).ascending()));
		@DBColumn
		DBNumber countOfDates = new DBNumber(this.column(this.creationDate).count().over().partition(this.column(this.carCompany)).unordered());
		@DBColumn
		DBNumber rowWithinCarCo = new DBNumber(this.column(this.creationDate).count()
				.over()
				.partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).ascending())
				.defaultFrame());
		@DBColumn
		DBLocalDateTime maxOfAll = new DBLocalDateTime(this.column(this.creationDate).toLocalDateTime().max().over().allRows());
		@DBColumn
		DBLocalDateTime maxOfDates = new DBLocalDateTime(this.column(this.creationDate).toLocalDateTime().max().over().partition(this.column(this.carCompany)).unordered());
		@DBColumn
		DBLocalDateTime minOfDates = new DBLocalDateTime(this.column(this.creationDate).toLocalDateTime().min().over().partition(this.column(this.carCompany)).unordered());
		@DBColumn
		DBLocalDateTime test1 = new DBLocalDateTime(this.column(this.creationDate)
				.toLocalDateTime()
				.min()
				.over().partition(this.column(this.carCompany))
				.unsorted());
		@DBColumn
		DBLocalDateTime test2 = new DBLocalDateTime(this.column(this.creationDate).toLocalDateTime().min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().unboundedPreceding().currentRow());
		@DBColumn
		DBLocalDateTime test3 = new DBLocalDateTime(this.column(this.creationDate).toLocalDateTime().min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().unboundedPreceding().currentRow());
		@DBColumn
		DBLocalDateTime test4 = new DBLocalDateTime(this.column(this.creationDate).toLocalDateTime().min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().currentRow().toCurrentRow());
		@DBColumn
		DBLocalDateTime test5 = new DBLocalDateTime(this.column(this.creationDate).toLocalDateTime().min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().offsetPrecedingAndCurrentRow(5));
		@DBColumn
		DBLocalDateTime test6 = new DBLocalDateTime(this.column(this.creationDate).toLocalDateTime().min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().currentRow().unboundedFollowing());
		@DBColumn
		DBLocalDateTime test7 = new DBLocalDateTime(this.column(this.creationDate).toLocalDateTime().min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().currentRow().forFollowing(3));
		@DBColumn
		DBLocalDateTime test8 = new DBLocalDateTime(this.column(this.creationDate).toLocalDateTime().min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().unboundedPreceding().unboundedFollowing());
		@DBColumn
		DBLocalDateTime test9 = new DBLocalDateTime(this.column(this.creationDate).toLocalDateTime().min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().unboundedPreceding().forFollowing(1));
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
		expectedValues.add(new Object[]{2, march23rd2013LocalDateTime, march23rd2013LocalDateTime});
		expectedValues.add(new Object[]{2, march23rd2013LocalDateTime, march23rd2013LocalDateTime});
		expectedValues.add(new Object[]{1, march23rd2013LocalDateTime, march23rd2013LocalDateTime});
		expectedValues.add(new Object[]{3, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{3, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{3, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		for (int i = 0; i < allRows.size(); i++) {
			got = allRows.get(i).get(marq);
			Object[] expect = expectedValues.get(i);
			Assert.assertThat(got.countOfDates.intValue(), is((Integer) expect[0]));
			Assert.assertThat(got.maxOfDates.getValue(), is((LocalDateTime) expect[1]));
			Assert.assertThat(got.minOfDates.getValue(), is((LocalDateTime) expect[2]));
		}
	}

	public static class MarqueWithAggregatorAndDateWindowingFunctions extends Marque {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBNumber countAggregator = new DBNumber(this.column(this.creationDate).count());
		@DBColumn
		DBNumber countOfAllRows = new DBNumber(this.column(this.creationDate).count().over().allRows());
		@DBColumn
		DBNumber countOfDates = new DBNumber(this.column(this.creationDate).count()
				.over()
				.partition(this.column(this.carCompany)).unordered());
		@DBColumn
		DBLocalDateTime maxOfAll = new DBLocalDateTime(this.column(this.creationDate)
				.toLocalDateTime().max().over().allRows());
		@DBColumn
		DBLocalDateTime maxOfDates = new DBLocalDateTime(this.column(this.creationDate)
				.toLocalDateTime().max().over()
				.partition(this.column(this.carCompany)).unordered());
		@DBColumn
		DBLocalDateTime minOfDates = new DBLocalDateTime(this.column(this.creationDate)
				.toLocalDateTime().min()
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
		expectedValues.add(new Object[]{2, march23rd2013LocalDateTime, march23rd2013LocalDateTime});
		expectedValues.add(new Object[]{2, march23rd2013LocalDateTime, march23rd2013LocalDateTime});
		expectedValues.add(new Object[]{1, march23rd2013LocalDateTime, march23rd2013LocalDateTime});
		expectedValues.add(new Object[]{3, march23rd2013LocalDateTime, april2nd2011});
		expectedValues.add(new Object[]{3, march23rd2013LocalDateTime, april2nd2011});
		expectedValues.add(new Object[]{3, march23rd2013LocalDateTime, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011});
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
		query.addConditions(marq.column(marq.creationDate).toLocalDateTime().isBetween(LocalDateTimeExpression.currentLocalDateTime(), LocalDateTimeExpression.nullLocalDateTime()));

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
				marq.column(marq.creationDate).toLocalDateTime()
						.day().isIn(
								LocalDateTimeExpression.now().addSeconds(-100).day(),
								LocalDateTimeExpression.now().addSeconds(100).day()),
				marq.column(marq.creationDate).toLocalDateTime().isGreaterThan(march23rd2013LocalDateTime)
		);

		got = query.getAllRows();

		Assert.assertThat(got.size(), is(1));

	}

	@Test
	public void testCurrentDateTimeAndAddSecondsFunctions() throws SQLException {

		Marque marq = new Marque();
		marq.creationDate.permittedRangeInclusive(new Date(), null);
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
		LocalDateTime march1st2013 = (new GregorianCalendar(2013, 2, 1)).toZonedDateTime().toLocalDateTime();
		LocalDateTime march2nd2013 = (new GregorianCalendar(2013, 2, 2)).toZonedDateTime().toLocalDateTime();
		LocalDateTime april1st2011 = (new GregorianCalendar(2011, 3, 1)).toZonedDateTime().toLocalDateTime();

		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDateTime().firstOfMonth().isBetween(march1st2013, march2nd2013));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDateTime().firstOfMonth().isBetween(april1st2011, april2nd2011LocalDateTime));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDateTime().firstOfMonth().isBetween(LocalDateTimeExpression.value(april1st2011), april2nd2011LocalDateTime));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDateTime().firstOfMonth().isBetween(april1st2011, LocalDateTimeExpression.value(april2nd2011LocalDateTime)));
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
		LocalDateTime march1st2013 = (new GregorianCalendar(2013, 2, 1)).toZonedDateTime().toLocalDateTime();
		LocalDateTime march2nd2013 = (new GregorianCalendar(2013, 2, 2)).toZonedDateTime().toLocalDateTime();
		LocalDateTime april1st2011 = (new GregorianCalendar(2011, 3, 1)).toZonedDateTime().toLocalDateTime();

		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDateTime().firstOfMonth().isBetweenInclusive(march1st2013, march2nd2013));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDateTime().firstOfMonth().isBetweenInclusive(april1st2011, april2nd2011LocalDateTime));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDateTime().firstOfMonth().isBetweenInclusive(LocalDateTimeExpression.value(april1st2011), april2nd2011LocalDateTime));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDateTime().firstOfMonth().isBetweenInclusive(april1st2011, LocalDateTimeExpression.value(april2nd2011LocalDateTime)));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDateTime().firstOfMonth().isNull());
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testisGreaterThan() throws SQLException {
		LocalDateTime march1st2013 = (new GregorianCalendar(2013, 2, 1)).toZonedDateTime().toLocalDateTime();
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDateTime().firstOfMonth().isGreaterThan(march1st2013));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));
	}

	@Test
	public void testisGreaterThanOrEqual() throws SQLException {
		LocalDateTime march1st2013 = (new GregorianCalendar(2013, 2, 1)).toZonedDateTime().toLocalDateTime();
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDateTime().firstOfMonth().isGreaterThanOrEqual(march1st2013));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));
	}

	@Test
	public void testisGreaterThanWithFallback() throws SQLException {
		LocalDateTime march1st2013 = (new GregorianCalendar(2013, 2, 1)).toZonedDateTime().toLocalDateTime();
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDateTime().firstOfMonth().isGreaterThan(march1st2013, marq.column(marq.name).isGreaterThan("T")));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));
	}

	@Test
	public void testisLessThan() throws SQLException {
		LocalDateTime march1st2013 = (new GregorianCalendar(2013, 2, 1)).toZonedDateTime().toLocalDateTime();
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDateTime().firstOfMonth().isLessThan(march1st2013));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));
	}

	@Test
	public void testisLessThanWithFallback() throws SQLException {
		LocalDateTime march1st2013 = (new GregorianCalendar(2013, 2, 1)).toZonedDateTime().toLocalDateTime();
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDateTime().firstOfMonth().isLessThan(march1st2013, marq.column(marq.name).isGreaterThan("T")));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));
	}

	@Test
	public void testBetweenExclusiveFunction() throws SQLException {
		LocalDateTime march1st2013 = (new GregorianCalendar(2013, 2, 1)).toZonedDateTime().toLocalDateTime();
		final LocalDateTime march2nd2013 = (new GregorianCalendar(2013, 2, 2)).toZonedDateTime().toLocalDateTime();
		final LocalDateTime april1st2011 = (new GregorianCalendar(2011, 3, 1)).toZonedDateTime().toLocalDateTime();

		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDateTime().firstOfMonth().isBetweenExclusive(march1st2013, march2nd2013));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDateTime().firstOfMonth().isBetweenExclusive(april1st2011, april2nd2011LocalDateTime));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDateTime().firstOfMonth().isBetweenExclusive(LocalDateTimeExpression.value(april1st2011), april2nd2011LocalDateTime));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDateTime().firstOfMonth().isBetweenExclusive(april1st2011, LocalDateTimeExpression.value(april2nd2011LocalDateTime)));
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
				marq.column(marq.creationDate).addMonths(1).dayOfWeek().is(LocalDateTimeExpression.TUESDAY));
		List<Marque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).addMonths(1).dayOfWeek().is(LocalDateTimeExpression.MONDAY));
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
				marque.column(marque.creationDate).toLocalDateTime()
						.isIn((LocalDateTime) null,
								LocalDateTime.parse(
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
				marque.column(marque.creationDate).toLocalDateTime()
						.isIn(
								(LocalDateTime) null,
								LocalDateTime.parse(
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
				marque.column(marque.creationDate).toLocalDateTime().isIn((LocalDateTime) null, LocalDateTime.parse(firstDateStr.subSequence(0, firstDateStr.length()), LOCALDATETIME_FORMAT))
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
				marque.column(marque.creationDate).toLocalDateTime().isIn(april2nd2011LocalDateTime, march23rd2013LocalDateTime)
		);

		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(21));
	}

	@Test
	public void testIsInWithList() throws SQLException, ParseException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationDate).toLocalDateTime()
						.isIn(
								(LocalDateTime) null,
								LocalDateTime.parse(firstDateStr.subSequence(0, firstDateStr.length()), LOCALDATETIME_FORMAT))
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(19));

		Marque newMarque = new Marque(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", null, 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(23));

		List<LocalDateTimeExpression> dates = new ArrayList<LocalDateTimeExpression>();
		dates.add(LocalDateTimeExpression.value(march23rd2013LocalDateTime));
		dates.add(LocalDateTimeExpression.value(april2nd2011LocalDateTime));

		dbQuery.addCondition(
				marque.column(marque.creationDate).toLocalDateTime().isIn(dates)
		);

		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(21));
	}

	@Test
	public void testIsWithNulls() throws SQLException, ParseException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationDate).toLocalDateTime().is((LocalDateTime) null)
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
				marque.column(marque.creationDate).toLocalDateTime().is((LocalDateTime) null)
		);

		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(2));
	}

	@Test
	public void testIsNull() throws SQLException, ParseException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationDate).toLocalDateTime().isNull()
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
				marque.column(marque.creationDate).toLocalDateTime().isNull()
		);

		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(2));
	}

	public static class DiffTestReport extends DBReport {

		private static final long serialVersionUID = 1L;

		public Marque marq = new Marque();
		public DBLocalDateTime dayNormal = new DBLocalDateTime(
				marq.column(marq.creationDate).toLocalDateTime());
		public DBLocalDateTime dayAdds = new DBLocalDateTime(
				marq.column(marq.creationDate).toLocalDateTime().addDays(2));
		public DBNumber dayDiff = new DBNumber(marq.column(marq.creationDate).toLocalDateTime().daysFrom(
				marq.column(marq.creationDate).toLocalDateTime().addDays(2)));
		public DBNumber dayDiffAsHours = new DBNumber(marq.column(marq.creationDate).toLocalDateTime().hoursFrom(
				marq.column(marq.creationDate).toLocalDateTime().addDays(2)));
		public DBNumber monthDiff = new DBNumber(
				marq.column(marq.creationDate).toLocalDateTime()
						.monthsFrom(
								marq.column(marq.creationDate).toLocalDateTime()
										.addMonths(2)));
	}

	@Test
//	@Ignore
	public void testDayDifferenceFunction() throws SQLException, ParseException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDateTime()
						.daysFrom(
								marq.column(marq.creationDate).toLocalDateTime().addDays(2))
						.is(2));
		List<Marque> got = query.getAllInstancesOf(marq);

		Marque nonNullMarque = new Marque();
		nonNullMarque.creationDate.excludeNull();
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		marq = new Marque();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDateTime()
						.hoursFrom(
								marq.column(marq.creationDate).toLocalDateTime().addDays(2))
						.isIn(48, 49)); //Interestingly  one of my examples is near DST transition and NuoDB gives 49 hours
		got = query.getAllInstancesOf(marq);

		nonNullMarque = new Marque();
		nonNullMarque.creationDate.permitOnlyNotNull();
		numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		LocalDateTime secondLocalDateTime = LocalDateTime.of(2011, Month.APRIL, 2, 1, 2, 3);
//		LocalDateTime secondLocalDateTime = LocalDateTime.parse(AbstractTest.secondDateStr.subSequence(0, secondDateStr.length()), LOCALDATETIME_FORMAT);
		marq = new Marque();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDateTime()
						.daysFrom(secondLocalDateTime)
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
				marq.column(marq.creationDate).toLocalDateTime()
						.weeksFrom(
								marq.column(marq.creationDate).toLocalDateTime().addWeeks(2))
						.is(2));
		List<Marque> got = query.getAllInstancesOf(marq);

		Marque nonNullMarque = new Marque();
		nonNullMarque.creationDate.excludeNull();
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		LocalDateTime secondLocalDateTime = LocalDateTime.of(2011, Month.APRIL, 2, 1, 2, 3);
//		LocalDateTime secondLocalDateTime = LocalDateTime.parse(AbstractTest.secondDateStr.subSequence(0, secondDateStr.length()), LOCALDATETIME_FORMAT);
		Date secondDate = AbstractTest.DATETIME_FORMAT.parse(AbstractTest.secondDateStr);
		marq = new Marque();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDateTime()
						.weeksFrom(secondLocalDateTime)
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
				marq.column(marq.creationDate).toLocalDateTime()
						.monthsFrom(
								marq.column(marq.creationDate).toLocalDateTime().addMonths(2))
						.is(2));
		List<Marque> got = query.getAllInstancesOf(marq);

		Marque nonNullMarque = new Marque();
		nonNullMarque.creationDate.excludeNull();
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		LocalDateTime secondLocalDateTime = LocalDateTime.of(2011, Month.APRIL, 2, 1, 2, 3);
//		LocalDateTime secondLocalDateTime = LocalDateTime.parse(AbstractTest.secondDateStr.subSequence(0, secondDateStr.length()), LOCALDATETIME_FORMAT);
		Date secondDate = AbstractTest.DATETIME_FORMAT.parse(AbstractTest.secondDateStr);
		marq = new Marque();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDateTime()
						.monthsFrom(secondLocalDateTime)
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
				marq.column(marq.creationDate).toLocalDateTime()
						.yearsFrom(
								marq.column(marq.creationDate).toLocalDateTime().addYears(2))
						.is(2));
		List<Marque> got = query.getAllInstancesOf(marq);

		Marque nonNullMarque = new Marque();
		nonNullMarque.creationDate.excludeNull();
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		LocalDateTime secondLocalDateTime = LocalDateTime.of(2011, Month.APRIL, 2, 1, 2, 3);
//		LocalDateTime secondLocalDateTime = LocalDateTime.parse(AbstractTest.secondDateStr.subSequence(0, secondDateStr.length()), LOCALDATETIME_FORMAT);
		Date secondDate = AbstractTest.DATETIME_FORMAT.parse(AbstractTest.secondDateStr);
		marq = new Marque();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDateTime()
						.yearsFrom(secondLocalDateTime)
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
//	@Ignore
	public void testHourDifferenceFunction() throws SQLException, ParseException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDateTime()
						.hoursFrom(
								marq.column(marq.creationDate).toLocalDateTime().addHours(2))
						.is(2));
		List<Marque> got = query.getAllInstancesOf(marq);

		Marque nonNullMarque = new Marque();
		nonNullMarque.creationDate.excludeNull();
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		LocalDateTime secondLocalDateTime = LocalDateTime.of(2011, Month.APRIL, 2, 1, 2, 3);
//		LocalDateTime secondLocalDateTime = LocalDateTime.parse(AbstractTest.secondDateStr.subSequence(0, secondDateStr.length()), LOCALDATETIME_FORMAT);
		Date secondDate = AbstractTest.DATETIME_FORMAT.parse(AbstractTest.secondDateStr);
		marq = new Marque();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDateTime()
						.hoursFrom(secondLocalDateTime)
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
				marq.column(marq.creationDate).toLocalDateTime()
						.minutesFrom(
								marq.column(marq.creationDate).toLocalDateTime().addMinutes(2))
						.is(2));
		List<Marque> got = query.getAllInstancesOf(marq);

		Marque nonNullMarque = new Marque();
		nonNullMarque.creationDate.excludeNull();
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		LocalDateTime secondLocalDateTime = LocalDateTime.of(2011, Month.APRIL, 2, 1, 2, 3);
//		LocalDateTime secondLocalDateTime = LocalDateTime.parse(AbstractTest.secondDateStr.subSequence(0, secondDateStr.length()), LOCALDATETIME_FORMAT);
		Date secondDate = AbstractTest.DATETIME_FORMAT.parse(AbstractTest.secondDateStr);
		marq = new Marque();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDateTime()
						.minutesFrom(secondLocalDateTime)
						.is(0));
		got = query.getAllInstancesOf(marq);

		Marque secondDateMarques = new Marque();
		secondDateMarques.creationDate.permittedValues(secondDate);
		int numberOfSecondDateRows = database.getDBTable(secondDateMarques).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfSecondDateRows));
	}

	@Test
	public void testSecondsDifferenceFunction() throws SQLException, ParseException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationDate).toLocalDateTime()
						.secondsFrom(
								marq.column(marq.creationDate).toLocalDateTime().addSeconds(2))
						.is(2));
		List<Marque> got = query.getAllInstancesOf(marq);

		Marque nonNullMarque = new Marque();
		nonNullMarque.creationDate.excludeNull();
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		LocalDateTime secondLocalDateTime = LocalDateTime.of(2011, Month.APRIL, 2, 1, 2, 3);
//		LocalDateTime secondLocalDateTime = LocalDateTime.parse(AbstractTest.secondDateStr.subSequence(0, secondDateStr.length()), LOCALDATETIME_FORMAT);
//		Date secondDate = AbstractTest.DATETIME_FORMAT.parse(AbstractTest.secondDateStr);
		Date secondDate = new SimpleDateFormat("dd/MMM/yyyy HH:mm:ss", Locale.getDefault()).parse(secondDateStr);
		marq = new Marque();
		query = database.getDBQuery(marq).setQueryLabel("FIND CREATION DATE WITHIN 1 SECOND OF SECOND DATE");
		query.addCondition(
				marq.column(marq.creationDate).toLocalDateTime()
						.secondsFrom(secondLocalDateTime)
						.is(0));

		got = query.getAllInstancesOf(marq);

		Marque secondDateMarques = new Marque();
		secondDateMarques.creationDate.permittedValues(secondDate);
		int numberOfSecondDateRows = database.getDBTable(secondDateMarques)
				.setQueryLabel("CHECK SECOND DATE CAN BE COUNTED")
				.setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfSecondDateRows));
	}

	public static class MarqueWithSecondsFromDate extends Marque {

		private static final long serialVersionUID = 1L;

		LocalDateTime date = Instant.ofEpochMilli(10L).atZone(ZoneId.systemDefault()).toLocalDateTime();
		@DBColumn
		DBNumber subseconds = new DBNumber(LocalDateTimeExpression.value(date).toLocalDateTime().subsecond());
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
		MarqueWithEndOfMonthOfLocalDateTimeColumn marq = new MarqueWithEndOfMonthOfLocalDateTimeColumn();
		DBTable<MarqueWithEndOfMonthOfLocalDateTimeColumn> table = database.getDBTable(marq);
		List<MarqueWithEndOfMonthOfLocalDateTimeColumn> allRows = table.setBlankQueryAllowed(true).getAllRows();

		Assert.assertThat(allRows.size(), is(22));
		final LocalDate march31st2013 = LocalDate.of(2013, Month.MARCH, 31);
		final LocalDate april30th2011 = LocalDate.of(2011, Month.APRIL, 30);
		final LocalDateTime march1st2013 = LocalDateTime.of(2013, Month.MARCH, 1, 12, 34, 56);
		final LocalDateTime april1st2011 = LocalDateTime.of(2011, Month.APRIL, 1, 1, 2, 3);
		final LocalDateTime nullDate = null;
		for (MarqueWithEndOfMonthOfLocalDateTimeColumn allRow : allRows) {
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

	public static class MarqueWithEndOfMonthOfLocalDateTimeColumn extends Marque {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBLocalDateTime firstOfMonth = new DBLocalDateTime(this.column(this.creationDate).toLocalDateTime().addDays(this.column(this.creationDate).toLocalDateTime().day().minus(1).bracket().times(-1)));
		@DBColumn
		DBLocalDate endOfMonth = this.column(this.creationDate).toLocalDateTime().endOfMonth().asExpressionColumn();
		@DBColumn
		DBLocalDateTime shouldBreakPostgreSQL = new DBLocalDateTime(this.column(this.creationDate).toLocalDateTime().addDays(this.column(this.creationDate).day().minus(1).times(-1)).addMonths(1).addDays(-1));
	}
}
