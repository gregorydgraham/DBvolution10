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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.DBLocalDateTime;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.exceptions.AccidentalCartesianJoinException;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Before;
import org.junit.Test;

public class LocalDateTimeExpressionTest extends AbstractTest {

	public LocalDateTimeExpressionTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	public static class CurrentDateReport extends DBReport {

		private static final long serialVersionUID = 1L;
		public MarqueWithLocalDateTime marque = new MarqueWithLocalDateTime();
		@DBColumn
		public DBString name = new DBString(marque.column(marque.name));
		@DBColumn
		public DBLocalDateTime savedJavaDate = new DBLocalDateTime(marque.column(marque.creationLocalDateTime));
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
		public DBLocalDateTime created = marque.column(marque.creationLocalDateTime).asExpressionColumn();
		@DBColumn
		public DBLocalDateTime currentTime = LocalDateTimeExpression.currentTime().asExpressionColumn();
	}

	LocalDateTime march23rd2013LocalDateTime = (new GregorianCalendar(2013, 2, 23, 12, 34, 56)).toZonedDateTime().toLocalDateTime();
	LocalDateTime april2nd2011LocalDateTime = (new GregorianCalendar(2011, 3, 2, 1, 2, 3)).toZonedDateTime().toLocalDateTime();

	@Test
	public void testIsNotDateExpression() throws SQLException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		final LocalDateTimeExpression fiveDaysPriorToCreation = marq.column(marq.creationLocalDateTime).addDays(-5);
		query.addCondition(
				LocalDateTimeExpression.leastOf(
						marq.column(marq.creationLocalDateTime),
						fiveDaysPriorToCreation,
						LocalDateTimeExpression.value(march23rd2013LocalDateTime).addWeeks(-5),
						LocalDateTimeExpression.value(march23rd2013LocalDateTime).addDays(-2))
						.isNot(fiveDaysPriorToCreation)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(18));
	}

	@Test
	public void testIsNotDate() throws SQLException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);

		query.addCondition(
				marq.column(marq.creationLocalDateTime).isNot(april2nd2011LocalDateTime)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(18));
	}

	@Test
	public void testLeastOf() throws SQLException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		final LocalDateTimeExpression fiveDaysPriorToCreation = marq.column(marq.creationLocalDateTime).addDays(-5);
		query.addCondition(
				LocalDateTimeExpression.leastOf(
						marq.column(marq.creationLocalDateTime),
						fiveDaysPriorToCreation,
						LocalDateTimeExpression.value(march23rd2013LocalDateTime).addWeeks(-5),
						LocalDateTimeExpression.value(march23rd2013LocalDateTime).addDays(-2))
						.is(fiveDaysPriorToCreation)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(3));
	}

	@Test
	public void testLeastOfWithList() throws SQLException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq).setQueryLabel("testLeastOfWithList").setBlankQueryAllowed(true);

		query.addCondition(marq.column(marq.creationLocalDateTime).isNotNull());
		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(21));

		final LocalDateTimeExpression creationLocalDateTime = marq.column(marq.creationLocalDateTime);
		final LocalDateTimeExpression fiveDaysPriorToCreation = creationLocalDateTime.addDays(-5);

		List<LocalDateTimeExpression> poss = new ArrayList<>();
		poss.add(creationLocalDateTime);
		poss.add(fiveDaysPriorToCreation);
		poss.add(LocalDateTimeExpression.value(march23rd2013LocalDateTime).addWeeks(-5));
		poss.add(LocalDateTimeExpression.value(march23rd2013LocalDateTime).addDays(-2));

		query.addCondition(
				LocalDateTimeExpression.leastOf(
						poss)
						.is(fiveDaysPriorToCreation)
		);

		allRows = query.getAllRows();

		assertThat(allRows.size(), is(3));
	}

	@Test
	public void testLeastOfWithDates() throws SQLException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq).setQueryLabel("testLeastOfWithDates").setBlankQueryAllowed(true);

		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(22));

		query.addCondition(
				LocalDateTimeExpression.leastOf(
						march23rd2013LocalDateTime,
						march23rd2013LocalDateTime)
						.is(march23rd2013LocalDateTime)
		);
		allRows = query.getAllRows();

		assertThat(allRows.size(), is(22));

		query.setConditions(
				LocalDateTimeExpression.leastOf(
						march23rd2013LocalDateTime,
						april2nd2011LocalDateTime)
						.is(april2nd2011LocalDateTime)
		);
		allRows = query.getAllRows();

		assertThat(allRows.size(), is(22));
	}

	@Test
	public void testGreatestOf() throws SQLException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		final LocalDateTimeExpression creationLocalDateTime = marq.column(marq.creationLocalDateTime);
		final LocalDateTimeExpression fiveDaysPriorToCreation = creationLocalDateTime.addDays(-5);
		query.addCondition(
				LocalDateTimeExpression.greatestOf(
						creationLocalDateTime,
						fiveDaysPriorToCreation,
						LocalDateTimeExpression.value(march23rd2013LocalDateTime).addWeeks(-5),
						LocalDateTimeExpression.value(march23rd2013LocalDateTime).addDays(-2))
						.is(creationLocalDateTime)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(18));
	}

	@Test
	public void testGreatestOfWithList() throws SQLException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		final LocalDateTimeExpression creationLocalDateTime = marq.column(marq.creationLocalDateTime);
		final LocalDateTimeExpression fiveDaysPriorToCreation = creationLocalDateTime.addDays(-5);
		List<LocalDateTimeExpression> poss = new ArrayList<>();
		poss.add(creationLocalDateTime);
		poss.add(fiveDaysPriorToCreation);
		poss.add(LocalDateTimeExpression.value(march23rd2013LocalDateTime).addWeeks(-5));
		poss.add(LocalDateTimeExpression.value(march23rd2013LocalDateTime).addDays(-2));
		query.addCondition(
				LocalDateTimeExpression.greatestOf(
						poss)
						.is(creationLocalDateTime)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(18));
	}

	@Test
	public void testGreatestOfWithDates() throws SQLException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				LocalDateTimeExpression.greatestOf(
						march23rd2013LocalDateTime,
						april2nd2011LocalDateTime)
						.is(march23rd2013LocalDateTime)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(22));
	}

	@Test
	public void testOverlapsDateExpressionDateResult() throws SQLException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(LocalDateTimeExpression.overlaps(
				marq.column(marq.creationLocalDateTime),
				marq.column(marq.creationLocalDateTime).addDays(-5),
				LocalDateTimeExpression.value(march23rd2013LocalDateTime).addWeeks(-5),
				LocalDateTimeExpression.value(march23rd2013LocalDateTime).addDays(-2))
		);
		List<DBQueryRow> allRows = query.getAllRows();
		assertThat(allRows.size(), is(18));
	}

	@Test
	public void testOverlapsAllDateResults() throws SQLException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(LocalDateTimeExpression.overlaps(
				marq.column(marq.creationLocalDateTime),
				marq.column(marq.creationLocalDateTime).addDays(-5),
				LocalDateTimeExpression.value(march23rd2013LocalDateTime).addWeeks(-5),
				LocalDateTimeExpression.value(march23rd2013LocalDateTime).addDays(-2))
		);
		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(18));
	}

	@Test
	public void testAggregators() throws SQLException {
		MarqueWithDateAggregators marq = new MarqueWithDateAggregators();
		DBQuery query = database.getDBQuery(marq).setBlankQueryAllowed(true);
		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(1));
		MarqueWithDateAggregators got = allRows.get(0).get(marq);
		assertThat(got.countOfDates.intValue(), is(21));
		assertThat(got.maxOfDates.localDateTimeValue(), is(march23rd2013LocalDateTime));
		assertThat(got.minOfDates.getValue(), is(april2nd2011LocalDateTime));

	}

	public static class MarqueWithDateAggregators extends MarqueWithLocalDateTime {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBNumber countOfDates = new DBNumber(this.column(this.creationLocalDateTime).count());
		@DBColumn
		DBLocalDateTime maxOfDates = new DBLocalDateTime(this.column(this.creationLocalDateTime).max());
		@DBColumn
		DBLocalDateTime minOfDates = new DBLocalDateTime(this.column(this.creationLocalDateTime).min());

		{
			this.setReturnFields(this.countOfDates, this.maxOfDates, this.minOfDates);
		}
	}

	@Test
	public void testWindowingFunctions() throws SQLException {
		MarqueWithDateWindowingFunctions marq = new MarqueWithDateWindowingFunctions();

		DBQuery query = database.getDBQuery(marq)
				.setBlankQueryAllowed(true)
				.setSortOrder(
						marq.column(marq.carCompany).ascending(),
						marq.column(marq.uidMarque).ascending()
				);

		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(22));

		MarqueWithDateWindowingFunctions got;// = allRows.get(0).get(marq);
		ArrayList<Object[]> expectedValues = new ArrayList<>();
		expectedValues.add(new Object[]{2, march23rd2013LocalDateTime, march23rd2013LocalDateTime, null, march23rd2013LocalDateTime});
		expectedValues.add(new Object[]{2, march23rd2013LocalDateTime, march23rd2013LocalDateTime, march23rd2013LocalDateTime, march23rd2013LocalDateTime});
		expectedValues.add(new Object[]{1, march23rd2013LocalDateTime, march23rd2013LocalDateTime, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{3, march23rd2013LocalDateTime, april2nd2011LocalDateTime, march23rd2013LocalDateTime, march23rd2013LocalDateTime});
		expectedValues.add(new Object[]{3, march23rd2013LocalDateTime, april2nd2011LocalDateTime, april2nd2011LocalDateTime, march23rd2013LocalDateTime});
		expectedValues.add(new Object[]{3, march23rd2013LocalDateTime, april2nd2011LocalDateTime, march23rd2013LocalDateTime, null});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime, march23rd2013LocalDateTime, march23rd2013LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime, null, march23rd2013LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime, march23rd2013LocalDateTime, march23rd2013LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime, march23rd2013LocalDateTime, march23rd2013LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime, march23rd2013LocalDateTime, march23rd2013LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime, march23rd2013LocalDateTime, march23rd2013LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime, march23rd2013LocalDateTime, march23rd2013LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime, march23rd2013LocalDateTime, march23rd2013LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime, april2nd2011LocalDateTime, march23rd2013LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime, march23rd2013LocalDateTime, march23rd2013LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime, march23rd2013LocalDateTime, march23rd2013LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime, march23rd2013LocalDateTime, march23rd2013LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime, march23rd2013LocalDateTime, march23rd2013LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime, march23rd2013LocalDateTime, april2nd2011LocalDateTime});
		expectedValues.add(new Object[]{15, march23rd2013LocalDateTime, april2nd2011LocalDateTime, march23rd2013LocalDateTime, null});
		for (int i = 0; i < allRows.size(); i++) {
			got = allRows.get(i).get(marq);
//			System.out.println("" + got.toString());
			Object[] expect = expectedValues.get(i);
			assertThat(got.countOfDates.intValue(), is((Integer) expect[0]));
			assertThat(got.maxOfAll.getValue(), is((LocalDateTime) expect[1]));
			assertThat(got.maxOfDates.getValue(), is((LocalDateTime) expect[1]));
			assertThat(got.minOfDates.getValue(), is((LocalDateTime) expect[2]));
			assertThat(got.lag1.getValue(), is((LocalDateTime) expect[3]));
			assertThat(got.lead1.getValue(), is((LocalDateTime) expect[4]));
		}
	}

	public static class MarqueWithDateWindowingFunctions extends MarqueWithLocalDateTime {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBNumber countOfAllRows = new DBNumber(this.column(this.creationLocalDateTime).count().over().allRows());
		@DBColumn
		DBNumber rowNumber = new DBNumber(this.column(this.creationLocalDateTime).count().over().AllRowsAndOrderBy(this.column(this.carCompany).ascending()));
		@DBColumn
		DBNumber countOfDates = new DBNumber(this.column(this.creationLocalDateTime).count().over().partition(this.column(this.carCompany)).unordered());
		@DBColumn
		DBNumber rowWithinCarCo = new DBNumber(this.column(this.creationLocalDateTime).count()
				.over()
				.partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).ascending())
				.defaultFrame());
		@DBColumn
		DBLocalDateTime maxOfAll = new DBLocalDateTime(this.column(this.creationLocalDateTime).max().over().allRows());
		@DBColumn
		DBLocalDateTime maxOfDates = new DBLocalDateTime(this.column(this.creationLocalDateTime).max().over().partition(this.column(this.carCompany)).unordered());
		@DBColumn
		DBLocalDateTime minOfDates = new DBLocalDateTime(this.column(this.creationLocalDateTime).min().over().partition(this.column(this.carCompany)).unordered());
		@DBColumn
		DBLocalDateTime test1 = new DBLocalDateTime(this.column(this.creationLocalDateTime)
				.min()
				.over().partition(this.column(this.carCompany))
				.unsorted());
		@DBColumn
		DBLocalDateTime test2 = new DBLocalDateTime(this.column(this.creationLocalDateTime).min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().unboundedPreceding().currentRow());
		@DBColumn
		DBLocalDateTime test3 = new DBLocalDateTime(this.column(this.creationLocalDateTime).min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().unboundedPreceding().currentRow());
		@DBColumn
		DBLocalDateTime test4 = new DBLocalDateTime(this.column(this.creationLocalDateTime).min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().currentRow().toCurrentRow());
		@DBColumn
		DBLocalDateTime test5 = new DBLocalDateTime(this.column(this.creationLocalDateTime).min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().offsetPrecedingAndCurrentRow(5));
		@DBColumn
		DBLocalDateTime test6 = new DBLocalDateTime(this.column(this.creationLocalDateTime).min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().currentRow().unboundedFollowing());
		@DBColumn
		DBLocalDateTime test7 = new DBLocalDateTime(this.column(this.creationLocalDateTime).min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().currentRow().forFollowing(3));
		@DBColumn
		DBLocalDateTime test8 = new DBLocalDateTime(this.column(this.creationLocalDateTime).min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().unboundedPreceding().unboundedFollowing());
		@DBColumn
		DBLocalDateTime test9 = new DBLocalDateTime(this.column(this.creationLocalDateTime).min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().unboundedPreceding().forFollowing(1));
		@DBColumn
		DBLocalDateTime lag1
				= this.column(this.creationLocalDateTime)
						.previousRowValue()
						.allRows()
						.orderBy(this.column(this.carCompany).ascending(), this.column(this.uidMarque).ascending())
						.asExpressionColumn();
		@DBColumn
		DBLocalDateTime lead1 = new DBLocalDateTime(this.column(
				this.creationLocalDateTime)
				.nextRowValue()
				.AllRowsAndOrderBy(
						this.column(this.carCompany).ascending(),
						this.column(this.uidMarque).ascending()
				)
		);
	}

	@Test
	public void testAggregatorWithWindowingFunctions() throws SQLException {
		MarqueWithAggregatorAndDateWindowingFunctions marq = new MarqueWithAggregatorAndDateWindowingFunctions();

		DBQuery query = database.getDBQuery(marq)
				.setBlankQueryAllowed(true)
				.setSortOrder(
						marq.column(marq.carCompany),
						marq.column(marq.name)
				);

		List<DBQueryRow> allRows = query.getAllRows();

//		query.printAllRows();
		assertThat(allRows.size(), is(22));

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
//			System.out.println("testAggregatorWithWindowingFunctions: " + got.toString());
			assertThat(got.countOfDates.intValue(), is((Integer) expect[0]));
			assertThat(got.maxOfDates.getValue(), is((LocalDateTime) expect[1]));
			assertThat(got.minOfDates.getValue(), is((LocalDateTime) expect[2]));
		}
	}

	public static class MarqueWithAggregatorAndDateWindowingFunctions extends MarqueWithLocalDateTime {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBNumber countAggregator = new DBNumber(this.column(this.creationLocalDateTime).count());
		@DBColumn
		DBNumber countOfAllRows = new DBNumber(this.column(this.creationLocalDateTime).count().over().allRows());
		@DBColumn
		DBNumber countOfDates = new DBNumber(this.column(this.creationLocalDateTime).count()
				.over()
				.partition(this.column(this.carCompany)).unordered());
		@DBColumn
		DBLocalDateTime maxOfAll = new DBLocalDateTime(this.column(this.creationLocalDateTime)
				.max().over().allRows());
		@DBColumn
		DBLocalDateTime maxOfDates = new DBLocalDateTime(this.column(this.creationLocalDateTime)
				.max().over()
				.partition(this.column(this.carCompany)).unordered());
		@DBColumn
		DBLocalDateTime minOfDates = new DBLocalDateTime(this.column(this.creationLocalDateTime)
				.min()
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

		assertThat(allRows.size(), is(22));

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

	public static class MarqueWithComplexWindowingFunction extends MarqueWithLocalDateTime {

		private static final long serialVersionUID = 1L;

		@DBColumn //(rank - 1) / (total partition rows - 1)
		DBNumber percentileRank = AnyExpression.percentageRank().AllRowsAndOrderBy(column(carCompany).ascending()).asExpressionColumn();
	}

	@Test
	public void testCheckDatabaseLocalDateTime() throws UnexpectedNumberOfRowsException, AccidentalCartesianJoinException, AccidentalBlankQueryException, SQLException {
		
		LocalDateTime databaseLocalDateTime = database.getCurrentLocalDatetime();

		final LocalDateTime applicationLocalDateTime = LocalDateTime.now();
		final LocalDateTime buffered = applicationLocalDateTime.minusMinutes(10);

		assertThat(databaseLocalDateTime, is(greaterThan(buffered)));

	}

	@Test
	public void testCurrentTime() throws SQLException {

		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addConditions(marq.column(marq.creationLocalDateTime).isBetween(LocalDateTimeExpression.currentLocalDateTime(), LocalDateTimeExpression.nullLocalDateTime()));

		List<DBQueryRow> got = query.getAllRows();

		assertThat(got.size(), is(0));

		database.insert(new MarqueWithLocalDateTime(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", LocalDateTime.now(), 3, null));

		MarqueWithLocalDateTime reportLimitingMarque = new MarqueWithLocalDateTime();
		reportLimitingMarque.name.permittedPatternIgnoreCase("% HUMMER %");
		CurrentDateReport currentDateReport = new CurrentDateReport();

		final List<CurrentDateReport> reportRows = DBReport.getRows(database, currentDateReport, reportLimitingMarque);

		assertThat(reportRows.size(), is(1));

		query = database.getDBQuery(marq);
		query.addConditions(
				marq.column(marq.creationLocalDateTime)
						.day().isIn(
								LocalDateTimeExpression.now().addSeconds(-100).day(),
								LocalDateTimeExpression.now().addSeconds(100).day()),
				marq.column(marq.creationLocalDateTime).isGreaterThan(march23rd2013LocalDateTime)
		);

		got = query.getAllRows();

		assertThat(got.size(), is(1));

	}

	@Test
	public void testCurrentDateTimeAndAddSecondsFunctions() throws SQLException {

		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		marq.creationLocalDateTime.permittedRangeInclusive(LocalDateTime.now(), null);
		List<MarqueWithLocalDateTime> got = database.get(marq);

		assertThat(got.size(), is(0));

		database.insert(new MarqueWithLocalDateTime(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", LocalDateTime.now(), 3, null));

		MarqueWithLocalDateTime reportLimitingMarque = new MarqueWithLocalDateTime();
		reportLimitingMarque.name.permittedPatternIgnoreCase("% HUMMER %");
		CurrentDateReport currentDateReport = new CurrentDateReport();
		final List<CurrentDateReport> reportRows = DBReport.getRows(database, currentDateReport, reportLimitingMarque);

		assertThat(reportRows.size(), is(1));

		marq.creationLocalDateTime.permittedRangeInclusive(LocalDateTimeExpression.currentLocalDateTime().addSeconds(-10), null);
		got = database.get(marq);

		assertThat(got.size(), is(1));

		marq.creationLocalDateTime.permittedRangeInclusive(null, LocalDateTimeExpression.currentLocalDateTime().addSeconds(-10));
		got = database.get(marq);

		assertThat(got.size(), is(21));

		marq.creationLocalDateTime.permittedRangeInclusive(
				LocalDateTimeExpression.currentLocalDateTime().addMinutes(-5), null);
		got = database.getDBTable(marq).getAllRows();

		assertThat(got.size(), is(1));
	}

	@Test
	public void testCurrentDateAndAddDaysFunctions() throws SQLException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		marq.creationLocalDateTime.permittedRangeInclusive(LocalDateTimeExpression.currentLocalDateTime(), null);
		List<MarqueWithLocalDateTime> got = database.get(marq);

		assertThat(got.size(), is(0));

		database.insert(new MarqueWithLocalDateTime(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", LocalDateTime.now(), 3, null));
		marq.creationLocalDateTime.permittedRangeInclusive(LocalDateTimeExpression.currentLocalDateTime().addDays(-1), null);
		got = database.get(marq);

		assertThat(got.size(), is(1));

		marq.creationLocalDateTime.permittedRangeInclusive(null, LocalDateTimeExpression.currentLocalDateTime().addDays(-1));
		got = database.get(marq);

		assertThat(got.size(), is(21));

		marq.creationLocalDateTime.permittedRangeInclusive(LocalDateTimeExpression.currentLocalDateTime().addDays(-1), LocalDateTimeExpression.currentLocalDateTime().addDays(+1));
		got = database.get(marq);

		assertThat(got.size(), is(1));
	}

	@Test
	public void testCurrentDateAndAddHoursFunctions() throws SQLException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		marq.creationLocalDateTime.permittedRangeInclusive(LocalDateTimeExpression.currentLocalDateTime(), null);
		List<MarqueWithLocalDateTime> got = database.get(marq);

		assertThat(got.size(), is(0));

		database.insert(new MarqueWithLocalDateTime(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", LocalDateTime.now(), 3, null));
		marq.creationLocalDateTime.permittedRangeInclusive(LocalDateTimeExpression.currentLocalDateTime().addHours(-1), null);
		got = database.get(marq);

		assertThat(got.size(), is(1));
	}

	@Test
	public void testCurrentDateAndAddWeeksFunctions() throws SQLException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		marq.creationLocalDateTime.permittedRangeInclusive(LocalDateTimeExpression.currentLocalDateTime(), null);
		List<MarqueWithLocalDateTime> got = database.get(marq);

		assertThat(got.size(), is(0));

		database.insert(new MarqueWithLocalDateTime(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", LocalDateTime.now(), 3, null));
		marq.creationLocalDateTime.permittedRangeInclusive(LocalDateTimeExpression.currentLocalDateTime().addWeeks(-1), null);
		got = database.get(marq);

		assertThat(got.size(), is(1));
	}

	@Test
	public void testCurrentDateAndAddMinutesFunctions() throws SQLException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		marq.creationLocalDateTime.permittedRangeInclusive(LocalDateTimeExpression.currentLocalDateTime(), null);
		List<MarqueWithLocalDateTime> got = database.get(marq);

		assertThat(got.size(), is(0));

		database.insert(new MarqueWithLocalDateTime(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", LocalDateTime.now(), 3, null));
		marq.creationLocalDateTime.permittedRangeInclusive(LocalDateTimeExpression.currentLocalDateTime().addMinutes(-1), null);
		got = database.get(marq);

		assertThat(got.size(), is(1));
	}

	@Test
	public void testCurrentDateAndAddMonthsFunctions() throws SQLException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		marq.creationLocalDateTime.permittedRangeInclusive(LocalDateTimeExpression.currentLocalDateTime(), null);
		List<MarqueWithLocalDateTime> got = database.get(marq);

		assertThat(got.size(), is(0));

		database.insert(new MarqueWithLocalDateTime(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", LocalDateTime.now(), 3, null));
		marq.creationLocalDateTime.permittedRangeInclusive(LocalDateTimeExpression.currentLocalDateTime().addMonths(-1), null);
		got = database.get(marq);

		assertThat(got.size(), is(1));
	}

	@Test
	public void testCurrentDateAndAddYearsFunctions() throws SQLException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		marq.creationLocalDateTime.permittedRangeInclusive(LocalDateTimeExpression.currentLocalDateTime(), null);
		List<MarqueWithLocalDateTime> got = database.get(marq);

		assertThat(got.size(), is(0));

		database.insert(new MarqueWithLocalDateTime(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", LocalDateTime.now(), 3, null));
		marq.creationLocalDateTime.permittedRangeInclusive(LocalDateTimeExpression.currentLocalDateTime().addYears(-1), null);
		got = database.get(marq);

		assertThat(got.size(), is(1));
	}

	@Test
	public void testYearFunction() throws SQLException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).year().is(2014));
		List<MarqueWithLocalDateTime> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(0));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).year().is(2013));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));
	}

	@Test
	public void testYearIsFunction() throws SQLException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).yearIs(2014));
		List<MarqueWithLocalDateTime> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(0));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).yearIs(2013));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).yearIs(NumberExpression.value(2014).numberResult()));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(0));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).yearIs(NumberExpression.value(2013)));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));
	}

	@Test
	public void testMonthFunction() throws SQLException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).monthIs(3));
		List<MarqueWithLocalDateTime> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).monthIs(4));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).monthIs(NumberExpression.value(3).numberResult()));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).monthIs(NumberExpression.value(4)));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));
	}

	@Test
	public void testEndOfMonthFunction() throws SQLException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).endOfMonth().day().is(31));
		List<MarqueWithLocalDateTime> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).endOfMonth().day().is(30));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).endOfMonth().day().isNull());
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(1));
	}

	@Test
	public void testFirstOfMonthFunction() throws SQLException {
		LocalDateTime march1st2013 = (new GregorianCalendar(2013, 2, 1)).toZonedDateTime().toLocalDateTime();
		LocalDateTime march2nd2013 = (new GregorianCalendar(2013, 2, 2)).toZonedDateTime().toLocalDateTime();
		LocalDateTime april1st2011 = (new GregorianCalendar(2011, 3, 1)).toZonedDateTime().toLocalDateTime();

		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).firstOfMonth().isBetween(march1st2013, march2nd2013));
		List<MarqueWithLocalDateTime> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).firstOfMonth().isBetween(april1st2011, april2nd2011LocalDateTime));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).firstOfMonth().isBetween(LocalDateTimeExpression.value(april1st2011), april2nd2011LocalDateTime));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).firstOfMonth().isBetween(april1st2011, LocalDateTimeExpression.value(april2nd2011LocalDateTime)));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).firstOfMonth().isNull());
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(1));
	}

	@Test
	public void testIsNotNull() throws SQLException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq).setQueryLabel("testIsNotNull");
		query.addCondition(
				marq.column(marq.creationLocalDateTime).firstOfMonth().isNotNull());
		List<MarqueWithLocalDateTime> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(21));
	}

	@Test
	public void testBetweenInclusiveFunction() throws SQLException {
		LocalDateTime march1st2013 = (new GregorianCalendar(2013, 2, 1)).toZonedDateTime().toLocalDateTime();
		LocalDateTime march2nd2013 = (new GregorianCalendar(2013, 2, 2)).toZonedDateTime().toLocalDateTime();
		LocalDateTime april1st2011 = (new GregorianCalendar(2011, 3, 1)).toZonedDateTime().toLocalDateTime();

		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).firstOfMonth().isBetweenInclusive(march1st2013, march2nd2013));
		List<MarqueWithLocalDateTime> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).firstOfMonth().isBetweenInclusive(april1st2011, april2nd2011LocalDateTime));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).firstOfMonth().isBetweenInclusive(LocalDateTimeExpression.value(april1st2011), april2nd2011LocalDateTime));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).firstOfMonth().isBetweenInclusive(april1st2011, LocalDateTimeExpression.value(april2nd2011LocalDateTime)));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).firstOfMonth().isNull());
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(1));
	}

	@Test
	public void testisGreaterThan() throws SQLException {
		LocalDateTime march1st2013 = (new GregorianCalendar(2013, 2, 1)).toZonedDateTime().toLocalDateTime();
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).firstOfMonth().isGreaterThan(march1st2013));
		List<MarqueWithLocalDateTime> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));
	}

	@Test
	public void testisGreaterThanOrEqual() throws SQLException {
		LocalDateTime march1st2013 = (new GregorianCalendar(2013, 2, 1)).toZonedDateTime().toLocalDateTime();
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).firstOfMonth().isGreaterThanOrEqual(march1st2013));
		List<MarqueWithLocalDateTime> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));
	}

	@Test
	public void testisGreaterThanWithFallback() throws SQLException {
		LocalDateTime march1st2013 = (new GregorianCalendar(2013, 2, 1)).toZonedDateTime().toLocalDateTime();
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).firstOfMonth().isGreaterThan(march1st2013, marq.column(marq.name).isGreaterThan("T")));
		List<MarqueWithLocalDateTime> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));
	}

	@Test
	public void testisLessThan() throws SQLException {
		LocalDateTime march1st2013 = (new GregorianCalendar(2013, 2, 1)).toZonedDateTime().toLocalDateTime();
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).firstOfMonth().isLessThan(march1st2013));
		List<MarqueWithLocalDateTime> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));
	}

	@Test
	public void testisLessThanWithFallback() throws SQLException {
		LocalDateTime march1st2013 = (new GregorianCalendar(2013, 2, 1)).toZonedDateTime().toLocalDateTime();
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).firstOfMonth().isLessThan(march1st2013, marq.column(marq.name).isGreaterThan("T")));
		List<MarqueWithLocalDateTime> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));
	}

	@Test
	public void testBetweenExclusiveFunction() throws SQLException {
		LocalDateTime march1st2013 = (new GregorianCalendar(2013, 2, 1)).toZonedDateTime().toLocalDateTime();
		final LocalDateTime march2nd2013 = (new GregorianCalendar(2013, 2, 2)).toZonedDateTime().toLocalDateTime();
		final LocalDateTime april1st2011 = (new GregorianCalendar(2011, 3, 1)).toZonedDateTime().toLocalDateTime();

		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).firstOfMonth().isBetweenExclusive(march1st2013, march2nd2013));
		List<MarqueWithLocalDateTime> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).firstOfMonth().isBetweenExclusive(april1st2011, april2nd2011LocalDateTime));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).firstOfMonth().isBetweenExclusive(LocalDateTimeExpression.value(april1st2011), april2nd2011LocalDateTime));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).firstOfMonth().isBetweenExclusive(april1st2011, LocalDateTimeExpression.value(april2nd2011LocalDateTime)));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).firstOfMonth().isNull());
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(1));

	}

	@Test
	public void testDayOfWeekFunction() throws SQLException {

		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).addMonths(1).dayOfWeek().is(LocalDateTimeExpression.TUESDAY));
		List<MarqueWithLocalDateTime> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).addMonths(1).dayOfWeek().is(LocalDateTimeExpression.MONDAY));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).addMonths(1).dayOfWeek().isNull());
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(1));

	}

	@Test
	public void testDayFunction() throws SQLException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).day().is(23));
		List<MarqueWithLocalDateTime> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).day().is(2));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));
	}

	@Test
	public void testDayIsFunction() throws SQLException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).dayIs(23));
		List<MarqueWithLocalDateTime> got = query.getAllInstancesOf(marq);
		assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).dayIs(2));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).dayIs(NumberExpression.value(2)));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).dayIs(NumberExpression.value(2).numberResult()));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));
	}

	@Test
	public void testHourFunction() throws SQLException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).hour().is(12));
		List<MarqueWithLocalDateTime> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).hour().is(1));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));
	}

	@Test
	public void testHourIsFunction() throws SQLException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).hourIs(12));
		List<MarqueWithLocalDateTime> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).hourIs(1));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).hourIs(NumberExpression.value(1).numberResult()));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).hourIs(NumberExpression.value(1)));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));
	}

	@Test
	public void testMinuteFunction() throws SQLException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).minute().is(34));
		List<MarqueWithLocalDateTime> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).minute().is(2));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));
	}

	@Test
	public void testMinuteIsFunction() throws SQLException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).minuteIs(34));
		List<MarqueWithLocalDateTime> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).minuteIs(2));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).minuteIs(NumberExpression.value(2).numberResult()));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).minuteIs(NumberExpression.value(2)));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));
	}

	@Test
	public void testSecondFunction() throws SQLException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).second().is(56));
		List<MarqueWithLocalDateTime> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).second().is(3));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));
	}

	@Test
	public void testSecondIsFunction() throws SQLException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).secondIs(56));
		List<MarqueWithLocalDateTime> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).secondIs(3));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime).secondIs(NumberExpression.value(3)));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));
	}

	@Test
	public void testIsInWithNulls() throws SQLException, ParseException {
		MarqueWithLocalDateTime marque = new MarqueWithLocalDateTime();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationLocalDateTime)
						.isIn((LocalDateTime) null,
								LocalDateTime.parse(
										firstDateStr.subSequence(0, firstDateStr.length()),
										LOCALDATETIME_FORMAT
								)
						)
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		assertThat(allRows.size(), is(19));

		MarqueWithLocalDateTime newMarque = new MarqueWithLocalDateTime(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", null, 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(23));

		dbQuery.addCondition(
				marque.column(marque.creationLocalDateTime)
						.isIn(
								(LocalDateTime) null,
								LocalDateTime.parse(
										firstDateStr.subSequence(0, firstDateStr.length()),
										LOCALDATETIME_FORMAT
								)
						)
		);

		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(20));
	}

	@Test
	public void testIsIn() throws SQLException, ParseException {
		MarqueWithLocalDateTime marque = new MarqueWithLocalDateTime();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationLocalDateTime).isIn((LocalDateTime) null, LocalDateTime.parse(firstDateStr.subSequence(0, firstDateStr.length()), LOCALDATETIME_FORMAT))
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(19));

		MarqueWithLocalDateTime newMarque = new MarqueWithLocalDateTime(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", null, 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(23));

		dbQuery.addCondition(
				marque.column(marque.creationLocalDateTime).isIn(april2nd2011LocalDateTime, march23rd2013LocalDateTime)
		);

		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(21));
	}

	@Test
	public void testIsInWithList() throws SQLException, ParseException {
		MarqueWithLocalDateTime marque = new MarqueWithLocalDateTime();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationLocalDateTime)
						.isIn(
								(LocalDateTime) null,
								LocalDateTime.parse(firstDateStr.subSequence(0, firstDateStr.length()), LOCALDATETIME_FORMAT))
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(19));

		MarqueWithLocalDateTime newMarque = new MarqueWithLocalDateTime(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", null, 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(23));

		List<LocalDateTimeExpression> dates = new ArrayList<LocalDateTimeExpression>();
		dates.add(LocalDateTimeExpression.value(march23rd2013LocalDateTime));
		dates.add(LocalDateTimeExpression.value(april2nd2011LocalDateTime));

		dbQuery.addCondition(
				marque.column(marque.creationLocalDateTime).isIn(dates)
		);

		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(21));
	}

	@Test
	public void testIsWithNulls() throws SQLException, ParseException {
		MarqueWithLocalDateTime marque = new MarqueWithLocalDateTime();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationLocalDateTime).is((LocalDateTime) null)
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));

		MarqueWithLocalDateTime newMarque = new MarqueWithLocalDateTime(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", null, 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(23));

		dbQuery.addCondition(
				marque.column(marque.creationLocalDateTime).is((LocalDateTime) null)
		);

		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(2));
	}

	@Test
	public void testIsNull() throws SQLException, ParseException {
		MarqueWithLocalDateTime marque = new MarqueWithLocalDateTime();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationLocalDateTime).isNull()
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));

		MarqueWithLocalDateTime newMarque = new MarqueWithLocalDateTime(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", null, 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(23));

		dbQuery.addCondition(
				marque.column(marque.creationLocalDateTime).isNull()
		);

		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(2));
	}

	public static class DiffTestReport extends DBReport {

		private static final long serialVersionUID = 1L;

		public MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		public DBLocalDateTime dayNormal = new DBLocalDateTime(
				marq.column(marq.creationLocalDateTime));
		public DBLocalDateTime dayAdds = new DBLocalDateTime(
				marq.column(marq.creationLocalDateTime).addDays(2));
		public DBNumber dayDiff = new DBNumber(marq.column(marq.creationLocalDateTime).daysFrom(
				marq.column(marq.creationLocalDateTime).addDays(2)));
		public DBNumber dayDiffAsHours = new DBNumber(marq.column(marq.creationLocalDateTime).hoursFrom(
				marq.column(marq.creationLocalDateTime).addDays(2)));
		public DBNumber monthDiff = new DBNumber(
				marq.column(marq.creationLocalDateTime)
						.monthsFrom(
								marq.column(marq.creationLocalDateTime)
										.addMonths(2)));
	}

	@Test
//	@Ignore
	public void testDayDifferenceFunction() throws SQLException, ParseException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime)
						.daysFrom(
								marq.column(marq.creationLocalDateTime).addDays(2))
						.is(2));
		List<MarqueWithLocalDateTime> got = query.getAllInstancesOf(marq);

		MarqueWithLocalDateTime nonNullMarque = new MarqueWithLocalDateTime();
		nonNullMarque.creationLocalDateTime.excludeNull();
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		assertThat(got.size(), is(numberOfRowsWithACreationDate));

		marq = new MarqueWithLocalDateTime();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime)
						.hoursFrom(
								marq.column(marq.creationLocalDateTime).addDays(2))
						.isIn(48, 49)); //Interestingly  one of my examples is near DST transition and NuoDB gives 49 hours
		got = query.getAllInstancesOf(marq);

		nonNullMarque = new MarqueWithLocalDateTime();
		nonNullMarque.creationLocalDateTime.permitOnlyNotNull();
		numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		assertThat(got.size(), is(numberOfRowsWithACreationDate));

		LocalDateTime secondLocalDateTime = LocalDateTime.of(2011, Month.APRIL, 2, 1, 2, 3);
//		LocalDateTime secondLocalDateTime = LocalDateTime.parse(AbstractTest.secondDateStr.subSequence(0, secondDateStr.length()), LOCALDATETIME_FORMAT);
		marq = new MarqueWithLocalDateTime();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime)
						.daysFrom(secondLocalDateTime)
						.is(0));
		got = query.getAllInstancesOf(marq);

		MarqueWithLocalDateTime secondDateMarques = new MarqueWithLocalDateTime();
//		Date secondDate = AbstractTest.DATETIME_FORMAT.parse(AbstractTest.secondDateStr);
		secondDateMarques.creationLocalDateTime.permittedValues(secondLocalDateTime);
		int numberOfSecondDateRows = database.getDBTable(secondDateMarques).setBlankQueryAllowed(true).count().intValue();
		assertThat(got.size(), is(numberOfSecondDateRows));
	}

	@Test
//	@Ignore
	public void testWeekDifferenceFunction() throws SQLException, ParseException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime)
						.weeksFrom(
								marq.column(marq.creationLocalDateTime).addWeeks(2))
						.is(2));
		List<MarqueWithLocalDateTime> got = query.getAllInstancesOf(marq);

		MarqueWithLocalDateTime nonNullMarque = new MarqueWithLocalDateTime();
		nonNullMarque.creationLocalDateTime.excludeNull();
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		assertThat(got.size(), is(numberOfRowsWithACreationDate));

		LocalDateTime secondLocalDateTime = LocalDateTime.of(2011, Month.APRIL, 2, 1, 2, 3);
//		LocalDateTime secondLocalDateTime = LocalDateTime.parse(AbstractTest.secondDateStr.subSequence(0, secondDateStr.length()), LOCALDATETIME_FORMAT);
//		Date secondDate = AbstractTest.DATETIME_FORMAT.parse(AbstractTest.secondDateStr);
		marq = new MarqueWithLocalDateTime();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime)
						.weeksFrom(secondLocalDateTime)
						.is(0));
		got = query.getAllInstancesOf(marq);

		MarqueWithLocalDateTime secondDateMarques = new MarqueWithLocalDateTime();
		secondDateMarques.creationLocalDateTime.permittedValues(secondLocalDateTime);
		int numberOfSecondDateRows = database.getDBTable(secondDateMarques).setBlankQueryAllowed(true).count().intValue();
		assertThat(got.size(), is(numberOfSecondDateRows));
	}

	@Test
//	@Ignore
	public void testMonthDifferenceFunction() throws SQLException, ParseException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime)
						.monthsFrom(
								marq.column(marq.creationLocalDateTime).addMonths(2))
						.is(2));
		List<MarqueWithLocalDateTime> got = query.getAllInstancesOf(marq);

		MarqueWithLocalDateTime nonNullMarque = new MarqueWithLocalDateTime();
		nonNullMarque.creationLocalDateTime.excludeNull();
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		assertThat(got.size(), is(numberOfRowsWithACreationDate));

		LocalDateTime secondLocalDateTime = LocalDateTime.of(2011, Month.APRIL, 2, 1, 2, 3);
//		LocalDateTime secondLocalDateTime = LocalDateTime.parse(AbstractTest.secondDateStr.subSequence(0, secondDateStr.length()), LOCALDATETIME_FORMAT);
//		Date secondDate = AbstractTest.DATETIME_FORMAT.parse(AbstractTest.secondDateStr);
		marq = new MarqueWithLocalDateTime();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime)
						.monthsFrom(secondLocalDateTime)
						.is(0));
		got = query.getAllInstancesOf(marq);

		MarqueWithLocalDateTime secondDateMarques = new MarqueWithLocalDateTime();
		secondDateMarques.creationLocalDateTime.permittedValues(secondLocalDateTime);
		int numberOfSecondDateRows = database.getDBTable(secondDateMarques).setBlankQueryAllowed(true).count().intValue();
		assertThat(got.size(), is(numberOfSecondDateRows));
	}

	@Test
//	@Ignore
	public void testYearDifferenceFunction() throws SQLException, ParseException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime)
						.yearsFrom(
								marq.column(marq.creationLocalDateTime).addYears(2))
						.is(2));
		List<MarqueWithLocalDateTime> got = query.getAllInstancesOf(marq);

		MarqueWithLocalDateTime nonNullMarque = new MarqueWithLocalDateTime();
		nonNullMarque.creationLocalDateTime.excludeNull();
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		assertThat(got.size(), is(numberOfRowsWithACreationDate));

		LocalDateTime secondLocalDateTime = LocalDateTime.of(2011, Month.APRIL, 2, 1, 2, 3);
//		LocalDateTime secondLocalDateTime = LocalDateTime.parse(AbstractTest.secondDateStr.subSequence(0, secondDateStr.length()), LOCALDATETIME_FORMAT);
//		Date secondDate = AbstractTest.DATETIME_FORMAT.parse(AbstractTest.secondDateStr);
		marq = new MarqueWithLocalDateTime();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime)
						.yearsFrom(secondLocalDateTime)
						.is(0));
		got = query.getAllInstancesOf(marq);

		MarqueWithLocalDateTime secondDateMarques = new MarqueWithLocalDateTime();
		secondDateMarques.creationLocalDateTime.permittedValues(secondLocalDateTime);
		int numberOfSecondDateRows
				= database
						.getDBTable(secondDateMarques)
						.setBlankQueryAllowed(true)
						.count()
						.intValue();
		assertThat(got.size(), is(numberOfSecondDateRows));
	}

	@Test
//	@Ignore
	public void testHourDifferenceFunction() throws SQLException, ParseException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime)
						.hoursFrom(
								marq.column(marq.creationLocalDateTime).addHours(2))
						.is(2));
		List<MarqueWithLocalDateTime> got = query.getAllInstancesOf(marq);

		MarqueWithLocalDateTime nonNullMarque = new MarqueWithLocalDateTime();
		nonNullMarque.creationLocalDateTime.excludeNull();
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		assertThat(got.size(), is(numberOfRowsWithACreationDate));

		LocalDateTime secondLocalDateTime = LocalDateTime.of(2011, Month.APRIL, 2, 1, 2, 3);
//		LocalDateTime secondLocalDateTime = LocalDateTime.parse(AbstractTest.secondDateStr.subSequence(0, secondDateStr.length()), LOCALDATETIME_FORMAT);
//		Date secondDate = AbstractTest.DATETIME_FORMAT.parse(AbstractTest.secondDateStr);
		marq = new MarqueWithLocalDateTime();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime)
						.hoursFrom(secondLocalDateTime)
						.is(0));
		got = query.getAllInstancesOf(marq);

		MarqueWithLocalDateTime secondDateMarques = new MarqueWithLocalDateTime();
		secondDateMarques.creationLocalDateTime.permittedValues(secondLocalDateTime);
		int numberOfSecondDateRows = database.getDBTable(secondDateMarques).setBlankQueryAllowed(true).count().intValue();
		assertThat(got.size(), is(numberOfSecondDateRows));
	}

	@Test
//	@Ignore
	public void testMinutesDifferenceFunction() throws SQLException, ParseException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime)
						.minutesFrom(
								marq.column(marq.creationLocalDateTime).addMinutes(2))
						.is(2));
		List<MarqueWithLocalDateTime> got = query.getAllInstancesOf(marq);

		MarqueWithLocalDateTime nonNullMarque = new MarqueWithLocalDateTime();
		nonNullMarque.creationLocalDateTime.excludeNull();
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		assertThat(got.size(), is(numberOfRowsWithACreationDate));

		LocalDateTime secondLocalDateTime = LocalDateTime.of(2011, Month.APRIL, 2, 1, 2, 3);
//		LocalDateTime secondLocalDateTime = LocalDateTime.parse(AbstractTest.secondDateStr.subSequence(0, secondDateStr.length()), LOCALDATETIME_FORMAT);
//		Date secondDate = AbstractTest.DATETIME_FORMAT.parse(AbstractTest.secondDateStr);
		marq = new MarqueWithLocalDateTime();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime)
						.minutesFrom(secondLocalDateTime)
						.is(0));
		got = query.getAllInstancesOf(marq);

		MarqueWithLocalDateTime secondDateMarques = new MarqueWithLocalDateTime();
		secondDateMarques.creationLocalDateTime.permittedValues(secondLocalDateTime);
		int numberOfSecondDateRows = database.getDBTable(secondDateMarques).setBlankQueryAllowed(true).count().intValue();
		assertThat(got.size(), is(numberOfSecondDateRows));
	}

	@Test
	public void testSecondsDifferenceFunction() throws SQLException, ParseException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDateTime)
						.secondsFrom(
								marq.column(marq.creationLocalDateTime).addSeconds(2))
						.is(2));
		List<MarqueWithLocalDateTime> got = query.getAllInstancesOf(marq);

		MarqueWithLocalDateTime nonNullMarque = new MarqueWithLocalDateTime();
		nonNullMarque.creationLocalDateTime.excludeNull();
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		assertThat(got.size(), is(numberOfRowsWithACreationDate));

		LocalDateTime secondLocalDateTime = LocalDateTime.of(2011, Month.APRIL, 2, 1, 2, 3);
//		LocalDateTime secondLocalDateTime = LocalDateTime.parse(AbstractTest.secondDateStr.subSequence(0, secondDateStr.length()), LOCALDATETIME_FORMAT);
//		Date secondDate = AbstractTest.DATETIME_FORMAT.parse(AbstractTest.secondDateStr);
//		Date secondDate = new SimpleDateFormat("dd/MMM/yyyy HH:mm:ss", Locale.getDefault()).parse(secondDateStr);
		marq = new MarqueWithLocalDateTime();
		query = database.getDBQuery(marq).setQueryLabel("FIND CREATION DATE WITHIN 1 SECOND OF SECOND DATE");
		query.addCondition(
				marq.column(marq.creationLocalDateTime)
						.secondsFrom(secondLocalDateTime)
						.is(0));

		got = query.getAllInstancesOf(marq);

		MarqueWithLocalDateTime secondDateMarques = new MarqueWithLocalDateTime();
		secondDateMarques.creationLocalDateTime.permittedValues(secondLocalDateTime);
		int numberOfSecondDateRows = database.getDBTable(secondDateMarques)
				.setQueryLabel("CHECK SECOND DATE CAN BE COUNTED")
				.setBlankQueryAllowed(true).count().intValue();
		assertThat(got.size(), is(numberOfSecondDateRows));
	}

	@Test
	public void testUpdateRefreshesExistingObject() throws SQLException, ParseException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime(359, "testUpdateRefreshesExistingObject", april2nd2011LocalDateTime);
		database.insert(marq);
		assertThat(marq.updateTime.getValue(), is(nullValue()));
		marq.name.setValue("Actual Car Company");
		database.update(marq);
		assertThat(marq.updateTime.getValue(), is(notNullValue()));
		database.delete(marq);
	}

	@Test
	public void testInsertRefreshesExistingObject() throws SQLException, ParseException {
		MarqueWithLocalDateTime marq = new MarqueWithLocalDateTime(360, "testInsertRefreshesExistingObject", april2nd2011LocalDateTime);
		assertThat(marq.insertTime.getValue(), is(nullValue()));
		database.insert(marq);
		assertThat(marq.insertTime.getValue(), is(notNullValue()));
		database.delete(marq);

	}

	public static class MarqueWithSecondsFromDate extends MarqueWithLocalDateTime {

		private static final long serialVersionUID = 1L;

		LocalDateTime date = Instant.ofEpochMilli(10L).atZone(ZoneId.systemDefault()).toLocalDateTime();
		@DBColumn
		DBNumber subseconds = new DBNumber(LocalDateTimeExpression.value(date).subsecond());
	}

	@Test
	public void testSecondsFromReturnsDecimal() throws SQLException {
		final List<MarqueWithSecondsFromDate> allRows = database.getDBTable(new MarqueWithSecondsFromDate()).setBlankQueryAllowed(true).getAllRows();

		for (MarqueWithSecondsFromDate row : allRows) {
			assertThat(row.subseconds.doubleValue(), is(0.01));
		}
	}

	@Test
	public void testEndOfMonthCalculation() throws SQLException {
		MarqueWithEndOfMonthOfLocalDateTimeColumn marq = new MarqueWithEndOfMonthOfLocalDateTimeColumn();
		DBTable<MarqueWithEndOfMonthOfLocalDateTimeColumn> table = database.getDBTable(marq);
		List<MarqueWithEndOfMonthOfLocalDateTimeColumn> allRows = table.setBlankQueryAllowed(true).getAllRows();

		assertThat(allRows.size(), is(22));
		final LocalDateTime march31st2013 = LocalDateTime.of(2013, Month.MARCH, 31, 12, 34, 56);
		final LocalDateTime april30th2011 = LocalDateTime.of(2011, Month.APRIL, 30, 1, 2, 3);
		final LocalDateTime march1st2013 = LocalDateTime.of(2013, Month.MARCH, 1, 12, 34, 56);
		final LocalDateTime april1st2011 = LocalDateTime.of(2011, Month.APRIL, 1, 1, 2, 3);
		final LocalDateTime nullDate = null;
		for (MarqueWithEndOfMonthOfLocalDateTimeColumn allRow : allRows) {
//			System.out.println(allRow);
			assertThat(allRow.endOfMonth.getValue(),
					anyOf(
							is(nullDate),
							is(march31st2013),
							is(april30th2011)
					));
			assertThat(allRow.firstOfMonth.getValue(),
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
			assertThat(
					row.get(marq).endOfMonth.getValue(),
					is(march31st2013));
			assertThat(
					row.get(marq).firstOfMonth.getValue(),
					is(march1st2013)
			);
		}
	}

	public static class MarqueWithEndOfMonthOfLocalDateTimeColumn extends MarqueWithLocalDateTime {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBLocalDateTime firstOfMonth = new DBLocalDateTime(this.column(this.creationLocalDateTime).addDays(this.column(this.creationLocalDateTime).day().minus(1).bracket().times(-1)));
		@DBColumn
		DBLocalDateTime endOfMonth = this.column(this.creationLocalDateTime).endOfMonth().asExpressionColumn();
		@DBColumn
		DBLocalDateTime shouldBreakPostgreSQL = new DBLocalDateTime(this.column(this.creationLocalDateTime).addDays(this.column(this.creationLocalDateTime).day().minus(1).times(-1)).addMonths(1).addDays(-1));
	}

	@Before
	public void setupMarqueWithLocalDateTime() throws Exception {
		DBDatabase db = database;
		db.preventDroppingOfTables(false);
		db.dropTableIfExists(new MarqueWithLocalDateTime());
		db.createTable(new MarqueWithLocalDateTime());

		List<MarqueWithLocalDateTime> toInsert = new ArrayList<>();
		toInsert.add(new MarqueWithLocalDateTime(4893059, "True", 1246974, null, 3, "UV", "PEUGEOT", null, "Y", null, 4, true));
		toInsert.add(new MarqueWithLocalDateTime(4893090, "False", 1246974, "", 1, "UV", "FORD", "", "Y", march23rd2013LocalDateTime, 2, false));
		toInsert.add(new MarqueWithLocalDateTime(4893101, "False", 1246974, "", 2, "UV", "HOLDEN", "", "Y", march23rd2013LocalDateTime, 3, null));
		toInsert.add(new MarqueWithLocalDateTime(4893112, "False", 1246974, "", 2, "UV", "MITSUBISHI", "", "Y", march23rd2013LocalDateTime, 4, null));
		toInsert.add(new MarqueWithLocalDateTime(4893150, "False", 1246974, "", 3, "UV", "SUZUKI", "", "Y", march23rd2013LocalDateTime, 4, null));
		toInsert.add(new MarqueWithLocalDateTime(4893263, "False", 1246974, "", 2, "UV", "HONDA", "", "Y", march23rd2013LocalDateTime, 4, null));
		toInsert.add(new MarqueWithLocalDateTime(4893353, "False", 1246974, "", 4, "UV", "NISSAN", "", "Y", march23rd2013LocalDateTime, 4, null));
		toInsert.add(new MarqueWithLocalDateTime(4893557, "False", 1246974, "", 2, "UV", "SUBARU", "", "Y", march23rd2013LocalDateTime, 4, null));
		toInsert.add(new MarqueWithLocalDateTime(4894018, "False", 1246974, "", 2, "UV", "MAZDA", "", "Y", march23rd2013LocalDateTime, 4, null));
		toInsert.add(new MarqueWithLocalDateTime(4895203, "False", 1246974, "", 2, "UV", "ROVER", "", "Y", march23rd2013LocalDateTime, 4, null));
		toInsert.add(new MarqueWithLocalDateTime(4896300, "False", 1246974, null, 2, "UV", "HYUNDAI", null, "Y", march23rd2013LocalDateTime, 1, null));
		toInsert.add(new MarqueWithLocalDateTime(4899527, "False", 1246974, "", 1, "UV", "JEEP", "", "Y", march23rd2013LocalDateTime, 3, null));
		toInsert.add(new MarqueWithLocalDateTime(7659280, "False", 1246972, "Y", 3, "", "DAIHATSU", "", "Y", march23rd2013LocalDateTime, 4, null));
		toInsert.add(new MarqueWithLocalDateTime(7681544, "False", 1246974, "", 2, "UV", "LANDROVER", "", "Y", march23rd2013LocalDateTime, 4, null));
		toInsert.add(new MarqueWithLocalDateTime(7730022, "False", 1246974, "", 2, "UV", "VOLVO", "", "Y", march23rd2013LocalDateTime, 4, null));
		toInsert.add(new MarqueWithLocalDateTime(8376505, "False", 1246974, "", null, "", "ISUZU", "", "Y", march23rd2013LocalDateTime, 4, null));
		toInsert.add(new MarqueWithLocalDateTime(8587147, "False", 1246974, "", null, "", "DAEWOO", "", "Y", march23rd2013LocalDateTime, 4, null));
		toInsert.add(new MarqueWithLocalDateTime(9971178, "False", 1246974, "", 1, "", "CHRYSLER", "", "Y", march23rd2013LocalDateTime, 4, null));
		toInsert.add(new MarqueWithLocalDateTime(13224369, "False", 1246974, "", 0, "", "VW", "", "Y", april2nd2011LocalDateTime, 4, null));
		toInsert.add(new MarqueWithLocalDateTime(6664478, "False", 1246974, "", 0, "", "BMW", "", "Y", april2nd2011LocalDateTime, 4, null));
		toInsert.add(new MarqueWithLocalDateTime(1, "False", 1246974, "", 0, "", "TOYOTA", "", "Y", march23rd2013LocalDateTime, 1, true));
		toInsert.add(new MarqueWithLocalDateTime(2, "False", 1246974, "", 0, "", "HUMMER", "", "Y", april2nd2011LocalDateTime, 3, null));

		db.insert(toInsert);
	}

	public static class MarqueWithEndOfMonthForLocalDateTimeColumn extends MarqueWithLocalDateTime {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBLocalDateTime firstOfMonth = this.column(this.creationLocalDateTime)
				.firstOfMonth()
				.asExpressionColumn();
		@DBColumn
		DBLocalDateTime endOfMonth = this.column(this.creationLocalDateTime).endOfMonth().asExpressionColumn();
	}

	@DBTableName("marque_with_localdatetime")
	public static class MarqueWithLocalDateTime extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn("uid_marque")
		@DBPrimaryKey
		public DBInteger uidMarque = new DBInteger();

		@DBColumn
		public DBString name = new DBString();

		@DBColumn("creation_localdatetime")
		public DBLocalDateTime creationLocalDateTime = new DBLocalDateTime();

		@DBForeignKey(CarCompany.class)
		@DBColumn("fk_carcompany")
		public DBInteger carCompany = new DBInteger();

		@DBColumn()
		public DBLocalDateTime insertTime = new DBLocalDateTime().setDefaultInsertValueToNow();

		@DBColumn()
		public DBLocalDateTime updateTime = new DBLocalDateTime().setDefaultUpdateValueToNow();

		/**
		 * Required Public No-Argument Constructor.
		 *
		 */
		public MarqueWithLocalDateTime() {
		}

		/**
		 * Convenience Constructor.
		 *
		 * @param uidMarque uidMarque
		 * @param isUsedForTAFROs isUsedForTAFROs
		 * @param statusClass statusClass
		 * @param carCompany carCompany
		 * @param intIndividualAllocationsAllowed intIndividualAllocationsAllowed
		 * @param pricingCodePrefix pricingCodePrefix
		 * @param updateCount updateCount
		 * @param name name
		 * @param reservationsAllowed reservationsAllowed
		 * @param autoCreated autoCreated
		 * @param creationLocalDateTime creationLocalDateTime
		 * @param enabled enabled
		 */
		public MarqueWithLocalDateTime(int uidMarque, String isUsedForTAFROs, int statusClass, String intIndividualAllocationsAllowed, Integer updateCount, String autoCreated, String name, String pricingCodePrefix, String reservationsAllowed, LocalDateTime creationLocalDateTime, int carCompany, Boolean enabled) {
			this.uidMarque.setValue(uidMarque);
			this.name.setValue(name);
			this.creationLocalDateTime.setValue(creationLocalDateTime);
			this.carCompany.setValue(carCompany);
		}

		public MarqueWithLocalDateTime(int uidMarque, String name, LocalDateTime creationLocalDateTime) {
			this.uidMarque.setValue(uidMarque);
			this.name.setValue(name);
			this.creationLocalDateTime.setValue(creationLocalDateTime);
			this.carCompany.setValue(carCompany);
		}
	}
}
