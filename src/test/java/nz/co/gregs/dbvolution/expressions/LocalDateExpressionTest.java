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
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBLocalDate;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Before;
import org.junit.Test;

public class LocalDateExpressionTest extends AbstractTest {

	public LocalDateExpressionTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	public static class CurrentDateReport extends DBReport {

		private static final long serialVersionUID = 1L;
		public MarqueWithLocalDate marque = new MarqueWithLocalDate();
		@DBColumn
		public DBString name = new DBString(marque.column(marque.name));
		@DBColumn
		public DBLocalDate savedJavaDate = new DBLocalDate(marque.column(marque.creationLocalDate).toLocalDate());
		@DBColumn
		public DBLocalDate actualJavaDate = new DBLocalDate(LocalDate.now());
		@DBColumn
		public DBLocalDate currentDate = new DBLocalDate(LocalDateExpression.today());
		@DBColumn
		public DBNumber dayFromCurrentDate = new DBNumber(LocalDateExpression.now().day());
		@DBColumn
		public DBLocalDate currentDateTime = new DBLocalDate(LocalDateExpression.currentDate());
		@DBColumn
		public DBLocalDate created = marque.column(marque.creationLocalDate).toLocalDate().asExpressionColumn();
		@DBColumn
		public DBLocalDate currentTime = LocalDateExpression.currentLocalDate().asExpressionColumn();
	}

	LocalDate march23rd2013LocalDate = (new GregorianCalendar(2013, 2, 23, 12, 34, 56)).toZonedDateTime().toLocalDate();
	LocalDate april2nd2011LocalDate = (new GregorianCalendar(2011, 3, 2, 1, 2, 3)).toZonedDateTime().toLocalDate();

	@Test
	public void testIsNotDateExpression() throws SQLException {
		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);
		final LocalDateExpression fiveDaysPriorToCreation = marq.column(marq.creationLocalDate).toLocalDate().addDays(-5);
		query.addCondition(LocalDateExpression.leastOf(marq.column(marq.creationLocalDate).toLocalDate(),
				fiveDaysPriorToCreation,
				LocalDateExpression.value(march23rd2013LocalDate).addWeeks(-5),
				LocalDateExpression.value(march23rd2013LocalDate).addDays(-2))
				.isNot(fiveDaysPriorToCreation)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(18));
	}

	@Test
	public void testIsNotDate() throws SQLException {

		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);

		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().isNot(april2nd2011LocalDate)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(18));
	}

	@Test
	public void testLeastOf() throws SQLException {
		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);
		final LocalDateExpression fiveDaysPriorToCreation = marq.column(marq.creationLocalDate).toLocalDate().addDays(-5);
		query.addCondition(LocalDateExpression.leastOf(marq.column(marq.creationLocalDate).toLocalDate(),
				fiveDaysPriorToCreation,
				LocalDateExpression.value(march23rd2013LocalDate).addWeeks(-5),
				LocalDateExpression.value(march23rd2013LocalDate).addDays(-2))
				.is(fiveDaysPriorToCreation)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(3));
	}

	@Test
	public void testLeastOfWithList() throws SQLException {
		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);
		final LocalDateExpression creationDate = marq.column(marq.creationLocalDate).toLocalDate();
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

		assertThat(allRows.size(), is(3));
	}

	@Test
	public void testLeastOfWithDates() throws SQLException {
		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);

		query.addCondition(LocalDateExpression
				.leastOf(march23rd2013LocalDate,
						april2nd2011LocalDate)
				.is(april2nd2011LocalDate)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(22));
	}

	@Test
	public void testGreatestOf() throws SQLException {
		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);
		final LocalDateExpression creationDate = marq.column(marq.creationLocalDate).toLocalDate();
		final LocalDateExpression fiveDaysPriorToCreation = creationDate.addDays(-5);
		query.addCondition(LocalDateExpression.greatestOf(creationDate,
				fiveDaysPriorToCreation,
				LocalDateExpression.value(march23rd2013LocalDate).addWeeks(-5),
				LocalDateExpression.value(march23rd2013LocalDate).addDays(-2))
				.is(creationDate)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(18));
	}

	@Test
	public void testGreatestOfWithList() throws SQLException {
		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);
		final LocalDateExpression creationDate = marq.column(marq.creationLocalDate).toLocalDate();
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

		assertThat(allRows.size(), is(18));
	}

	@Test
	public void testGreatestOfWithDates() throws SQLException {
		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				LocalDateExpression
						.greatestOf(
								march23rd2013LocalDate,
								april2nd2011LocalDate
						).is(march23rd2013LocalDate)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(22));
	}

	@Test
	public void testOverlapsDateExpressionDateResult() throws SQLException {
		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(LocalDateExpression.overlaps(marq.column(marq.creationLocalDate).toLocalDate(),
				marq.column(marq.creationLocalDate).toLocalDate().addDays(-5),
				LocalDateExpression.value(march23rd2013LocalDate).addWeeks(-5),
				LocalDateExpression.value(march23rd2013LocalDate).addDays(-2))
		);
		List<DBQueryRow> allRows = query.getAllRows();
		assertThat(allRows.size(), is(18));
	}

	@Test
	public void testOverlapsAllDateResults() throws SQLException {
		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(LocalDateExpression.overlaps(marq.column(marq.creationLocalDate).toLocalDate(),
				marq.column(marq.creationLocalDate).toLocalDate().addDays(-5),
				LocalDateExpression.value(march23rd2013LocalDate).addWeeks(-5),
				LocalDateExpression.value(march23rd2013LocalDate).addDays(-2))
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
		assertThat(got.maxOfDates.localDateValue(), is(march23rd2013LocalDate));
		assertThat(got.minOfDates.getValue(), is(april2nd2011LocalDate));

	}

	public static class MarqueWithDateAggregators extends MarqueWithLocalDate {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBNumber countOfDates = new DBNumber(this.column(this.creationLocalDate).count());
		@DBColumn
		DBLocalDate maxOfDates = new DBLocalDate(this.column(this.creationLocalDate).toLocalDate().max());
		@DBColumn
		DBLocalDate minOfDates = new DBLocalDate(this.column(this.creationLocalDate).toLocalDate().min());

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

		assertThat(allRows.size(), is(22));

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
			assertThat(got.countOfDates.intValue(), is((Integer) expect[0]));
			assertThat(got.maxOfAll.getValue(), is((LocalDate) expect[1]));
			assertThat(got.maxOfDates.getValue(), is((LocalDate) expect[1]));
			assertThat(got.minOfDates.getValue(), is((LocalDate) expect[2]));
		}
	}

	public static class MarqueWithDateWindowingFunctions extends MarqueWithLocalDate {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBNumber countOfAllRows = new DBNumber(this.column(this.creationLocalDate).toLocalDate().count().over().allRows());
		@DBColumn
		DBNumber rowNumber = new DBNumber(this.column(this.creationLocalDate).toLocalDate().count().over().AllRowsAndOrderBy(this.column(this.carCompany).ascending()));
		@DBColumn
		DBNumber countOfDates = new DBNumber(this.column(this.creationLocalDate).toLocalDate().count().over().partition(this.column(this.carCompany)).unordered());
		@DBColumn
		DBNumber rowWithinCarCo = new DBNumber(this.column(this.creationLocalDate).toLocalDate().count()
				.over()
				.partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).ascending())
				.defaultFrame());
		@DBColumn
		DBLocalDate maxOfAll = new DBLocalDate(this.column(this.creationLocalDate).toLocalDate().max().over().allRows());
		@DBColumn
		DBLocalDate maxOfDates = new DBLocalDate(this.column(this.creationLocalDate).toLocalDate().max().over().partition(this.column(this.carCompany)).unordered());
		@DBColumn
		DBLocalDate minOfDates = new DBLocalDate(this.column(this.creationLocalDate).toLocalDate().min().over().partition(this.column(this.carCompany)).unordered());
		@DBColumn
		DBLocalDate test1 = new DBLocalDate(this.column(this.creationLocalDate)
				.toLocalDate()
				.min()
				.over().partition(this.column(this.carCompany))
				.unsorted());
		@DBColumn
		DBLocalDate test2 = new DBLocalDate(this.column(this.creationLocalDate).toLocalDate().min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().unboundedPreceding().currentRow());
		@DBColumn
		DBLocalDate test3 = new DBLocalDate(this.column(this.creationLocalDate).toLocalDate().min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().unboundedPreceding().currentRow());
		@DBColumn
		DBLocalDate test4 = new DBLocalDate(this.column(this.creationLocalDate).toLocalDate().min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().currentRow().toCurrentRow());
		@DBColumn
		DBLocalDate test5 = new DBLocalDate(this.column(this.creationLocalDate).toLocalDate().min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().offsetPrecedingAndCurrentRow(5));
		@DBColumn
		DBLocalDate test6 = new DBLocalDate(this.column(this.creationLocalDate).toLocalDate().min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().currentRow().unboundedFollowing());
		@DBColumn
		DBLocalDate test7 = new DBLocalDate(this.column(this.creationLocalDate).toLocalDate().min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().currentRow().forFollowing(3));
		@DBColumn
		DBLocalDate test8 = new DBLocalDate(this.column(this.creationLocalDate).toLocalDate().min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().unboundedPreceding().unboundedFollowing());
		@DBColumn
		DBLocalDate test9 = new DBLocalDate(this.column(this.creationLocalDate).toLocalDate().min()
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

		assertThat(allRows.size(), is(22));

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
			assertThat(got.countOfDates.intValue(), is((Integer) expect[0]));
			assertThat(got.maxOfDates.getValue(), is((LocalDate) expect[1]));
			assertThat(got.minOfDates.getValue(), is((LocalDate) expect[2]));
		}
	}

	public static class MarqueWithAggregatorAndDateWindowingFunctions extends MarqueWithLocalDate {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBNumber countAggregator = new DBNumber(this.column(this.creationLocalDate).toLocalDate().count());
		@DBColumn
		DBNumber countOfAllRows = new DBNumber(this.column(this.creationLocalDate).toLocalDate().count().over().allRows());
		@DBColumn
		DBNumber countOfDates = new DBNumber(this.column(this.creationLocalDate).toLocalDate().count()
				.over()
				.partition(this.column(this.carCompany)).unordered());
		@DBColumn
		DBLocalDate maxOfAll = new DBLocalDate(this.column(this.creationLocalDate)
				.toLocalDate().max().over().allRows());
		@DBColumn
		DBLocalDate maxOfDates = new DBLocalDate(this.column(this.creationLocalDate)
				.toLocalDate().max().over()
				.partition(this.column(this.carCompany)).unordered());
		@DBColumn
		DBLocalDate minOfDates = new DBLocalDate(this.column(this.creationLocalDate)
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

		assertThat(allRows.size(), is(22));

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

	public static class MarqueWithComplexWindowingFunction extends MarqueWithLocalDate {

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
		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);
		query.addConditions(marq.column(marq.creationLocalDate).toLocalDate().isBetween(LocalDateExpression.currentLocalDate(), LocalDateExpression.nullLocalDate()));

		List<DBQueryRow> got = query.getAllRows();

		assertThat(got.size(), is(0));

		database.insert(new MarqueWithLocalDate(3, "False", 1246974, "", 0, "", "     LADA               ", "", "Y", LocalDate.now(), 3, null));

		MarqueWithLocalDate reportLimitingMarque = new MarqueWithLocalDate();
		reportLimitingMarque.name.permittedPatternIgnoreCase("% LADA %");
		CurrentDateReport currentDateReport = new CurrentDateReport();

		final List<CurrentDateReport> reportRows = DBReport.getRows(database, currentDateReport, reportLimitingMarque);

		assertThat(reportRows.size(), is(1));

		query = database.getDBQuery(marq);
		final LocalDateExpression now = LocalDateExpression.now();
		query.addConditions(
				marq.column(marq.creationLocalDate).toLocalDate().day()
						.isIn(
								now.day(),
								now.addDays(-1).day(),
								now.addDays(1).day()),
				marq.column(marq.creationLocalDate).toLocalDate().isGreaterThan(march23rd2013LocalDate)
		);

		got = query.getAllRows();

		assertThat(got.size(), is(1));

	}

	@Test
	public void testCurrentDateAndAddDaysFunctions() throws SQLException {
		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		marq.creationLocalDate.permittedRangeInclusive(LocalDateExpression.currentDate(), null);
		List<MarqueWithLocalDate> got = database.get(marq);

		assertThat(got.size(), is(0));

		database.insert(new MarqueWithLocalDate(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", LocalDate.now(), 3, null));
		marq.creationLocalDate.permittedRangeInclusive(LocalDateExpression.currentDate().addDays(-1), null);
		got = database.get(marq);

		assertThat(got.size(), is(1));

		marq.creationLocalDate.permittedRangeInclusive(null, LocalDateExpression.currentDate().addDays(-1));
		got = database.get(marq);

		assertThat(got.size(), is(21));

		marq.creationLocalDate.permittedRangeInclusive(LocalDateExpression.currentDate().addDays(-1), LocalDateExpression.currentDate().addDays(+1));
		got = database.get(marq);

		assertThat(got.size(), is(1));
	}

	@Test
	public void testCurrentDateAndAddWeeksFunctions() throws SQLException {
		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		marq.creationLocalDate.permittedRangeInclusive(LocalDateExpression.currentDate(), null);
		List<MarqueWithLocalDate> got = database.get(marq);

		assertThat(got.size(), is(0));

		database.insert(new MarqueWithLocalDate(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", LocalDate.now(), 3, null));
		marq.creationLocalDate.permittedRangeInclusive(LocalDateExpression.currentDate().addWeeks(-1), null);
		got = database.get(marq);

		assertThat(got.size(), is(1));
	}

	@Test
	public void testCurrentDateAndAddMonthsFunctions() throws SQLException {
		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		marq.creationLocalDate.permittedRangeInclusive(LocalDateExpression.currentDate(), null);
		List<MarqueWithLocalDate> got = database.get(marq);

		assertThat(got.size(), is(0));

		database.insert(new MarqueWithLocalDate(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", LocalDate.now(), 3, null));
		marq.creationLocalDate.permittedRangeInclusive(LocalDateExpression.currentDate().addMonths(-1), null);
		got = database.get(marq);

		assertThat(got.size(), is(1));
	}

	@Test
	public void testCurrentDateAndAddYearsFunctions() throws SQLException {
		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		marq.creationLocalDate.permittedRangeInclusive(LocalDateExpression.currentDate(), null);
		List<MarqueWithLocalDate> got = database.get(marq);

		assertThat(got.size(), is(0));

		database.insert(new MarqueWithLocalDate(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", LocalDate.now(), 3, null));
		marq.creationLocalDate.permittedRangeInclusive(LocalDateExpression.currentDate().addYears(-1), null);
		got = database.get(marq);

		assertThat(got.size(), is(1));
	}

	@Test
	public void testYearFunction() throws SQLException {
		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().year().is(2014));
		List<MarqueWithLocalDate> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(0));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().year().is(2013));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));
	}

	@Test
	public void testYearIsFunction() throws SQLException {
		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().yearIs(2014));
		List<MarqueWithLocalDate> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(0));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().yearIs(2013));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().yearIs(NumberExpression.value(2014).numberResult()));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(0));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().yearIs(NumberExpression.value(2013)));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));
	}

	@Test
	public void testMonthFunction() throws SQLException {
		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().monthIs(3));
		List<MarqueWithLocalDate> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().monthIs(4));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().monthIs(NumberExpression.value(3).numberResult()));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().monthIs(NumberExpression.value(4)));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));
	}

	@Test
	public void testEndOfMonthFunction() throws SQLException {
		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().endOfMonth().day().is(31));
		List<MarqueWithLocalDate> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().endOfMonth().day().is(30));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().endOfMonth().day().isNull());
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(1));
	}

	@Test
	public void testFirstOfMonthFunction() throws SQLException {
		LocalDate february28th2013 = LocalDate.of(2013, Month.FEBRUARY, 28);
		LocalDate march2nd2013 = LocalDate.of(2013, Month.MARCH, 2);
		LocalDate march31st2011 = LocalDate.of(2011, Month.MARCH, 31);

		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().firstOfMonth().isBetween(february28th2013, march2nd2013));
		List<MarqueWithLocalDate> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(marq.column(marq.creationLocalDate).toLocalDate().firstOfMonth().isBetween(march31st2011, april2nd2011LocalDate));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(marq.column(marq.creationLocalDate).toLocalDate().firstOfMonth().isBetween(LocalDateExpression.value(march31st2011), april2nd2011LocalDate));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(marq.column(marq.creationLocalDate).toLocalDate().firstOfMonth().isBetween(march31st2011, LocalDateExpression.value(april2nd2011LocalDate)));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().firstOfMonth().isNull());
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(1));
	}

	@Test
	public void testIsNotNull() throws SQLException {
		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().firstOfMonth().isNotNull());
		List<MarqueWithLocalDate> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(21));
	}

	@Test
	public void testBetweenInclusiveFunction() throws SQLException {
		LocalDate march1st2013 = LocalDate.of(2013, Month.MARCH, 1);
		LocalDate march2nd2013 = LocalDate.of(2013, Month.MARCH, 2);
		LocalDate april1st2011 = LocalDate.of(2011, Month.APRIL, 1);

		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().firstOfMonth().isBetweenInclusive(march1st2013, march2nd2013));
		List<MarqueWithLocalDate> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(marq.column(marq.creationLocalDate).toLocalDate().firstOfMonth().isBetweenInclusive(april1st2011, april2nd2011LocalDate));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(marq.column(marq.creationLocalDate).toLocalDate().firstOfMonth().isBetweenInclusive(LocalDateExpression.value(april1st2011), april2nd2011LocalDate));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(marq.column(marq.creationLocalDate).toLocalDate().firstOfMonth().isBetweenInclusive(april1st2011, LocalDateExpression.value(april2nd2011LocalDate)));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().firstOfMonth().isNull());
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(1));
	}

	@Test
	public void testisGreaterThan() throws SQLException {
		LocalDate february28th2013 = LocalDate.of(2013, Month.FEBRUARY, 28);

		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().firstOfMonth().isGreaterThan(february28th2013));
		List<MarqueWithLocalDate> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));
	}

	@Test
	public void testisGreaterThanOrEqual() throws SQLException {
		LocalDate march1st2013 = LocalDate.of(2013, Month.MARCH, 1);

		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().firstOfMonth().isGreaterThanOrEqual(march1st2013));
		List<MarqueWithLocalDate> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));
	}

	@Test
	public void testisGreaterThanWithFallback() throws SQLException {
		LocalDate february28th2013 = LocalDate.of(2013, Month.FEBRUARY, 28);

		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().firstOfMonth().isGreaterThan(february28th2013, marq.column(marq.name).isGreaterThan("T")));

		List<MarqueWithLocalDate> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));
	}

	@Test
	public void testisLessThan() throws SQLException {
		LocalDate march1st2013 = LocalDate.of(2013, Month.MARCH, 1);

		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().firstOfMonth().isLessThan(march1st2013));
		List<MarqueWithLocalDate> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));
	}

	@Test
	public void testisLessThanWithFallback() throws SQLException {
		LocalDate february28th2013 = LocalDate.of(2013, Month.FEBRUARY, 28);

		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().firstOfMonth().isLessThan(february28th2013, marq.column(marq.name).isGreaterThan("T")));
		List<MarqueWithLocalDate> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));
	}

	@Test
	public void testBetweenExclusiveFunction() throws SQLException {
		LocalDate february28th2013 = LocalDate.of(2013, Month.FEBRUARY, 28);
		LocalDate march2nd2013 = LocalDate.of(2013, Month.MARCH, 2);
		LocalDate march31st2011 = LocalDate.of(2011, Month.MARCH, 31);

		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().firstOfMonth().isBetweenExclusive(february28th2013, march2nd2013));

		List<MarqueWithLocalDate> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(marq.column(marq.creationLocalDate).toLocalDate().firstOfMonth().isBetweenExclusive(march31st2011, april2nd2011LocalDate));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(marq.column(marq.creationLocalDate).toLocalDate().firstOfMonth().isBetweenExclusive(LocalDateExpression.value(march31st2011), april2nd2011LocalDate));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(marq.column(marq.creationLocalDate).toLocalDate().firstOfMonth().isBetweenExclusive(march31st2011, LocalDateExpression.value(april2nd2011LocalDate)));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().firstOfMonth().isNull());
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(1));

	}

	@Test
	public void testDayOfWeekFunction() throws SQLException {

		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().addMonths(1).dayOfWeek().is(LocalDateExpression.TUESDAY));
		List<MarqueWithLocalDate> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().addMonths(1).dayOfWeek().is(LocalDateExpression.MONDAY));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().addMonths(1).dayOfWeek().isNull());
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(1));

	}

	@Test
	public void testDayFunction() throws SQLException {
		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().day().is(23));
		List<MarqueWithLocalDate> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().day().is(2));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));
	}

	@Test
	public void testDayIsFunction() throws SQLException {
		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().dayIs(23));
		List<MarqueWithLocalDate> got = query.getAllInstancesOf(marq);
		assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().dayIs(2));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().dayIs(NumberExpression.value(2)));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate().dayIs(NumberExpression.value(2).numberResult()));
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(3));
	}

	@Test
	public void testIsInWithNulls() throws SQLException, ParseException {
		MarqueWithLocalDate marque = new MarqueWithLocalDate();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationLocalDate).toLocalDate()
						.isIn((LocalDate) null,
								LocalDate.parse(
										firstDateStr.subSequence(0, firstDateStr.length()),
										LOCALDATETIME_FORMAT
								)
						)
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		assertThat(allRows.size(), is(19));

		MarqueWithLocalDate newMarque = new MarqueWithLocalDate(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", null, 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(23));

		dbQuery.addCondition(
				marque.column(marque.creationLocalDate).toLocalDate()
						.isIn(
								(LocalDate) null,
								LocalDate.parse(
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
		MarqueWithLocalDate marque = new MarqueWithLocalDate();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationLocalDate).toLocalDate().isIn((LocalDate) null, LocalDate.parse(firstDateStr.subSequence(0, firstDateStr.length()), LOCALDATETIME_FORMAT))
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(19));

		MarqueWithLocalDate newMarque = new MarqueWithLocalDate(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", null, 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(23));

		dbQuery.addCondition(marque.column(marque.creationLocalDate).toLocalDate().isIn(april2nd2011LocalDate, march23rd2013LocalDate)
		);

		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(21));
	}

	@Test
	public void testIsInWithList() throws SQLException, ParseException {
		MarqueWithLocalDate marque = new MarqueWithLocalDate();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationLocalDate).toLocalDate()
						.isIn(
								(LocalDate) null,
								LocalDate.parse(firstDateStr.subSequence(0, firstDateStr.length()), LOCALDATETIME_FORMAT))
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(19));

		MarqueWithLocalDate newMarque = new MarqueWithLocalDate(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", null, 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(23));

		List<LocalDateExpression> dates = new ArrayList<LocalDateExpression>();
		dates.add(LocalDateExpression.value(march23rd2013LocalDate));
		dates.add(LocalDateExpression.value(april2nd2011LocalDate));

		dbQuery.addCondition(
				marque.column(marque.creationLocalDate).toLocalDate().isIn(dates)
		);

		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(21));
	}

	@Test
	public void testIsWithNulls() throws SQLException, ParseException {
		MarqueWithLocalDate marque = new MarqueWithLocalDate();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationLocalDate).toLocalDate().is((LocalDate) null)
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));

		MarqueWithLocalDate newMarque = new MarqueWithLocalDate(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", null, 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(23));

		dbQuery.addCondition(
				marque.column(marque.creationLocalDate).toLocalDate().is((LocalDate) null)
		);

		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(2));
	}

	@Test
	public void testIsNull() throws SQLException, ParseException {
		MarqueWithLocalDate marque = new MarqueWithLocalDate();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationLocalDate).toLocalDate().isNull()
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));

		MarqueWithLocalDate newMarque = new MarqueWithLocalDate(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", null, 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(23));

		dbQuery.addCondition(
				marque.column(marque.creationLocalDate).toLocalDate().isNull()
		);

		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(2));
	}

	@Test
//	@Ignore
	public void testDayDifferenceFunction() throws SQLException, ParseException {
		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate()
						.daysFrom(
								marq.column(marq.creationLocalDate).toLocalDate().addDays(2))
						.is(2));
		List<MarqueWithLocalDate> got = query.getAllInstancesOf(marq);

		MarqueWithLocalDate nonNullMarque = new MarqueWithLocalDate();
		nonNullMarque.creationLocalDate.excludeNull();
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		assertThat(got.size(), is(numberOfRowsWithACreationDate));

		LocalDate secondLocalDate = LocalDate.of(2011, Month.APRIL, 2);
//		LocalDate secondLocalDate = LocalDate.parse(AbstractTest.secondDateStr.subSequence(0, secondDateStr.length()), LOCALDATETIME_FORMAT);
		marq = new MarqueWithLocalDate();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate()
						.daysFrom(secondLocalDate)
						.is(0));
		got = query.getAllInstancesOf(marq);

		MarqueWithLocalDate secondDateMarques = new MarqueWithLocalDate();
		secondDateMarques.creationLocalDate.permittedValues(secondLocalDate);
		int numberOfSecondDateRows = database.getDBTable(secondDateMarques).setBlankQueryAllowed(true).count().intValue();
		assertThat(got.size(), is(numberOfSecondDateRows));
	}

	@Test
	public void testWeekDifferenceFunction() throws SQLException, ParseException {
		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate()
						.weeksFrom(
								marq.column(marq.creationLocalDate).toLocalDate().addWeeks(2))
						.is(2));
		List<MarqueWithLocalDate> got = query.getAllInstancesOf(marq);

		MarqueWithLocalDate nonNullMarque = new MarqueWithLocalDate();
		nonNullMarque.creationLocalDate.excludeNull();
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		assertThat(got.size(), is(numberOfRowsWithACreationDate));

		LocalDate secondLocalDate = LocalDate.of(2011, Month.APRIL, 2);
		marq = new MarqueWithLocalDate();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate()
						.weeksFrom(secondLocalDate)
						.is(0));
		got = query.getAllInstancesOf(marq);

		MarqueWithLocalDate secondDateMarques = new MarqueWithLocalDate();
		secondDateMarques.creationLocalDate.permittedValues(secondLocalDate);
		int numberOfSecondDateRows = database.getDBTable(secondDateMarques).setBlankQueryAllowed(true).count().intValue();
		assertThat(got.size(), is(numberOfSecondDateRows));
	}

	@Test
	public void testMonthDifferenceFunction() throws SQLException, ParseException {
		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate()
						.monthsFrom(
								marq.column(marq.creationLocalDate).toLocalDate().addMonths(2))
						.is(2));
		List<MarqueWithLocalDate> got = query.getAllInstancesOf(marq);

		MarqueWithLocalDate nonNullMarque = new MarqueWithLocalDate();
		nonNullMarque.creationLocalDate.excludeNull();
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		assertThat(got.size(), is(numberOfRowsWithACreationDate));

		LocalDate secondLocalDate = LocalDate.of(2011, Month.APRIL, 2);
		marq = new MarqueWithLocalDate();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate()
						.monthsFrom(secondLocalDate)
						.is(0));
		got = query.getAllInstancesOf(marq);

		MarqueWithLocalDate secondDateMarques = new MarqueWithLocalDate();
		secondDateMarques.creationLocalDate.permittedValues(secondLocalDate);
		int numberOfSecondDateRows = database.getDBTable(secondDateMarques).setBlankQueryAllowed(true).count().intValue();
		assertThat(got.size(), is(numberOfSecondDateRows));
	}

	@Test
	public void testYearDifferenceFunction() throws SQLException, ParseException {
		MarqueWithLocalDate marq = new MarqueWithLocalDate();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate()
						.yearsFrom(
								marq.column(marq.creationLocalDate).toLocalDate().addYears(2))
						.is(2));
		List<MarqueWithLocalDate> got = query.getAllInstancesOf(marq);

		MarqueWithLocalDate nonNullMarque = new MarqueWithLocalDate();
		nonNullMarque.creationLocalDate.excludeNull();
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		assertThat(got.size(), is(numberOfRowsWithACreationDate));

		LocalDate secondLocalDate = LocalDate.of(2011, Month.APRIL, 2);
		marq = new MarqueWithLocalDate();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationLocalDate).toLocalDate()
						.yearsFrom(secondLocalDate)
						.is(0));
		got = query.getAllInstancesOf(marq);

		MarqueWithLocalDate secondDateMarques = new MarqueWithLocalDate();
		secondDateMarques.creationLocalDate.permittedValues(secondLocalDate);
		int numberOfSecondDateRows
				= database
						.getDBTable(secondDateMarques)
						.setBlankQueryAllowed(true)
						.count()
						.intValue();
		assertThat(got.size(), is(numberOfSecondDateRows));
	}

	@Test
	public void testEndOfMonthCalculation() throws SQLException {

		MarqueWithEndOfMonthForLocalDateColumn marq = new MarqueWithEndOfMonthForLocalDateColumn();
		DBTable<MarqueWithEndOfMonthForLocalDateColumn> table = database.getDBTable(marq);
		List<MarqueWithEndOfMonthForLocalDateColumn> allRows = table.setBlankQueryAllowed(true).getAllRows();

		assertThat(allRows.size(), is(22));
		final LocalDate march31st2013 = LocalDate.of(2013, Month.MARCH, 31);
		final LocalDate april30th2011 = LocalDate.of(2011, Month.APRIL, 30);
		final LocalDate march1st2013 = LocalDate.of(2013, Month.MARCH, 1);
		final LocalDate april1st2011 = LocalDate.of(2011, Month.APRIL, 1);
		final LocalDate nullDate = null;
		for (MarqueWithEndOfMonthForLocalDateColumn allRow : allRows) {
			assertThat(allRow.endOfMonth.localDateValue(),
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
	
	public static class MarqueWithEndOfMonthForLocalDateColumn extends MarqueWithLocalDate {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBLocalDate firstOfMonth = new DBLocalDate(this.column(this.creationLocalDate).addDays(this.column(this.creationLocalDate).day().minus(1).bracket().times(-1)));
		@DBColumn
		DBLocalDate endOfMonth = this.column(this.creationLocalDate).endOfMonth().asExpressionColumn();
		@DBColumn
		DBLocalDate shouldBreakPostgreSQL = new DBLocalDate(this.column(this.creationLocalDate).addDays(this.column(this.creationLocalDate).day().minus(1).times(-1)).addMonths(1).addDays(-1));
	}

	@Before
	public void setupMarqueWithLocalDateTime() throws Exception {
		DBDatabase db = database;
		db.preventDroppingOfTables(false);
		db.dropTableIfExists(new MarqueWithLocalDate());
		db.createTable(new MarqueWithLocalDate());

		List<MarqueWithLocalDate> toInsert = new ArrayList<>();
		toInsert.add(new MarqueWithLocalDate(4893059, "True", 1246974, null, 3, "UV", "PEUGEOT", null, "Y", null, 4, true));
		toInsert.add(new MarqueWithLocalDate(4893090, "False", 1246974, "", 1, "UV", "FORD", "", "Y", march23rd2013LocalDate, 2, false));
		toInsert.add(new MarqueWithLocalDate(4893101, "False", 1246974, "", 2, "UV", "HOLDEN", "", "Y", march23rd2013LocalDate, 3, null));
		toInsert.add(new MarqueWithLocalDate(4893112, "False", 1246974, "", 2, "UV", "MITSUBISHI", "", "Y", march23rd2013LocalDate, 4, null));
		toInsert.add(new MarqueWithLocalDate(4893150, "False", 1246974, "", 3, "UV", "SUZUKI", "", "Y", march23rd2013LocalDate, 4, null));
		toInsert.add(new MarqueWithLocalDate(4893263, "False", 1246974, "", 2, "UV", "HONDA", "", "Y", march23rd2013LocalDate, 4, null));
		toInsert.add(new MarqueWithLocalDate(4893353, "False", 1246974, "", 4, "UV", "NISSAN", "", "Y", march23rd2013LocalDate, 4, null));
		toInsert.add(new MarqueWithLocalDate(4893557, "False", 1246974, "", 2, "UV", "SUBARU", "", "Y", march23rd2013LocalDate, 4, null));
		toInsert.add(new MarqueWithLocalDate(4894018, "False", 1246974, "", 2, "UV", "MAZDA", "", "Y", march23rd2013LocalDate, 4, null));
		toInsert.add(new MarqueWithLocalDate(4895203, "False", 1246974, "", 2, "UV", "ROVER", "", "Y", march23rd2013LocalDate, 4, null));
		toInsert.add(new MarqueWithLocalDate(4896300, "False", 1246974, null, 2, "UV", "HYUNDAI", null, "Y", march23rd2013LocalDate, 1, null));
		toInsert.add(new MarqueWithLocalDate(4899527, "False", 1246974, "", 1, "UV", "JEEP", "", "Y", march23rd2013LocalDate, 3, null));
		toInsert.add(new MarqueWithLocalDate(7659280, "False", 1246972, "Y", 3, "", "DAIHATSU", "", "Y", march23rd2013LocalDate, 4, null));
		toInsert.add(new MarqueWithLocalDate(7681544, "False", 1246974, "", 2, "UV", "LANDROVER", "", "Y", march23rd2013LocalDate, 4, null));
		toInsert.add(new MarqueWithLocalDate(7730022, "False", 1246974, "", 2, "UV", "VOLVO", "", "Y", march23rd2013LocalDate, 4, null));
		toInsert.add(new MarqueWithLocalDate(8376505, "False", 1246974, "", null, "", "ISUZU", "", "Y", march23rd2013LocalDate, 4, null));
		toInsert.add(new MarqueWithLocalDate(8587147, "False", 1246974, "", null, "", "DAEWOO", "", "Y", march23rd2013LocalDate, 4, null));
		toInsert.add(new MarqueWithLocalDate(9971178, "False", 1246974, "", 1, "", "CHRYSLER", "", "Y", march23rd2013LocalDate, 4, null));
		toInsert.add(new MarqueWithLocalDate(13224369, "False", 1246974, "", 0, "", "VW", "", "Y", april2nd2011LocalDate, 4, null));
		toInsert.add(new MarqueWithLocalDate(6664478, "False", 1246974, "", 0, "", "BMW", "", "Y", april2nd2011LocalDate, 4, null));
		toInsert.add(new MarqueWithLocalDate(1, "False", 1246974, "", 0, "", "TOYOTA", "", "Y", march23rd2013LocalDate, 1, true));
		toInsert.add(new MarqueWithLocalDate(2, "False", 1246974, "", 0, "", "HUMMER", "", "Y", april2nd2011LocalDate, 3, null));

		db.insert(toInsert);
	}

	public static class MarqueWithEndOfMonthForInstantColumn extends MarqueWithLocalDate {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBLocalDate firstOfMonth = this.column(this.creationLocalDate)
				.firstOfMonth()
				.asExpressionColumn();
		@DBColumn
		DBLocalDate endOfMonth = this.column(this.creationLocalDate).endOfMonth().asExpressionColumn();
	}

	@DBTableName("marque_with_instant")
	public static class MarqueWithLocalDate extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn("uid_marque")
		@DBPrimaryKey
		public DBInteger uidMarque = new DBInteger();

		@DBColumn
		public DBString name = new DBString();

		@DBColumn("creation_date")
		public DBLocalDate creationLocalDate = new DBLocalDate();

		@DBForeignKey(CarCompany.class)
		@DBColumn("fk_carcompany")
		public DBInteger carCompany = new DBInteger();

		/**
		 * Required Public No-Argument Constructor.
		 *
		 */
		public MarqueWithLocalDate() {
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
		 * @param creationLocalDateTime creationLocalDate
		 * @param enabled enabled
		 */
		public MarqueWithLocalDate(int uidMarque, String isUsedForTAFROs, int statusClass, String intIndividualAllocationsAllowed, Integer updateCount, String autoCreated, String name, String pricingCodePrefix, String reservationsAllowed, LocalDate creationLocalDateTime, int carCompany, Boolean enabled) {
			this.uidMarque.setValue(uidMarque);
			this.name.setValue(name);
			this.creationLocalDate.setValue(creationLocalDateTime);
			this.carCompany.setValue(carCompany);
		}
	}

	@Test
	public void testLagAndLeadFunctions() throws SQLException {
		MarqueWithLagAndLeadFunctions marq = new MarqueWithLagAndLeadFunctions();

		DBQuery query = database.getDBQuery(marq)
				.setBlankQueryAllowed(true)
				.setSortOrder(
						marq.column(marq.carCompany).ascending(),
						marq.column(marq.uidMarque).ascending()
				);

		List<DBQueryRow> allRows = query.getAllRows();
		assertThat(allRows.size(), is(22));

		MarqueWithLagAndLeadFunctions got;// = allRows.get(0).get(marq);
		ArrayList<Object[]> expectedValues = new ArrayList<>();

		expectedValues.add(new Object[]{22, 1, 2, 2, null, march23rd2013LocalDate});
		expectedValues.add(new Object[]{22, 2, 2, 2, march23rd2013LocalDate, march23rd2013LocalDate});
		expectedValues.add(new Object[]{22, 3, 1, 1, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{22, 4, 3, 3, march23rd2013LocalDate, march23rd2013LocalDate});
		expectedValues.add(new Object[]{22, 5, 3, 3, april2nd2011LocalDate, march23rd2013LocalDate});
		expectedValues.add(new Object[]{22, 6, 3, 3, march23rd2013LocalDate, null});
		expectedValues.add(new Object[]{22, 7, 16, 16, march23rd2013LocalDate, march23rd2013LocalDate});
		expectedValues.add(new Object[]{22, 8, 16, 16, null, march23rd2013LocalDate});
		expectedValues.add(new Object[]{22, 9, 16, 16, march23rd2013LocalDate, march23rd2013LocalDate});
		expectedValues.add(new Object[]{22, 10, 16, 16, march23rd2013LocalDate, march23rd2013LocalDate});
		expectedValues.add(new Object[]{22, 11, 16, 16, march23rd2013LocalDate, march23rd2013LocalDate});
		expectedValues.add(new Object[]{22, 12, 16, 16, march23rd2013LocalDate, march23rd2013LocalDate});
		expectedValues.add(new Object[]{22, 13, 16, 16, march23rd2013LocalDate, march23rd2013LocalDate});
		expectedValues.add(new Object[]{22, 14, 16, 16, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{22, 15, 16, 16, march23rd2013LocalDate, march23rd2013LocalDate});
		expectedValues.add(new Object[]{22, 16, 16, 16, april2nd2011LocalDate, march23rd2013LocalDate});
		expectedValues.add(new Object[]{22, 17, 16, 16, march23rd2013LocalDate, march23rd2013LocalDate});
		expectedValues.add(new Object[]{22, 18, 16, 16, march23rd2013LocalDate, march23rd2013LocalDate});
		expectedValues.add(new Object[]{22, 19, 16, 16, march23rd2013LocalDate, march23rd2013LocalDate});
		expectedValues.add(new Object[]{22, 20, 16, 16, march23rd2013LocalDate, march23rd2013LocalDate});
		expectedValues.add(new Object[]{22, 21, 16, 16, march23rd2013LocalDate, april2nd2011LocalDate});
		expectedValues.add(new Object[]{22, 22, 16, 16, march23rd2013LocalDate, (null)});

		for (int i = 0; i < allRows.size(); i++) {
			got = allRows.get(i).get(marq);
//			System.out.println("" + got.toString());
			Object[] expect = expectedValues.get(i);
			assertThat(got.countOfAllRows.intValue(), is((Integer) expect[0]));
			assertThat(got.rowNumber.intValue(), is((Integer) expect[1]));
			assertThat(got.countOfEnabled.intValue(), is((Integer) expect[2]));
			assertThat(got.rowWithinCarCo.intValue(), is((Integer) expect[3]));
			assertThat(got.lag.getValue(), is((LocalDate) expect[4]));
			assertThat(got.lead.getValue(), is((LocalDate) expect[5]));
		}
	}

	public static class MarqueWithLagAndLeadFunctions extends MarqueWithLocalDate {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBNumber countOfAllRows = new DBNumber(this.column(this.carCompany).count().over().allRows());
		@DBColumn
		DBNumber rowNumber = new DBNumber(this.column(this.uidMarque).count().over().AllRowsAndOrderBy(this.column(this.carCompany).ascending(), this.column(this.uidMarque).ascending()));
		@DBColumn
		DBNumber countOfEnabled = new DBNumber(this.column(this.carCompany).count().over().partition(this.column(this.carCompany)).unordered());
		@DBColumn
		DBNumber rowWithinCarCo = new DBNumber(this.column(this.carCompany).count()
				.over()
				.partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).ascending())
				.defaultFrame());
		@DBColumn
		DBLocalDate lag
				= this.column(this.creationLocalDate)
						.lag()
						.allRows()
						.orderBy(this.column(this.carCompany).ascending(), this.column(this.uidMarque).ascending())
						.asExpressionColumn();
		@DBColumn
		DBLocalDate lead = new DBLocalDate(this.column(this.creationLocalDate)
				.nextRowValue()
				.AllRowsAndOrderBy(
						this.column(this.carCompany).ascending(),
						this.column(this.uidMarque).ascending()
				)
		);
	}
}
