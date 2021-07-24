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
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
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
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBInstant;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.DBStringTrimmed;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.exceptions.AccidentalCartesianJoinException;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.hamcrest.Matchers;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class InstantExpressionTest extends AbstractTest {

	public InstantExpressionTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	public static class CurrentDateReport extends DBReport {

		private static final long serialVersionUID = 1L;
		public MarqueWithInstant marque = new MarqueWithInstant();
		@DBColumn
		public DBString name = new DBString(marque.column(marque.name));
		@DBColumn
		public DBInstant savedJavaDate = new DBInstant(marque.column(marque.creationInstant).toInstant());
		@DBColumn
		public DBInstant actualJavaDate = new DBInstant(Instant.now());
		@DBColumn
		public DBInstant currentDate = new DBInstant(InstantExpression.now());
		@DBColumn
		public DBNumber dayFromCurrentDate = new DBNumber(InstantExpression.now().day());
		@DBColumn
		public DBInstant currentDateTime = new DBInstant(InstantExpression.currentTime());
		@DBColumn
		public DBInstant currentDateTimeMinus10Seconds = new DBInstant(InstantExpression.now().addSeconds(-10));
		@DBColumn
		public DBInstant currentDateTimePlus10Seconds = new DBInstant(InstantExpression.now().addSeconds(10));
		@DBColumn
		public DBInstant created = marque.column(marque.creationInstant).toInstant().asExpressionColumn();
		@DBColumn
		public DBInstant currentTime = InstantExpression.currentTime().asExpressionColumn();
	}

	Instant march23rd2013Instant = (new GregorianCalendar(2013, 2, 23, 12, 34, 56)).toZonedDateTime().withZoneSameLocal(ZoneOffset.UTC).toInstant();
	Instant april2nd2011Instant = (new GregorianCalendar(2011, 3, 2, 1, 2, 3)).toZonedDateTime().withZoneSameLocal(ZoneOffset.UTC).toInstant();

	@Test
	public void testCheckDatabaseInstant() throws UnexpectedNumberOfRowsException, AccidentalCartesianJoinException, AccidentalBlankQueryException, SQLException {

		Instant systemInstant = database.getCurrentInstant();

		System.out.println("SYSTEMINSTANT: " + systemInstant);
		final Instant now = Instant.now();
		final Instant buffered = now.minus(10, ChronoUnit.MINUTES);

		Assert.assertThat(systemInstant, is(greaterThan(buffered)));

	}

	@Test
	public void testIsNotDateExpression() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		final InstantExpression fiveDaysPriorToCreation = marq.column(marq.creationInstant).toInstant().addDays(-5);
		query.addCondition(InstantExpression.leastOf(
				marq.column(marq.creationInstant).toInstant(),
				fiveDaysPriorToCreation,
				InstantExpression.value(march23rd2013Instant).addWeeks(-5),
				InstantExpression.value(march23rd2013Instant).addDays(-2))
				.isNot(fiveDaysPriorToCreation)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(18));
	}

	@Test
	public void testIsNotDate() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);

		query.addCondition(
				marq.column(marq.creationInstant).isNot(april2nd2011Instant)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(18));
	}

	@Test
	public void testLeastOf() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		final InstantExpression fiveDaysPriorToCreation = marq.column(marq.creationInstant).toInstant().addDays(-5);
		query.addCondition(
				InstantExpression.leastOf(
						marq.column(marq.creationInstant).toInstant(),
						fiveDaysPriorToCreation,
						InstantExpression.value(march23rd2013Instant).addWeeks(-5),
						InstantExpression.value(march23rd2013Instant).addDays(-2))
						.is(fiveDaysPriorToCreation)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testLeastOfWithList() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);

		final InstantExpression creationDate = marq.column(marq.creationInstant).toInstant();
		final InstantExpression fiveDaysPriorToCreation = creationDate.addDays(-5);

		List<InstantExpression> poss = new ArrayList<>();
		poss.add(creationDate);
		poss.add(fiveDaysPriorToCreation);
		poss.add(AnyExpression.value(march23rd2013Instant).addWeeks(-5));
		poss.add(AnyExpression.value(march23rd2013Instant).addDays(-2));

		query.addCondition(
				InstantExpression.leastOf(
						poss)
						.is(fiveDaysPriorToCreation)
		);

		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testLeastOfWithDates() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);

		query.addCondition(
				InstantExpression.leastOf(
						march23rd2013Instant,
						april2nd2011Instant
				).is(april2nd2011Instant)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(22));
	}

	@Test
	public void testGreatestOf() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		final InstantExpression creationDate = marq.column(marq.creationInstant).toInstant();
		final InstantExpression fiveDaysPriorToCreation = creationDate.addDays(-5);
		query.addCondition(
				InstantExpression.greatestOf(
						creationDate,
						fiveDaysPriorToCreation,
						AnyExpression.value(march23rd2013Instant).addWeeks(-5),
						AnyExpression.value(march23rd2013Instant).addDays(-2)
				).is(creationDate)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(18));
	}

	@Test
	public void testGreatestOfWithList() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		final InstantExpression creationDate = marq.column(marq.creationInstant).toInstant();
		final InstantExpression fiveDaysPriorToCreation = creationDate.addDays(-5);
		List<InstantExpression> poss = new ArrayList<>();
		poss.add(creationDate);
		poss.add(fiveDaysPriorToCreation);
		poss.add(InstantExpression.value(march23rd2013Instant).addWeeks(-5));
		poss.add(InstantExpression.value(march23rd2013Instant).addDays(-2));
		query.addCondition(
				InstantExpression.greatestOf(poss).is(creationDate)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(18));
	}

	@Test
	public void testGreatestOfWithDates() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(InstantExpression.greatestOf(march23rd2013Instant,
				april2nd2011Instant)
				.is(march23rd2013Instant)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(22));
	}

	@Test
	public void testOverlapsDateExpressionDateResult() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(InstantExpression.overlaps(marq.column(marq.creationInstant).toInstant(),
				marq.column(marq.creationInstant).toInstant().addDays(-5),
				InstantExpression.value(march23rd2013Instant).addWeeks(-5),
				InstantExpression.value(march23rd2013Instant).addDays(-2))
		);
		List<DBQueryRow> allRows = query.getAllRows();
		Assert.assertThat(allRows.size(), is(18));
	}

	@Test
	public void testOverlapsAllDateResults() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(InstantExpression.overlaps(marq.column(marq.creationInstant).toInstant(),
				marq.column(marq.creationInstant).toInstant().addDays(-5),
				InstantExpression.value(march23rd2013Instant).addWeeks(-5),
				InstantExpression.value(march23rd2013Instant).addDays(-2))
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
		Assert.assertThat(got.maxOfDates.instantValue(), is(march23rd2013Instant));
		Assert.assertThat(got.minOfDates.getValue(), is(april2nd2011Instant));

	}

	public static class MarqueWithDateAggregators extends MarqueWithInstant {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBNumber countOfDates = new DBNumber(this.column(this.creationInstant).count());
		@DBColumn
		DBInstant maxOfDates = new DBInstant(this.column(this.creationInstant).max());
		@DBColumn
		DBInstant minOfDates = this.column(this.creationInstant).min().asExpressionColumn();

		{
			this.setReturnFields(this.countOfDates, this.maxOfDates, this.minOfDates);
		}
	}

	@Test
	public void testWindowingFunctions() throws SQLException {
		MarqueWithDateWindowingFunctions marq = new MarqueWithDateWindowingFunctions();

		DBQuery query = database.getDBQuery(marq).setQueryLabel("TEST WINDOWING FUNCTIONS")
				.setBlankQueryAllowed(true)
				.setSortOrder(marq.column(marq.carCompany));

		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(22));

		MarqueWithDateWindowingFunctions got;// = allRows.get(0).get(marq);
		ArrayList<Object[]> expectedValues = new ArrayList<>();
		expectedValues.add(new Object[]{2, march23rd2013Instant, march23rd2013Instant});
		expectedValues.add(new Object[]{2, march23rd2013Instant, march23rd2013Instant});
		expectedValues.add(new Object[]{1, march23rd2013Instant, march23rd2013Instant});
		expectedValues.add(new Object[]{3, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{3, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{3, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		for (int i = 0; i < allRows.size(); i++) {
			got = allRows.get(i).get(marq);
			Object[] expect = expectedValues.get(i);
			Assert.assertThat(got.countOfDates.intValue(), is((Integer) expect[0]));
			Assert.assertThat(got.maxOfAll.getValue(), is((Instant) expect[1]));
			Assert.assertThat(got.maxOfDates.getValue(), is((Instant) expect[1]));
			Assert.assertThat(got.minOfDates.getValue(), is((Instant) expect[2]));
		}
	}

	public static class MarqueWithDateWindowingFunctions extends MarqueWithInstant {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBNumber countOfAllRows = new DBNumber(this.column(this.creationInstant).count().over().allRows());
		@DBColumn
		DBNumber rowNumber = new DBNumber(this.column(this.creationInstant).count().over().AllRowsAndOrderBy(this.column(this.carCompany).ascending()));
		@DBColumn
		DBNumber countOfDates = new DBNumber(this.column(this.creationInstant).count().over().partition(this.column(this.carCompany)).unordered());
		@DBColumn
		DBNumber rowWithinCarCo = new DBNumber(this.column(this.creationInstant).count()
				.over()
				.partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).ascending())
				.defaultFrame());
		@DBColumn
		DBInstant maxOfAll = new DBInstant(this.column(this.creationInstant).max().over().allRows());
		@DBColumn
		DBInstant maxOfDates = new DBInstant(this.column(this.creationInstant).toInstant().max().over().partition(this.column(this.carCompany)).unordered());
		@DBColumn
		DBInstant minOfDates = this.column(this.creationInstant).toInstant().min().over().partition(this.column(this.carCompany)).unordered().asExpressionColumn();
		@DBColumn
		DBInstant test1 = this.column(this.creationInstant)
				.toInstant()
				.min()
				.over().partition(this.column(this.carCompany))
				.unsorted().asExpressionColumn();
		@DBColumn
		DBInstant test2 = (this.column(this.creationInstant).toInstant().min()
				.over().partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).descending())
				.rows().unboundedPreceding().currentRow()).asExpressionColumn();
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
		expectedValues.add(new Object[]{2, march23rd2013Instant, march23rd2013Instant});
		expectedValues.add(new Object[]{2, march23rd2013Instant, march23rd2013Instant});
		expectedValues.add(new Object[]{1, march23rd2013Instant, march23rd2013Instant});
		expectedValues.add(new Object[]{3, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{3, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{3, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		for (int i = 0; i < allRows.size(); i++) {
			got = allRows.get(i).get(marq);
			Object[] expect = expectedValues.get(i);
			Assert.assertThat(got.countOfDates.intValue(), is((Integer) expect[0]));
			Assert.assertThat(got.maxOfDates.getValue(), is((Instant) expect[1]));
			Assert.assertThat(got.minOfDates.getValue(), is((Instant) expect[2]));
		}
	}

	public static class MarqueWithAggregatorAndDateWindowingFunctions extends MarqueWithInstant {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBNumber countAggregator = new DBNumber(this.column(this.creationInstant).count());
		@DBColumn
		DBNumber countOfAllRows = new DBNumber(this.column(this.creationInstant).count().over().allRows());
		@DBColumn
		DBNumber countOfDates = new DBNumber(this.column(this.creationInstant).count()
				.over()
				.partition(this.column(this.carCompany)).unordered());
		@DBColumn
		DBInstant maxOfAll = this.column(this.creationInstant)
				.toInstant().max().over().allRows().asExpressionColumn();
		@DBColumn
		DBInstant maxOfDates = this.column(this.creationInstant)
				.toInstant().max().over()
				.partition(this.column(this.carCompany)).unordered().asExpressionColumn();
		@DBColumn
		DBInstant minOfDates = this.column(this.creationInstant)
				.toInstant().min()
				.over()
				.partition(this.column(this.carCompany)).unordered()
				.asExpressionColumn();
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
//		database.print(allRows);

		Assert.assertThat(allRows.size(), is(22));

		MarqueWithComplexWindowingFunction got;// = allRows.get(0).get(marq);
		ArrayList<Object[]> expectedValues = new ArrayList<>();
		expectedValues.add(new Object[]{2, march23rd2013Instant, march23rd2013Instant});
		expectedValues.add(new Object[]{2, march23rd2013Instant, march23rd2013Instant});
		expectedValues.add(new Object[]{1, march23rd2013Instant, march23rd2013Instant});
		expectedValues.add(new Object[]{3, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{3, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{3, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		expectedValues.add(new Object[]{15, march23rd2013Instant, april2nd2011Instant});
		for (int i = 0; i < allRows.size(); i++) {
			got = allRows.get(i).get(marq);
			Object[] expect = expectedValues.get(i);
		}
	}

	public static class MarqueWithComplexWindowingFunction extends MarqueWithInstant {

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
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.addConditions(
				marq.column(marq.creationInstant).toInstant().isBetween(InstantExpression.currentInstant(), InstantExpression.nullInstant())
		);

		List<DBQueryRow> got = query.getAllRows();

		Assert.assertThat(got.size(), is(0));
		final Instant then = Instant.now();

		database.insert(new MarqueWithInstant(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", then, 3, null));

		MarqueWithInstant reportLimitingMarque = new MarqueWithInstant();
		reportLimitingMarque.name.permittedPatternIgnoreCase("% HUMMER %");
		CurrentDateReport currentDateReport = new CurrentDateReport();

		final List<CurrentDateReport> reportRows = DBReport.getRows(database, currentDateReport, reportLimitingMarque);

		Assert.assertThat(reportRows.size(), is(1));

		query = database.getDBQuery(marq);
		query.addConditions(
				marq.column(marq.creationInstant)
						.day().isIn(
								InstantExpression.now().addSeconds(-10).day(),
								InstantExpression.now().addSeconds(10).day()
						),
				marq.column(marq.creationInstant)
						.isGreaterThan(march23rd2013Instant)
		);

		got = query.getAllRows();
		if (got.size() != 1) {
			System.out.println("CREATION DATE EXPECTED: " + then);
			System.out.println("FOUND ROWS: " + got.size());
			database.getDBTable(marq).setBlankQueryAllowed(true).print();
		}
		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testCurrentDateTimeAndAddSecondsFunctions() throws SQLException {

		MarqueWithInstant marq = new MarqueWithInstant();
		marq.creationInstant.permittedRangeInclusive(Instant.now(), null);
		List<MarqueWithInstant> got = database.get(marq);

		Assert.assertThat(got.size(), is(0));

		database.insert(new MarqueWithInstant(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", Instant.now(), 3, null));

		MarqueWithInstant reportLimitingMarque = new MarqueWithInstant();
		reportLimitingMarque.name.permittedPatternIgnoreCase("% HUMMER %");
		CurrentDateReport currentDateReport = new CurrentDateReport();
		final List<CurrentDateReport> reportRows = DBReport.getRows(database, currentDateReport, reportLimitingMarque);

		Assert.assertThat(reportRows.size(), is(1));

		marq.creationInstant.permittedRangeInclusive(InstantExpression.currentDate().addSeconds(-10), null);
		got = database.get(marq);

		Assert.assertThat(got.size(), is(1));

		marq.creationInstant.permittedRangeInclusive(null, InstantExpression.currentDate().addSeconds(-10));
		got = database.get(marq);

		Assert.assertThat(got.size(), is(21));

		marq.creationInstant.permittedRangeInclusive(
				InstantExpression.currentDate().addMinutes(-5), null);
		got = database.getDBTable(marq).getAllRows();

		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testCurrentDateAndAddDaysFunctions() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		marq.creationInstant.permittedRangeInclusive(InstantExpression.currentDate(), null);
		List<MarqueWithInstant> got = database.get(marq);

		Assert.assertThat(got.size(), is(0));

		database.insert(new MarqueWithInstant(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", Instant.now(), 3, null));
		marq.creationInstant.permittedRangeInclusive(InstantExpression.currentDate().addDays(-1), null);
		got = database.get(marq);

		Assert.assertThat(got.size(), is(1));

		marq.creationInstant.permittedRangeInclusive(null, InstantExpression.currentDate().addDays(-1));
		got = database.get(marq);

		Assert.assertThat(got.size(), is(21));

		marq.creationInstant.permittedRangeInclusive(InstantExpression.currentDate().addDays(-1), InstantExpression.currentDate().addDays(+1));
		got = database.get(marq);

		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testCurrentDateAndAddHoursFunctions() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		marq.creationInstant.permittedRangeInclusive(InstantExpression.currentDate(), null);
		List<MarqueWithInstant> got = database.get(marq);

		Assert.assertThat(got.size(), is(0));

		database.insert(new MarqueWithInstant(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", Instant.now(), 3, null));
		marq.creationInstant.permittedRangeInclusive(InstantExpression.currentDate().addHours(-1), null);
		got = database.get(marq);

		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testCurrentDateAndAddWeeksFunctions() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		marq.creationInstant.permittedRangeInclusive(InstantExpression.currentDate(), null);
		List<MarqueWithInstant> got = database.get(marq);

		Assert.assertThat(got.size(), is(0));

		database.insert(new MarqueWithInstant(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", Instant.now(), 3, null));
		marq.creationInstant.permittedRangeInclusive(InstantExpression.currentDate().addWeeks(-1), null);
		got = database.get(marq);

		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testCurrentDateAndAddMinutesFunctions() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		marq.creationInstant.permittedRangeInclusive(InstantExpression.currentDate(), null);
		List<MarqueWithInstant> got = database.get(marq);

		Assert.assertThat(got.size(), is(0));

		database.insert(new MarqueWithInstant(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", Instant.now(), 3, null));
		marq.creationInstant.permittedRangeInclusive(InstantExpression.currentDate().addMinutes(-1), null);
		got = database.get(marq);

		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testCurrentDateAndAddMonthsFunctions() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		marq.creationInstant.permittedRangeInclusive(InstantExpression.currentDate(), null);
		List<MarqueWithInstant> got = database.get(marq);

		Assert.assertThat(got.size(), is(0));

		database.insert(new MarqueWithInstant(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", Instant.now(), 3, null));
		marq.creationInstant.permittedRangeInclusive(InstantExpression.currentDate().addMonths(-1), null);
		got = database.get(marq);

		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testCurrentDateAndAddYearsFunctions() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		marq.creationInstant.permittedRangeInclusive(InstantExpression.currentDate(), null);
		List<MarqueWithInstant> got = database.get(marq);

		Assert.assertThat(got.size(), is(0));

		database.insert(new MarqueWithInstant(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", Instant.now(), 3, null));
		marq.creationInstant.permittedRangeInclusive(InstantExpression.currentDate().addYears(-1), null);
		got = database.get(marq);

		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testYearFunction() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).year().is(2014));
		List<MarqueWithInstant> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(0));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).year().is(2013));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));
	}

	@Test
	public void testYearIsFunction() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).yearIs(2014));
		List<MarqueWithInstant> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(0));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).yearIs(2013));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).yearIs(NumberExpression.value(2014).numberResult()));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(0));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).yearIs(NumberExpression.value(2013)));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));
	}

	@Test
	public void testMonthFunction() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).monthIs(3));
		List<MarqueWithInstant> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).monthIs(4));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).monthIs(NumberExpression.value(3).numberResult()));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).monthIs(NumberExpression.value(4)));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));
	}

	@Test
	public void testEndOfMonthFunction() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).endOfMonth().day().is(31));
		List<MarqueWithInstant> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).endOfMonth().day().is(30));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).endOfMonth().day().isNull());
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testFirstOfMonthFunction() throws SQLException {
		Instant march1st2013 = (new GregorianCalendar(2013, 2, 1)).toZonedDateTime().toInstant();
		Instant march2nd2013 = (new GregorianCalendar(2013, 2, 2)).toZonedDateTime().toInstant();
		Instant april1st2011 = (new GregorianCalendar(2011, 3, 1)).toZonedDateTime().toInstant();

		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).toInstant().firstOfMonth().atStartOfDay().isBetween(march1st2013, march2nd2013));
		List<MarqueWithInstant> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(marq.column(marq.creationInstant).toInstant().firstOfMonth().atStartOfDay().isBetween(april1st2011, april2nd2011Instant));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(marq.column(marq.creationInstant).toInstant().firstOfMonth().atStartOfDay().isBetween(InstantExpression.value(april1st2011), april2nd2011Instant));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(marq.column(marq.creationInstant).toInstant().firstOfMonth().atStartOfDay().isBetween(april1st2011, InstantExpression.value(april2nd2011Instant)));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).firstOfMonth().isNull());
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testIsNotNull() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).firstOfMonth().isNotNull());
		List<MarqueWithInstant> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(21));
	}

	@Test
	public void testBetweenInclusiveFunction() throws SQLException {
		Instant february28th2013 = LocalDateTime.of(2013, Month.FEBRUARY, 28, 0, 0, 0).atOffset(ZoneOffset.UTC).toInstant();
		Instant march2nd2013 = LocalDateTime.of(2013, Month.MARCH, 2, 0, 0, 0).atOffset(ZoneOffset.UTC).toInstant();
		Instant april1st2011 = LocalDateTime.of(2011, Month.APRIL, 1, 0, 0, 0).atOffset(ZoneOffset.UTC).toInstant();

		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).firstOfMonth().isBetweenInclusive(february28th2013, march2nd2013));

		List<MarqueWithInstant> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(marq.column(marq.creationInstant).toInstant().firstOfMonth().isBetweenInclusive(april1st2011, april2nd2011Instant));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(marq.column(marq.creationInstant).toInstant().firstOfMonth().isBetweenInclusive(InstantExpression.value(april1st2011), april2nd2011Instant));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(marq.column(marq.creationInstant).toInstant().firstOfMonth().isBetweenInclusive(april1st2011, InstantExpression.value(april2nd2011Instant)));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).toInstant().firstOfMonth().isNull());
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testisGreaterThan() throws SQLException {
		Instant march1st2013 = (new GregorianCalendar(2013, 2, 1)).toZonedDateTime().toInstant();
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).toInstant().firstOfMonth().isGreaterThan(march1st2013));
		List<MarqueWithInstant> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));
	}

	@Test
	public void testisGreaterThanOrEqual() throws SQLException {
		Instant march1st2013 = (new GregorianCalendar(2013, 2, 1)).toZonedDateTime().toInstant();
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).toInstant().firstOfMonth().isGreaterThanOrEqual(march1st2013));
		List<MarqueWithInstant> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));
	}

	@Test
	public void testisGreaterThanWithFallback() throws SQLException {
		Instant march1st2013 = (new GregorianCalendar(2013, 2, 1)).toZonedDateTime().toInstant();
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).toInstant().firstOfMonth().isGreaterThan(march1st2013, marq.column(marq.name).isGreaterThan("T")));
		List<MarqueWithInstant> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));
	}

	@Test
	public void testisLessThan() throws SQLException {
		Instant march1st2013 = (new GregorianCalendar(2013, 2, 1)).toZonedDateTime().toInstant();
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).toInstant().firstOfMonth().isLessThan(march1st2013));
		List<MarqueWithInstant> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));
	}

	@Test
	public void testisLessThanWithFallback() throws SQLException {
		Instant march1st2013 = (new GregorianCalendar(2013, 2, 1)).toZonedDateTime().toInstant();
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).toInstant().firstOfMonth().isLessThan(march1st2013, marq.column(marq.name).isGreaterThan("T")));
		List<MarqueWithInstant> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));
	}

	@Test
	public void testBetweenExclusiveFunction() throws SQLException {
		final Instant march1st2013 = (new GregorianCalendar(2013, 2, 1)).toZonedDateTime().toInstant();
		final Instant march2nd2013 = (new GregorianCalendar(2013, 2, 2)).toZonedDateTime().toInstant();
		final Instant april1st2011 = (new GregorianCalendar(2011, 3, 1)).toZonedDateTime().toInstant();

		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).toInstant().firstOfMonth().atStartOfDay().isBetweenExclusive(march1st2013, march2nd2013));
		List<MarqueWithInstant> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(marq.column(marq.creationInstant).toInstant().firstOfMonth().atStartOfDay().isBetweenExclusive(april1st2011, april2nd2011Instant));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(marq.column(marq.creationInstant).toInstant().firstOfMonth().atStartOfDay().isBetweenExclusive(InstantExpression.value(april1st2011), april2nd2011Instant));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(marq.column(marq.creationInstant).toInstant().firstOfMonth().atStartOfDay().isBetweenExclusive(april1st2011, InstantExpression.value(april2nd2011Instant)));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).firstOfMonth().isNull());
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(1));

	}

	@Test
	public void testDayOfWeekFunction() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq).setQueryLabel("FIND DAY OF WEEK NEXT MONTH IS TUESDAY");
		query.addCondition(
				marq.column(marq.creationInstant).addMonths(1).dayOfWeek().is(InstantExpression.TUESDAY));
		List<MarqueWithInstant> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq).setQueryLabel("FIND DAY OF WEEK NEXT MONTH IS MONDAY");
		query.addCondition(
				marq.column(marq.creationInstant).addMonths(1).dayOfWeek().is(InstantExpression.MONDAY));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq).setQueryLabel("FIND DAY OF WEEK NEXT MONTH IS NULL");
		query.addCondition(
				marq.column(marq.creationInstant).addMonths(1).dayOfWeek().isNull());
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testDayFunction() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).day().is(23));
		List<MarqueWithInstant> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).day().is(2));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));
	}

	@Test
	public void testDayIsFunction() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.setQueryLabel("First");
		query.addCondition(
				marq.column(marq.creationInstant).dayIs(23));
		List<MarqueWithInstant> got = query.getAllInstancesOf(marq);
		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).dayIs(2));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).dayIs(NumberExpression.value(2)));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).dayIs(NumberExpression.value(2).numberResult()));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));
	}

	@Test
	public void testHourFunction() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq).setQueryLabel("Find Creationdate with hour=12");
		query.addCondition(
				marq.column(marq.creationInstant).hour().is(12));
		List<MarqueWithInstant> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).hour().is(1));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));
	}

	@Test
	public void testHourIsFunction() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).hourIs(12));
		List<MarqueWithInstant> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).hourIs(1));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).hourIs(NumberExpression.value(1).numberResult()));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).hourIs(NumberExpression.value(1)));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));
	}

	@Test
	public void testMinuteFunction() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).minute().is(34));
		List<MarqueWithInstant> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).minute().is(2));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));
	}

	@Test
	public void testMinuteIsFunction() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).minuteIs(34));
		List<MarqueWithInstant> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).minuteIs(2));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).minuteIs(NumberExpression.value(2).numberResult()));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).minuteIs(NumberExpression.value(2)));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));
	}

	@Test
	public void testSecondFunction() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).second().is(56));
		List<MarqueWithInstant> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).second().is(3));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));
	}

	@Test
	public void testSecondIsFunction() throws SQLException {
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).secondIs(56));
		List<MarqueWithInstant> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).secondIs(3));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));

		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).secondIs(NumberExpression.value(3)));
		got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(3));
	}

	@Test
	public void testIsInWithNulls() throws SQLException, ParseException {
		MarqueWithInstant marque = new MarqueWithInstant();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationInstant).toInstant()
						.isIn((Instant) null,
								LocalDateTime.parse(
										firstDateStr.subSequence(0, firstDateStr.length()),
										LOCALDATETIME_FORMAT
								).toInstant(ZoneOffset.UTC)
						)
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		Assert.assertThat(allRows.size(), is(19));

		MarqueWithInstant newMarque = new MarqueWithInstant(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", null, 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(23));

		dbQuery.addCondition(
				marque.column(marque.creationInstant).toInstant()
						.isIn(
								(Instant) null,
								LocalDateTime.parse(
										firstDateStr.subSequence(0, firstDateStr.length()),
										LOCALDATETIME_FORMAT
								).toInstant(ZoneOffset.UTC)
						)
		);

		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(20));
	}

	@Test
	public void testIsIn() throws SQLException, ParseException {
		MarqueWithInstant marque = new MarqueWithInstant();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationInstant).toInstant().isIn(
						(Instant) null,
						LocalDateTime.parse(firstDateStr.subSequence(0, firstDateStr.length()), LOCALDATETIME_FORMAT).toInstant(ZoneOffset.UTC)
				)
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(19));

		MarqueWithInstant newMarque = new MarqueWithInstant(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", null, 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(23));

		dbQuery.addCondition(marque.column(marque.creationInstant).toInstant().isIn(april2nd2011Instant, march23rd2013Instant)
		);

		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(21));
	}

	@Test
	public void testIsInWithList() throws SQLException, ParseException {
		MarqueWithInstant marque = new MarqueWithInstant();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationInstant).toInstant()
						.isIn(
								(Instant) null,
								LocalDateTime.parse(firstDateStr.subSequence(0, firstDateStr.length()), LOCALDATETIME_FORMAT).toInstant(ZoneOffset.UTC)
						)
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(19));

		MarqueWithInstant newMarque = new MarqueWithInstant(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", null, 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(23));

		List<InstantExpression> dates = new ArrayList<InstantExpression>();
		dates.add(InstantExpression.value(march23rd2013Instant));
		dates.add(InstantExpression.value(april2nd2011Instant));

		dbQuery.addCondition(
				marque.column(marque.creationInstant).toInstant().isIn(dates)
		);

		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(21));
	}

	@Test
	public void testIsWithNulls() throws SQLException, ParseException {
		MarqueWithInstant marque = new MarqueWithInstant();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationInstant).toInstant().is((Instant) null)
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));

		MarqueWithInstant newMarque = new MarqueWithInstant(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", null, 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(23));

		dbQuery.addCondition(
				marque.column(marque.creationInstant).toInstant().is((Instant) null)
		);

		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(2));
	}

	@Test
	public void testIsNull() throws SQLException, ParseException {
		MarqueWithInstant marque = new MarqueWithInstant();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.creationInstant).toInstant().isNull()
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).get(new MarqueWithInstant()).creationInstant.getValue(), Matchers.nullValue());

		MarqueWithInstant newMarque = new MarqueWithInstant(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", null, 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(23));

		dbQuery.addCondition(
				marque.column(marque.creationInstant).toInstant().isNull()
		);

		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(2));
	}

	public static class DiffTestReport extends DBReport {

		private static final long serialVersionUID = 1L;

		public MarqueWithInstant marq = new MarqueWithInstant();
		public DBInstant dayNormal
				= marq.column(marq.creationInstant).toInstant().asExpressionColumn();
		public DBInstant dayAdds
				= marq.column(marq.creationInstant).toInstant().addDays(2).asExpressionColumn();
		public DBNumber dayDiff = new DBNumber(marq.column(marq.creationInstant).toInstant().daysFrom(
				marq.column(marq.creationInstant).toInstant().addDays(2)));
		public DBNumber dayDiffAsHours = new DBNumber(marq.column(marq.creationInstant).toInstant().hoursFrom(
				marq.column(marq.creationInstant).toInstant().addDays(2)));
		public DBNumber monthDiff = new DBNumber(
				marq.column(marq.creationInstant).toInstant()
						.monthsFrom(
								marq.column(marq.creationInstant).toInstant()
										.addMonths(2)));
	}

	@Test
//	@Ignore
	public void testDayDifferenceFunction() throws SQLException, ParseException {
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).toInstant()
						.daysFrom(
								marq.column(marq.creationInstant).toInstant().addDays(2))
						.is(2));
		List<MarqueWithInstant> got = query.getAllInstancesOf(marq);

		MarqueWithInstant nonNullMarque = new MarqueWithInstant();
		nonNullMarque.creationInstant.excludeNull();
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		marq = new MarqueWithInstant();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).toInstant()
						.hoursFrom(
								marq.column(marq.creationInstant).toInstant().addDays(2))
						.isIn(48, 49)); //Interestingly  one of my examples is near DST transition and NuoDB gives 49 hours
		got = query.getAllInstancesOf(marq);

		nonNullMarque = new MarqueWithInstant();
		nonNullMarque.creationInstant.permitOnlyNotNull();
		numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		Instant secondInstant = LocalDateTime.of(2011, Month.APRIL, 2, 1, 2, 3).toInstant(ZoneOffset.UTC);
		marq = new MarqueWithInstant();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).toInstant()
						.daysFrom(secondInstant)
						.is(0));
		got = query.getAllInstancesOf(marq);

		MarqueWithInstant secondDateMarques = new MarqueWithInstant();
		Instant secondDate = april2nd2011Instant;//AbstractTest.DATETIME_FORMAT.parse(AbstractTest.secondDateStr).toInstant();
		secondDateMarques.creationInstant.permittedValues(secondDate);
		int numberOfSecondDateRows = database.getDBTable(secondDateMarques).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfSecondDateRows));
	}

	@Test
//	@Ignore
	public void testWeekDifferenceFunction() throws SQLException, ParseException {
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).toInstant()
						.weeksFrom(
								marq.column(marq.creationInstant).toInstant().addWeeks(2))
						.is(2));
		List<MarqueWithInstant> got = query.getAllInstancesOf(marq);

		MarqueWithInstant nonNullMarque = new MarqueWithInstant();
		nonNullMarque.creationInstant.excludeNull();
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		Instant secondInstant = LocalDateTime.of(2011, Month.APRIL, 2, 1, 2, 3).toInstant(ZoneOffset.UTC);
		marq = new MarqueWithInstant();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).toInstant()
						.weeksFrom(secondInstant)
						.is(0));
		got = query.getAllInstancesOf(marq);

		MarqueWithInstant secondDateMarques = new MarqueWithInstant();
		secondDateMarques.creationInstant.permittedValues(secondInstant);
		int numberOfSecondDateRows = database.getDBTable(secondDateMarques).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfSecondDateRows));
	}

	@Test
//	@Ignore
	public void testMonthDifferenceFunction() throws SQLException, ParseException {
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).toInstant()
						.monthsFrom(
								marq.column(marq.creationInstant).toInstant().addMonths(2))
						.is(2));
		List<MarqueWithInstant> got = query.getAllInstancesOf(marq);

		MarqueWithInstant nonNullMarque = new MarqueWithInstant();
		nonNullMarque.creationInstant.excludeNull();
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		Instant secondInstant = LocalDateTime.of(2011, Month.APRIL, 2, 1, 2, 3).toInstant(ZoneOffset.UTC);
		marq = new MarqueWithInstant();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).toInstant()
						.monthsFrom(secondInstant)
						.is(0));
		got = query.getAllInstancesOf(marq);

		MarqueWithInstant secondDateMarques = new MarqueWithInstant();
		secondDateMarques.creationInstant.permittedValues(secondInstant);
		int numberOfSecondDateRows = database.getDBTable(secondDateMarques).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfSecondDateRows));
	}

	@Test
//	@Ignore
	public void testYearDifferenceFunction() throws SQLException, ParseException {
//		System.out.println("nz.co.gregs.dbvolution.expressions.InstantExpressionTest.testYearDifferenceFunction()");
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).toInstant()
						.yearsFrom(
								marq.column(marq.creationInstant).toInstant().addYears(2))
						.is(2));
		List<MarqueWithInstant> got = query.getAllInstancesOf(marq);

		MarqueWithInstant nonNullMarque = new MarqueWithInstant();
		nonNullMarque.creationInstant.excludeNull();
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		Instant secondInstant = april2nd2011Instant;//LocalDateTime.of(2011, Month.APRIL, 2, 1, 2, 3).toInstant(ZoneOffset.UTC);
		Instant secondDate = april2nd2011Instant;//AbstractTest.DATETIME_FORMAT.parse(AbstractTest.secondDateStr).toInstant();
		marq = new MarqueWithInstant();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).toInstant()
						.yearsFrom(secondInstant)
						.is(0));
		got = query.getAllInstancesOf(marq);

		MarqueWithInstant secondDateMarques = new MarqueWithInstant();
		secondDateMarques.creationInstant.permittedValues(secondDate);
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
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).toInstant()
						.hoursFrom(
								marq.column(marq.creationInstant).toInstant().addHours(2))
						.is(2));
		List<MarqueWithInstant> got = query.getAllInstancesOf(marq);

		MarqueWithInstant nonNullMarque = new MarqueWithInstant();
		nonNullMarque.creationInstant.excludeNull();
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		Instant secondInstant = april2nd2011Instant;//LocalDateTime.of(2011, Month.APRIL, 2, 1, 2, 3).toInstant(ZoneOffset.UTC);
		Instant secondDate = april2nd2011Instant;//AbstractTest.DATETIME_FORMAT.parse(AbstractTest.secondDateStr).toInstant();
		marq = new MarqueWithInstant();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).toInstant()
						.hoursFrom(secondInstant)
						.is(0));
		got = query.getAllInstancesOf(marq);

		MarqueWithInstant secondDateMarques = new MarqueWithInstant();
		secondDateMarques.creationInstant.permittedValues(secondDate);
		int numberOfSecondDateRows = database.getDBTable(secondDateMarques).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfSecondDateRows));
	}

	@Test
//	@Ignore
	public void testMinutesDifferenceFunction() throws SQLException, ParseException {
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).toInstant()
						.minutesFrom(
								marq.column(marq.creationInstant).toInstant().addMinutes(2))
						.is(2));
		List<MarqueWithInstant> got = query.getAllInstancesOf(marq);

		MarqueWithInstant nonNullMarque = new MarqueWithInstant();
		nonNullMarque.creationInstant.excludeNull();
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		Instant secondInstant = april2nd2011Instant;//LocalDateTime.of(2011, Month.APRIL, 2, 1, 2, 3).toInstant(ZoneOffset.UTC);
		Instant secondDate = april2nd2011Instant;//AbstractTest.DATETIME_FORMAT.parse(AbstractTest.secondDateStr).toInstant();
		marq = new MarqueWithInstant();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).toInstant()
						.minutesFrom(secondInstant)
						.is(0));
		got = query.getAllInstancesOf(marq);

		MarqueWithInstant secondDateMarques = new MarqueWithInstant();
		secondDateMarques.creationInstant.permittedValues(secondDate);
		int numberOfSecondDateRows = database.getDBTable(secondDateMarques).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfSecondDateRows));
	}

	@Test
	public void testSecondsDifferenceFunction() throws SQLException, ParseException {
		MarqueWithInstant marq = new MarqueWithInstant();
		DBQuery query = database.getDBQuery(marq).setQueryLabel("Find rows where creation date +2seconds is 2 seconds from creation date ");
		query.addCondition(
				marq.column(marq.creationInstant)
						.secondsFrom(
								marq.column(marq.creationInstant).addSeconds(2))
						.is(2));
		List<MarqueWithInstant> got = query.getAllInstancesOf(marq);

		MarqueWithInstant nonNullMarque = new MarqueWithInstant();
		nonNullMarque.creationInstant.excludeNull();
		int numberOfRowsWithACreationDate = database.getDBTable(nonNullMarque).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfRowsWithACreationDate));

		Instant secondInstant = april2nd2011Instant;//LocalDateTime.of(2011, Month.APRIL, 2, 1, 2, 3).toInstant(ZoneOffset.UTC);
		Instant secondDate = april2nd2011Instant;//AbstractTest.DATETIME_FORMAT.parse(AbstractTest.secondDateStr).toInstant();
		marq = new MarqueWithInstant();
		query = database.getDBQuery(marq);
		query.addCondition(
				marq.column(marq.creationInstant).toInstant()
						.secondsFrom(secondInstant)
						.is(0));

		got = query.getAllInstancesOf(marq);

		MarqueWithInstant secondDateMarques = new MarqueWithInstant();
		secondDateMarques.creationInstant.permittedValues(secondDate);
		int numberOfSecondDateRows = database.getDBTable(secondDateMarques).setBlankQueryAllowed(true).count().intValue();
		Assert.assertThat(got.size(), is(numberOfSecondDateRows));
	}

	public static class MarqueWithSecondsFromDate extends MarqueWithInstant {

		private static final long serialVersionUID = 1L;

		Instant date = Instant.ofEpochMilli(10L).atZone(ZoneId.systemDefault()).toInstant();
		@DBColumn
		DBNumber subseconds = new DBNumber(AnyExpression.value(date).toInstant().subsecond());
	}

	@Test
	public void testSecondsFromReturnsDecimal() throws SQLException {
		final List<MarqueWithSecondsFromDate> allRows = database.getDBTable(new MarqueWithSecondsFromDate()).setBlankQueryAllowed(true).getAllRows();
		allRows.forEach((row)
				-> Assert.assertThat(row.subseconds.doubleValue(), is(0.01))
		);
	}

	@Test
	public void testEndOfMonthCalculation() throws SQLException {
		MarqueWithEndOfMonthForInstantColumn marq = new MarqueWithEndOfMonthForInstantColumn();
		DBTable<MarqueWithEndOfMonthForInstantColumn> table = database
				.getDBTable(marq)
				.setBlankQueryAllowed(true)
				.setQueryLabel("testEndOfMonthCalculation");
		List<MarqueWithEndOfMonthForInstantColumn> allRows = table.getAllRows();

		Assert.assertThat(allRows.size(), is(22));

		final Instant march31st2013 = LocalDateTime.of(2013, Month.MARCH, 31, 12, 34, 56).toInstant(ZoneOffset.UTC);
		final Instant april30th2011 = LocalDateTime.of(2011, Month.APRIL, 30, 1, 2, 3).toInstant(ZoneOffset.UTC);
		final Instant march1st2013 = LocalDateTime.of(2013, Month.MARCH, 1, 12, 34, 56).toInstant(ZoneOffset.UTC);
		final Instant april1st2011 = LocalDateTime.of(2011, Month.APRIL, 1, 1, 2, 3).toInstant(ZoneOffset.UTC);
		final LocalDateTime nullDate = null;
		for (MarqueWithEndOfMonthForInstantColumn allRow : allRows) {
//			System.out.println(allRow);
			Assert.assertThat(allRow.endOfMonth.getValue(),
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

	@Before
	public void setupMarqueWithInstant() throws Exception {
		DBDatabase db = database;
		db.preventDroppingOfTables(false);
		db.dropTableIfExists(new MarqueWithInstant());
		db.createTable(new MarqueWithInstant());

		List<MarqueWithInstant> toInsert = new ArrayList<>();
		toInsert.add(new MarqueWithInstant(4893059, "True", 1246974, null, 3, "UV", "PEUGEOT", null, "Y", null, 4, true));
		toInsert.add(new MarqueWithInstant(4893090, "False", 1246974, "", 1, "UV", "FORD", "", "Y", march23rd2013Instant, 2, false));
		toInsert.add(new MarqueWithInstant(4893101, "False", 1246974, "", 2, "UV", "HOLDEN", "", "Y", march23rd2013Instant, 3, null));
		toInsert.add(new MarqueWithInstant(4893112, "False", 1246974, "", 2, "UV", "MITSUBISHI", "", "Y", march23rd2013Instant, 4, null));
		toInsert.add(new MarqueWithInstant(4893150, "False", 1246974, "", 3, "UV", "SUZUKI", "", "Y", march23rd2013Instant, 4, null));
		toInsert.add(new MarqueWithInstant(4893263, "False", 1246974, "", 2, "UV", "HONDA", "", "Y", march23rd2013Instant, 4, null));
		toInsert.add(new MarqueWithInstant(4893353, "False", 1246974, "", 4, "UV", "NISSAN", "", "Y", march23rd2013Instant, 4, null));
		toInsert.add(new MarqueWithInstant(4893557, "False", 1246974, "", 2, "UV", "SUBARU", "", "Y", march23rd2013Instant, 4, null));
		toInsert.add(new MarqueWithInstant(4894018, "False", 1246974, "", 2, "UV", "MAZDA", "", "Y", march23rd2013Instant, 4, null));
		toInsert.add(new MarqueWithInstant(4895203, "False", 1246974, "", 2, "UV", "ROVER", "", "Y", march23rd2013Instant, 4, null));
		toInsert.add(new MarqueWithInstant(4896300, "False", 1246974, null, 2, "UV", "HYUNDAI", null, "Y", march23rd2013Instant, 1, null));
		toInsert.add(new MarqueWithInstant(4899527, "False", 1246974, "", 1, "UV", "JEEP", "", "Y", march23rd2013Instant, 3, null));
		toInsert.add(new MarqueWithInstant(7659280, "False", 1246972, "Y", 3, "", "DAIHATSU", "", "Y", march23rd2013Instant, 4, null));
		toInsert.add(new MarqueWithInstant(7681544, "False", 1246974, "", 2, "UV", "LANDROVER", "", "Y", march23rd2013Instant, 4, null));
		toInsert.add(new MarqueWithInstant(7730022, "False", 1246974, "", 2, "UV", "VOLVO", "", "Y", march23rd2013Instant, 4, null));
		toInsert.add(new MarqueWithInstant(8376505, "False", 1246974, "", null, "", "ISUZU", "", "Y", march23rd2013Instant, 4, null));
		toInsert.add(new MarqueWithInstant(8587147, "False", 1246974, "", null, "", "DAEWOO", "", "Y", march23rd2013Instant, 4, null));
		toInsert.add(new MarqueWithInstant(9971178, "False", 1246974, "", 1, "", "CHRYSLER", "", "Y", march23rd2013Instant, 4, null));
		toInsert.add(new MarqueWithInstant(13224369, "False", 1246974, "", 0, "", "VW", "", "Y", april2nd2011Instant, 4, null));
		toInsert.add(new MarqueWithInstant(6664478, "False", 1246974, "", 0, "", "BMW", "", "Y", april2nd2011Instant, 4, null));
		toInsert.add(new MarqueWithInstant(1, "False", 1246974, "", 0, "", "TOYOTA", "", "Y", march23rd2013Instant, 1, true));
		toInsert.add(new MarqueWithInstant(2, "False", 1246974, "", 0, "", "HUMMER", "", "Y", april2nd2011Instant, 3, null));

		db.insert(toInsert);
	}

	public static class MarqueWithEndOfMonthForInstantColumn extends MarqueWithInstant {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBInstant firstOfMonth = this.column(this.creationInstant)
				.firstOfMonth()
				.asExpressionColumn();
		@DBColumn
		DBInstant endOfMonth = this.column(this.creationInstant)
				.endOfMonth().asExpressionColumn();
	}

	@DBTableName("marque_with_instant")
	public static class MarqueWithInstant extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn("numeric_code")
		public DBNumber numericCode = new DBNumber();

		@DBColumn("uid_marque")
		@DBPrimaryKey
		public DBInteger uidMarque = new DBInteger();

		@DBColumn("isusedfortafros")
		public DBString isUsedForTAFROs = new DBString();

		@DBColumn("fk_toystatusclass")
		public DBNumber statusClassID = new DBNumber();

		@DBColumn("intindallocallowed")
		public DBString individualAllocationsAllowed = new DBString();

		@DBColumn("upd_count")
		public DBInteger updateCount = new DBInteger();

		@DBColumn
		public DBStringTrimmed auto_created = new DBStringTrimmed();

		@DBColumn
		public DBString name = new DBString();

		@DBColumn("pricingcodeprefix")
		public DBString pricingCodePrefix = new DBString();

		@DBColumn("reservationsalwd")
		public DBString reservationsAllowed = new DBString();

		@DBColumn("creation_date")
		public DBInstant creationInstant = new DBInstant();

		@DBColumn("enabled")
		public DBBoolean enabled = new DBBoolean();

		@DBForeignKey(CarCompany.class)
		@DBColumn("fk_carcompany")
		public DBInteger carCompany = new DBInteger();

		/**
		 * Required Public No-Argument Constructor.
		 *
		 */
		public MarqueWithInstant() {
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
		 * @param creationDate creationInstant
		 * @param enabled enabled
		 */
		public MarqueWithInstant(int uidMarque, String isUsedForTAFROs, int statusClass, String intIndividualAllocationsAllowed, Integer updateCount, String autoCreated, String name, String pricingCodePrefix, String reservationsAllowed, Instant creationDate, int carCompany, Boolean enabled) {
			this.uidMarque.setValue(uidMarque);
			this.isUsedForTAFROs.setValue(isUsedForTAFROs);
			this.statusClassID.setValue(statusClass);
			this.individualAllocationsAllowed.setValue(intIndividualAllocationsAllowed);
			this.updateCount.setValue(updateCount);
			this.auto_created.setValue(autoCreated);
			this.name.setValue(name);
			this.pricingCodePrefix.setValue(pricingCodePrefix);
			this.reservationsAllowed.setValue(reservationsAllowed);
			this.creationInstant.setValue(creationDate);
			this.carCompany.setValue(carCompany);
			this.enabled.setValue(enabled);
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
		Assert.assertThat(allRows.size(), is(22));

		MarqueWithLagAndLeadFunctions got;// = allRows.get(0).get(marq);
		ArrayList<Object[]> expectedValues = new ArrayList<>();

		expectedValues.add(new Object[]{21, 1, 2, 2, null, march23rd2013Instant});
		expectedValues.add(new Object[]{21, 2, 2, 2, march23rd2013Instant, march23rd2013Instant});
		expectedValues.add(new Object[]{21, 3, 1, 1, (march23rd2013Instant), (april2nd2011Instant)});
		expectedValues.add(new Object[]{21, 4, 3, 3, march23rd2013Instant, (march23rd2013Instant)});
		expectedValues.add(new Object[]{21, 5, 3, 3, april2nd2011Instant, (march23rd2013Instant)});
		expectedValues.add(new Object[]{21, 6, 3, 3, march23rd2013Instant, null});
		expectedValues.add(new Object[]{21, 7, 15, 15, march23rd2013Instant, (march23rd2013Instant)});
		expectedValues.add(new Object[]{21, 8, 15, 15, null, march23rd2013Instant});
		expectedValues.add(new Object[]{21, 9, 15, 15, march23rd2013Instant, (march23rd2013Instant)});
		expectedValues.add(new Object[]{21, 10, 15, 15, (march23rd2013Instant), (march23rd2013Instant)});
		expectedValues.add(new Object[]{21, 11, 15, 15, (march23rd2013Instant), (march23rd2013Instant)});
		expectedValues.add(new Object[]{21, 12, 15, 15, (march23rd2013Instant), (march23rd2013Instant)});
		expectedValues.add(new Object[]{21, 13, 15, 15, (march23rd2013Instant), (march23rd2013Instant)});
		expectedValues.add(new Object[]{21, 14, 15, 15, (march23rd2013Instant), (april2nd2011Instant)});
		expectedValues.add(new Object[]{21, 15, 15, 15, (march23rd2013Instant), (march23rd2013Instant)});
		expectedValues.add(new Object[]{21, 16, 15, 15, (april2nd2011Instant), (march23rd2013Instant)});
		expectedValues.add(new Object[]{21, 17, 15, 15, (march23rd2013Instant), (march23rd2013Instant)});
		expectedValues.add(new Object[]{21, 18, 15, 15, (march23rd2013Instant), (march23rd2013Instant)});
		expectedValues.add(new Object[]{21, 19, 15, 15, (march23rd2013Instant), (march23rd2013Instant)});
		expectedValues.add(new Object[]{21, 20, 15, 15, (march23rd2013Instant), (march23rd2013Instant)});
		expectedValues.add(new Object[]{21, 21, 15, 15, (march23rd2013Instant), (april2nd2011Instant)});
		expectedValues.add(new Object[]{21, 22, 15, 15, (march23rd2013Instant), (null)});

		for (int i = 0; i < allRows.size(); i++) {
			got = allRows.get(i).get(marq);
//			System.out.println("" + got.toString());
			Object[] expect = expectedValues.get(i);
			Assert.assertThat(got.countOfAllRows.intValue(), is((Integer) expect[0]));
			Assert.assertThat(got.rowNumber.intValue(), is((Integer) expect[1]));
			Assert.assertThat(got.countOfEnabled.intValue(), is((Integer) expect[2]));
			Assert.assertThat(got.rowWithinCarCo.intValue(), is((Integer) expect[3]));
			Assert.assertThat(got.lag.getValue(), is((Instant) expect[4]));
			Assert.assertThat(got.lead.getValue(), is((Instant) expect[5]));
		}
	}

	public static class MarqueWithLagAndLeadFunctions extends MarqueWithInstant {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBNumber countOfAllRows = new DBNumber(this.column(this.creationInstant).count().over().allRows());
		@DBColumn
		DBNumber rowNumber = new DBNumber(this.column(this.uidMarque).count().over().AllRowsAndOrderBy(this.column(this.carCompany).ascending(), this.column(this.uidMarque).ascending()));
		@DBColumn
		DBNumber countOfEnabled = new DBNumber(this.column(this.creationInstant).count().over().partition(this.column(this.carCompany)).unordered());
		@DBColumn
		DBNumber rowWithinCarCo = new DBNumber(this.column(this.creationInstant).count()
				.over()
				.partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).ascending())
				.defaultFrame());
		@DBColumn
		DBInstant lag
				= this.column(this.creationInstant)
						.lag()
						.allRows()
						.orderBy(this.column(this.carCompany).ascending(), this.column(this.uidMarque).ascending())
						.asExpressionColumn();
		@DBColumn
		DBInstant lead = new DBInstant(this.column(this.creationInstant)
				.nextRowValue()
				.AllRowsAndOrderBy(
						this.column(this.carCompany).ascending(),
						this.column(this.uidMarque).ascending()
				)
		);
	}
}
