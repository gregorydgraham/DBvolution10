/*
 * Copyright 2015 gregory.graham.
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
import java.util.Date;
import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.columns.DateColumn;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.expressions.DateExpression;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static nz.co.gregs.dbvolution.generic.AbstractTest.april2nd2011;
import static nz.co.gregs.dbvolution.generic.AbstractTest.march23rd2013;
import nz.co.gregs.dbvolution.internal.datatypes.DateRepeatImpl;
import org.junit.Test;
import org.junit.Assert;
import static org.hamcrest.Matchers.*;
import org.joda.time.Period;

/**
 *
 * @author gregory.graham
 */
public class DBDateRepeatTest extends AbstractTest {

	public DBDateRepeatTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void basicTest() throws SQLException {
		final DateRepeatTable intervalTable = new DateRepeatTable();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(intervalTable);
		database.createTable(intervalTable);
		final Period testPeriod = new Period().withMillis(1).withSeconds(2).withMinutes(3).withHours(4).withDays(5).withWeeks(6).withMonths(7).withYears(8);
		intervalTable.intervalCol.setValue(testPeriod);
		database.insert(intervalTable);
		DBTable<DateRepeatTable> tab = database.getDBTable(intervalTable).setBlankQueryAllowed(true);
		List<DateRepeatTable> allRows = tab.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));

		Assert.assertThat(allRows.get(0).intervalCol.periodValue().normalizedStandard(), is(testPeriod.normalizedStandard()));
	}

	public static class MarqueWithDateRepeatExprCol extends Marque {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBDateRepeat interval = new DBDateRepeat(this.column(this.creationDate).getDateRepeatFrom(april2nd2011));

		@DBColumn
		DBNumber yearPart = new DBNumber(this.column(this.creationDate).getDateRepeatFrom(april2nd2011).getYears());

		@DBColumn
		DBDate creationDatePlus1Year = new DBDate(this.column(this.creationDate).minus(new Period().withYears(1)));
	}

	@Test
	public void testDateExpressionProducingDateRepeats() throws SQLException {
		Marque marq;
		marq = new MarqueWithDateRepeatExprCol();
		DBQuery query = database.getDBQuery(marq).setBlankQueryAllowed(true);
		List<DBQueryRow> allRows = query.getAllRows();
		database.print(allRows);
		final Period oneYear = new Period().withYears(1);
//		query.addCondition(marq.column(marq.creationDate).getDateRepeatFrom(oneYear).isGreaterThan(april2nd2011));
		query.addCondition(marq.column(marq.creationDate).getDateRepeatFrom(april2nd2011).isGreaterThan(oneYear));
		allRows = query.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(18));

		query = database.getDBQuery(marq);
		query.addCondition(marq.column(marq.creationDate).minus(oneYear).isLessThan(april2nd2011));
		query.addCondition(marq.column(marq.creationDate).getDateRepeatFrom(april2nd2011).isLessThan(oneYear));
		allRows = query.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testOverlaps() throws SQLException {
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		final DateColumn creationDateCol = marq.column(marq.creationDate);
		final DateExpression creationDateMinus5Days = creationDateCol.minus(new Period().withDays(5));
		final DateExpression march23rd2013Minus5Weeks = DateExpression.value(march23rd2013).minus(new Period().withWeeks(5));
		final DateExpression march23rd2013minus2Days = DateExpression.value(march23rd2013).minus(new Period().withDays(2));
		query.addCondition(DateExpression.overlaps(
				creationDateCol, creationDateMinus5Days,
				march23rd2013Minus5Weeks, march23rd2013minus2Days)
		);
		List<DBQueryRow> allRows = query.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(18));
	}

	public static class DateRepeatSeconds extends Marque {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBString intervalString = new DBString(this.column(this.creationDate).getDateRepeatFrom(march23rd2013).stringResult());
		@DBColumn
		DBNumber numberOfSeconds = new DBNumber(this.column(this.creationDate).getDateRepeatFrom(march23rd2013).getSeconds());
	}

	public static class DateRepeatYears extends Marque {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBString intervalString = new DBString(this.column(this.creationDate).getDateRepeatFrom(march23rd2013).stringResult());

		@DBColumn
		DBNumber numberOfYears = new DBNumber(this.column(this.creationDate).getDateRepeatFrom(march23rd2013).getYears());
	}

	public static class DateRepeatMaths extends Marque {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBString intervalString = new DBString(this.column(this.creationDate).getDateRepeatFrom(march23rd2013).stringResult());

		@DBColumn
		DBNumber numberOfYears = new DBNumber(this.column(this.creationDate).getDateRepeatFrom(march23rd2013).getYears());

		@DBColumn
		DBNumber numberOfMonths = new DBNumber(this.column(this.creationDate).getDateRepeatFrom(march23rd2013).getMonths());

		@DBColumn
		DBNumber numberOfDays = new DBNumber(this.column(this.creationDate).getDateRepeatFrom(march23rd2013).getDays());

		@DBColumn
		DBNumber numberOfHours = new DBNumber(this.column(this.creationDate).getDateRepeatFrom(march23rd2013).getHours());

		@DBColumn
		DBNumber numberOfMinutes = new DBNumber(this.column(this.creationDate).getDateRepeatFrom(march23rd2013).getMinutes());

//		@DBColumn
//		DBNumber numberOfSeconds = new DBNumber(this.column(this.creationDate).getDateRepeatFrom(march23rd2013).getSeconds());
//		@DBColumn
//		DBNumber numberOfMillis = new DBNumber(this.column(this.creationDate).getDateRepeatFrom(march23rd2013).getMilliseconds());
	}

	@Test
	public void testGetYears() throws SQLException {
		DateRepeatYears marq = new DateRepeatYears();
		DBQuery query = database.getDBQuery(marq);
		database.print(query.setBlankQueryAllowed(true).getAllInstancesOf(marq));

		query.addCondition(
				marq.column(marq.creationDate).getDateRepeatFrom(march23rd2013).getYears().is(-2)
		);
		List<DBQueryRow> allRows = query.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testGetMonths() throws SQLException {
		DateRepeatMaths marq = new DateRepeatMaths();
		DBQuery query = database.getDBQuery(marq);
		database.print(query.setBlankQueryAllowed(true).getAllInstancesOf(marq));

		query.addCondition(
				marq.column(marq.creationDate).getDateRepeatFrom(march23rd2013).getMonths().is(1)
		);
		List<DBQueryRow> allRows = query.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testGetDays() throws SQLException {
		DateRepeatMaths marq = new DateRepeatMaths();
		DBQuery query = database.getDBQuery(marq);
		database.print(query.setBlankQueryAllowed(true).getAllInstancesOf(marq));

		query.addCondition(
				marq.column(marq.creationDate).getDateRepeatFrom(march23rd2013).getDays().is(-21)
		);
		List<DBQueryRow> allRows = query.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testGetHours() throws SQLException {
		DateRepeatMaths marq = new DateRepeatMaths();
		DBQuery query = database.getDBQuery(marq);
		database.print(query.setBlankQueryAllowed(true).getAllInstancesOf(marq));

		query.addCondition(
				marq.column(marq.creationDate).getDateRepeatFrom(march23rd2013).getHours().is(-11)
		);
		List<DBQueryRow> allRows = query.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testGetMinutes() throws SQLException {
		DateRepeatMaths marq = new DateRepeatMaths();
		DBQuery query = database.getDBQuery(marq);
		database.print(query.setBlankQueryAllowed(true).getAllInstancesOf(marq));

		query.addCondition(
				marq.column(marq.creationDate).getDateRepeatFrom(march23rd2013).getMinutes().is(-32)
		);
		List<DBQueryRow> allRows = query.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testGetSeconds() throws SQLException {
		DateRepeatSeconds marq = new DateRepeatSeconds();
//		marq.creationDate.excludedValues((Date)null);
		DBQuery query = database.getDBQuery(marq);
		database.print(query.setBlankQueryAllowed(true).getAllInstancesOf(marq));

		query.addCondition(
				marq.column(marq.creationDate).getDateRepeatFrom(march23rd2013).getSeconds().is(-53)
		);
		List<DBQueryRow> allRows = query.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testParsing() {
		String twoYearPlusDateRepeat = "P2Y-1M0D11h32n53s";
		String zeroDateRepeat = "P0Y0M0D0h0n0s";
		String plus2Days = "P0Y0M2D0h0n0.0s";
		Assert.assertThat(DateRepeatImpl.compareDateRepeatStrings(twoYearPlusDateRepeat, "P1Y0M0D0h0n0s"), is(1));
		Assert.assertThat(DateRepeatImpl.compareDateRepeatStrings(zeroDateRepeat, "P1Y0M0D0h0n0s"), is(-1));
		Assert.assertThat(DateRepeatImpl.compareDateRepeatStrings(plus2Days, "P1Y0M0D0h0n0s"), is(-1));
		Assert.assertThat(DateRepeatImpl.compareDateRepeatStrings("P1Y0M0D0h0n0s", "P1Y0M0D0h0n0s"), is(0));
	}

	@Test
	@SuppressWarnings("deprecation")
	public void testSubtracting() {
		String oneYear = "P1Y0M0D0h0n0s";
		Date resultDate = DateRepeatImpl.subtractDateAndDateRepeatString(april2nd2011, DateRepeatImpl.getZeroDateRepeatString());
		Assert.assertThat(resultDate.getYear() + 1900, is(2011));
		Assert.assertThat(resultDate.getMonth() + 1, is(4));
		Assert.assertThat(resultDate.getDate(), is(2));

		resultDate = DateRepeatImpl.subtractDateAndDateRepeatString(april2nd2011, oneYear);
		Assert.assertThat(resultDate.getYear() + 1900, is(2010));
		Assert.assertThat(resultDate.getMonth() + 1, is(4));
		Assert.assertThat(resultDate.getDate(), is(2));
	}

	public static class DateRepeatTable extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		DBInteger pkid = new DBInteger();

		@DBColumn
		DBDateRepeat intervalCol = new DBDateRepeat();
	}
}
