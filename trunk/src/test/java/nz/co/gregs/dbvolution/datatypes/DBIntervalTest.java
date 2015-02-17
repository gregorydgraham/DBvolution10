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
import nz.co.gregs.dbvolution.databases.supports.SupportsIntervalDatatypeNatively;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.expressions.DateExpression;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static nz.co.gregs.dbvolution.generic.AbstractTest.march23rd2013;
import nz.co.gregs.dbvolution.internal.datatypes.IntervalImpl;
import org.junit.Test;
import org.junit.Assert;
import static org.hamcrest.Matchers.*;
import org.joda.time.Period;

/**
 *
 * @author gregory.graham
 */
public class DBIntervalTest extends AbstractTest {

	public DBIntervalTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void basicTest() throws SQLException {
		if (database instanceof SupportsIntervalDatatypeNatively) {
			final intervalTable intervalTable = new intervalTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(intervalTable);
			database.createTable(intervalTable);
			final Period testPeriod = new Period().withMillis(1).withSeconds(2).withMinutes(3).withHours(4).withDays(5).withWeeks(6).withMonths(7).withYears(8);
			intervalTable.intervalCol.setValue(testPeriod);
			database.insert(intervalTable);
			DBTable<intervalTable> tab = database.getDBTable(intervalTable).setBlankQueryAllowed(true);
			List<intervalTable> allRows = tab.getAllRows();
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(1));

			Assert.assertThat(allRows.get(0).intervalCol.periodValue().normalizedStandard(), is(testPeriod.normalizedStandard()));
		}
	}

	public static class MarqueWithIntervalExprCol extends Marque {

		@DBColumn
		DBInterval interval = new DBInterval(this.column(this.creationDate).minus(april2nd2011));

		Period oneYear = new Period().withYears(1);
		@DBColumn
		DBDate creationDatePlus1Year = new DBDate(this.column(this.creationDate).minus(oneYear));
	}

	@Test
	public void testDateExpressionProducingIntervals() throws SQLException {
		if (database instanceof SupportsIntervalDatatypeNatively) {
			Marque marq;
			marq = new MarqueWithIntervalExprCol();
			DBQuery query = database.getDBQuery(marq).setBlankQueryAllowed(true);
			List<DBQueryRow> allRows = query.getAllRows();
			database.print(allRows);
			final Period oneYear = new Period().withYears(1);
			query.addCondition(marq.column(marq.creationDate).minus(oneYear).isGreaterThan(april2nd2011));
			query.addCondition(marq.column(marq.creationDate).minus(april2nd2011).isGreaterThan(oneYear));
			allRows = query.getAllRows();
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(18));

			query = database.getDBQuery(marq);
			query.addCondition(marq.column(marq.creationDate).minus(oneYear).isLessThan(april2nd2011));
			query.addCondition(marq.column(marq.creationDate).minus(april2nd2011).isLessThan(oneYear));
			allRows = query.getAllRows();
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(3));
		}
	}

	@Test
	public void testOverlaps() throws SQLException {
		if (database instanceof SupportsIntervalDatatypeNatively) {
			Marque marq = new Marque();
			DBQuery query = database.getDBQuery(marq);
			query.addCondition(DateExpression.overlaps(
					marq.column(marq.creationDate), marq.column(marq.creationDate).plus(new Period().withDays(-5)),
					DateExpression.value(march23rd2013).plus(new Period().withWeeks(-5)), DateExpression.value(march23rd2013).plus(new Period().withDays(-2)))
			);
			List<DBQueryRow> allRows = query.getAllRows();
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(18));
		}
	}

	@Test
	public void testParsing() {
		String twoYearPlusInterval = "P2Y-1M0D11h32m53s0";
		String zeroInterval = "P0Y0M0D0h0m0s0";
		IntervalImpl.compareIntervalStrings(twoYearPlusInterval, "P1Y0M0D0h0m0s0");
		IntervalImpl.compareIntervalStrings(zeroInterval, "P1Y0M0D0h0m0s0");
	}

	@Test
	public void testSubtracting() {
		String oneYear = "P1Y0M0D0h0m0s0";
		Date resultDate = IntervalImpl.subtractDateAndIntervalString(april2nd2011,IntervalImpl.getZeroIntervalString());
		System.out.println("RESULTDATE: "+resultDate);
		Assert.assertThat(resultDate.getYear()+1900, is(2011));
		Assert.assertThat(resultDate.getMonth()+1, is(4));
		Assert.assertThat(resultDate.getDate(), is(2));
		
		resultDate = IntervalImpl.subtractDateAndIntervalString(april2nd2011, oneYear);
		System.out.println("RESULTDATE: "+resultDate);
		Assert.assertThat(resultDate.getYear()+1900, is(2010));
		Assert.assertThat(resultDate.getMonth()+1, is(4));
		Assert.assertThat(resultDate.getDate(), is(2));
	}

	public static class intervalTable extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		DBInteger pkid = new DBInteger();

		@DBColumn
		DBInterval intervalCol = new DBInterval();
	}

}
