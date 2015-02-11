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
import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.databases.supports.SupportsIntervalDatatype;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
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
		if (database instanceof SupportsIntervalDatatype) {
			final intervalTable intervalTable = new intervalTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(intervalTable);
			database.createTable(intervalTable);
			intervalTable.intervalCol.setValue(new Period().withMillis(1).withSeconds(2).withMinutes(3).withHours(4).withDays(5).withWeeks(6).withMonths(7).withYears(8));
			database.insert(intervalTable);
			DBTable<intervalTable> tab = database.getDBTable(intervalTable).setBlankQueryAllowed(true);
			List<intervalTable> allRows = tab.getAllRows();
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(1));
		}
	}

	@Test
	public void testDateExpressionProducingIntervals() throws SQLException {
		if (database instanceof SupportsIntervalDatatype) {
			Marque marq = new Marque();
			DBQuery query = database.getDBQuery(marq);
			query.addCondition(marq.column(marq.creationDate).minus(april2nd2011).isGreaterThan(new Period().withYears(1)));
			List<DBQueryRow> allRows = query.getAllRows();
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(18));
			
			query = database.getDBQuery(marq);
			query.addCondition(marq.column(marq.creationDate).minus(april2nd2011).isLessThan(new Period().withYears(1)));
			allRows = query.getAllRows();
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(3));
		}
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
