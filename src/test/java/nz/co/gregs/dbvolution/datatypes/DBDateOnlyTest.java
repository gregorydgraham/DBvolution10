/*
 * Copyright 2014 gregory.graham.
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
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregory.graham
 */
public class DBDateOnlyTest extends AbstractTest {

	public DBDateOnlyTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	/**
	 * Test of getSQLDatatype method, of class DBDateOnly.
	 *
	 * @throws java.sql.SQLException
	 */
	@Test
	@SuppressWarnings("deprecation")
	public void testGetSQLDatatype() throws SQLException {
		DateOnlyTest dateOnlyTest = new DateOnlyTest();
		dateOnlyTest.dateOnly.setValue(new Date());
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(dateOnlyTest);
		database.createTable(dateOnlyTest);
		database.insert(dateOnlyTest);
		List<DateOnlyTest> allRows = database.getDBTable(new DateOnlyTest()).setBlankQueryAllowed(true).getAllRows();
		Assert.assertThat(allRows.size(), is(1));
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(dateOnlyTest);
	}

	public static class DateOnlyTest extends DBRow {

		private static final long serialVersionUID = 1L;
		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		DBInteger ddateonlypk = new DBInteger();

		@DBColumn
		DBDateOnly dateOnly = new DBDateOnly();
	}

}
