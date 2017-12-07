/*
 * Copyright 2015 gregorygraham.
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
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.is;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.is;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class DBNumberTest extends AbstractTest {

	public DBNumberTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	/**
	 * Test of getSQLDatatype method, of class DBDateOnly.
	 *
	 * @throws java.sql.SQLException
	 */
	@Test
	public void testGetSQLDatatype() throws SQLException {
		NumberTest numberTest = new NumberTest();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(numberTest);
		database.createTable(numberTest);

		numberTest.numberColumn.setValue(2.2);
		database.insert(numberTest);
		List<NumberTest> allRows = database.getDBTable(new NumberTest()).setBlankQueryAllowed(true).getAllRows();
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).numberColumn.doubleValue(), is(2.2));

		numberTest = new NumberTest();
		numberTest.numberColumn.setValue(new DBNumber(2L));
		database.insert(numberTest);
		allRows = database.getDBTable(new NumberTest()).setBlankQueryAllowed(true).getAllRows();
		Assert.assertThat(allRows.size(), is(2));
		Assert.assertThat(allRows.get(0).numberColumn.longValue(), is(2L));
		Assert.assertThat(allRows.get(1).numberColumn.longValue(), is(2L));

		numberTest = new NumberTest();
		numberTest.numberColumn.permittedRange(0.1, 2.1);
		allRows = database.getDBTable(numberTest).getAllRows();
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).numberColumn.doubleValue(), is(2.0));

		numberTest = new NumberTest();
		numberTest.numberColumn.excludedRange(0.1, 2.1);
		allRows = database.getDBTable(numberTest).getAllRows();
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).numberColumn.doubleValue(), is(2.2));

		numberTest = new NumberTest();
		numberTest.numberColumn.excludedRangeExclusive(0.1, 2.1);
		allRows = database.getDBTable(numberTest).getAllRows();
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).numberColumn.doubleValue(), is(2.2));

		numberTest = new NumberTest();
		numberTest.numberColumn.permittedRangeExclusive(0.1, 2.1);
		allRows = database.getDBTable(numberTest).getAllRows();
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).numberColumn.doubleValue(), is(2.0));

		numberTest = new NumberTest();
		numberTest.numberColumn.excludedRangeInclusive(0.1, 2.1);
		allRows = database.getDBTable(numberTest).getAllRows();
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).numberColumn.doubleValue(), is(2.2));

		numberTest = new NumberTest();
		numberTest.numberColumn.permittedRangeInclusive(0.1, 2.1);
		allRows = database.getDBTable(numberTest).getAllRows();
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).numberColumn.doubleValue(), is(2.0));

		numberTest = new NumberTest();
		numberTest.numberColumn.excludedValues(0.1, 2.0);
		allRows = database.getDBTable(numberTest).getAllRows();
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).numberColumn.doubleValue(), is(2.2));

		numberTest = new NumberTest();
		numberTest.numberColumn.permittedValues(0.1, 2.0);
		allRows = database.getDBTable(numberTest).getAllRows();
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).numberColumn.doubleValue(), is(2.0));

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(numberTest);
	}

	public static class NumberTest extends DBRow {

		private static final long serialVersionUID = 1L;
		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		DBInteger ddateonlypk = new DBInteger();

		@DBColumn
		DBNumber numberColumn = new DBNumber();
	}

}
