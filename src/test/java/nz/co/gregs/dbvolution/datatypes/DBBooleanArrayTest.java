/*
 * Copyright 2014 gregorygraham.
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

import java.util.Set;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
//import org.junit.Ignore;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class DBBooleanArrayTest extends AbstractTest {

	@BeforeClass
	public static void setUpClass() {
	}

	@AfterClass
	public static void tearDownClass() {
	}

	public DBBooleanArrayTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Before
	public void setUp() {
	}

	@After
	public void tearDown() {
	}

	/**
	 * Test of equals method, of class DBBooleanArray.
	 */
	@Test
	public void testEquals() {
		final Boolean[] trueFalseTrueArray = new Boolean[]{true, false, true};
		QueryableDatatype<?> other = new DBBooleanArray(trueFalseTrueArray);
		final Boolean[] allTrueArray = new Boolean[]{true, true, true};
		DBBooleanArray instance = new DBBooleanArray(allTrueArray);
		boolean expResult = false;
		boolean result = instance.equals(other);
		assertEquals(expResult, result);

		other = new DBBooleanArray(allTrueArray);
		expResult = true;
		result = instance.equals(other);
		assertEquals(expResult, result);
	}

	/**
	 * Test of getSQLDatatype method, of class DBBooleanArray.
	 */
	@Test
	public void testGetSQLDatatype() {
		DBBooleanArray instance = new DBBooleanArray();
		String expResult = "ARRAY";
		String result = instance.getSQLDatatype();
		assertEquals(expResult, result);
	}

	/**
	 * Test of setValue method, of class DBBooleanArray.
	 */
	@Test
	public void testSetValue_Object() {
		Boolean[] newLiteralValue = new Boolean[]{true, false, true};
		DBBooleanArray instance = new DBBooleanArray();
		instance.setValue(newLiteralValue);
		Assert.assertThat(instance.booleanArrayValue(), is(newLiteralValue));
	}

	/**
	 * Test of formatValueForSQLStatement method, of class DBBooleanArray.
	 */
//	@Ignore
	@Test
	public void testFormatValueForSQLStatement() {
		DBBooleanArray instance = new DBBooleanArray();
		Boolean[] expResult = new Boolean[]{false, true, true};
		instance.setValue(expResult);
		String result = instance.formatValueForSQLStatement(database.getDefinition());
		Assert.assertThat(
				result,
				anyOf(
						is("'011'"), //MS SQLServer
						is("b'110'"), //MySQL
						is("(FALSE,TRUE,TRUE)"), //H2
						is("'{0,1,1}'") // Postgres
				)
		);
	}

	/**
	 * Test of byteArrayValue method, of class DBBooleanArray.
	 */
	@Test
	public void testByteArrayValue() {
		DBBooleanArray instance = new DBBooleanArray();
		Boolean[] tftArray = new Boolean[]{true, false, true};
		instance.setValue(tftArray);
		Boolean[] result = instance.booleanArrayValue();
		Assert.assertThat(result, is(tftArray));
	}

	/**
	 * Test of copy method, of class DBBooleanArray.
	 */
	@Test
	public void testCopy() {
		Boolean[] tftArray = new Boolean[]{true, false, true};
		DBBooleanArray instance = new DBBooleanArray(tftArray);
		DBBooleanArray result = instance.copy();
		Assert.assertThat(result.booleanArrayValue(), is(instance.booleanArrayValue()));
	}

	/**
	 * Test of getValue method, of class DBBooleanArray.
	 */
	@Test
	public void testGetValue() {
		final String theValueStr = "The Value";
		Boolean[] tftArray = new Boolean[]{true, false, true};
		DBBooleanArray instance = new DBBooleanArray(tftArray);
		Boolean[] expResult = tftArray;
		Boolean[] result = instance.getValue();
		Assert.assertThat(result, is(expResult));
	}

	/**
	 * Test of getQueryableDatatypeForExpressionValue method, of class
	 * DBBooleanArray.
	 */
	@Test
	public void testGetQueryableDatatypeForExpressionValue() {
		DBBooleanArray instance = new DBBooleanArray();
		DBBooleanArray result = instance.getQueryableDatatypeForExpressionValue();
		assertEquals(instance.getClass(), result.getClass());
	}

	/**
	 * Test of isAggregator method, of class DBBooleanArray.
	 */
	@Test
	public void testIsAggregator() {
		DBBooleanArray instance = new DBBooleanArray();
		boolean expResult = false;
		boolean result = instance.isAggregator();
		assertEquals(expResult, result);
	}

	/**
	 * Test of getTablesInvolved method, of class DBBooleanArray.
	 */
	@Test
	public void testGetTablesInvolved() {
		DBBooleanArray instance = new DBBooleanArray();
		Set<DBRow> result = instance.getTablesInvolved();
		assertEquals(0, result.size());
	}

	/**
	 * Test of getIncludesNull method, of class DBBooleanArray.
	 */
	@Test
	public void testGetIncludesNull() {
		DBBooleanArray instance = new DBBooleanArray();
		boolean expResult = false;
		boolean result = instance.getIncludesNull();
		assertEquals(expResult, result);
	}

	/**
	 * Test of getFromResultSet method, of class DBBooleanArray.
	 *
	 * @throws java.lang.Exception
	 */
//	@Ignore
	@Test
	public void testGetFromResultSet() throws Exception {
		final BooleanArrayTable boolArrayTable = new BooleanArrayTable();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(boolArrayTable);
		database.createTable(boolArrayTable);
		final Boolean[] theValue = new Boolean[]{false, true, true};
		boolArrayTable.boolArrayColumn.setValue(theValue);
		database.insert(boolArrayTable);
		BooleanArrayTable bitsRow = database.getDBQuery(boolArrayTable).getOnlyInstanceOf(boolArrayTable);
		Assert.assertThat(bitsRow.boolArrayColumn.booleanArrayValue(), is(theValue));
	}

	public static class BooleanArrayTable extends DBRow {

		private static final long serialVersionUID = 1L;
		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		DBInteger pk = new DBInteger();

		@DBColumn
		DBBooleanArray boolArrayColumn = new DBBooleanArray();
	}

}
