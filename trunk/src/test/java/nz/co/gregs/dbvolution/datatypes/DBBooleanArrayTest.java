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
import org.junit.Ignore;

/**
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
		System.out.println("equals");
		final boolean[] trueFalseTrueArray = new boolean[]{true, false, true};
		QueryableDatatype other = new DBBooleanArray(trueFalseTrueArray);
		final boolean[] allTrueArray = new boolean[]{true, true, true};
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
		System.out.println("getSQLDatatype");
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
		System.out.println("setValue");
		Object newLiteralValue = new boolean[]{true, false, true};
		DBBooleanArray instance = new DBBooleanArray();
		instance.setValue(newLiteralValue);
		Assert.assertThat(instance.booleanArrayValue(), is(newLiteralValue));
	}

	/**
	 * Test of setValue method, of class DBBooleanArray.
	 */
	@Test(expected = ClassCastException.class)
	public void testSetValue_BadObject() {

		Object newLiteralValue = 5;
		DBBooleanArray instance = new DBBooleanArray();
		instance.setValue(newLiteralValue);
	}

	/**
	 * Test of formatValueForSQLStatement method, of class DBBooleanArray.
	 */
	@Ignore
	@Test
	public void testFormatValueForSQLStatement() {
		System.out.println("formatValueForSQLStatement");
		DBBooleanArray instance = new DBBooleanArray();
		String expResult = "The Value";
		instance.setValue(expResult.getBytes());
		String result = instance.formatValueForSQLStatement(database);
		assertEquals(expResult, result);
	}

	/**
	 * Test of byteArrayValue method, of class DBBooleanArray.
	 */
	@Test
	public void testByteArrayValue() {
		System.out.println("byteArrayValue");
		DBBooleanArray instance = new DBBooleanArray();
		boolean[] tftArray = new boolean[]{true, false, true};
		instance.setValue(tftArray);
		boolean[] result = instance.booleanArrayValue();
		Assert.assertThat(result, is(tftArray));
	}

	/**
	 * Test of copy method, of class DBBooleanArray.
	 */
	@Test
	public void testCopy() {
		System.out.println("copy");
		boolean[] tftArray = new boolean[]{true, false, true};
		DBBooleanArray instance = new DBBooleanArray(tftArray);
		DBBooleanArray result = instance.copy();
		Assert.assertThat(result.booleanArrayValue(), is(instance.booleanArrayValue()));
	}

	/**
	 * Test of getValue method, of class DBBooleanArray.
	 */
	@Test
	public void testGetValue() {
		System.out.println("getValue");
		final String theValueStr = "The Value";
		boolean[] tftArray = new boolean[]{true, false, true};
		DBBooleanArray instance = new DBBooleanArray(tftArray);
		boolean[] expResult = tftArray;
		boolean[] result = instance.getValue();
		Assert.assertThat(result, is(expResult));
	}

	/**
	 * Test of getQueryableDatatypeForExpressionValue method, of class
	 * DBBooleanArray.
	 */
	@Test
	public void testGetQueryableDatatypeForExpressionValue() {
		System.out.println("getQueryableDatatypeForExpressionValue");
		DBBooleanArray instance = new DBBooleanArray();
		DBBooleanArray result = instance.getQueryableDatatypeForExpressionValue();
		assertEquals(instance.getClass(), result.getClass());
	}

	/**
	 * Test of isAggregator method, of class DBBooleanArray.
	 */
	@Test
	public void testIsAggregator() {
		System.out.println("isAggregator");
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
		System.out.println("getTablesInvolved");
		DBBooleanArray instance = new DBBooleanArray();
		Set<DBRow> result = instance.getTablesInvolved();
		assertEquals(0, result.size());
	}

	/**
	 * Test of getIncludesNull method, of class DBBooleanArray.
	 */
	@Test
	public void testGetIncludesNull() {
		System.out.println("getIncludesNull");
		DBBooleanArray instance = new DBBooleanArray();
		boolean expResult = false;
		boolean result = instance.getIncludesNull();
		assertEquals(expResult, result);
	}

	/**
	 * Test of getFromResultSet method, of class DBBooleanArray.
	 */
	@Ignore
	@Test
	public void testGetFromResultSet() throws Exception {
		System.out.println("getFromResultSet");
		final BitsTable bitsTable = new BitsTable();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(bitsTable);
		database.createTable(bitsTable);
		final boolean[] theValue = new boolean[]{true, false, true};
		bitsTable.bitsColumn.setValue(theValue);
		database.insert(bitsTable);
		BitsTable bitsRow = database.getDBQuery(bitsTable).getOnlyInstanceOf(bitsTable);
		Assert.assertThat(bitsRow.bitsColumn.booleanArrayValue(), is(theValue));
	}

	public static class BitsTable extends DBRow {

		private static final long serialVersionUID = 1L;
		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		DBInteger pk = new DBInteger();

		@DBColumn
		DBBooleanArray bitsColumn = new DBBooleanArray();
	}

}
