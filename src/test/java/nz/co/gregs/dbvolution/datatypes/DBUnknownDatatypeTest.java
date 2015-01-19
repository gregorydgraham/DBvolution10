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

import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author gregorygraham
 */
public class DBUnknownDatatypeTest {
	
	public DBUnknownDatatypeTest() {
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testGetSQLDatatype() {
		System.out.println("getSQLDatatype");
		DBUnknownDatatype instance = new DBUnknownDatatype();
		String expResult = "";
		String result = instance.getSQLDatatype();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testFormatValueForSQLStatement() {
		System.out.println("formatValueForSQLStatement");
		DBDatabase db = null;
		DBUnknownDatatype instance = new DBUnknownDatatype();
		String expResult = "";
		String result = instance.formatValueForSQLStatement(db);
	}

	@Test
	public void testGetQueryableDatatypeForExpressionValue() {
		System.out.println("getQueryableDatatypeForExpressionValue");
		DBUnknownDatatype instance = new DBUnknownDatatype();
		DBUnknownDatatype expResult = new DBUnknownDatatype();
		DBUnknownDatatype result = instance.getQueryableDatatypeForExpressionValue();
		assertEquals(expResult.getClass(), result.getClass());
	}

	@Test
	public void testIsAggregator() {
		System.out.println("isAggregator");
		DBUnknownDatatype instance = new DBUnknownDatatype();
		boolean expResult = false;
		boolean result = instance.isAggregator();
		assertEquals(false, result);
	}

	@Test
	public void testGetTablesInvolved() {
		System.out.println("getTablesInvolved");
		DBUnknownDatatype instance = new DBUnknownDatatype();
		Set<DBRow> expResult = null;
		Set<DBRow> result = instance.getTablesInvolved();
		assertEquals(0, result.size());
	}
	
}
