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
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class DBUnknownDatatypeTest extends AbstractTest {

	public DBUnknownDatatypeTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testGetSQLDatatype() {
		DBUnknownDatatype instance = new DBUnknownDatatype();
		String expResult = "";
		String result = instance.getSQLDatatype();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testFormatValueForSQLStatement() {
		DBUnknownDatatype instance = new DBUnknownDatatype();
		String result = instance.formatValueForSQLStatement(database.getDefinition());
	}

	@Test
	public void testGetQueryableDatatypeForExpressionValue() {
		DBUnknownDatatype instance = new DBUnknownDatatype();
		DBUnknownDatatype expResult = new DBUnknownDatatype();
		DBUnknownDatatype result = instance.getQueryableDatatypeForExpressionValue();
		assertEquals(expResult.getClass(), result.getClass());
	}

	@Test
	public void testIsAggregator() {
		DBUnknownDatatype instance = new DBUnknownDatatype();
		boolean expResult = false;
		boolean result = instance.isAggregator();
		assertEquals(false, result);
	}

	@Test
	public void testGetTablesInvolved() {
		DBUnknownDatatype instance = new DBUnknownDatatype();
		Set<DBRow> expResult = null;
		Set<DBRow> result = instance.getTablesInvolved();
		assertEquals(0, result.size());
	}

}
