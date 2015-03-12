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
package nz.co.gregs.dbvolution.expressions;

import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.DBBooleanArray;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author gregorygraham
 */
public class BooleanArrayExpressionTest extends AbstractTest{

	public BooleanArrayExpressionTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}
	
	@Test
	public void testGetQueryableDatatypeForExpressionValue() {
		System.out.println("getQueryableDatatypeForExpressionValue");
		BooleanArrayExpression instance = new BooleanArrayExpression();
		QueryableDatatype expResult = new DBBooleanArray();
		QueryableDatatype result = instance.getQueryableDatatypeForExpressionValue();
		assertEquals(expResult.getClass(), result.getClass());
	}

	@Test
	public void testIsAggregator() {
		System.out.println("isAggregator");
		BooleanArrayExpression instance = new BooleanArrayExpression();
		boolean expResult = false;
		boolean result = instance.isAggregator();
		assertEquals(expResult, result);
	}

	@Test
	public void testGetTablesInvolved() {
		System.out.println("getTablesInvolved");
		BooleanArrayExpression instance = new BooleanArrayExpression();
		Set<DBRow> expResult = null;
		Set<DBRow> result = instance.getTablesInvolved();
		assertEquals(new HashSet<DBRow>(), result);
	}

	@Test
	public void testGetIncludesNull() {
		System.out.println("getIncludesNull");
		BooleanArrayExpression instance = new BooleanArrayExpression();
		boolean expResult = false;
		boolean result = instance.getIncludesNull();
		assertEquals(expResult, result);
	}

	@Test
	public void testIsPurelyFunctional() {
		System.out.println("isPurelyFunctional");
		BooleanArrayExpression instance = new BooleanArrayExpression();
		boolean expResult = true;
		boolean result = instance.isPurelyFunctional();
		assertEquals(expResult, result);
	}
}
