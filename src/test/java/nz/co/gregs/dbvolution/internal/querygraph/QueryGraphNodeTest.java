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
package nz.co.gregs.dbvolution.internal.querygraph;

import nz.co.gregs.dbvolution.internal.query.DBRowClass;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class QueryGraphNodeTest {

	public QueryGraphNodeTest() {
	}

	@BeforeClass
	public static void setUpClass() {
	}

	@AfterClass
	public static void tearDownClass() {
	}

	@Before
	public void setUp() {
	}

	@After
	public void tearDown() {
	}

	/**
	 * Test of toString method, of class QueryGraphNode.
	 */
	@Test
	public void testToString() {
		QueryGraphNode instance = new QueryGraphNode(new DBRowClass(new Marque()));
		String expResult = "Marque";
		String result = instance.toString();
		assertEquals(expResult, result);
	}

	/**
	 * Test of equals method, of class QueryGraphNode.
	 */
	@Test
	public void testEquals() {
		Object o = new QueryGraphNode(new DBRowClass(new CarCompany()));
		QueryGraphNode instance = new QueryGraphNode(new DBRowClass(new Marque()));
		boolean expResult = false;
		boolean result = instance.equals(o);
		assertEquals(expResult, result);

		o = new QueryGraphNode(new DBRowClass(new Marque()));
		expResult = true;
		result = instance.equals(o);
		assertEquals(expResult, result);
	}

}
