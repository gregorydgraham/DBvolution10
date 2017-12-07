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

import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class QueryGraphEdgeLabelTransformerTest extends AbstractTest {

	public QueryGraphEdgeLabelTransformerTest(Object testIterationName, Object db) {
		super(testIterationName, db);
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
	 * Test of transform method, of class QueryGraphEdgeLabelTransformer.
	 */
	@Test
	public void testTransform() {
		Marque marque = new Marque();
		DBExpression v = marque.column(marque.carCompany).is(2);
		DBQuery dbQuery = database.getDBQuery(marque);
		QueryGraphEdgeLabelTransformer instance = new QueryGraphEdgeLabelTransformer(dbQuery);
		String expResult = "fk_carcompany = 2";
		String result = instance.transform(v);
		Assert.assertThat(result.toLowerCase(), containsString(expResult));
	}

}
