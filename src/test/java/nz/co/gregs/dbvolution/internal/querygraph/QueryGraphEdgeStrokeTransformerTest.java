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

import java.awt.Stroke;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.example.CarCompany;
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
import static org.junit.Assert.*;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class QueryGraphEdgeStrokeTransformerTest extends AbstractTest {

	@BeforeClass
	public static void setUpClass() {
	}

	@AfterClass
	public static void tearDownClass() {
	}

	public QueryGraphEdgeStrokeTransformerTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Before
	public void setUp() {
	}

	@After
	public void tearDown() {
	}

	/**
	 * Test of transform method, of class QueryGraphEdgeStrokeTransformer.
	 */
	@Test
	public void testTransform() {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);
		DBExpression input = marque.column(marque.carCompany).is(2);
		QueryGraphEdgeStrokeTransformer instance = new QueryGraphEdgeStrokeTransformer(dbQuery);
		Stroke expResult = QueryGraphEdgeStrokeTransformer.STROKE_FOR_REQUIRED_EXPRESSION;
		Stroke result = instance.transform(input);
		Assert.assertThat(result, is(expResult));

		final CarCompany carCompany = new CarCompany();
		dbQuery.addOptional(carCompany);
		input = carCompany.column(carCompany.name).is("TOYOTA");
		expResult = QueryGraphEdgeStrokeTransformer.STROKE_FOR_OPTIONAL_EXPRESSION;
		result = instance.transform(input);
		Assert.assertThat(result, is(expResult));
	}

}
