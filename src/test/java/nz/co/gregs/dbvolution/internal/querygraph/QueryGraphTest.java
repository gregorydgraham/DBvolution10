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

import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.SparseMultigraph;
import java.util.ArrayList;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
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
public class QueryGraphTest extends AbstractTest {

	@BeforeClass
	public static void setUpClass() {
	}

	@AfterClass
	public static void tearDownClass() {
	}

	public QueryGraphTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Before
	@Override
	public void setUp() {
	}

	@After
	@Override
	public void tearDown() {
	}

	/**
	 * Test of getJungGraph method, of class QueryGraph.
	 */
	@Test
	public void testGetJungGraph() {
		QueryGraph instance = new QueryGraph(new ArrayList<DBRow>(), new ArrayList<BooleanExpression>());
		Graph<QueryGraphNode, DBExpression> expResult = new SparseMultigraph<QueryGraphNode, DBExpression>();
		Graph<QueryGraphNode, DBExpression> result = instance.getJungGraph();
		Assert.assertNotNull(result);
		Assert.assertThat(result.getClass().getSimpleName(), is(expResult.getClass().getSimpleName()));
	}

}
