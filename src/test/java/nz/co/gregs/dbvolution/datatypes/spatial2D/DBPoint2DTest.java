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
package nz.co.gregs.dbvolution.datatypes.spatial2D;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class DBPoint2DTest extends AbstractTest {

	public DBPoint2DTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testSetValue() {
		Coordinate coordinate = new Coordinate(2, 3);
		GeometryFactory geomFactory = new GeometryFactory();
		Point point = geomFactory.createPoint(coordinate);
		DBPoint2D instance = new DBPoint2D();
		instance.setValue(point);
		Point value = instance.getValue();
		Assert.assertThat(value.getCoordinates()[0].x, Matchers.is(2.0));
		Assert.assertThat(value.getCoordinates()[0].y, Matchers.is(3.0));
	}

	@Test
	public void testGetSQLDatatype() {
		DBPoint2D instance = new DBPoint2D();
		String expResult = " POINT ";
		String result = instance.getSQLDatatype();
		assertEquals(expResult, result);
	}

	@Test
	public void testIsAggregator() {
		DBPoint2D instance = new DBPoint2D();
		boolean expResult = false;
		boolean result = instance.isAggregator();
		assertEquals(expResult, result);
	}

	@Test
	public void testGetIncludesNull() {
		DBPoint2D instance = new DBPoint2D();
		boolean expResult = false;
		boolean result = instance.getIncludesNull();
		assertEquals(expResult, result);
	}

}
