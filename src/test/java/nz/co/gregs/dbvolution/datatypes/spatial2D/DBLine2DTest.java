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
import com.vividsolutions.jts.geom.LineString;
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
public class DBLine2DTest extends AbstractTest {

	public DBLine2DTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testSetValueLineString() {
		Coordinate coordinate1 = new Coordinate(2, 3);
		Coordinate coordinate2 = new Coordinate(3, 4);
		Coordinate coordinate3 = new Coordinate(4, 5);
		GeometryFactory geomFactory = new GeometryFactory();
		LineString line = geomFactory.createLineString(new Coordinate[]{coordinate1, coordinate2, coordinate3});
		DBLine2D instance = new DBLine2D();
		instance.setValue(line);
		LineString value = instance.getValue();
		Assert.assertThat(value.getCoordinates()[0].x, Matchers.is(2.0));
		Assert.assertThat(value.getCoordinates()[0].y, Matchers.is(3.0));
	}

	@Test
	public void testSetValueCoorrdinates() {
		Coordinate coordinate1 = new Coordinate(2, 3);
		Coordinate coordinate2 = new Coordinate(3, 4);
		Coordinate coordinate3 = new Coordinate(4, 5);
		DBLine2D instance = new DBLine2D();
		instance.setValue(coordinate1, coordinate2, coordinate3);
		LineString value = instance.getValue();
		Assert.assertThat(value.getCoordinates()[0].x, Matchers.is(2.0));
		Assert.assertThat(value.getCoordinates()[0].y, Matchers.is(3.0));
	}

	@Test
	public void testSetValuePoints() {
		GeometryFactory geomFactory = new GeometryFactory();
		Coordinate coordinate1 = new Coordinate(2, 3);
		Coordinate coordinate2 = new Coordinate(3, 4);
		Coordinate coordinate3 = new Coordinate(4, 5);
		Point point1 = geomFactory.createPoint(coordinate1);
		Point point2 = geomFactory.createPoint(coordinate2);
		Point point3 = geomFactory.createPoint(coordinate3);
		DBLine2D instance = new DBLine2D();
		instance.setValue(point1, point2, point3);
		LineString value = instance.getValue();
		Assert.assertThat(value.getCoordinates()[0].x, Matchers.is(2.0));
		Assert.assertThat(value.getCoordinates()[0].y, Matchers.is(3.0));
	}

	@Test
	public void testGetSQLDatatype() {
		DBLine2D instance = new DBLine2D();
		String expResult = " LINESTRING ";
		String result = instance.getSQLDatatype();
		assertEquals(expResult, result);
	}

	@Test
	public void testIsAggregator() {
		DBLine2D instance = new DBLine2D();
		boolean expResult = false;
		boolean result = instance.isAggregator();
		assertEquals(expResult, result);
	}

	@Test
	public void testGetIncludesNull() {
		DBLine2D instance = new DBLine2D();
		boolean expResult = false;
		boolean result = instance.getIncludesNull();
		assertEquals(expResult, result);
	}

}
