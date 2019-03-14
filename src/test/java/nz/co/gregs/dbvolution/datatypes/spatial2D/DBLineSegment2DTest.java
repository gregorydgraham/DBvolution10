/*
 * Copyright 2015 gregory.graham.
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
import com.vividsolutions.jts.geom.LineSegment;
import com.vividsolutions.jts.geom.Point;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregory.graham
 */
public class DBLineSegment2DTest extends AbstractTest {

	public DBLineSegment2DTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testSetValueLineString() {
		Coordinate coordinate1 = new Coordinate(2, 3);
		Coordinate coordinate2 = new Coordinate(3, 4);
		LineSegment line = new LineSegment(coordinate1, coordinate2);
		DBLineSegment2D instance = new DBLineSegment2D();
		instance.setValue(line);
		LineSegment value = instance.getValue();
		Assert.assertThat(value.getCoordinate(0).x, is(2.0));
		Assert.assertThat(value.getCoordinate(0).y, is(3.0));
	}

	@Test
	public void testSetValueCoorrdinates() {
		Coordinate coordinate1 = new Coordinate(2, 3);
		Coordinate coordinate2 = new Coordinate(3, 4);
		DBLineSegment2D instance = new DBLineSegment2D();
		instance.setValue(coordinate1, coordinate2);
		LineSegment value = instance.getValue();
		Assert.assertThat(value.getCoordinate(0).x, is(2.0));
		Assert.assertThat(value.getCoordinate(0).y, is(3.0));
	}

	@Test
	public void testSetValuePoints() {
		GeometryFactory geomFactory = new GeometryFactory();
		Coordinate coordinate1 = new Coordinate(2, 3);
		Coordinate coordinate2 = new Coordinate(3, 4);
		Point point1 = geomFactory.createPoint(coordinate1);
		Point point2 = geomFactory.createPoint(coordinate2);
		DBLineSegment2D instance = new DBLineSegment2D();
		instance.setValue(point1, point2);
		LineSegment value = instance.getValue();
		Assert.assertThat(value.getCoordinate(0).x, is(2.0));
		Assert.assertThat(value.getCoordinate(0).y, is(3.0));
	}

	@Test
	public void testGetSQLDatatype() {
		DBLineSegment2D instance = new DBLineSegment2D();
		String expResult = " LINESTRING ";
		String result = instance.getSQLDatatype();
		assertEquals(expResult, result);
	}

	@Test
	public void testIsAggregator() {
		DBLineSegment2D instance = new DBLineSegment2D();
		boolean expResult = false;
		boolean result = instance.isAggregator();
		assertEquals(expResult, result);
	}

	@Test
	public void testGetIncludesNull() {
		DBLineSegment2D instance = new DBLineSegment2D();
		boolean expResult = false;
		boolean result = instance.getIncludesNull();
		assertEquals(expResult, result);
	}

}
