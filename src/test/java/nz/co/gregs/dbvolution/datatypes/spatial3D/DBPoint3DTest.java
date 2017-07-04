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
package nz.co.gregs.dbvolution.datatypes.spatial3D;

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
 * @author gregorygraham
 */
public class DBPoint3DTest extends AbstractTest {

	public DBPoint3DTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testSetValue() {
		PointZ point = new GeometryFactory3D().createPointZ(new Coordinate(2, 3, 5));
		DBPoint3D instance = new DBPoint3D();
		instance.setValue(point);
		PointZ value = instance.getValue();
		final Coordinate coordinate = value.getCoordinate();
		Assert.assertThat(coordinate.x, Matchers.is(2.0));
		Assert.assertThat(coordinate.y, Matchers.is(3.0));
		Assert.assertThat(coordinate.z, Matchers.is(5.0));
	}

	@Test
	public void testGetSQLDatatype() {
		DBPoint3D instance = new DBPoint3D();
		String expResult = " POINTZ ";
		String result = instance.getSQLDatatype();
		assertEquals(expResult, result);
	}

	@Test
	public void testIsAggregator() {
		DBPoint3D instance = new DBPoint3D();
		boolean expResult = false;
		boolean result = instance.isAggregator();
		assertEquals(expResult, result);
	}

	@Test
	public void testGetIncludesNull() {
		DBPoint3D instance = new DBPoint3D();
		boolean expResult = false;
		boolean result = instance.getIncludesNull();
		assertEquals(expResult, result);
	}

}
