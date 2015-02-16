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

import com.vividsolutions.jts.geom.*;
import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.databases.supports.SupportsGeometryDatatype;
import nz.co.gregs.dbvolution.datatypes.spatial.DBGeometry;
import nz.co.gregs.dbvolution.expressions.GeometryExpression;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

public class SpatialTest extends AbstractTest {

	public SpatialTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void basicSpatialTest() throws SQLException {
		if (database instanceof SupportsGeometryDatatype) {
			BasicSpatialTable spatial = new BasicSpatialTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			GeometryFactory fac = new GeometryFactory();
			Point createPoint = fac.createPoint(new Coordinate(5, 10));
			spatial.myfirstgeom.setValue(createPoint);
			database.insert(spatial);

			List<BasicSpatialTable> allRows = database.getDBTable(new BasicSpatialTable()).setBlankQueryAllowed(true).getAllRows();
			Assert.assertThat(allRows.size(), is(1));

			Assert.assertThat(allRows.get(0).myfirstgeom.getValue().getGeometryType(), is("Point"));
			Assert.assertThat(allRows.get(0).myfirstgeom.getValue().equals(createPoint), is(true));
			
			createPoint = fac.createPoint(new Coordinate(12, 12));
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(createPoint);
			database.insert(spatial);
			allRows = database.getDBTable(new BasicSpatialTable()).setBlankQueryAllowed(true).getAllRows();
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(2));
		}
	}
	
	@Test
	public void testIntersects() throws SQLException {
		if (database instanceof SupportsGeometryDatatype) {
			BasicSpatialTable spatial = new BasicSpatialTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			GeometryFactory fac = new GeometryFactory();
			Point createPoint = fac.createPoint(new Coordinate(5, 10));
			spatial.myfirstgeom.setValue(createPoint);
			database.insert(spatial);

			createPoint = fac.createPoint(new Coordinate(12, 12));
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(createPoint);
			database.insert(spatial);

			database.print(database.getDBTable(new BasicSpatialTable()).setBlankQueryAllowed(true).getAllRows());
			
			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(0, 0), new Coordinate(11, 0), new Coordinate(11, 11), new Coordinate(0, 11), new Coordinate(0, 0)});
			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(GeometryExpression.value(polygon).intersects(spatial.column(spatial.myfirstgeom)));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));
			
			polygon = fac.createPolygon(new Coordinate[]{new Coordinate(0, 0), new Coordinate(13, 0), new Coordinate(13, 13), new Coordinate(0, 13), new Coordinate(0, 0)});
			query = database.getDBQuery(new BasicSpatialTable()).addCondition(GeometryExpression.value(polygon).intersects(spatial.column(spatial.myfirstgeom)));
			allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(2));
			
			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).intersects(polygon));
			allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(2));
		}
	}
	
	@Test
	public void testContains() throws SQLException {
		if (database instanceof SupportsGeometryDatatype) {
			BasicSpatialTable spatial = new BasicSpatialTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			GeometryFactory fac = new GeometryFactory();
			Point createPoint = fac.createPoint(new Coordinate(5, 10));
			spatial.myfirstgeom.setValue(createPoint);
			database.insert(spatial);

			createPoint = fac.createPoint(new Coordinate(12, 12));
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(createPoint);
			database.insert(spatial);

			database.print(database.getDBTable(new BasicSpatialTable()).setBlankQueryAllowed(true).getAllRows());
			
			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(0, 0), new Coordinate(11, 0), new Coordinate(11, 11), new Coordinate(0, 11), new Coordinate(0, 0)});
			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(GeometryExpression.value(polygon).contains(spatial.column(spatial.myfirstgeom)));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));
			
			polygon = fac.createPolygon(new Coordinate[]{new Coordinate(0, 0), new Coordinate(13, 0), new Coordinate(13, 13), new Coordinate(0, 13), new Coordinate(0, 0)});
			query = database.getDBQuery(new BasicSpatialTable()).addCondition(GeometryExpression.value(polygon).contains(spatial.column(spatial.myfirstgeom)));
			allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(2));
			
			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).contains(polygon));
			allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(0));
		}
	}
	
	@Test
	public void testDoesNotIntersect() throws SQLException {
		if (database instanceof SupportsGeometryDatatype) {
			BasicSpatialTable spatial = new BasicSpatialTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			GeometryFactory fac = new GeometryFactory();
			Point createPoint = fac.createPoint(new Coordinate(5, 10));
			spatial.myfirstgeom.setValue(createPoint);
			database.insert(spatial);

			createPoint = fac.createPoint(new Coordinate(12, 12));
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(createPoint);
			database.insert(spatial);

			database.print(database.getDBTable(new BasicSpatialTable()).setBlankQueryAllowed(true).getAllRows());
			
			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(0, 0), new Coordinate(11, 0), new Coordinate(11, 11), new Coordinate(0, 11), new Coordinate(0, 0)});
			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(GeometryExpression.value(polygon).doesNotIntersect(spatial.column(spatial.myfirstgeom)));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(2));
			
			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).doesNotIntersect(polygon));
			allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(1));
			
			polygon = fac.createPolygon(new Coordinate[]{new Coordinate(0, 0), new Coordinate(13, 0), new Coordinate(13, 13), new Coordinate(0, 13), new Coordinate(0, 0)});
			query = database.getDBQuery(new BasicSpatialTable()).addCondition(GeometryExpression.value(polygon).doesNotIntersect(spatial.column(spatial.myfirstgeom)));
			allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(0));
			
			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).doesNotIntersect(polygon));
			allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(0));
		}
	}
	
	@Test
	public void testIs() throws SQLException {
		if (database instanceof SupportsGeometryDatatype) {
			BasicSpatialTable spatial = new BasicSpatialTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			GeometryFactory fac = new GeometryFactory();
			Point createPoint = fac.createPoint(new Coordinate(5, 10));
			spatial.myfirstgeom.setValue(createPoint);
			database.insert(spatial);

			createPoint = fac.createPoint(new Coordinate(12, 12));
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(createPoint);
			database.insert(spatial);

			database.print(database.getDBTable(new BasicSpatialTable()).setBlankQueryAllowed(true).getAllRows());
			
			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(0, 0), new Coordinate(11, 0), new Coordinate(11, 11), new Coordinate(0, 11), new Coordinate(0, 0)});
			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(GeometryExpression.value(polygon).is(spatial.column(spatial.myfirstgeom)));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(0));
			
			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).is(createPoint));
			allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(2));
			
			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).is(createPoint).not());
			allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));
		}
	}
	
	@Test
	public void testOverlaps() throws SQLException {
		if (database instanceof SupportsGeometryDatatype) {
			BasicSpatialTable spatial = new BasicSpatialTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			GeometryFactory fac = new GeometryFactory();
			Point createPoint = fac.createPoint(new Coordinate(5, 10));
			spatial.myfirstgeom.setValue(createPoint);
			database.insert(spatial);

			createPoint = fac.createPoint(new Coordinate(12, 12));
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(createPoint);
			database.insert(spatial);

			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(-1, -1), new Coordinate(2, -1), new Coordinate(2, 2), new Coordinate(-1, 2), new Coordinate(-1, -1)});
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			database.print(database.getDBTable(new BasicSpatialTable()).setBlankQueryAllowed(true).getAllRows());
			
			polygon = fac.createPolygon(new Coordinate[]{new Coordinate(0, 0), new Coordinate(11, 0), new Coordinate(11, 11), new Coordinate(0, 11), new Coordinate(0, 0)});
			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).overlaps(polygon));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));
			
			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).overlaps(polygon));
			allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));
			
			polygon = fac.createPolygon(new Coordinate[]{new Coordinate(0, 0), new Coordinate(13, 0), new Coordinate(13, 13), new Coordinate(0, 13), new Coordinate(0, 0)});
			query = database.getDBQuery(new BasicSpatialTable()).addCondition(GeometryExpression.value(polygon).overlaps(spatial.column(spatial.myfirstgeom)));
			allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));
			
			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).overlaps(polygon));
			allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));
		}
	}

	@Test
	public void testTouches() throws SQLException {
		if (database instanceof SupportsGeometryDatatype) {
			BasicSpatialTable spatial = new BasicSpatialTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			GeometryFactory fac = new GeometryFactory();
			Point createPoint = fac.createPoint(new Coordinate(5, 10));
			spatial.myfirstgeom.setValue(createPoint);
			database.insert(spatial);

			createPoint = fac.createPoint(new Coordinate(12, 12));
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(createPoint);
			database.insert(spatial);

			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(-1, -1), new Coordinate(2, -1), new Coordinate(2, 2), new Coordinate(-1, 2), new Coordinate(-1, -1)});
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			database.print(database.getDBTable(new BasicSpatialTable()).setBlankQueryAllowed(true).getAllRows());
			
			polygon = fac.createPolygon(new Coordinate[]{new Coordinate(0, 0), new Coordinate(11, 0), new Coordinate(11, 11), new Coordinate(0, 11), new Coordinate(0, 0)});
			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).touches(polygon));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(0));
			
			polygon = fac.createPolygon(new Coordinate[]{new Coordinate(2, 2), new Coordinate(13, 2), new Coordinate(13, 13), new Coordinate(2, 13), new Coordinate(2, 2)});
			query = database.getDBQuery(new BasicSpatialTable()).addCondition(GeometryExpression.value(polygon).touches(spatial.column(spatial.myfirstgeom)));
			allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));
			
			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).touches(polygon));
			allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));
		}
	}
	
	@Test
	public void testWithin() throws SQLException {
		if (database instanceof SupportsGeometryDatatype) {
			BasicSpatialTable spatial = new BasicSpatialTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			GeometryFactory fac = new GeometryFactory();
			Point createPoint = fac.createPoint(new Coordinate(5, 10));
			spatial.myfirstgeom.setValue(createPoint);
			database.insert(spatial);

			createPoint = fac.createPoint(new Coordinate(12, 12));
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(createPoint);
			database.insert(spatial);

			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(-1, -1), new Coordinate(2, -1), new Coordinate(2, 2), new Coordinate(-1, 2), new Coordinate(-1, -1)});
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			database.print(database.getDBTable(new BasicSpatialTable()).setBlankQueryAllowed(true).getAllRows());
			
			polygon = fac.createPolygon(new Coordinate[]{new Coordinate(0, 0), new Coordinate(11, 0), new Coordinate(11, 11), new Coordinate(0, 11), new Coordinate(0, 0)});
			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).within(polygon));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));
			
			polygon = fac.createPolygon(new Coordinate[]{new Coordinate(0, 0), new Coordinate(1, 0), new Coordinate(1, 1), new Coordinate(0, 1), new Coordinate(0, 0)});
			query = database.getDBQuery(new BasicSpatialTable()).addCondition(GeometryExpression.value(polygon).within(spatial.column(spatial.myfirstgeom)));
			allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));
		}
	}
	
	@Test
	public void testDimension() throws SQLException {
		if (database instanceof SupportsGeometryDatatype) {
			BasicSpatialTable spatial = new BasicSpatialTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			GeometryFactory fac = new GeometryFactory();
			Point createPoint = fac.createPoint(new Coordinate(5, 10));
			spatial.myfirstgeom.setValue(createPoint);
			database.insert(spatial);

			createPoint = fac.createPoint(new Coordinate(12, 12));
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(createPoint);
			database.insert(spatial);

			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(-1, -1), new Coordinate(2, -1), new Coordinate(2, 2), new Coordinate(-1, 2), new Coordinate(-1, -1)});
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			database.print(database.getDBTable(new BasicSpatialTable()).setBlankQueryAllowed(true).getAllRows());
			
			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).dimension().is(0));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(2));
			Assert.assertThat(allRows.get(0).pkid.intValue(), anyOf(is(1),is(2)));
			Assert.assertThat(allRows.get(1).pkid.intValue(), anyOf(is(1),is(2)));
			
			query = database.getDBQuery(new BasicSpatialTable()).addCondition(NumberExpression.value(0).is(spatial.column(spatial.myfirstgeom).dimension()));
			allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(2));
			Assert.assertThat(allRows.get(0).pkid.intValue(), anyOf(is(1),is(2)));
			Assert.assertThat(allRows.get(1).pkid.intValue(), anyOf(is(1),is(2)));
			
			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).dimension().is(2));
			allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));
		}
	}
	
	@Test
	public void testArea() throws SQLException {
		if (database instanceof SupportsGeometryDatatype) {
			BasicSpatialTable spatial = new BasicSpatialTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			GeometryFactory fac = new GeometryFactory();
			Point createPoint = fac.createPoint(new Coordinate(5, 10));
			spatial.myfirstgeom.setValue(createPoint);
			database.insert(spatial);

			createPoint = fac.createPoint(new Coordinate(12, 12));
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(createPoint);
			database.insert(spatial);

			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(-1, -1), new Coordinate(2, -1), new Coordinate(2, 2), new Coordinate(-1, 2), new Coordinate(-1, -1)});
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			database.print(database.getDBTable(new BasicSpatialTable()).setBlankQueryAllowed(true).getAllRows());
			
			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).area().is(0));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(2));
			Assert.assertThat(allRows.get(0).pkid.intValue(), anyOf(is(1),is(2)));
			Assert.assertThat(allRows.get(1).pkid.intValue(), anyOf(is(1),is(2)));
			
			query = database.getDBQuery(new BasicSpatialTable()).addCondition(NumberExpression.value(0).is(spatial.column(spatial.myfirstgeom).area()));
			allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(2));
			Assert.assertThat(allRows.get(0).pkid.intValue(), anyOf(is(1),is(2)));
			Assert.assertThat(allRows.get(1).pkid.intValue(), anyOf(is(1),is(2)));
			
			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).area().is(9));
			allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));
		}
	}
	
	@Test
	public void testBoundingBox() throws SQLException {
		if (database instanceof SupportsGeometryDatatype) {
			BasicSpatialTable spatial = new BasicSpatialTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			GeometryFactory fac = new GeometryFactory();
			Point createPoint = fac.createPoint(new Coordinate(5, 10));
			spatial.myfirstgeom.setValue(createPoint);
			database.insert(spatial);

			createPoint = fac.createPoint(new Coordinate(12, 12));
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(createPoint);
			database.insert(spatial);

			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(-1, -1), new Coordinate(2, -1), new Coordinate(2, 2), new Coordinate(-1, 2), new Coordinate(-1, -1)});
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			database.print(database.getDBTable(new BasicSpatialTable()).setBlankQueryAllowed(true).getAllRows());
			
			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).boundingBox().is(polygon));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));
			
			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).boundingBox().is(
					fac.createPolygon(new Coordinate[]{new Coordinate(5, 10), new Coordinate(5, 10), new Coordinate(5, 10), new Coordinate(5, 10)}))
			);
			allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));
			
			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).boundingBox().isNot(polygon));
			allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(2));
			Assert.assertThat(allRows.get(0).pkid.intValue(), anyOf(is(1),is(2)));
			Assert.assertThat(allRows.get(1).pkid.intValue(), anyOf(is(1),is(2)));
		}
	}
	
	@Test
	public void testExteriorRing() throws SQLException {
		if (database instanceof SupportsGeometryDatatype) {
			BasicSpatialTable spatial = new BasicSpatialTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			GeometryFactory fac = new GeometryFactory();
			Point createPoint = fac.createPoint(new Coordinate(5, 10));
			spatial.myfirstgeom.setValue(createPoint);
			database.insert(spatial);

			createPoint = fac.createPoint(new Coordinate(12, 12));
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(createPoint);
			database.insert(spatial);

			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(-1, -1), new Coordinate(2, -1), new Coordinate(2, 2), new Coordinate(-1, 2), new Coordinate(-1, -1)});
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			database.print(database.getDBTable(new BasicSpatialTable()).setBlankQueryAllowed(true).getAllRows());
			
			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).exteriorRing().is(polygon));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));
			
			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).exteriorRing().is(
					fac.createPolygon(new Coordinate[]{new Coordinate(5, 10), new Coordinate(5, 10), new Coordinate(5, 10), new Coordinate(5, 10)}))
			);
			allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));
			
			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).exteriorRing().isNot(polygon));
			allRows = query.getAllInstancesOf(spatial);
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(2));
			Assert.assertThat(allRows.get(0).pkid.intValue(), anyOf(is(1),is(2)));
			Assert.assertThat(allRows.get(1).pkid.intValue(), anyOf(is(1),is(2)));
		}
	}

	public static class BasicSpatialTable extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		DBInteger pkid = new DBInteger();

		@DBColumn
		DBGeometry myfirstgeom = new DBGeometry();

		@DBColumn
		DBGeometry boundingBox = new DBGeometry(this.column(this.myfirstgeom).exteriorRing());

	}

}
