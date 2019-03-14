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

import com.vividsolutions.jts.geom.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.columns.Polygon2DColumn;
import nz.co.gregs.dbvolution.databases.supports.SupportsPolygonDatatype;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.exceptions.AccidentalDroppingOfTableException;
import nz.co.gregs.dbvolution.exceptions.AutoCommitActionDuringTransactionException;
import nz.co.gregs.dbvolution.expressions.spatial2D.Polygon2DExpression;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.expressions.spatial2D.Point2DExpression;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

public class DBPolygon2DTest extends AbstractTest {

	public DBPolygon2DTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	static Polygon createPolygonFromPoint(Point point) {
		GeometryFactory geoFactory = new GeometryFactory();
		double x = point.getX();
		double y = point.getY();
		Polygon createPolygon = geoFactory.createPolygon(new Coordinate[]{
			new Coordinate(x, y),
			new Coordinate(x + 1, y),
			new Coordinate(x + 1, y + 1),
			new Coordinate(x, y + 1),
			new Coordinate(x, y)});
		return createPolygon;
	}

	@Override
	public void setup(DBDatabase db) throws Exception {
		super.setup(db);
	}

	@Test
	public void basicSpatialTest() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			BasicSpatialTable spatial = new BasicSpatialTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			GeometryFactory geoFactory = new GeometryFactory();
			Point createPoint = geoFactory.createPoint(new Coordinate(5, 10));
			final Polygon createPolygonFromPoint = createPolygonFromPoint(createPoint);
			spatial.myfirstgeom.setValue(createPolygonFromPoint);
			database.insert(spatial);

			List<BasicSpatialTable> allRows = database.getDBTable(new BasicSpatialTable()).setBlankQueryAllowed(true).getAllRows();
			Assert.assertThat(allRows.size(), is(1));

			final Polygon gotPolygon = allRows.get(0).myfirstgeom.getValue();
			Assert.assertThat(gotPolygon.getGeometryType(), is("Polygon"));
			Assert.assertThat(gotPolygon, is(createPolygonFromPoint));

			createPoint = geoFactory.createPoint(new Coordinate(12, 12));
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(createPolygonFromPoint(createPoint));
			database.insert(spatial);
			allRows = database.getDBTable(new BasicSpatialTable()).setBlankQueryAllowed(true).getAllRows();

			Assert.assertThat(allRows.size(), is(2));
		}
	}

	@Test
	public void testValueFromPointArrayWithIntersects() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			BasicSpatialTable spatial = new BasicSpatialTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			GeometryFactory fac = new GeometryFactory();
			Point createPoint = fac.createPoint(new Coordinate(5, 10));
			spatial.myfirstgeom.setValue(createPolygonFromPoint(createPoint));
			database.insert(spatial);

			createPoint = fac.createPoint(new Coordinate(12, 12));
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(createPolygonFromPoint(createPoint));
			database.insert(spatial);

			GeometryFactory geomFactory = new GeometryFactory();
			Coordinate[] coordArray = new Coordinate[]{new Coordinate(0, 0), new Coordinate(11, 0), new Coordinate(11, 11), new Coordinate(0, 11), new Coordinate(0, 0)};
			List<Point> pointList = new ArrayList<Point>();
			for (Coordinate coordArray1 : coordArray) {
				Point point = geomFactory.createPoint(coordArray1);
				pointList.add(point);
			}
			Point[] pointArray = pointList.toArray(new Point[]{});

//			Polygon polygon = fac.createPolygon(coordArray);
			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(Polygon2DExpression.value(pointArray).intersects(spatial.column(spatial.myfirstgeom)));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));
		}
	}

	@Test
	public void testValueFromPointExpressionArrayWithIntersects() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			BasicSpatialTable spatial = new BasicSpatialTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			GeometryFactory fac = new GeometryFactory();
			Point createPoint = fac.createPoint(new Coordinate(5, 10));
			spatial.myfirstgeom.setValue(createPolygonFromPoint(createPoint));
			database.insert(spatial);

			createPoint = fac.createPoint(new Coordinate(12, 12));
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(createPolygonFromPoint(createPoint));
			database.insert(spatial);

			GeometryFactory geomFactory = new GeometryFactory();
			Coordinate[] coordArray = new Coordinate[]{new Coordinate(0, 0), new Coordinate(11, 0), new Coordinate(11, 11), new Coordinate(0, 11), new Coordinate(0, 0)};
			List<Point2DExpression> pointList = new ArrayList<Point2DExpression>();
			for (Coordinate coordArray1 : coordArray) {
				Point point = geomFactory.createPoint(coordArray1);
				pointList.add(Point2DExpression.value(point));
			}
			Point2DExpression[] pointArray = pointList.toArray(new Point2DExpression[]{});

//			Polygon polygon = fac.createPolygon(coordArray);
			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(Polygon2DExpression.value(pointArray).intersects(spatial.column(spatial.myfirstgeom)));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));
		}
	}

	@Test
	public void testValueFromPolygonResultWithIntersects() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			BasicSpatialTable spatial = new BasicSpatialTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			GeometryFactory fac = new GeometryFactory();
			Point createPoint = fac.createPoint(new Coordinate(5, 10));
			spatial.myfirstgeom.setValue(createPolygonFromPoint(createPoint));
			database.insert(spatial);

			createPoint = fac.createPoint(new Coordinate(12, 12));
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(createPolygonFromPoint(createPoint));
			database.insert(spatial);

			GeometryFactory geomFactory = new GeometryFactory();
			Coordinate[] coordArray = new Coordinate[]{new Coordinate(0, 0), new Coordinate(11, 0), new Coordinate(11, 11), new Coordinate(0, 11), new Coordinate(0, 0)};
			List<Point> pointList = new ArrayList<Point>();
			for (Coordinate coordArray1 : coordArray) {
				Point point = geomFactory.createPoint(coordArray1);
				pointList.add(point);
			}
			Point[] pointArray = pointList.toArray(new Point[]{});
			Polygon2DExpression polygonResult = Polygon2DExpression.value(pointArray);

//			Polygon polygon = fac.createPolygon(coordArray);
			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(polygonResult.intersects(spatial.column(spatial.myfirstgeom)));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));
		}
	}

	@Test
	public void testValueFromNumberArrayWithIntersects() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			BasicSpatialTable spatial = new BasicSpatialTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			GeometryFactory fac = new GeometryFactory();
			Point createPoint = fac.createPoint(new Coordinate(5, 10));
			spatial.myfirstgeom.setValue(createPolygonFromPoint(createPoint));
			database.insert(spatial);

			createPoint = fac.createPoint(new Coordinate(12, 12));
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(createPolygonFromPoint(createPoint));
			database.insert(spatial);

			GeometryFactory geomFactory = new GeometryFactory();
			Number[] coordArray = new Number[]{0, 0, 11, 0, 11, 11, 0, 11, 0, 0};

//			Polygon polygon = fac.createPolygon(coordArray);
			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(Polygon2DExpression.value(coordArray).intersects(spatial.column(spatial.myfirstgeom)));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));
		}
	}

	@Test
	public void testIntersects() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			BasicSpatialTable spatial = new BasicSpatialTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			GeometryFactory fac = new GeometryFactory();
			Point createPoint = fac.createPoint(new Coordinate(5, 10));
			spatial.myfirstgeom.setValue(createPolygonFromPoint(createPoint));
			database.insert(spatial);

			createPoint = fac.createPoint(new Coordinate(12, 12));
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(createPolygonFromPoint(createPoint));
			database.insert(spatial);

			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(0, 0), new Coordinate(11, 0), new Coordinate(11, 11), new Coordinate(0, 11), new Coordinate(0, 0)});
			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(Polygon2DExpression.value(polygon).intersects(spatial.column(spatial.myfirstgeom)));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));

			polygon = fac.createPolygon(new Coordinate[]{new Coordinate(0, 0), new Coordinate(13, 0), new Coordinate(13, 13), new Coordinate(0, 13), new Coordinate(0, 0)});
			query = database.getDBQuery(new BasicSpatialTable()).addCondition(Polygon2DExpression.value(polygon).intersects(spatial.column(spatial.myfirstgeom)));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(2));

			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).intersects(polygon));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(2));
		}
	}

	@Test
	public void testIfThenElse() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			BasicSpatialTable spatial = new BasicSpatialTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			GeometryFactory fac = new GeometryFactory();
			Point createPoint = fac.createPoint(new Coordinate(5, 10));
			spatial.myfirstgeom.setValue(createPolygonFromPoint(createPoint));
			database.insert(spatial);

			createPoint = fac.createPoint(new Coordinate(12, 12));
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(createPolygonFromPoint(createPoint));
			database.insert(spatial);

			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(0, 0), new Coordinate(11, 0), new Coordinate(11, 11), new Coordinate(0, 11), new Coordinate(0, 0)});
			Polygon thenPoly = fac.createPolygon(new Coordinate[]{new Coordinate(1, 0), new Coordinate(11, 0), new Coordinate(11, 11), new Coordinate(0, 11), new Coordinate(1, 0)});
			Polygon elsePolygon = fac.createPolygon(new Coordinate[]{new Coordinate(2, 0), new Coordinate(11, 0), new Coordinate(11, 11), new Coordinate(0, 11), new Coordinate(2, 0)});
			DBQuery query = database
					.getDBQuery(new BasicSpatialTable())
					.addCondition(
							Polygon2DExpression.value(polygon).intersects(spatial.column(spatial.myfirstgeom))
									.ifThenElse(thenPoly, elsePolygon).is(thenPoly));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));
		}
	}

	@Test
	public void testContains() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			BasicSpatialTable spatial = new BasicSpatialTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			GeometryFactory fac = new GeometryFactory();
			Point createPoint = fac.createPoint(new Coordinate(5, 10));
			spatial.myfirstgeom.setValue(createPolygonFromPoint(createPoint));
			database.insert(spatial);

			createPoint = fac.createPoint(new Coordinate(12, 12));
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(createPolygonFromPoint(createPoint));
			database.insert(spatial);

			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(0, 0), new Coordinate(11, 0), new Coordinate(11, 11), new Coordinate(0, 11), new Coordinate(0, 0)});
			final Polygon2DColumn column = spatial.column(spatial.myfirstgeom);
			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(Polygon2DExpression.value(polygon).contains(column));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));

			polygon = fac.createPolygon(new Coordinate[]{new Coordinate(0, 0), new Coordinate(13, 0), new Coordinate(13, 13), new Coordinate(0, 13), new Coordinate(0, 0)});
			query = database.getDBQuery(new BasicSpatialTable()).addCondition(Polygon2DExpression.value(polygon).contains(column));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(2));

			query = database.getDBQuery(new BasicSpatialTable()).addCondition(column.contains(polygon));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(0));
		}
	}

	@Test
	public void testDoesNotIntersect() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			BasicSpatialTable spatial = new BasicSpatialTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			GeometryFactory fac = new GeometryFactory();
			Point createPoint = fac.createPoint(new Coordinate(5, 10));
			spatial.myfirstgeom.setValue(createPolygonFromPoint(createPoint));
			database.insert(spatial);

			createPoint = fac.createPoint(new Coordinate(12, 12));
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(createPolygonFromPoint(createPoint));
			database.insert(spatial);

			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(0, 0), new Coordinate(11, 0), new Coordinate(11, 11), new Coordinate(0, 11), new Coordinate(0, 0)});
			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(Polygon2DExpression.value(polygon).doesNotIntersect(spatial.column(spatial.myfirstgeom)));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(2));

			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).doesNotIntersect(polygon));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));

			polygon = fac.createPolygon(new Coordinate[]{new Coordinate(0, 0), new Coordinate(13, 0), new Coordinate(13, 13), new Coordinate(0, 13), new Coordinate(0, 0)});
			query = database.getDBQuery(new BasicSpatialTable()).addCondition(Polygon2DExpression.value(polygon).doesNotIntersect(spatial.column(spatial.myfirstgeom)));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(0));

			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).doesNotIntersect(polygon));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(0));
		}
	}

	@Test
	public void testIs() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			BasicSpatialTable spatial = new BasicSpatialTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			GeometryFactory fac = new GeometryFactory();
			Point createPoint = fac.createPoint(new Coordinate(5, 10));
			spatial.myfirstgeom.setValue(createPolygonFromPoint(createPoint));
			database.insert(spatial);

			createPoint = fac.createPoint(new Coordinate(12, 12));
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(createPolygonFromPoint(createPoint));
			database.insert(spatial);

			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(0, 0), new Coordinate(11, 0), new Coordinate(11, 11), new Coordinate(0, 11), new Coordinate(0, 0)});
			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(Polygon2DExpression.value(polygon).is(polygon));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(2));

			query = database.getDBQuery(new BasicSpatialTable()).addCondition(Polygon2DExpression.value(polygon).is(spatial.column(spatial.myfirstgeom)));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(0));

			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).is(createPolygonFromPoint(createPoint)));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(2));

			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).is(createPolygonFromPoint(createPoint)).not());
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));
		}
	}

	@Test
	public void testIsNot() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			BasicSpatialTable spatial = new BasicSpatialTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			GeometryFactory fac = new GeometryFactory();
			Point createPoint = fac.createPoint(new Coordinate(5, 10));
			spatial.myfirstgeom.setValue(createPolygonFromPoint(createPoint));
			database.insert(spatial);

			createPoint = fac.createPoint(new Coordinate(12, 12));
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(createPolygonFromPoint(createPoint));
			database.insert(spatial);

			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(0, 0), new Coordinate(11, 0), new Coordinate(11, 11), new Coordinate(0, 11), new Coordinate(0, 0)});
			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(Polygon2DExpression.value(polygon).isNot(spatial.column(spatial.myfirstgeom)));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(2));

			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).isNot(createPolygonFromPoint(createPoint)));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));

			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).isNot(createPolygonFromPoint(createPoint)).not());
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(2));
		}
	}

	@Test
	public void testOverlaps() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory fac = addStandardDataSet();

			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(-1, -1), new Coordinate(2, -1), new Coordinate(2, 2), new Coordinate(-1, 2), new Coordinate(-1, -1)});
			BasicSpatialTable spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			polygon = fac.createPolygon(new Coordinate[]{new Coordinate(0, 0), new Coordinate(11, 0), new Coordinate(11, 11), new Coordinate(0, 11), new Coordinate(0, 0)});
			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).overlaps(polygon));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));

			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).overlaps(polygon));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));

			polygon = fac.createPolygon(new Coordinate[]{new Coordinate(0, 0), new Coordinate(13, 0), new Coordinate(13, 13), new Coordinate(0, 13), new Coordinate(0, 0)});
			query = database.getDBQuery(new BasicSpatialTable()).addCondition(Polygon2DExpression.value(polygon).overlaps(spatial.column(spatial.myfirstgeom)));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));

			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).overlaps(polygon));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));
		}
	}

	@Test
	public void testTouches() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			BasicSpatialTable spatial;
			GeometryFactory fac = addStandardDataSet();

			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(-1, -1), new Coordinate(2, -1), new Coordinate(2, 2), new Coordinate(-1, 2), new Coordinate(-1, -1)});
			spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			polygon = fac.createPolygon(new Coordinate[]{new Coordinate(0, 0), new Coordinate(11, 0), new Coordinate(11, 11), new Coordinate(0, 11), new Coordinate(0, 0)});
			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).touches(polygon));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(0));

			polygon = fac.createPolygon(new Coordinate[]{new Coordinate(2, 2), new Coordinate(13, 2), new Coordinate(13, 13), new Coordinate(2, 13), new Coordinate(2, 2)});
			query = database.getDBQuery(new BasicSpatialTable()).addCondition(Polygon2DExpression.value(polygon).touches(spatial.column(spatial.myfirstgeom)));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));

			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).touches(polygon));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));
		}
	}

	@Test
	public void testWithin() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory fac = addStandardDataSet();

			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(-1, -1), new Coordinate(2, -1), new Coordinate(2, 2), new Coordinate(-1, 2), new Coordinate(-1, -1)});
			BasicSpatialTable spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			polygon = fac.createPolygon(new Coordinate[]{new Coordinate(0, 0), new Coordinate(11, 0), new Coordinate(11, 11), new Coordinate(0, 11), new Coordinate(0, 0)});
			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).within(polygon));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));

			polygon = fac.createPolygon(new Coordinate[]{new Coordinate(0, 0), new Coordinate(1, 0), new Coordinate(1, 1), new Coordinate(0, 1), new Coordinate(0, 0)});
			query = database.getDBQuery(new BasicSpatialTable()).addCondition(Polygon2DExpression.value(polygon).within(spatial.column(spatial.myfirstgeom)));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));
		}
	}

	@Test
	public void testDimension() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory fac = addStandardDataSet();

			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(-1, -1), new Coordinate(2, -1), new Coordinate(2, 2), new Coordinate(-1, 2), new Coordinate(-1, -1)});
			BasicSpatialTable spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).measurableDimensions().is(2));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(3));
			Assert.assertThat(allRows.get(0).pkid.intValue(), anyOf(is(1), is(2)));
			Assert.assertThat(allRows.get(1).pkid.intValue(), anyOf(is(1), is(2)));

			query = database.getDBQuery(new BasicSpatialTable()).addCondition(NumberExpression.value(2).is(spatial.column(spatial.myfirstgeom).measurableDimensions()));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(3));
			Assert.assertThat(allRows.get(0).pkid.intValue(), anyOf(is(1), is(2)));
			Assert.assertThat(allRows.get(1).pkid.intValue(), anyOf(is(1), is(2)));

			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).measurableDimensions().is(2));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(3));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));
		}
	}

	@Test
	public void testMaxX() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory fac = addStandardDataSet();

			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(-1, -1), new Coordinate(2, -1), new Coordinate(2, 2), new Coordinate(-1, 2), new Coordinate(-1, -1)});
			BasicSpatialTable spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).maxX().is(2));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));
		}
	}

	@Test
	public void testMinX() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory fac = addStandardDataSet();

			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(-1, -1), new Coordinate(2, -1), new Coordinate(2, 2), new Coordinate(-1, 2), new Coordinate(-1, -1)});
			BasicSpatialTable spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).minX().is(-1));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));
		}
	}

	@Test
	public void testMaxY() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory fac = addStandardDataSet();

			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(-1, -1), new Coordinate(2, -1), new Coordinate(2, 2), new Coordinate(-1, 2), new Coordinate(-1, -1)});
			BasicSpatialTable spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).maxY().is(2));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));
		}
	}

	@Test
	public void testMinY() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory fac = addStandardDataSet();

			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(-1, -1), new Coordinate(2, -1), new Coordinate(2, 2), new Coordinate(-1, 2), new Coordinate(-1, -1)});
			BasicSpatialTable spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).minY().is(-1));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));
		}
	}

	@Test
	public void testArea() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory fac = addStandardDataSet();

			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(-1, -1), new Coordinate(2, -1), new Coordinate(2, 2), new Coordinate(-1, 2), new Coordinate(-1, -1)});
			BasicSpatialTable spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).area().is(1));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(2));
			Assert.assertThat(allRows.get(0).pkid.intValue(), anyOf(is(1), is(2)));
			Assert.assertThat(allRows.get(1).pkid.intValue(), anyOf(is(1), is(2)));

			query = database.getDBQuery(new BasicSpatialTable()).addCondition(NumberExpression.value(1).is(spatial.column(spatial.myfirstgeom).area()));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(2));
			Assert.assertThat(allRows.get(0).pkid.intValue(), anyOf(is(1), is(2)));
			Assert.assertThat(allRows.get(1).pkid.intValue(), anyOf(is(1), is(2)));

			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).area().is(9));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));
		}
	}

	private GeometryFactory addStandardDataSet() throws AutoCommitActionDuringTransactionException, SQLException, AccidentalDroppingOfTableException {
		BasicSpatialTable spatial = new BasicSpatialTable();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(spatial);
		database.createTable(spatial);
		GeometryFactory fac = new GeometryFactory();
		Point createPoint = fac.createPoint(new Coordinate(5, 10));
		spatial.myfirstgeom.setValue(createPolygonFromPoint(createPoint));
		database.insert(spatial);
		createPoint = fac.createPoint(new Coordinate(12, 12));
		spatial = new BasicSpatialTable();
		spatial.myfirstgeom.setValue(createPolygonFromPoint(createPoint));
		database.insert(spatial);
		return fac;
	}

	@Test
	public void testBoundingBox() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory fac = addStandardDataSet();

			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(-1, -1), new Coordinate(2, -1), new Coordinate(2, 2), new Coordinate(-1, 2), new Coordinate(-1, -1)});
			BasicSpatialTable spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).boundingBox().is(polygon));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));

			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).boundingBox().is(
					fac.createPolygon(new Coordinate[]{new Coordinate(5, 10), new Coordinate(6, 10), new Coordinate(6, 11), new Coordinate(5, 11), new Coordinate(5, 10)}))
			);
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));

			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).boundingBox().isNot(polygon));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(2));
			Assert.assertThat(allRows.get(0).pkid.intValue(), anyOf(is(1), is(2)));
			Assert.assertThat(allRows.get(1).pkid.intValue(), anyOf(is(1), is(2)));
		}
	}

	@Test
	public void testStringResult() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory fac = addStandardDataSet();

			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(-1, -1), new Coordinate(2, -1), new Coordinate(2, 2), new Coordinate(-1, 2), new Coordinate(-1, -1)});
			BasicSpatialTable spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).stringResult().isLike("%13%"));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.getValue(), is(2l));
		}
	}

	@Test
	public void testHasMagnitude() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory fac = addStandardDataSet();

			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(-1, -1), new Coordinate(2, -1), new Coordinate(2, 2), new Coordinate(-1, 2), new Coordinate(-1, -1)});
			BasicSpatialTable spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).hasMagnitude());
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(0));
		}
	}

	@Test
	public void testMagnitude() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory fac = addStandardDataSet();

			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(-1, -1), new Coordinate(2, -1), new Coordinate(2, 2), new Coordinate(-1, 2), new Coordinate(-1, -1)});
			BasicSpatialTable spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).magnitude().isNull());
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(3));
		}
	}

	@Test
	public void testSpatialDimensions() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory fac = addStandardDataSet();

			Polygon polygon = fac.createPolygon(new Coordinate[]{new Coordinate(-1, -1), new Coordinate(2, -1), new Coordinate(2, 2), new Coordinate(-1, 2), new Coordinate(-1, -1)});
			BasicSpatialTable spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).spatialDimensions().is(2));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(3));

			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).spatialDimensions().is(3));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(0));
		}
	}

	@Test
	public void testExteriorRing() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory fac = addStandardDataSet();

//POLYGON ((-1 -1, 2 -1, 2 2, -1 2, -1 -1))
			LineString lineString = fac.createLineString(new Coordinate[]{new Coordinate(-1, -1), new Coordinate(2, -1), new Coordinate(2, 2), new Coordinate(-1, 2), new Coordinate(-1, -1)});
			BasicSpatialTable spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(fac.createPolygon(lineString.getCoordinateSequence()));
			database.insert(spatial);

			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).exteriorRing().is(lineString));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));

			query = database.getDBQuery(new BasicSpatialTable())
					.addCondition(spatial.column(spatial.myfirstgeom).exteriorRing().is(
							fac.createLineString(new Coordinate[]{new Coordinate(5, 10), new Coordinate(6, 10), new Coordinate(6, 11), new Coordinate(5, 11), new Coordinate(5, 10)}))
					);
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));

			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).exteriorRing().isNot(lineString));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(2));
			Assert.assertThat(allRows.get(0).pkid.intValue(), anyOf(is(1), is(2)));
			Assert.assertThat(allRows.get(1).pkid.intValue(), anyOf(is(1), is(2)));
		}
	}

	@Test
	public void testPointInPolygon() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory fac = addStandardDataSet();

//POLYGON ((-1 -1, 2 -1, 2 2, -1 2, -1 -1))
			LineString lineString = fac.createLineString(new Coordinate[]{new Coordinate(-1, -1), new Coordinate(2, -1), new Coordinate(2, 2), new Coordinate(-1, 2), new Coordinate(-1, -1)});
			BasicSpatialTable spatial = new BasicSpatialTable();
			spatial.myfirstgeom.setValue(fac.createPolygon(lineString.getCoordinateSequence()));
			database.insert(spatial);

			Point point = fac.createPoint(new Coordinate(5.5, 10.5));

			DBQuery query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).contains(point));
			List<BasicSpatialTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));

			query = database.getDBQuery(new BasicSpatialTable()).addCondition(spatial.column(spatial.myfirstgeom).contains(point).not());
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(2));
			Assert.assertThat(allRows.get(0).pkid.intValue(), anyOf(is(2), is(3)));
			Assert.assertThat(allRows.get(1).pkid.intValue(), anyOf(is(2), is(3)));
		}
	}

	public static class BasicSpatialTable extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		DBInteger pkid = new DBInteger();

		@DBColumn
		DBPolygon2D myfirstgeom = new DBPolygon2D();
	}

}
