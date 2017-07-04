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
import com.vividsolutions.jts.geom.LineString;
import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.columns.Polygon3DColumn;
import nz.co.gregs.dbvolution.databases.supports.SupportsPolygonDatatype;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.exceptions.AccidentalDroppingOfTableException;
import nz.co.gregs.dbvolution.exceptions.AutoCommitActionDuringTransactionException;
import nz.co.gregs.dbvolution.expressions.Polygon3DExpression;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

public class DBPolygon3DTest extends AbstractTest {

	public DBPolygon3DTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

//	static Polygon createPolygonFromPoint(Point point) {
//		return createPolygonFromPoint(new Coordinate(point.getX(),point.getY(), 5));
//	}
	
	static PolygonZ createPolygonFromPoint(Coordinate point) {
		GeometryFactory3D geoFactory = new GeometryFactory3D();
		double x = point.x;
		double y = point.y;
		double z = point.z;
		PolygonZ createPolygon = geoFactory.createPolygonZ(new Coordinate[]{
			new Coordinate(x, y, z),
			new Coordinate(x + 1, y, z),
			new Coordinate(x + 1, y + 1, z),
			new Coordinate(x + 1, y + 1, z+1),
			new Coordinate(x, y + 1, z+1),
			new Coordinate(x, y, z+1),
			new Coordinate(x, y, z)});
		return createPolygon;
	}
	private static final PolygonZ POLYGON5106 = createPolygonFromPoint(new Coordinate(5,10,6));
	private static final PolygonZ POLYGON121212 = createPolygonFromPoint(new Coordinate(12,12,12));
	private static final Coordinate[] COORDS011 = new Coordinate[]{new Coordinate(0, 0, 0), new Coordinate(11, 0, 0), new Coordinate(11, 11, 0), new Coordinate(11, 11, 11), new Coordinate(0, 11, 11), new Coordinate(0, 0, 11), new Coordinate(0, 0, 0)};
	private static final Coordinate[] COORDS013 = new Coordinate[]{new Coordinate(0, 0, 0), new Coordinate(13, 0, 0), new Coordinate(13, 13, 0), new Coordinate(13, 13, 13), new Coordinate(0, 13, 13), new Coordinate(0, 0, 13), new Coordinate(0, 0, 0)};
	private static final Polygon3DExpression POLYEXPR011 = Polygon3DExpression.value(COORDS011);
	private static final Polygon3DExpression POLYEXPR013 = Polygon3DExpression.value(COORDS013);
	
	@Override
	public void setup(DBDatabase db) throws Exception {
		super.setup(db);
	}

	@Test
	public void basicSpatialTest() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			BasicSpatial3DTable spatial = new BasicSpatial3DTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			final PolygonZ createPolygonFromPoint = createPolygonFromPoint(new Coordinate(5, 10,6));
			spatial.myfirstgeom.setValue(createPolygonFromPoint);
			database.insert(spatial);

			List<BasicSpatial3DTable> allRows = database.getDBTable(new BasicSpatial3DTable()).setBlankQueryAllowed(true).getAllRows();
			Assert.assertThat(allRows.size(), is(1));
			
			final PolygonZ gotPolygon = allRows.get(0).myfirstgeom.getValue();
			Assert.assertThat(gotPolygon.getGeometryType(), is("PolygonZ"));
			Assert.assertThat(gotPolygon, is(createPolygonFromPoint));

			spatial = new BasicSpatial3DTable();
			spatial.myfirstgeom.setValue(createPolygonFromPoint(new Coordinate(12, 12,12)));
			database.insert(spatial);
			allRows = database.getDBTable(new BasicSpatial3DTable()).setBlankQueryAllowed(true).getAllRows();
			
			Assert.assertThat(allRows.size(), is(2));
		}
	}

	@Test
	public void testValueFromPointArrayWithIntersects() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			BasicSpatial3DTable spatial = new BasicSpatial3DTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			spatial.myfirstgeom.setValue(POLYGON5106);
			database.insert(spatial);

			spatial = new BasicSpatial3DTable();
			spatial.myfirstgeom.setValue(POLYGON121212);
			database.insert(spatial);

			
			
			DBQuery query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(
					POLYEXPR011
							.intersects(spatial.column(spatial.myfirstgeom)));
			List<BasicSpatial3DTable> allRows = query.getAllInstancesOf(spatial);
			
			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));
		}
	}

	@Test
	public void testValueFromPointExpressionArrayWithIntersects() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			BasicSpatial3DTable spatial = new BasicSpatial3DTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			GeometryFactory3D fac = new GeometryFactory3D();
			spatial.myfirstgeom.setValue(POLYGON5106);
			database.insert(spatial);

			spatial = new BasicSpatial3DTable();
			spatial.myfirstgeom.setValue(POLYGON121212);
			database.insert(spatial);

			DBQuery query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(POLYEXPR011.intersects(spatial.column(spatial.myfirstgeom)));
			List<BasicSpatial3DTable> allRows = query.getAllInstancesOf(spatial);
			
			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));
		}
	}

	@Test
	public void testValueFromPolygonResultWithIntersects() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			BasicSpatial3DTable spatial = new BasicSpatial3DTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			spatial.myfirstgeom.setValue(POLYGON5106);
			database.insert(spatial);

			spatial = new BasicSpatial3DTable();
			spatial.myfirstgeom.setValue(POLYGON121212);
			database.insert(spatial);

			DBQuery query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(POLYEXPR011.intersects(spatial.column(spatial.myfirstgeom)));
			List<BasicSpatial3DTable> allRows = query.getAllInstancesOf(spatial);
			
			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));
		}
	}

	@Test
	public void testValueFromNumberArrayWithIntersects() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			BasicSpatial3DTable spatial = new BasicSpatial3DTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			spatial.myfirstgeom.setValue(POLYGON5106);
			database.insert(spatial);

			spatial = new BasicSpatial3DTable();
			spatial.myfirstgeom.setValue(POLYGON121212);
			database.insert(spatial);

			Number[] coordArray = new Number[]{0,0,0, 11,0,0, 11,11,0, 11,11,11, 0,11,11, 0,0,11, 0,0,0};

			DBQuery query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(Polygon3DExpression.value(coordArray).intersects(spatial.column(spatial.myfirstgeom)));
			List<BasicSpatial3DTable> allRows = query.getAllInstancesOf(spatial);
			
			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));
		}
	}

	@Test
	public void testIntersects() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			BasicSpatial3DTable spatial = new BasicSpatial3DTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			spatial.myfirstgeom.setValue(POLYGON5106);
			database.insert(spatial);

			spatial = new BasicSpatial3DTable();
			spatial.myfirstgeom.setValue(POLYGON121212);
			database.insert(spatial);

			DBQuery query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(POLYEXPR011.intersects(spatial.column(spatial.myfirstgeom)));
			List<BasicSpatial3DTable> allRows = query.getAllInstancesOf(spatial);
			
			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));

			query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(POLYEXPR013.intersects(spatial.column(spatial.myfirstgeom)));
			allRows = query.getAllInstancesOf(spatial);
			
			Assert.assertThat(allRows.size(), is(2));

			query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).intersects(POLYEXPR013));
			allRows = query.getAllInstancesOf(spatial);
			
			Assert.assertThat(allRows.size(), is(2));
		}
	}

	@Test
	public void testIfThenElse() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			BasicSpatial3DTable spatial = new BasicSpatial3DTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			spatial.myfirstgeom.setValue(POLYGON5106);
			database.insert(spatial);

			spatial = new BasicSpatial3DTable();
			spatial.myfirstgeom.setValue(POLYGON121212);
			database.insert(spatial);

			GeometryFactory3D fac = new GeometryFactory3D();
			PolygonZ polygon = fac.createPolygonZ(COORDS011);
			PolygonZ thenPoly = fac.createPolygonZ(new Coordinate[]{new Coordinate(1, 0,0), new Coordinate(11, 0,0), new Coordinate(11, 11,0), new Coordinate(11, 11,11), new Coordinate(1, 11,11), new Coordinate(1, 0, 11), new Coordinate(1, 0, 0)});
			PolygonZ elsePolygon = fac.createPolygonZ(new Coordinate[]{new Coordinate(2, 0, 0), new Coordinate(11, 0, 0), new Coordinate(11, 11, 0), new Coordinate(11, 11, 11), new Coordinate(2, 11, 11), new Coordinate(2, 0, 11), new Coordinate(2, 0, 0)});
			DBQuery query = database
					.getDBQuery(new BasicSpatial3DTable())
					.addCondition(
							Polygon3DExpression.value(polygon).intersects(spatial.column(spatial.myfirstgeom))
									.ifThenElse(thenPoly, elsePolygon).is(thenPoly));
			List<BasicSpatial3DTable> allRows = query.getAllInstancesOf(spatial);
			
			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));
		}
	}

	@Test
	public void testContains() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			BasicSpatial3DTable spatial = new BasicSpatial3DTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			GeometryFactory3D fac = new GeometryFactory3D();
			spatial.myfirstgeom.setValue(POLYGON5106);
			database.insert(spatial);

			spatial = new BasicSpatial3DTable();
			spatial.myfirstgeom.setValue(POLYGON121212);
			database.insert(spatial);

			PolygonZ polygon = (fac.createPolygonZ(COORDS011));
			final Polygon3DColumn column = spatial.column(spatial.myfirstgeom);
			DBQuery query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(Polygon3DExpression.value(polygon).contains(column));
			List<BasicSpatial3DTable> allRows = query.getAllInstancesOf(spatial);
			
			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));

			polygon = (fac.createPolygonZ(COORDS013));
			query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(Polygon3DExpression.value(polygon).contains(column));
			allRows = query.getAllInstancesOf(spatial);
			
			Assert.assertThat(allRows.size(), is(2));

			query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(column.contains(polygon));
			allRows = query.getAllInstancesOf(spatial);
			
			Assert.assertThat(allRows.size(), is(0));
		}
	}

	@Test
	public void testDoesNotIntersect() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			BasicSpatial3DTable spatial = new BasicSpatial3DTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			GeometryFactory3D fac = new GeometryFactory3D();
			spatial.myfirstgeom.setValue(POLYGON5106);
			database.insert(spatial);

			spatial = new BasicSpatial3DTable();
			spatial.myfirstgeom.setValue(POLYGON121212);
			database.insert(spatial);

			PolygonZ polygon = (fac.createPolygonZ(COORDS011));
			DBQuery query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(Polygon3DExpression.value(polygon).doesNotIntersect(spatial.column(spatial.myfirstgeom)));
			List<BasicSpatial3DTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(2));

			query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).doesNotIntersect(polygon));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));

			polygon = (fac.createPolygonZ(COORDS013));
			query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(Polygon3DExpression.value(polygon).doesNotIntersect(spatial.column(spatial.myfirstgeom)));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(0));

			query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).doesNotIntersect(polygon));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(0));
		}
	}

	@Test
	public void testIs() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			BasicSpatial3DTable spatial = new BasicSpatial3DTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			GeometryFactory3D fac = new GeometryFactory3D();
			spatial.myfirstgeom.setValue(POLYGON5106);
			database.insert(spatial);

			spatial = new BasicSpatial3DTable();
			spatial.myfirstgeom.setValue(POLYGON121212);
			database.insert(spatial);

			PolygonZ polygon = (fac.createPolygonZ(COORDS011));
			DBQuery query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(Polygon3DExpression.value(polygon).is(polygon));
			List<BasicSpatial3DTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(2));

			query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(Polygon3DExpression.value(polygon).is(spatial.column(spatial.myfirstgeom)));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(0));

			query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).is(POLYGON121212));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(2));

			query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).is(POLYGON121212).not());
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));
		}
	}

	@Test
	public void testIsNot() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			BasicSpatial3DTable spatial = new BasicSpatial3DTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			GeometryFactory3D fac = new GeometryFactory3D();
			spatial.myfirstgeom.setValue(POLYGON5106);
			database.insert(spatial);

			spatial = new BasicSpatial3DTable();
			spatial.myfirstgeom.setValue(POLYGON121212);
			database.insert(spatial);

			PolygonZ polygon = fac.createPolygonZ(COORDS011);
			DBQuery query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(Polygon3DExpression.value(polygon).isNot(spatial.column(spatial.myfirstgeom)));
			List<BasicSpatial3DTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(2));

			query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).isNot(POLYGON121212));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));

			query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).isNot(POLYGON121212).not());
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(2));
		}
	}

	@Test
	public void testOverlaps() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory3D fac = addStandardDataSet();

			PolygonZ polygon = fac.createPolygonZ(new Coordinate[]{new Coordinate(-1, -1,-1), new Coordinate(2, -1,-1), new Coordinate(2, 2,-1), new Coordinate(2, 2,2), new Coordinate(-1, 2,2), new Coordinate(-1, -1,2), new Coordinate(-1, -1,-1)});
			BasicSpatial3DTable spatial = new BasicSpatial3DTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			polygon = (fac.createPolygonZ(COORDS011));
			DBQuery query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).overlaps(polygon));
			List<BasicSpatial3DTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));

			query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).overlaps(polygon));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));

			polygon = (fac.createPolygonZ(COORDS013));
			query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(Polygon3DExpression.value(polygon).overlaps(spatial.column(spatial.myfirstgeom)));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));

			query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).overlaps(polygon));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));
		}
	}

	@Test
	public void testTouches() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			BasicSpatial3DTable spatial;
			GeometryFactory3D fac = addStandardDataSet();

			PolygonZ polygon = (fac.createPolygonZ(new Coordinate[]{new Coordinate(-1, -1,-1), new Coordinate(2, -1,-1), new Coordinate(2, 2,-1), new Coordinate(2, 2,2), new Coordinate(-1, 2,2), new Coordinate(-1, -1,2), new Coordinate(-1, -1,-1)}));
			spatial = new BasicSpatial3DTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			polygon = (fac.createPolygonZ(COORDS011));
			DBQuery query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).touches(polygon));
			List<BasicSpatial3DTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(0));

			polygon = (fac.createPolygonZ(new Coordinate[]{new Coordinate(2, 2,2), new Coordinate(13, 2,2), new Coordinate(13, 13,2), new Coordinate(13, 13,13), new Coordinate(2, 13,13), new Coordinate(2, 2,13), new Coordinate(2, 2,2)}));
			query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(Polygon3DExpression.value(polygon).touches(spatial.column(spatial.myfirstgeom)));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));

			query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).touches(polygon));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));
		}
	}

	@Test
	public void testWithin() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory3D fac = addStandardDataSet();

			PolygonZ polygon = fac.createPolygonZ(new Coordinate[]{new Coordinate(-1, -1,-1), new Coordinate(2, -1,-1), new Coordinate(2, 2,-1), new Coordinate(2, 2,2), new Coordinate(-1, 2,2), new Coordinate(-1, -1,2), new Coordinate(-1, -1,-1)});
			BasicSpatial3DTable spatial = new BasicSpatial3DTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			polygon = fac.createPolygonZ(COORDS011);
			DBQuery query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).within(polygon));
			List<BasicSpatial3DTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));

			polygon = fac.createPolygonZ(new Coordinate[]{new Coordinate(0, 0,0), new Coordinate(1, 0,0), new Coordinate(1, 1,0), new Coordinate(1, 1, 1), new Coordinate(0, 1, 1), new Coordinate(0, 0,1), new Coordinate(0, 0, 0)});
			query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(Polygon3DExpression.value(polygon).within(spatial.column(spatial.myfirstgeom)));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));
		}
	}

	@Test
	public void testDimension() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory3D fac = addStandardDataSet();

			PolygonZ polygon = fac.createPolygonZ(new Coordinate[]{new Coordinate(-1, -1,-1), new Coordinate(2, -1,-1), new Coordinate(2, 2,-1), new Coordinate(2, 2,2), new Coordinate(-1, 2,2), new Coordinate(-1, -1,2), new Coordinate(-1, -1,-1)});
			BasicSpatial3DTable spatial = new BasicSpatial3DTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			DBQuery query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).measurableDimensions().is(2));
			List<BasicSpatial3DTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(3));
			Assert.assertThat(allRows.get(0).pkid.intValue(), anyOf(is(1), is(2)));
			Assert.assertThat(allRows.get(1).pkid.intValue(), anyOf(is(1), is(2)));

			query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(NumberExpression.value(2).is(spatial.column(spatial.myfirstgeom).measurableDimensions()));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(3));
			Assert.assertThat(allRows.get(0).pkid.intValue(), anyOf(is(1), is(2)));
			Assert.assertThat(allRows.get(1).pkid.intValue(), anyOf(is(1), is(2)));

			query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).measurableDimensions().is(2));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(3));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));
		}
	}

	@Test
	public void testMaxX() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory3D fac = addStandardDataSet();

			PolygonZ polygon = fac.createPolygonZ(new Coordinate[]{new Coordinate(-1, -1,-1), new Coordinate(2, -1,-1), new Coordinate(2, 2,-1), new Coordinate(2, 2,2), new Coordinate(-1, 2,2), new Coordinate(-1, -1,2), new Coordinate(-1, -1,-1)});
			BasicSpatial3DTable spatial = new BasicSpatial3DTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			DBQuery query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).maxX().is(2));
			List<BasicSpatial3DTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));
		}
	}

	@Test
	public void testMinX() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory3D fac = addStandardDataSet();

			PolygonZ polygon = fac.createPolygonZ(new Coordinate[]{new Coordinate(-1, -1,-1), new Coordinate(2, -1,-1), new Coordinate(2, 2,-1), new Coordinate(2, 2,2), new Coordinate(-1, 2,2), new Coordinate(-1, -1,2), new Coordinate(-1, -1,-1)});
			BasicSpatial3DTable spatial = new BasicSpatial3DTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			DBQuery query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).minX().is(-1));
			List<BasicSpatial3DTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));
		}
	}

	@Test
	public void testMaxY() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory3D fac = addStandardDataSet();

			PolygonZ polygon = fac.createPolygonZ(new Coordinate[]{new Coordinate(-1, -1,-1), new Coordinate(2, -1,-1), new Coordinate(2, 2,-1), new Coordinate(2, 2,2), new Coordinate(-1, 2,2), new Coordinate(-1, -1,2), new Coordinate(-1, -1,-1)});
			BasicSpatial3DTable spatial = new BasicSpatial3DTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			DBQuery query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).maxY().is(2));
			List<BasicSpatial3DTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));
		}
	}

	@Test
	public void testMinY() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory3D fac = addStandardDataSet();

			PolygonZ polygon = fac.createPolygonZ(new Coordinate[]{new Coordinate(-1, -1,-1), new Coordinate(2, -1,-1), new Coordinate(2, 2,-1), new Coordinate(2, 2,2), new Coordinate(-1, 2,2), new Coordinate(-1, -1,2), new Coordinate(-1, -1,-1)});
			BasicSpatial3DTable spatial = new BasicSpatial3DTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			DBQuery query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).minY().is(-1));
			List<BasicSpatial3DTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));
		}
	}

	@Test
	public void testMaxZ() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory3D fac = addStandardDataSet();

			PolygonZ polygon = fac.createPolygonZ(new Coordinate[]{new Coordinate(-1, -1,-1), new Coordinate(2, -1,-1), new Coordinate(2, 2,-1), new Coordinate(2, 2,2), new Coordinate(-1, 2,2), new Coordinate(-1, -1,2), new Coordinate(-1, -1,-1)});
			BasicSpatial3DTable spatial = new BasicSpatial3DTable();
			spatial.myfirstgeom.setValue(polygon);
			DBActionList insert = database.insert(spatial);
			System.out.println("nz.co.gregs.dbvolution.datatypes.spatial3D.DBPolygon3DTest.testMaxZ(): "+insert.getSQL(database));

			DBQuery query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).maxZ().is(2));
			List<BasicSpatial3DTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));
		}
	}

	@Test
	public void testMinZ() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory3D fac = addStandardDataSet();

			PolygonZ polygon = fac.createPolygonZ(new Coordinate[]{new Coordinate(-1, -1,-1), new Coordinate(2, -1,-1), new Coordinate(2, 2,-1), new Coordinate(2, 2,2), new Coordinate(-1, 2,2), new Coordinate(-1, -1,2), new Coordinate(-1, -1,-1)});
			BasicSpatial3DTable spatial = new BasicSpatial3DTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			DBQuery query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).minZ().is(-1));
			List<BasicSpatial3DTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));
		}
	}

	@Test
	public void testArea() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory3D fac = addStandardDataSet();

			PolygonZ polygon = fac.createPolygonZ(new Coordinate[]{new Coordinate(-1, -1,-1), new Coordinate(2, -1,-1), new Coordinate(2, 2,-1), new Coordinate(2, 2,2), new Coordinate(-1, 2,2), new Coordinate(-1, -1,2), new Coordinate(-1, -1,-1)});
			BasicSpatial3DTable spatial = new BasicSpatial3DTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			DBQuery query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).area().is(1));
			List<BasicSpatial3DTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(2));
			Assert.assertThat(allRows.get(0).pkid.intValue(), anyOf(is(1), is(2)));
			Assert.assertThat(allRows.get(1).pkid.intValue(), anyOf(is(1), is(2)));

			query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(NumberExpression.value(1).is(spatial.column(spatial.myfirstgeom).area()));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(2));
			Assert.assertThat(allRows.get(0).pkid.intValue(), anyOf(is(1), is(2)));
			Assert.assertThat(allRows.get(1).pkid.intValue(), anyOf(is(1), is(2)));

			query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).area().is(9));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));
		}
	}

	private GeometryFactory3D addStandardDataSet() throws AutoCommitActionDuringTransactionException, SQLException, AccidentalDroppingOfTableException {
		BasicSpatial3DTable spatial = new BasicSpatial3DTable();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(spatial);
		database.createTable(spatial);
		spatial.myfirstgeom.setValue(POLYGON5106);
		database.insert(spatial);
		spatial = new BasicSpatial3DTable();
		spatial.myfirstgeom.setValue(POLYGON121212);
		database.insert(spatial);
		GeometryFactory3D fac = new GeometryFactory3D();
		return fac;
	}

	@Test
	public void testBoundingBox() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory3D fac = addStandardDataSet();

			PolygonZ polygon = fac.createPolygonZ(new Coordinate[]{new Coordinate(-1, -1,-1), new Coordinate(2, -1,-1), new Coordinate(2, 2,-1), new Coordinate(2, 2,2), new Coordinate(-1, 2,2), new Coordinate(-1, -1,2), new Coordinate(-1, -1,-1)});
			BasicSpatial3DTable spatial = new BasicSpatial3DTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			DBQuery query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).boundingBox().is(polygon));
			List<BasicSpatial3DTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));

			query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).boundingBox().is(
					fac.createPolygonZ(new Coordinate[]{new Coordinate(5, 10, 6), new Coordinate(6, 10, 6), new Coordinate(6, 11, 6), new Coordinate(6, 11,7), new Coordinate(5, 11, 7), new Coordinate(5, 10, 7), new Coordinate(5, 10, 6)}))
			);
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));

			query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).boundingBox().isNot(polygon));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(2));
			Assert.assertThat(allRows.get(0).pkid.intValue(), anyOf(is(1), is(2)));
			Assert.assertThat(allRows.get(1).pkid.intValue(), anyOf(is(1), is(2)));
		}
	}

	@Test
	public void testStringResult() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory3D fac = addStandardDataSet();

			PolygonZ polygon = fac.createPolygonZ(new Coordinate[]{new Coordinate(-1, -1,-1), new Coordinate(2, -1,-1), new Coordinate(2, 2,-1), new Coordinate(2, 2,2), new Coordinate(-1, 2,2), new Coordinate(-1, -1,2), new Coordinate(-1, -1,-1)});
			BasicSpatial3DTable spatial = new BasicSpatial3DTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			DBQuery query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).stringResult().isLike("%13%"));
			List<BasicSpatial3DTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.getValue(), is(2l));
		}
	}

	@Test
	public void testHasMagnitude() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory3D fac = addStandardDataSet();

			PolygonZ polygon = fac.createPolygonZ(new Coordinate[]{new Coordinate(-1, -1,-1), new Coordinate(2, -1,-1), new Coordinate(2, 2,-1), new Coordinate(2, 2,2), new Coordinate(-1, 2,2), new Coordinate(-1, -1,2), new Coordinate(-1, -1,-1)});
			BasicSpatial3DTable spatial = new BasicSpatial3DTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			DBQuery query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).hasMagnitude());
			List<BasicSpatial3DTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(0));
		}
	}

	@Test
	public void testMagnitude() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory3D fac = addStandardDataSet();

			PolygonZ polygon = fac.createPolygonZ(new Coordinate[]{new Coordinate(-1, -1,-1), new Coordinate(2, -1,-1), new Coordinate(2, 2,-1), new Coordinate(2, 2,2), new Coordinate(-1, 2,2), new Coordinate(-1, -1,2), new Coordinate(-1, -1,-1)});
			BasicSpatial3DTable spatial = new BasicSpatial3DTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			DBQuery query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).magnitude().isNull());
			List<BasicSpatial3DTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(3));
		}
	}

	@Test
	public void testSpatialDimensions() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory3D fac = addStandardDataSet();

			PolygonZ polygon = fac.createPolygonZ(new Coordinate[]{new Coordinate(-1, -1,-1), new Coordinate(2, -1,-1), new Coordinate(2, 2,-1), new Coordinate(2, 2,2), new Coordinate(-1, 2,2), new Coordinate(-1, -1,2), new Coordinate(-1, -1,-1)});
			BasicSpatial3DTable spatial = new BasicSpatial3DTable();
			spatial.myfirstgeom.setValue(polygon);
			database.insert(spatial);

			DBQuery query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).spatialDimensions().is(3));
			List<BasicSpatial3DTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(3));
			
			query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).spatialDimensions().is(2));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(0));
		}
	}

	@Test
	public void testExteriorRing() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory3D fac = addStandardDataSet();

			LineString lineString = fac.createLineString(new Coordinate[]{new Coordinate(-1, -1, -1), new Coordinate(2, -1, -1), new Coordinate(2, 2, -1), new Coordinate(2, 2, 2), new Coordinate(-1, 2, 2), new Coordinate(-1, -1, 2), new Coordinate(-1, -1, -1)});
			BasicSpatial3DTable spatial = new BasicSpatial3DTable();
			spatial.myfirstgeom.setValue(fac.createPolygonZ(lineString.getCoordinateSequence()));
			database.insert(spatial);

			DBQuery query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).exteriorRing().is(lineString));
			List<BasicSpatial3DTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(3));

			query = database.getDBQuery(new BasicSpatial3DTable())
					.addCondition(spatial.column(spatial.myfirstgeom).exteriorRing().is(
									fac.createLineString(new Coordinate[]{new Coordinate(5, 10, 6), new Coordinate(6, 10, 6), new Coordinate(6, 11, 6), new Coordinate(6, 11, 7), new Coordinate(5, 11, 7), new Coordinate(5, 10, 7),new Coordinate(5, 10, 6)}))
					);
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));

			query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).exteriorRing().isNot(lineString));
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(2));
			Assert.assertThat(allRows.get(0).pkid.intValue(), anyOf(is(1), is(2)));
			Assert.assertThat(allRows.get(1).pkid.intValue(), anyOf(is(1), is(2)));
		}
	}

	@Test
	public void testPointInPolygon() throws SQLException {
		if (database instanceof SupportsPolygonDatatype) {
			GeometryFactory3D fac = addStandardDataSet();

			LineString lineString = fac.createLineString(new Coordinate[]{new Coordinate(-1, -1, -1), new Coordinate(2, -1, -1), new Coordinate(2, 2, -1), new Coordinate(2, 2, 2), new Coordinate(-1, 2, 2), new Coordinate(-1, -1, 2), new Coordinate(-1, -1, -1)});
			BasicSpatial3DTable spatial = new BasicSpatial3DTable();
			spatial.myfirstgeom.setValue(fac.createPolygonZ(lineString.getCoordinateSequence()));
			database.insert(spatial);

			Coordinate point = new Coordinate(5.5, 10.5, 6.5);

			DBQuery query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).contains(point));
			List<BasicSpatial3DTable> allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(1));
			Assert.assertThat(allRows.get(0).pkid.intValue(), is(1));

			query = database.getDBQuery(new BasicSpatial3DTable()).addCondition(spatial.column(spatial.myfirstgeom).contains(point).not());
			allRows = query.getAllInstancesOf(spatial);

			Assert.assertThat(allRows.size(), is(2));
			Assert.assertThat(allRows.get(0).pkid.intValue(), anyOf(is(2), is(3)));
			Assert.assertThat(allRows.get(1).pkid.intValue(), anyOf(is(2), is(3)));
		}
	}

	public static class BasicSpatial3DTable extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		DBInteger pkid = new DBInteger();

		@DBColumn
		DBPolygon3D myfirstgeom = new DBPolygon3D();
	}

}
