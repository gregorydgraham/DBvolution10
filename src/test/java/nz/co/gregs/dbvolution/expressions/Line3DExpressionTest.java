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
package nz.co.gregs.dbvolution.expressions;

import com.vividsolutions.jts.geom.Coordinate;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.datatypes.spatial3D.*;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author gregorygraham
 */
public class Line3DExpressionTest extends AbstractTest {

	final GeometryFactory3D geometryFactory = new GeometryFactory3D();

	public Line3DExpressionTest(Object testIterationName, DBDatabase db) throws SQLException {
		super(testIterationName, db);
		LineTestTable lineTestTable = new LineTestTable();

		db.preventDroppingOfTables(false);
		db.dropTableNoExceptions(lineTestTable);
		db.createTable(lineTestTable);

		Coordinate coordinate1 = new Coordinate(2, 3, 1);
		Coordinate coordinate2 = new Coordinate(3, 4, 1);
		Coordinate coordinate3 = new Coordinate(4, 5, 1);
		lineTestTable.line.setValue(coordinate1, coordinate2);
		db.insert(lineTestTable);

		lineTestTable = new LineTestTable();
		lineTestTable.line.setValue(geometryFactory.createLineStringZ(new Coordinate[]{coordinate1, coordinate2, coordinate3}));
		db.insert(lineTestTable);

		lineTestTable = new LineTestTable();
		lineTestTable.line.setValue(geometryFactory.createPointZ(coordinate2), geometryFactory.createPointZ(coordinate3));
		db.insert(lineTestTable);
	}

	public static class LineTestTable extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		public DBInteger line_id = new DBInteger();

		@DBColumn("line_col")
		public DBLine3D line = new DBLine3D();
//		@DBColumn()
//		public DBMultiPoint3D lineIntersections = new DBMultiPoint3D(this.column(this.line).intersectionPoints(Line3DExpression.value(new Coordinate(3, 3),new Coordinate(2, 4),new Coordinate(1, 4))));
	}

	@Test
	public void testValue() throws SQLException {
		LineStringZ line = geometryFactory.createLineStringZ(new Coordinate[]{new Coordinate(2.0, 3.0, 1.0), new Coordinate(3.0, 4.0, 1.0)});
		final LineTestTable pointTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(Line3DExpression.value(line).is(pointTestTable.column(pointTestTable.line)));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
		Assert.assertThat(allRows.get(0).line.lineStringZValue(), is(line));
		Assert.fail();
	}

	@Test
	public void testValueWithLine3DResult() throws SQLException {
		LineStringZ point = geometryFactory.createLineStringZ(new Coordinate[]{new Coordinate(2.0, 3.0, 1.0), new Coordinate(3.0, 4.0, 1.0)});
		final LineTestTable pointTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(Line3DExpression.value(pointTestTable.column(pointTestTable.line)).is(point));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
		Assert.assertThat(allRows.get(0).line.lineStringZValue(), is(point));
	}

	@Test
	public void testGetQueryableDatatypeForExpressionValue() {
		Line3DExpression instance = new Line3DExpression();
		DBLine3D result = instance.getQueryableDatatypeForExpressionValue();
		assertEquals(DBLine3D.class, result.getClass());
	}

	@Test
	public void testCopy() {
		Line3DExpression instance = new Line3DExpression();
		Line3DExpression result = instance.copy();
		assertEquals(instance, result);
	}

	@Test
	public void testIsAggregator() {
		Line3DExpression instance = new Line3DExpression();
		boolean expResult = false;
		boolean result = instance.isAggregator();
		assertEquals(expResult, result);
	}

	@Test
	public void testGetTablesInvolved() {
		final LineTestTable pointTestTable = new LineTestTable();
		Line3DExpression instance = new Line3DExpression(pointTestTable.column(pointTestTable.line));
		Set<DBRow> result = instance.getTablesInvolved();
		Assert.assertThat(result.size(), is(1));
		DBRow[] resultArray = result.toArray(new DBRow[]{});
		DBRow aRow = resultArray[0];
		if (!(aRow instanceof LineTestTable)) {
			fail("Set should include LineTestTable");
		}
	}

	@Test
	public void testIsPurelyFunctional() {
		Line3DExpression instance = new Line3DExpression();
		boolean result = instance.isPurelyFunctional();
		Assert.assertThat(result, is(true));

		final LineTestTable pointTestTable = new LineTestTable();
		instance = new Line3DExpression(pointTestTable.column(pointTestTable.line));
		Assert.assertThat(instance.isPurelyFunctional(), is(false));
	}

	@Test
	public void testGetIncludesNull() {
		Line3DExpression instance = new Line3DExpression((LineStringZ) null);
		Assert.assertThat(instance.getIncludesNull(), is(true));

		final LineTestTable pointTestTable = new LineTestTable();
		instance = new Line3DExpression(pointTestTable.column(pointTestTable.line));
		Assert.assertThat(instance.getIncludesNull(), is(false));
	}

	@Test
	public void testStringResult() throws SQLException {
		LineStringZ line = geometryFactory.createLineStringZ(new Coordinate[]{new Coordinate(2.0, 3.0, 1.0), new Coordinate(3.0, 4.0, 1.0)});
		final LineTestTable pointTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(Line3DExpression.value(line).stringResult().is(pointTestTable.column(pointTestTable.line).stringResult()));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
	}

	@Test
	public void testIs_Line() throws SQLException {
		LineStringZ line = geometryFactory.createLineStringZ(new Coordinate[]{new Coordinate(2.0, 3.0, 1.0), new Coordinate(3.0, 4.0, 1.0)});
		final LineTestTable pointTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(pointTestTable.column(pointTestTable.line).is(line));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
	}

	@Test
	public void testIs_Line3DResult() throws SQLException {
		LineStringZ line = geometryFactory.createLineStringZ(new Coordinate[]{new Coordinate(2.0, 3.0, 1.0), new Coordinate(3.0, 4.0, 1.0)});
		final LineTestTable lineTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(Line3DExpression.value(line).is(lineTestTable.column(lineTestTable.line)));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
	}

	@Test
	public void testGetMaxX() throws SQLException {
		final LineTestTable lineTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.setSortOrder(lineTestTable.column(lineTestTable.line_id));
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).maxX().is(4));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		
		Assert.assertThat(allRows.size(), is(2));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(2));
		Assert.assertThat(allRows.get(1).line_id.intValue(), is(3));
	}

	@Test
	public void testGetMinX() throws SQLException {
		final LineTestTable lineTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).minX().is(3));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(3));
	}

	@Test
	public void testGetMaxY() throws SQLException {
		final LineTestTable lineTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).maxY().is(4));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));

		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).maxY().is(5));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		
		Assert.assertThat(allRows.size(), is(2));
		Assert.assertThat(allRows.get(0).line_id.intValue(), isOneOf(2,3));
	}

	@Test
	public void testGetMinY() throws SQLException {
		final LineTestTable lineTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).minY().is(3));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		
		Assert.assertThat(allRows.size(), is(2));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
		Assert.assertThat(allRows.get(1).line_id.intValue(), is(2));
	}

	@Test
	public void testDimension() throws SQLException {
		final LineTestTable lineTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).measurableDimensions().is(0));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).measurableDimensions().is(1));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testHasMagnitude() throws SQLException {
		final LineTestTable lineTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).hasMagnitude().not());
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testMagnitude() throws SQLException {
		final LineTestTable lineTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).magnitude().isNull());
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testSpatialDimension() throws SQLException {
		final LineTestTable lineTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).spatialDimensions().is(3));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testIntersects() throws SQLException {
		final LineTestTable lineTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinate1 = new Coordinate(1, 2, 0);
		Coordinate coordinate2 = new Coordinate(1, 3, 2);
		final Line3DExpression nonCrossingLine = Line3DExpression.value(coordinate1, coordinate2);
		
		Coordinate coordinateA = new Coordinate(3, 3, 1);
		Coordinate coordinateB = new Coordinate(2, 4, 1);
		final Line3DExpression crossingLine = Line3DExpression.value(coordinateA, coordinateB);

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersects(crossingLine));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersects(nonCrossingLine));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIntersectsUsingCoordsArray() throws SQLException {
		final LineTestTable lineTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinate1 = new Coordinate(1, 2, 0);
		Coordinate coordinate2 = new Coordinate(1, 3, 2);
		final Coordinate[] nonCrossingLine = new Coordinate[]{coordinate1, coordinate2};
		
		Coordinate coordinateA = new Coordinate(3, 3, 1);
		Coordinate coordinateB = new Coordinate(2, 4, 1);
		final Coordinate[] crossingLine = new Coordinate[]{coordinateA, coordinateB};

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersects(crossingLine));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersects(nonCrossingLine));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIntersectsUsingLineString() throws SQLException {
		final LineTestTable lineTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinate1 = new Coordinate(1, 2, 0);
		Coordinate coordinate2 = new Coordinate(1, 3, 2);
		final LineStringZ nonCrossingLine = geometryFactory.createLineStringZ(new Coordinate[]{coordinate1, coordinate2});
		
		Coordinate coordinateA = new Coordinate(3, 3, 1);
		Coordinate coordinateB = new Coordinate(2, 4, 1);
		final LineStringZ crossingLine = geometryFactory.createLineStringZ(new Coordinate[]{coordinateA, coordinateB});

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersects(crossingLine));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersects(nonCrossingLine));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIntersectsUsingPointArray() throws SQLException {
		final LineTestTable lineTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		PointZ coordinate1 = geometryFactory.createPointZ(new Coordinate(1, 2, 0));
		PointZ coordinate2 = geometryFactory.createPointZ(new Coordinate(1, 3, 2));
		final PointZ[] nonCrossingLine = new PointZ[]{coordinate1, coordinate2};
		
		PointZ coordinateA = geometryFactory.createPointZ(new Coordinate(3, 3, 1));
		PointZ coordinateB = geometryFactory.createPointZ(new Coordinate(2, 4, 1));
		final PointZ[] crossingLine = new PointZ[]{coordinateA, coordinateB};

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersects(crossingLine));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersects(nonCrossingLine));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIntersectionWith() throws SQLException {
		final LineTestTable lineTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinate1 = new Coordinate(1, 2, 0);
		Coordinate coordinate2 = new Coordinate(1, 3, 1);
		Coordinate coordinate3 = new Coordinate(5, 3, 2);
		final Line3DExpression nonCrossingLine = Line3DExpression.value(coordinate1, coordinate2, coordinate3);
		
		Coordinate coordinateA = new Coordinate(3, 3, 0);
		Coordinate coordinateB = new Coordinate(2, 4, 0);
		Coordinate coordinateC = new Coordinate(1, 4, 0);
		final Line3DExpression crossingLine = Line3DExpression.value(coordinateA, coordinateB, coordinateC);
		dbQuery.setBlankQueryAllowed(true);
		
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(crossingLine).is(Point3DExpression.value(2.5D, 3.5D, 1D)));
		System.out.println(dbQuery.getSQLForQuery());
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		
		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(nonCrossingLine).is((PointZ)null));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIntersectionWithCoordArray() throws SQLException {
		final LineTestTable lineTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinate1 = new Coordinate(1, 2, 0);
		Coordinate coordinate2 = new Coordinate(1, 3, 1);
		Coordinate coordinate3 = new Coordinate(5, 3, 2);
		final Coordinate[] nonCrossingLine = new Coordinate[]{coordinate1, coordinate2, coordinate3};
		
		Coordinate coordinateA = new Coordinate(3, 3, 1);
		Coordinate coordinateB = new Coordinate(2, 4, 1);
		Coordinate coordinateC = new Coordinate(1, 4, 1);
		Coordinate[] crossingLine = new Coordinate[]{coordinateA, coordinateB, coordinateC};
		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(crossingLine).is(Point3DExpression.value(2.5D, 3.5D, 1D)));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(nonCrossingLine).is((PointZ)null));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIntersectionWithLineString() throws SQLException {
		final LineTestTable lineTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinate1 = new Coordinate(1, 2, 0);
		Coordinate coordinate2 = new Coordinate(1, 3, 1);
		Coordinate coordinate3 = new Coordinate(5, 3, 2);
		final Coordinate[] nonCrossingLine = new Coordinate[]{coordinate1, coordinate2, coordinate3};
		
		Coordinate coordinateA = new Coordinate(3, 3, 1);
		Coordinate coordinateB = new Coordinate(2, 4, 1);
		Coordinate coordinateC = new Coordinate(1, 4, 1);
		Coordinate[] coords = new Coordinate[]{coordinateA, coordinateB, coordinateC};
		LineStringZ crossingLine = geometryFactory.createLineStringZ(coords);
//		final Line3DExpression crossingLine = Line3DExpression.value(coordinateA, coordinateB, coordinateC);
		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(crossingLine).is(Point3DExpression.value(2.5D, 3.5D, 1D)));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(nonCrossingLine).is((PointZ)null));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIntersectionWithMultiPoint3DExpression() throws SQLException {
		final LineTestTable lineTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinate1 = new Coordinate(1, 2, 0);
		Coordinate coordinate2 = new Coordinate(1, 3, 1);
		Coordinate coordinate3 = new Coordinate(5, 3, 2);
		final MultiPoint3DExpression nonCrossingLine = MultiPoint3DExpression.value(coordinate1, coordinate2, coordinate3);
		
		Coordinate coordinateA = new Coordinate(3, 3, 1);
		Coordinate coordinateB = new Coordinate(2, 4, 1);
		Coordinate coordinateC = new Coordinate(1, 4, 1);
		MultiPoint3DExpression crossingLine = MultiPoint3DExpression.value(coordinateA, coordinateB, coordinateC);
		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(crossingLine).is(Point3DExpression.value(2.5D, 3.5D, 1D)));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(nonCrossingLine).is((PointZ)null));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIntersectionWithPointArray() throws SQLException {
		final LineTestTable lineTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		PointZ coordinate1 = geometryFactory.createPointZ(new Coordinate(1, 2, 0));
		PointZ coordinate2 = geometryFactory.createPointZ(new Coordinate(1, 3, 1));
		PointZ coordinate3 = geometryFactory.createPointZ(new Coordinate(5, 3, 2));
		final PointZ[] nonCrossingLine = new PointZ[]{coordinate1, coordinate2, coordinate3};
		
		PointZ coordinateA = geometryFactory.createPointZ(new Coordinate(3, 3, 1));
		PointZ coordinateB = geometryFactory.createPointZ(new Coordinate(2, 4, 1));
		PointZ coordinateC = geometryFactory.createPointZ(new Coordinate(1, 4, 1));
		PointZ[] crossingLine = new PointZ[]{coordinateA, coordinateB, coordinateC};
		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(crossingLine).is(Point3DExpression.value(2.5D, 3.5D, 1D)));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(nonCrossingLine).is((PointZ)null));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIntersectionPoints() throws SQLException {
		final LineTestTable lineTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinate1 = new Coordinate(1, 2, 0);
		Coordinate coordinate2 = new Coordinate(1, 3, 1);
		Coordinate coordinate3 = new Coordinate(5, 3, 2);
		final Line3DExpression nonCrossingLine = Line3DExpression.value(coordinate1, coordinate2, coordinate3);
		
		Coordinate coordinateA = new Coordinate(3, 3, 1);
		Coordinate coordinateB = new Coordinate(2, 4, 1);
		Coordinate coordinateC = new Coordinate(1, 4, 1);
		final Line3DExpression crossingLine = Line3DExpression.value(coordinateA, coordinateB, coordinateC);
		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionPoints(crossingLine).is(MultiPoint3DExpression.value(new Coordinate(2.5D, 3.5D))));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionPoints(nonCrossingLine).is((MultiPointZ)null));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	public static class BoundingBoxTest extends LineTestTable {

		private static final long serialVersionUID = 1L;

		@DBColumn
		public DBString stringLine = new DBString(this.column(this.line).stringResult());

		@DBColumn
		public DBPolygon3D boundingBox = new DBPolygon3D(this.column(this.line).boundingBox());
		
	}

	@Test
	public void testBoundingBox() throws SQLException {
		LineTestTable pointTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable).setBlankQueryAllowed(true);
		dbQuery.addCondition(pointTestTable.column(pointTestTable.line).maxY().is(4));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		
		BoundingBoxTest boundingBoxTestTable = new BoundingBoxTest();
		dbQuery = database.getDBQuery(boundingBoxTestTable).setBlankQueryAllowed(true);
		dbQuery.addCondition(boundingBoxTestTable.column(boundingBoxTestTable.line).maxY().is(4));
		List<BoundingBoxTest> rows = dbQuery.getAllInstancesOf(boundingBoxTestTable);
		
		Assert.assertThat(rows.size(), is(1));
		Assert.assertThat(rows.get(0).line_id.intValue(), is(1));
		final String boundingText = rows.get(0).boundingBox.jtsPolygonValue().toText();
		Assert.assertThat(boundingText, is("POLYGON ((2 3 1, 3 3 1, 3 4 1, 3 4 1, 2 4 1, 2 3 1, 2 3 1))"));
	}	
	
//	@Test
//	public void intersects(){
//		String firstLine = "LINESTRING (2 3 1, 3 4 1)";
//		String secondLine = "LINESTRING (3 3 0, 2 4 0, 1 4 0)" ;
//		if (firstLine == null || secondLine == null) {
//			Assert.fail("Inputs null");
//		}
//		String[] split1 = firstLine.split("[ (),]+");
//		for(int i = 0; i< split1.length; i++){
//			System.out.println(i+": "+split1[i]);
//		}
//		String[] split2 = secondLine.split("[ (),]+");
//		for(int i = 0; i< split2.length; i++){
//			System.out.println(i+": "+split2[i]);
//		}
//		for (int index1 = 0; index1 < split1.length - 4; index1 += 3) {
//			double p0x = Double.parseDouble(split1[index1 + 1]);
//			double p0y = Double.parseDouble(split1[index1 + 2]);
//			double p0z = Double.parseDouble(split1[index1 + 3]);
//			double p1x = Double.parseDouble(split1[index1 + 4]);
//			double p1y = Double.parseDouble(split1[index1 + 5]);
//			double p1z = Double.parseDouble(split1[index1 + 6]);
//
//			for (int index2 = 0; index2 < split2.length - 4; index2 += 3) {
//				double p2x = Double.parseDouble(split2[index2 + 1]);
//				double p2y = Double.parseDouble(split2[index2 + 2]);
//				double p2z = Double.parseDouble(split2[index2 + 3]);
//				double p3x = Double.parseDouble(split2[index2 + 4]);
//				double p3y = Double.parseDouble(split2[index2 + 5]);
//				double p3z = Double.parseDouble(split2[index2 + 6]);
//
//				double s1_x, s1_y, s1_z, s2_x, s2_y, s2_z;
//				double i_x, i_y, t_z, s_z;
//				s1_x = p1x - p0x;
//				s1_y = p1y - p0y;
//				s1_z = p1z - p0z;
//				s2_x = p3x - p2x;
//				s2_y = p3y - p2y;
//				s2_z = p3z - p2z;
//
//				double s, t;
//
//				s = (-s1_y * (p0x - p2x) + s1_x * (p0y - p2y)) / (-s2_x * s1_y + s1_x * s2_y);
//				t = (s2_x * (p0y - p2y) - s2_y * (p0x - p2x)) / (-s2_x * s1_y + s1_x * s2_y);
//
//				if (s >= 0 && s <= 1 && t >= 0 && t <= 1) {
//					t_z = p0z + (t * s1_z);
//					s_z = p2z + (s * s2_z);
//					if (t_z == s_z) {
//						// t and s create the same z so there is an inersection
//						// Collision detected
//						//return true;
//						i_x = p0x + (t * s1_x);
//						i_y = p0y + (t * s1_y);
//						System.err.println("POINT (" + i_x + " " + i_y + " " + t_z + ")");
//					}
//				}
//			}
//		}
//		Assert.fail("got to end");
//	}



}
