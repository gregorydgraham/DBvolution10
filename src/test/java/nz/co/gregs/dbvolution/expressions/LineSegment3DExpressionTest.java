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
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.spatial3D.DBLineSegment3D;
import nz.co.gregs.dbvolution.datatypes.spatial3D.DBPolygon3D;
import nz.co.gregs.dbvolution.datatypes.spatial3D.GeometryFactory3D;
import nz.co.gregs.dbvolution.datatypes.spatial3D.LineSegmentZ;
import nz.co.gregs.dbvolution.datatypes.spatial3D.PointZ;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Assert;
import static org.hamcrest.Matchers.*;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

/**
 *
 * @author gregory.graham
 */
public class LineSegment3DExpressionTest extends AbstractTest {

	final GeometryFactory3D geometryFactory = new GeometryFactory3D();

	public LineSegment3DExpressionTest(Object testIterationName, DBDatabase db) throws SQLException {
		super(testIterationName, db);
		LineSegmentTestTable lineTestTable = new LineSegmentTestTable();

		db.preventDroppingOfTables(false);
		db.dropTableNoExceptions(lineTestTable);
		db.createTable(lineTestTable);

		Coordinate coordinate1 = new Coordinate(2, 3, 4);
		Coordinate coordinate2 = new Coordinate(3, 4, 4);
		Coordinate coordinate3 = new Coordinate(4, 5, 4);
		lineTestTable.line.setValue(coordinate1, coordinate2);
		db.insert(lineTestTable);

		lineTestTable = new LineSegmentTestTable();
		lineTestTable.line.setValue(new LineSegmentZ(coordinate2, coordinate3));
		db.insert(lineTestTable);

		lineTestTable = new LineSegmentTestTable();
		lineTestTable.line.setValue(geometryFactory.createPointZ(coordinate1), geometryFactory.createPointZ(coordinate3));
		db.insert(lineTestTable);
	}

	public static class LineSegmentTestTable extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		public DBInteger line_id = new DBInteger();

		@DBColumn("line_col")
		public DBLineSegment3D line = new DBLineSegment3D();

//		@DBColumn
//		public DBPoint3D getXis2 = new DBPoint3D(this.column(this.line).intersectionWith(3d,3d,2d,4d));
	}

	@Test
	public void testToWKTValue() throws SQLException {
		LineSegmentZ lineSegment = new LineSegmentZ(new Coordinate(2.0, 3.0, 4.0), new Coordinate(3.0, 4.0, 4.0));
		final LineSegmentTestTable pointTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(LineSegment3DExpression.value(lineSegment).toWKTFormat().is(pointTestTable.column(pointTestTable.line).toWKTFormat()));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
		Assert.assertThat(allRows.get(0).line.jtsLineSegmentValue(), is(lineSegment));
	}

	@Test
	public void testValue() throws SQLException {
		LineSegmentZ lineSegment = new LineSegmentZ(new Coordinate(2.0, 3.0, 4.0), new Coordinate(3.0, 4.0, 4.0));
		final LineSegmentTestTable pointTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(LineSegment3DExpression.value(lineSegment).is(pointTestTable.column(pointTestTable.line)));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
		Assert.assertThat(allRows.get(0).line.jtsLineSegmentValue(), is(lineSegment));
	}

	@Test
	public void testValueWithLineSegment3DResult() throws SQLException {
		LineSegmentZ lineSegment = new LineSegmentZ(new Coordinate(2.0, 3.0, 4.0), new Coordinate(3.0, 4.0, 4.0));
		final LineSegmentTestTable pointTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(LineSegment3DExpression.value(LineSegment3DExpression.value(2.0, 3.0, 3.0, 4.0)).is(pointTestTable.column(pointTestTable.line)));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
		Assert.assertThat(allRows.get(0).line.jtsLineSegmentValue(), is(lineSegment));
	}

	@Test
	public void testValueWithDoubles() throws SQLException {
		LineSegmentZ lineSegment = new LineSegmentZ(new Coordinate(2.0, 3.0, 4.0), new Coordinate(3.0, 4.0, 4.0));
		final LineSegmentTestTable pointTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(LineSegment3DExpression.value(2.0, 3.0, 3.0, 4.0).is(pointTestTable.column(pointTestTable.line)));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
		Assert.assertThat(allRows.get(0).line.jtsLineSegmentValue(), is(lineSegment));
	}

	@Test
	public void testValueWithPoints() throws SQLException {
		LineSegmentZ lineSegment = new LineSegmentZ(new Coordinate(2.0, 3.0, 4.0), new Coordinate(3.0, 4.0, 4.0));
		PointZ pointA = geometryFactory.createPointZ(new Coordinate(2.0, 3.0, 4.0));
		PointZ pointB = geometryFactory.createPointZ(new Coordinate(3.0, 4.0, 4.0));
		final LineSegmentTestTable pointTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(LineSegment3DExpression.value(pointA, pointB).is(pointTestTable.column(pointTestTable.line)));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
		Assert.assertThat(allRows.get(0).line.jtsLineSegmentValue(), is(lineSegment));
	}

	@Test
	public void testHasMagnitude() throws SQLException {
		final LineSegmentTestTable pointTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(pointTestTable.column(pointTestTable.line).hasMagnitude().not());
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(3));
		Assert.assertThat(allRows.get(0).line_id.intValue(), isOneOf(1, 2, 3));
	}

	@Test
	public void testMagnitude() throws SQLException {
		final LineSegmentTestTable pointTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(pointTestTable.column(pointTestTable.line).magnitude().isNull());
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(3));
		Assert.assertThat(allRows.get(0).line_id.intValue(), isOneOf(1, 2, 3));
	}

	@Test
	public void testSpatialDimensions() throws SQLException {
		final LineSegmentTestTable pointTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(pointTestTable.column(pointTestTable.line).spatialDimensions().is(2));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(3));
		Assert.assertThat(allRows.get(0).line_id.intValue(), isOneOf(1, 2, 3));
	}

	@Test
	public void testGetQueryableDatatypeForExpressionValue() {
		LineSegment3DExpression instance = new LineSegment3DExpression();
		DBLineSegment3D result = instance.getQueryableDatatypeForExpressionValue();
		assertEquals(DBLineSegment3D.class, result.getClass());
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
		final LineSegmentTestTable pointTestTable = new LineSegmentTestTable();
		LineSegment3DExpression instance = new LineSegment3DExpression(pointTestTable.column(pointTestTable.line));
		Set<DBRow> result = instance.getTablesInvolved();
		Assert.assertThat(result.size(), is(1));
		DBRow[] resultArray = result.toArray(new DBRow[]{});
		DBRow aRow = resultArray[0];
		if (!(aRow instanceof LineSegmentTestTable)) {
			Assert.fail("Set should include LineTestTable");
		}
	}

	@Test
	public void testIsPurelyFunctional() {
		LineSegment3DExpression instance = new LineSegment3DExpression();
		boolean result = instance.isPurelyFunctional();
		Assert.assertThat(result, is(true));

		final LineSegmentTestTable pointTestTable = new LineSegmentTestTable();
		instance = new LineSegment3DExpression(pointTestTable.column(pointTestTable.line));
		Assert.assertThat(instance.isPurelyFunctional(), is(false));
	}

	@Test
	public void testGetIncludesNull() {
		LineSegment3DExpression instance = new LineSegment3DExpression((LineSegmentZ) null);
		Assert.assertThat(instance.getIncludesNull(), is(true));

		final LineSegmentTestTable pointTestTable = new LineSegmentTestTable();
		instance = new LineSegment3DExpression(pointTestTable.column(pointTestTable.line));
		Assert.assertThat(instance.getIncludesNull(), is(false));
	}

	@Test
	public void testStringResult() throws SQLException {
		LineSegmentZ line = new LineSegmentZ(new Coordinate(2.0, 3.0, 4.0), new Coordinate(3.0, 4.0, 4.0));
		final LineSegmentTestTable pointTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(LineSegment3DExpression.value(line).stringResult().is(pointTestTable.column(pointTestTable.line).stringResult()));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
	}

	@Test
	public void testIsNot_Line() throws SQLException {
		LineSegmentZ line = new LineSegmentZ(new Coordinate(2.0, 3.0, 4.0), new Coordinate(3.0, 4.0, 4.0));
		final LineSegmentTestTable pointTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(pointTestTable.column(pointTestTable.line).isNot(line));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(2));
		Assert.assertThat(allRows.get(0).line_id.intValue(), isOneOf(2, 3));
	}

	@Test
	public void testIsNot_LineSegment3DResult() throws SQLException {
		LineSegmentZ line = new LineSegmentZ(new Coordinate(2.0, 3.0, 4.0), new Coordinate(3.0, 4.0, 4.0));
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(LineSegment3DExpression.value(line).isNot(lineTestTable.column(lineTestTable.line)));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(2));
		Assert.assertThat(allRows.get(0).line_id.intValue(), isOneOf(2, 3));
	}

	@Test
	public void testIs_Line() throws SQLException {
		LineSegmentZ line = new LineSegmentZ(new Coordinate(2.0, 3.0, 4.0), new Coordinate(3.0, 4.0, 4.0));
		final LineSegmentTestTable pointTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(pointTestTable.column(pointTestTable.line).is(line));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
	}

	@Test
	public void testIs_LineSegment3DResult() throws SQLException {
		LineSegmentZ line = new LineSegmentZ(new Coordinate(2.0, 3.0, 4.0), new Coordinate(3.0, 4.0, 4.0));
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(LineSegment3DExpression.value(line).is(lineTestTable.column(lineTestTable.line)));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
	}

	@Test
	public void testGetMaxX() throws SQLException {
//		LineString line = geometryFactory.createLineString(new Coordinate[]{new Coordinate(2.0, 3.0, 4.0), new Coordinate(3.0, 4.0, 4.0)});
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.setSortOrder(lineTestTable.column(lineTestTable.line_id));
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).maxX().is(4));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		database.print(allRows);

		Assert.assertThat(allRows.size(), is(2));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(2));
		Assert.assertThat(allRows.get(1).line_id.intValue(), is(3));
	}

	@Test
	public void testGetMinX() throws SQLException {
//		LineString line = geometryFactory.createLineString(new Coordinate[]{new Coordinate(2.0, 3.0, 4.0), new Coordinate(3.0, 4.0, 4.0)});
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).minX().is(3));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(2));
	}

	@Test
	public void testGetMaxY() throws SQLException {
//		LineString line = geometryFactory.createLineString(new Coordinate[]{new Coordinate(2.0, 3.0, 4.0), new Coordinate(3.0, 4.0, 4.0)});
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).maxY().is(4));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
	}

	@Test
	public void testGetMinY() throws SQLException {
//		LineString line = geometryFactory.createLineString(new Coordinate[]{new Coordinate(2.0, 3.0, 4.0), new Coordinate(3.0, 4.0, 4.0)});
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).minY().is(3));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(2));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
		Assert.assertThat(allRows.get(1).line_id.intValue(), is(3));
	}

	@Test
	public void testGetMaxZ() throws SQLException {
//		LineString line = geometryFactory.createLineString(new Coordinate[]{new Coordinate(2.0, 3.0, 4.0), new Coordinate(3.0, 4.0, 4.0)});
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).maxZ().is(4));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(3));
		Assert.assertThat(allRows.get(0).line_id.intValue(), isOneOf(1, 2,3));
		
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).maxZ().is(5));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testGetMinZ() throws SQLException {
//		LineString line = geometryFactory.createLineString(new Coordinate[]{new Coordinate(2.0, 3.0, 4.0), new Coordinate(3.0, 4.0, 4.0)});
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).minY().is(4));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(3));
		Assert.assertThat(allRows.get(0).line_id.intValue(), isOneOf(1, 2,3));
		
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).minY().is(3));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testDimension() throws SQLException {
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).measurableDimensions().is(0));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		
		Assert.assertThat(allRows.size(), is(0));
		
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).measurableDimensions().is(1));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		
		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testIntersects() throws SQLException {
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinateA = new Coordinate(3, 3, 3);
		Coordinate coordinateB = new Coordinate(2, 4, 5);
		final LineSegment3DExpression crossingLine = LineSegment3DExpression.value(coordinateA, coordinateB);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersects(crossingLine));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		
		Assert.assertThat(allRows.size(), is(2));
		
		Coordinate coordinate1 = new Coordinate(1, 2, 4);
		Coordinate coordinate2 = new Coordinate(1, 3, 4);
		final LineSegment3DExpression nonCrossingLine = LineSegment3DExpression.value(coordinate1, coordinate2);
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersects(nonCrossingLine));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIntersectsUsingCoordinates() throws SQLException {
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinateA = new Coordinate(3, 3, 4);
		Coordinate coordinateB = new Coordinate(2, 4, 4);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersects(coordinateA, coordinateB));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		
		Assert.assertThat(allRows.size(), is(2));
		
		dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinate1 = new Coordinate(1, 2, 3);
		Coordinate coordinate2 = new Coordinate(1, 3, 5);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersects(coordinate1, coordinate2));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIntersectsUsingLineSegment() throws SQLException {
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinateA = new Coordinate(3, 3, 4);
		Coordinate coordinateB = new Coordinate(2, 4, 4);
		final LineSegmentZ crossingLine = new LineSegmentZ(coordinateA, coordinateB);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersects(crossingLine));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		
		Assert.assertThat(allRows.size(), is(2));
		
		dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinate1 = new Coordinate(1, 2, 3);
		Coordinate coordinate2 = new Coordinate(1, 3, 5);
		final LineSegment3DExpression nonCrossingLine = LineSegment3DExpression.value(coordinate1, coordinate2);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersects(nonCrossingLine));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIntersectsUsingPoints() throws SQLException {
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		PointZ point1 =  geometryFactory.createPointZ(new Coordinate(1, 2, 3));
		PointZ point2 =  geometryFactory.createPointZ(new Coordinate(1, 3, 5));
		final LineSegment3DExpression nonCrossingLine = LineSegment3DExpression.value(point1, point2);

		PointZ pointA = geometryFactory.createPointZ(new Coordinate(3, 3, 4));
		PointZ pointB = geometryFactory.createPointZ(new Coordinate(2, 4, 4));
//		final LineSegment3DExpression crossingLine = LineSegment3DExpression.value(coordinateA, coordinateB);

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersects(pointA, pointB));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersects(nonCrossingLine));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIntersectionWith() throws SQLException {
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinate1 = new Coordinate(1, 2, 0);
		Coordinate coordinate2 = new Coordinate(1, 3, 1);
		final LineSegment3DExpression nonCrossingLine = LineSegment3DExpression.value(coordinate1, coordinate2);

		Coordinate coordinateA = new Coordinate(3, 3, 0.5);
		Coordinate coordinateB = new Coordinate(2, 4, 0.5);
		final LineSegment3DExpression crossingLine = LineSegment3DExpression.value(coordinateA, coordinateB);
		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(crossingLine).is(Point3DExpression.value(2.5D, 3.5D, 0.5D)));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(nonCrossingLine).is((PointZ) null));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIntersectionWithCoordinates() throws SQLException {
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinate1 = new Coordinate(1, 2, 0);
		Coordinate coordinate2 = new Coordinate(1, 3, 1);
		final LineSegment3DExpression nonCrossingLine = LineSegment3DExpression.value(coordinate1, coordinate2);

		Coordinate coordinateA = new Coordinate(3, 3, 0.5);
		Coordinate coordinateB = new Coordinate(2, 4, 0.5);
//		final LineSegment3DExpression crossingLine = LineSegment3DExpression.value(coordinateA, coordinateB);
		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(coordinateA, coordinateB).is(Point3DExpression.value(2.5D, 3.5D, 0.5D)));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(nonCrossingLine).is((PointZ) null));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIntersectionWithDoubles() throws SQLException {
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinate1 = new Coordinate(1, 2, 0.5);
		Coordinate coordinate2 = new Coordinate(1, 3, 0.5);
		final LineSegment3DExpression nonCrossingLine = LineSegment3DExpression.value(coordinate1, coordinate2);

		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(3.0, 3.0, 0.0, 2.0, 4.0, 1.0).is(Point3DExpression.value(2.5D, 3.5D, 0.5D)));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(nonCrossingLine).is((PointZ) null));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIntersectionWithLineSegment() throws SQLException {
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinate1 = new Coordinate(1, 2, 0);
		Coordinate coordinate2 = new Coordinate(1, 3, 2);
		final LineSegment3DExpression nonCrossingLine = LineSegment3DExpression.value(coordinate1, coordinate2);

		Coordinate coordinateA = new Coordinate(3, 3, 1);
		Coordinate coordinateB = new Coordinate(2, 4, 1);
		final LineSegmentZ crossingLine = new LineSegmentZ(coordinateA, coordinateB);
		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(crossingLine).is(Point3DExpression.value(2.5D, 3.5D, 1D)));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(nonCrossingLine).is((PointZ) null));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIntersectionWithPoint() throws SQLException {
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinate1 = new Coordinate(1, 2, 0);
		Coordinate coordinate2 = new Coordinate(1, 3, 2);
		final LineSegment3DExpression nonCrossingLine = LineSegment3DExpression.value(coordinate1, coordinate2);

		PointZ coordinateA = geometryFactory.createPointZ(new Coordinate(3, 3, 1));
		PointZ coordinateB = geometryFactory.createPointZ(new Coordinate(2, 4, 1));
//		final LineSegmentZ crossingLine = new LineSegmentZ(coordinateA, coordinateB);
		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(coordinateA, coordinateB).is(Point3DExpression.value(2.5D, 3.5D, 1D)));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(nonCrossingLine).is((PointZ) null));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	public static class BoundingBoxTest extends LineSegmentTestTable {

		private static final long serialVersionUID = 1L;

		@DBColumn
		public DBString stringLine = new DBString(this.column(this.line).stringResult().substringBetween("(", " "));

		@DBColumn
		public DBPolygon3D boundingBox = new DBPolygon3D(this.column(this.line).boundingBox());

	}

	@Test
	public void testBoundingBox() throws SQLException {
		final BoundingBoxTest pointTestTable = new BoundingBoxTest();
		DBQuery dbQuery = database.getDBQuery(pointTestTable).setBlankQueryAllowed(true);
		dbQuery.addCondition(pointTestTable.column(pointTestTable.line).maxY().is(4));
		List<BoundingBoxTest> allRows = dbQuery.getAllInstancesOf(pointTestTable);

		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
		final String boundingText = allRows.get(0).boundingBox.jtsPolygonValue().toText();
		Assert.assertThat(boundingText, is("POLYGON ((2 3 4, 3 3 4, 3 4 4, 3 4 4, 2 4 4, 2 3 4, 2 3 4))"));
	}

//	@Test
//	public void testTest() {
//		String firstLine = "LINESTRING (2 3 0, 3 4 0)";
//		String secondLine = "LINESTRING (3 3 0, 2 4 0)";
//		if (firstLine == null || secondLine == null) {
//			Assert.fail("NULL");
//		}
//		String[] split = firstLine.split("[ (),]+");
//		double p0x = Double.parseDouble(split[1]);
//		double p0y = Double.parseDouble(split[2]);
//		double p0z = Double.parseDouble(split[3]);
//		double p1x = Double.parseDouble(split[4]);
//		double p1y = Double.parseDouble(split[5]);
//		double p1z = Double.parseDouble(split[6]);
//
//		split = secondLine.split("[ (),]+");
//		double p2x = Double.parseDouble(split[1]);
//		double p2y = Double.parseDouble(split[2]);
//		double p2z = Double.parseDouble(split[3]);
//		double p3x = Double.parseDouble(split[4]);
//		double p3y = Double.parseDouble(split[5]);
//		double p3z = Double.parseDouble(split[6]);
//
//		double s1_x, s1_y, s2_x, s2_y;
//		double i_x, i_y;
//		s1_x = p1x - p0x;
//		s1_y = p1y - p0y;
//		s2_x = p3x - p2x;
//		s2_y = p3y - p2y;
//
//		double s, t;
//
//		s = (-s1_y * (p0x - p2x) + s1_x * (p0y - p2y)) / (-s2_x * s1_y + s1_x * s2_y);
//		t = (s2_x * (p0y - p2y) - s2_y * (p0x - p2x)) / (-s2_x * s1_y + s1_x * s2_y);
//
//		if (s >= 0 && s <= 1 && t >= 0 && t <= 1) {
//			double s1_z = p1z - p0z;
//			double s2_z = p3z - p2z;
//			double t_z = p0z + (t * s1_z);
//			double s_z = p2z + (s * s2_z);
//			if (t_z == s_z) {
//				// t and s create the same z so there is an inersection\n"
//				// Collision detected\n"
//				//i_x = p0x + (t * s1_x);
//				//i_y = p0y + (t * s1_y);
//				System.out.println("INTERSECTION");
//			} else {
//				Assert.fail();
//			}
//		} else {
//			// No collision\n"
//			Assert.fail();
//		}
//		Assert.fail();
//	}
}
