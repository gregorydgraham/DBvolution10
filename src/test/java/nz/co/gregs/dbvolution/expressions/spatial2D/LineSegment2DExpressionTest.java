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
package nz.co.gregs.dbvolution.expressions.spatial2D;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineSegment;
import com.vividsolutions.jts.geom.Point;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBLineSegment2D;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBPolygon2D;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Assert;
import static org.hamcrest.Matchers.*;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregory.graham
 */
public class LineSegment2DExpressionTest extends AbstractTest {

	final GeometryFactory geometryFactory = new GeometryFactory();

	public LineSegment2DExpressionTest(Object testIterationName, DBDatabase db) throws SQLException {
		super(testIterationName, db);
		LineSegmentTestTable lineTestTable = new LineSegmentTestTable();

		db.preventDroppingOfTables(false);
		db.dropTableNoExceptions(lineTestTable);
		db.createTable(lineTestTable);

		Coordinate coordinate1 = new Coordinate(2, 3);
		Coordinate coordinate2 = new Coordinate(3, 4);
		Coordinate coordinate3 = new Coordinate(4, 5);
		lineTestTable.line.setValue(coordinate1, coordinate2);
		db.insert(lineTestTable);

		lineTestTable = new LineSegmentTestTable();
		lineTestTable.line.setValue(new LineSegment(coordinate2, coordinate3));
		db.insert(lineTestTable);

		lineTestTable = new LineSegmentTestTable();
		lineTestTable.line.setValue(geometryFactory.createPoint(coordinate1), geometryFactory.createPoint(coordinate3));
		db.insert(lineTestTable);
	}

	public static class LineSegmentTestTable extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		public DBInteger line_id = new DBInteger();

		@DBColumn("line_col")
		public DBLineSegment2D line = new DBLineSegment2D();
	}

	@Test
	public void testToWKTValue() throws SQLException {
		LineSegment lineSegment = new LineSegment(new Coordinate(2.0, 3.0), new Coordinate(3.0, 4.0));
		final LineSegmentTestTable pointTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(LineSegment2DExpression.value(lineSegment).toWKTFormat().is(pointTestTable.column(pointTestTable.line).toWKTFormat()));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
		Assert.assertThat(allRows.get(0).line.jtsLineSegmentValue(), is(lineSegment));
	}

	@Test
	public void testValue() throws SQLException {
		LineSegment lineSegment = new LineSegment(new Coordinate(2.0, 3.0), new Coordinate(3.0, 4.0));
		final LineSegmentTestTable pointTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(LineSegment2DExpression.value(lineSegment).is(pointTestTable.column(pointTestTable.line)));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
		Assert.assertThat(allRows.get(0).line.jtsLineSegmentValue(), is(lineSegment));
	}

	@Test
	public void testValueWithLineSegment2DResult() throws SQLException {
		LineSegment lineSegment = new LineSegment(new Coordinate(2.0, 3.0), new Coordinate(3.0, 4.0));
		final LineSegmentTestTable pointTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(LineSegment2DExpression.value(LineSegment2DExpression.value(2.0, 3.0, 3.0, 4.0)).is(pointTestTable.column(pointTestTable.line)));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
		Assert.assertThat(allRows.get(0).line.jtsLineSegmentValue(), is(lineSegment));
	}

	@Test
	public void testValueWithDoubles() throws SQLException {
		LineSegment lineSegment = new LineSegment(new Coordinate(2.0, 3.0), new Coordinate(3.0, 4.0));
		final LineSegmentTestTable pointTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(LineSegment2DExpression.value(2.0, 3.0, 3.0, 4.0).is(pointTestTable.column(pointTestTable.line)));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
		Assert.assertThat(allRows.get(0).line.jtsLineSegmentValue(), is(lineSegment));
	}

	@Test
	public void testValueWithPoints() throws SQLException {
		LineSegment lineSegment = new LineSegment(new Coordinate(2.0, 3.0), new Coordinate(3.0, 4.0));
		Point pointA = geometryFactory.createPoint(new Coordinate(2.0, 3.0));
		Point pointB = geometryFactory.createPoint(new Coordinate(3.0, 4.0));
		final LineSegmentTestTable pointTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(LineSegment2DExpression.value(pointA, pointB).is(pointTestTable.column(pointTestTable.line)));
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
		LineSegment2DExpression instance = new LineSegment2DExpression();
		DBLineSegment2D result = instance.getQueryableDatatypeForExpressionValue();
		assertEquals(DBLineSegment2D.class, result.getClass());
	}

	@Test
	public void testCopy() {
		LineSegment2DExpression instance = new LineSegment2DExpression();
		LineSegment2DExpression result = instance.copy();
		assertEquals(instance.getInnerResult(), result.getInnerResult());
		assertEquals(instance.getIncludesNull(), result.getIncludesNull());
	}

	@Test
	public void testIsAggregator() {
		Line2DExpression instance = new Line2DExpression();
		boolean expResult = false;
		boolean result = instance.isAggregator();
		assertEquals(expResult, result);
	}

	@Test
	public void testGetTablesInvolved() {
		final LineSegmentTestTable pointTestTable = new LineSegmentTestTable();
		LineSegment2DExpression instance = new LineSegment2DExpression(pointTestTable.column(pointTestTable.line));
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
		LineSegment2DExpression instance = new LineSegment2DExpression();
		boolean result = instance.isPurelyFunctional();
		Assert.assertThat(result, is(true));

		final LineSegmentTestTable pointTestTable = new LineSegmentTestTable();
		instance = new LineSegment2DExpression(pointTestTable.column(pointTestTable.line));
		Assert.assertThat(instance.isPurelyFunctional(), is(false));
	}

	@Test
	public void testGetIncludesNull() {
		LineSegment2DExpression instance = new LineSegment2DExpression((LineSegment) null);
		Assert.assertThat(instance.getIncludesNull(), is(true));

		final LineSegmentTestTable pointTestTable = new LineSegmentTestTable();
		instance = new LineSegment2DExpression(pointTestTable.column(pointTestTable.line));
		Assert.assertThat(instance.getIncludesNull(), is(false));
	}

	@Test
	public void testStringResult() throws SQLException {
		LineSegment line = new LineSegment(new Coordinate(2.0, 3.0), new Coordinate(3.0, 4.0));
		final LineSegmentTestTable pointTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(LineSegment2DExpression.value(line).stringResult().is(pointTestTable.column(pointTestTable.line).stringResult()));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
	}

	@Test
	public void testIsNot_Line() throws SQLException {
		LineSegment line = new LineSegment(new Coordinate(2.0, 3.0), new Coordinate(3.0, 4.0));
		final LineSegmentTestTable pointTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(pointTestTable.column(pointTestTable.line).isNot(line));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(2));
		Assert.assertThat(allRows.get(0).line_id.intValue(), isOneOf(2, 3));
	}

	@Test
	public void testIsNot_LineSegment2DResult() throws SQLException {
		LineSegment line = new LineSegment(new Coordinate(2.0, 3.0), new Coordinate(3.0, 4.0));
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(LineSegment2DExpression.value(line).isNot(lineTestTable.column(lineTestTable.line)));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(2));
		Assert.assertThat(allRows.get(0).line_id.intValue(), isOneOf(2, 3));
	}

	@Test
	public void testIs_Line() throws SQLException {
		LineSegment line = new LineSegment(new Coordinate(2.0, 3.0), new Coordinate(3.0, 4.0));
		final LineSegmentTestTable pointTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(pointTestTable.column(pointTestTable.line).is(line));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
	}

	@Test
	public void testIs_LineSegment2DResult() throws SQLException {
		LineSegment line = new LineSegment(new Coordinate(2.0, 3.0), new Coordinate(3.0, 4.0));
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(LineSegment2DExpression.value(line).is(lineTestTable.column(lineTestTable.line)));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
	}

	@Test
	public void testGetMaxX() throws SQLException {
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.setSortOrder(lineTestTable.column(lineTestTable.line_id));
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).maxX().is(4));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(2));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(2));
		Assert.assertThat(allRows.get(1).line_id.intValue(), is(3));
	}

	@Test
	public void testGetMinX() throws SQLException {
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).minX().is(3));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(2));
	}

	@Test
	public void testGetMaxY() throws SQLException {
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).maxY().is(4));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
	}

	@Test
	public void testGetMinY() throws SQLException {
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).minY().is(3));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(2));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
		Assert.assertThat(allRows.get(1).line_id.intValue(), is(3));
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
		Coordinate coordinate1 = new Coordinate(1, 2);
		Coordinate coordinate2 = new Coordinate(1, 3);
		final LineSegment2DExpression nonCrossingLine = LineSegment2DExpression.value(coordinate1, coordinate2);

		Coordinate coordinateA = new Coordinate(3, 3);
		Coordinate coordinateB = new Coordinate(2, 4);
		final LineSegment2DExpression crossingLine = LineSegment2DExpression.value(coordinateA, coordinateB);

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersects(crossingLine));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersects(nonCrossingLine));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIntersectsUsingCoordinates() throws SQLException {
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinate1 = new Coordinate(1, 2);
		Coordinate coordinate2 = new Coordinate(1, 3);
		final LineSegment2DExpression nonCrossingLine = LineSegment2DExpression.value(coordinate1, coordinate2);

		Coordinate coordinateA = new Coordinate(3, 3);
		Coordinate coordinateB = new Coordinate(2, 4);
		final LineSegment2DExpression crossingLine = LineSegment2DExpression.value(coordinateA, coordinateB);

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersects(coordinateA, coordinateB));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersects(nonCrossingLine));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIntersectsUsingLineSegment() throws SQLException {
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinate1 = new Coordinate(1, 2);
		Coordinate coordinate2 = new Coordinate(1, 3);
		final LineSegment2DExpression nonCrossingLine = LineSegment2DExpression.value(coordinate1, coordinate2);

		Coordinate coordinateA = new Coordinate(3, 3);
		Coordinate coordinateB = new Coordinate(2, 4);
		final LineSegment crossingLine = new LineSegment(coordinateA, coordinateB);

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersects(crossingLine));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersects(nonCrossingLine));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIntersectsUsingPoints() throws SQLException {
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinate1 = new Coordinate(1, 2);
		Coordinate coordinate2 = new Coordinate(1, 3);
		final LineSegment2DExpression nonCrossingLine = LineSegment2DExpression.value(coordinate1, coordinate2);

		Point coordinateA = geometryFactory.createPoint(new Coordinate(3, 3));
		Point coordinateB = geometryFactory.createPoint(new Coordinate(2, 4));

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersects(coordinateA, coordinateB));
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
		Coordinate coordinate1 = new Coordinate(1, 2);
		Coordinate coordinate2 = new Coordinate(1, 3);
		final LineSegment2DExpression nonCrossingLine = LineSegment2DExpression.value(coordinate1, coordinate2);

		Coordinate coordinateA = new Coordinate(3, 3);
		Coordinate coordinateB = new Coordinate(2, 4);
		final LineSegment2DExpression crossingLine = LineSegment2DExpression.value(coordinateA, coordinateB);
		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(crossingLine).is(Point2DExpression.value(2.5D, 3.5D)));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(nonCrossingLine).is((Point) null));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIntersectionWithCoordinates() throws SQLException {
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinate1 = new Coordinate(1, 2);
		Coordinate coordinate2 = new Coordinate(1, 3);
		final LineSegment2DExpression nonCrossingLine = LineSegment2DExpression.value(coordinate1, coordinate2);

		Coordinate coordinateA = new Coordinate(3, 3);
		Coordinate coordinateB = new Coordinate(2, 4);
		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(coordinateA, coordinateB).is(Point2DExpression.value(2.5D, 3.5D)));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(nonCrossingLine).is((Point) null));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIntersectionWithDoubles() throws SQLException {
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinate1 = new Coordinate(1, 2);
		Coordinate coordinate2 = new Coordinate(1, 3);
		final LineSegment2DExpression nonCrossingLine = LineSegment2DExpression.value(coordinate1, coordinate2);

		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(3.0, 3.0, 2.0, 4.0).is(Point2DExpression.value(2.5D, 3.5D)));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(nonCrossingLine).is((Point) null));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIntersectionWithLineSegment() throws SQLException {
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinate1 = new Coordinate(1, 2);
		Coordinate coordinate2 = new Coordinate(1, 3);
		final LineSegment2DExpression nonCrossingLine = LineSegment2DExpression.value(coordinate1, coordinate2);

		Coordinate coordinateA = new Coordinate(3, 3);
		Coordinate coordinateB = new Coordinate(2, 4);
		final LineSegment crossingLine = new LineSegment(coordinateA, coordinateB);
		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(crossingLine).is(Point2DExpression.value(2.5D, 3.5D)));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(nonCrossingLine).is((Point) null));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIntersectionWithPoint() throws SQLException {
		final LineSegmentTestTable lineTestTable = new LineSegmentTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinate1 = new Coordinate(1, 2);
		Coordinate coordinate2 = new Coordinate(1, 3);
		final LineSegment2DExpression nonCrossingLine = LineSegment2DExpression.value(coordinate1, coordinate2);

		Point coordinateA = geometryFactory.createPoint(new Coordinate(3, 3));
		Point coordinateB = geometryFactory.createPoint(new Coordinate(2, 4));
		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(coordinateA, coordinateB).is(Point2DExpression.value(2.5D, 3.5D)));
		List<LineSegmentTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(nonCrossingLine).is((Point) null));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	public static class BoundingBoxTest extends LineSegmentTestTable {

		private static final long serialVersionUID = 1L;

		@DBColumn
		public DBString stringLine = new DBString(this.column(this.line).stringResult().substringBetween("(", " "));

		@DBColumn
		public DBPolygon2D boundingBox = new DBPolygon2D(this.column(this.line).boundingBox());

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
		Assert.assertThat(boundingText, is("POLYGON ((2 3, 3 3, 3 4, 2 4, 2 3))"));
	}

}
