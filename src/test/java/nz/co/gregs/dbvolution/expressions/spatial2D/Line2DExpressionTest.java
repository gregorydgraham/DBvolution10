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
package nz.co.gregs.dbvolution.expressions.spatial2D;

import nz.co.gregs.dbvolution.expressions.spatial2D.Point2DExpression;
import nz.co.gregs.dbvolution.expressions.spatial2D.MultiPoint2DExpression;
import nz.co.gregs.dbvolution.expressions.spatial2D.Line2DExpression;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiPoint;
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
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.datatypes.spatial2D.*;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
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
public class Line2DExpressionTest extends AbstractTest {

	final GeometryFactory geometryFactory = new GeometryFactory();

	public Line2DExpressionTest(Object testIterationName, DBDatabase db) throws SQLException {
		super(testIterationName, db);
		LineTestTable lineTestTable = new LineTestTable();

		db.preventDroppingOfTables(false);
		db.dropTableNoExceptions(lineTestTable);
		db.createTable(lineTestTable);

		Coordinate coordinate1 = new Coordinate(2, 3);
		Coordinate coordinate2 = new Coordinate(3, 4);
		Coordinate coordinate3 = new Coordinate(4, 5);
		lineTestTable.line.setValue(coordinate1, coordinate2);
		db.insert(lineTestTable);

		lineTestTable = new LineTestTable();
		lineTestTable.line.setValue(geometryFactory.createLineString(new Coordinate[]{coordinate1, coordinate2, coordinate3}));
		db.insert(lineTestTable);

		lineTestTable = new LineTestTable();
		lineTestTable.line.setValue(geometryFactory.createPoint(coordinate2), geometryFactory.createPoint(coordinate3));
		db.insert(lineTestTable);
	}

	public static class LineTestTable extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		public DBInteger line_id = new DBInteger();

		@DBColumn("line_col")
		public DBLine2D line = new DBLine2D();
	}

	@Test
	public void testValue() throws SQLException {
		LineString point = geometryFactory.createLineString(new Coordinate[]{new Coordinate(2.0, 3.0), new Coordinate(3.0, 4.0)});
		final LineTestTable pointTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(Line2DExpression.value(point).is(pointTestTable.column(pointTestTable.line)));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
		Assert.assertThat(allRows.get(0).line.jtsLineStringValue(), is(point));
	}

	@Test
	public void testValueWithLine2DResult() throws SQLException {
		LineString point = geometryFactory.createLineString(new Coordinate[]{new Coordinate(2.0, 3.0), new Coordinate(3.0, 4.0)});
		final LineTestTable pointTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(Line2DExpression.value(pointTestTable.column(pointTestTable.line)).is(point));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
		Assert.assertThat(allRows.get(0).line.jtsLineStringValue(), is(point));
	}

	@Test
	public void testGetQueryableDatatypeForExpressionValue() {
		Line2DExpression instance = new Line2DExpression();
		DBLine2D result = instance.getQueryableDatatypeForExpressionValue();
		assertEquals(DBLine2D.class, result.getClass());
	}

	@Test
	public void testCopy() {
		Line2DExpression instance = new Line2DExpression();
		Line2DExpression result = instance.copy();
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
		final LineTestTable pointTestTable = new LineTestTable();
		Line2DExpression instance = new Line2DExpression(pointTestTable.column(pointTestTable.line));
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
		Line2DExpression instance = new Line2DExpression();
		boolean result = instance.isPurelyFunctional();
		Assert.assertThat(result, is(true));

		final LineTestTable pointTestTable = new LineTestTable();
		instance = new Line2DExpression(pointTestTable.column(pointTestTable.line));
		Assert.assertThat(instance.isPurelyFunctional(), is(false));
	}

	@Test
	public void testGetIncludesNull() {
		Line2DExpression instance = new Line2DExpression((LineString) null);
		Assert.assertThat(instance.getIncludesNull(), is(true));

		final LineTestTable pointTestTable = new LineTestTable();
		instance = new Line2DExpression(pointTestTable.column(pointTestTable.line));
		Assert.assertThat(instance.getIncludesNull(), is(false));
	}

	@Test
	public void testStringResult() throws SQLException {
		LineString line = geometryFactory.createLineString(new Coordinate[]{new Coordinate(2.0, 3.0), new Coordinate(3.0, 4.0)});
		final LineTestTable pointTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(Line2DExpression.value(line).stringResult().is(pointTestTable.column(pointTestTable.line).stringResult()));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
	}

	@Test
	public void testIs_Line() throws SQLException {
		LineString line = geometryFactory.createLineString(new Coordinate[]{new Coordinate(2.0, 3.0), new Coordinate(3.0, 4.0)});
		final LineTestTable pointTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(pointTestTable.column(pointTestTable.line).is(line));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
	}

	@Test
	public void testIs_Line2DResult() throws SQLException {
		LineString line = geometryFactory.createLineString(new Coordinate[]{new Coordinate(2.0, 3.0), new Coordinate(3.0, 4.0)});
		final LineTestTable lineTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(Line2DExpression.value(line).is(lineTestTable.column(lineTestTable.line)));
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
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).spatialDimensions().is(2));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testIntersects() throws SQLException {
		final LineTestTable lineTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinate1 = new Coordinate(1, 2);
		Coordinate coordinate2 = new Coordinate(1, 3);
		final Line2DExpression nonCrossingLine = Line2DExpression.value(coordinate1, coordinate2);

		Coordinate coordinateA = new Coordinate(3, 3);
		Coordinate coordinateB = new Coordinate(2, 4);
		final Line2DExpression crossingLine = Line2DExpression.value(coordinateA, coordinateB);

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
		Coordinate coordinate1 = new Coordinate(1, 2);
		Coordinate coordinate2 = new Coordinate(1, 3);
		final Coordinate[] nonCrossingLine = new Coordinate[]{coordinate1, coordinate2};

		Coordinate coordinateA = new Coordinate(3, 3);
		Coordinate coordinateB = new Coordinate(2, 4);
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
		Coordinate coordinate1 = new Coordinate(1, 2);
		Coordinate coordinate2 = new Coordinate(1, 3);
		final LineString nonCrossingLine = geometryFactory.createLineString(new Coordinate[]{coordinate1, coordinate2});

		Coordinate coordinateA = new Coordinate(3, 3);
		Coordinate coordinateB = new Coordinate(2, 4);
		final LineString crossingLine = geometryFactory.createLineString(new Coordinate[]{coordinateA, coordinateB});

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
		Point coordinate1 = geometryFactory.createPoint(new Coordinate(1, 2));
		Point coordinate2 = geometryFactory.createPoint(new Coordinate(1, 3));
		final Point[] nonCrossingLine = new Point[]{coordinate1, coordinate2};

		Point coordinateA = geometryFactory.createPoint(new Coordinate(3, 3));
		Point coordinateB = geometryFactory.createPoint(new Coordinate(2, 4));
		final Point[] crossingLine = new Point[]{coordinateA, coordinateB};

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
		Coordinate coordinate1 = new Coordinate(1, 2);
		Coordinate coordinate2 = new Coordinate(1, 3);
		Coordinate coordinate3 = new Coordinate(5, 3);
		final Line2DExpression nonCrossingLine = Line2DExpression.value(coordinate1, coordinate2, coordinate3);

		Coordinate coordinateA = new Coordinate(3, 3);
		Coordinate coordinateB = new Coordinate(2, 4);
		Coordinate coordinateC = new Coordinate(1, 4);
		final Line2DExpression crossingLine = Line2DExpression.value(coordinateA, coordinateB, coordinateC);
		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(crossingLine).is(Point2DExpression.value(2.5D, 3.5D)));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(nonCrossingLine).is((Point) null));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIntersectionWithCoordArray() throws SQLException {
		final LineTestTable lineTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinate1 = new Coordinate(1, 2);
		Coordinate coordinate2 = new Coordinate(1, 3);
		Coordinate coordinate3 = new Coordinate(5, 3);
		final Coordinate[] nonCrossingLine = new Coordinate[]{coordinate1, coordinate2, coordinate3};

		Coordinate coordinateA = new Coordinate(3, 3);
		Coordinate coordinateB = new Coordinate(2, 4);
		Coordinate coordinateC = new Coordinate(1, 4);
		Coordinate[] crossingLine = new Coordinate[]{coordinateA, coordinateB, coordinateC};
		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(crossingLine).is(Point2DExpression.value(2.5D, 3.5D)));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(nonCrossingLine).is((Point) null));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIntersectionWithLineString() throws SQLException {
		final LineTestTable lineTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinate1 = new Coordinate(1, 2);
		Coordinate coordinate2 = new Coordinate(1, 3);
		Coordinate coordinate3 = new Coordinate(5, 3);
		final Coordinate[] nonCrossingLine = new Coordinate[]{coordinate1, coordinate2, coordinate3};

		Coordinate coordinateA = new Coordinate(3, 3);
		Coordinate coordinateB = new Coordinate(2, 4);
		Coordinate coordinateC = new Coordinate(1, 4);
		Coordinate[] coords = new Coordinate[]{coordinateA, coordinateB, coordinateC};
		LineString crossingLine = geometryFactory.createLineString(coords);
		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(crossingLine).is(Point2DExpression.value(2.5D, 3.5D)));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(nonCrossingLine).is((Point) null));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIntersectionWithMultiPoint2DExpression() throws SQLException {
		final LineTestTable lineTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinate1 = new Coordinate(1, 2);
		Coordinate coordinate2 = new Coordinate(1, 3);
		Coordinate coordinate3 = new Coordinate(5, 3);
		final MultiPoint2DExpression nonCrossingLine = MultiPoint2DExpression.value(coordinate1, coordinate2, coordinate3);

		Coordinate coordinateA = new Coordinate(3, 3);
		Coordinate coordinateB = new Coordinate(2, 4);
		Coordinate coordinateC = new Coordinate(1, 4);
		MultiPoint2DExpression crossingLine = MultiPoint2DExpression.value(coordinateA, coordinateB, coordinateC);
		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(crossingLine).is(Point2DExpression.value(2.5D, 3.5D)));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(nonCrossingLine).is((Point) null));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIntersectionWithPointArray() throws SQLException {
		final LineTestTable lineTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		Point coordinate1 = geometryFactory.createPoint(new Coordinate(1, 2));
		Point coordinate2 = geometryFactory.createPoint(new Coordinate(1, 3));
		Point coordinate3 = geometryFactory.createPoint(new Coordinate(5, 3));
		final Point[] nonCrossingLine = new Point[]{coordinate1, coordinate2, coordinate3};

		Point coordinateA = geometryFactory.createPoint(new Coordinate(3, 3));
		Point coordinateB = geometryFactory.createPoint(new Coordinate(2, 4));
		Point coordinateC = geometryFactory.createPoint(new Coordinate(1, 4));
		Point[] crossingLine = new Point[]{coordinateA, coordinateB, coordinateC};
		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(crossingLine).is(Point2DExpression.value(2.5D, 3.5D)));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionWith(nonCrossingLine).is((Point) null));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIntersectionPoints() throws SQLException {
		final LineTestTable lineTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		Coordinate coordinate1 = new Coordinate(1, 2);
		Coordinate coordinate2 = new Coordinate(1, 3);
		Coordinate coordinate3 = new Coordinate(5, 3);
		final Line2DExpression nonCrossingLine = Line2DExpression.value(coordinate1, coordinate2, coordinate3);

		Coordinate coordinateA = new Coordinate(3, 3);
		Coordinate coordinateB = new Coordinate(2, 4);
		Coordinate coordinateC = new Coordinate(1, 4);
		final Line2DExpression crossingLine = Line2DExpression.value(coordinateA, coordinateB, coordinateC);
		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionPoints(crossingLine).is(MultiPoint2DExpression.value(new Coordinate(2.5D, 3.5D))));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);

		Assert.assertThat(allRows.size(), is(2));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).intersectionPoints(nonCrossingLine).is((MultiPoint) null));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
	}

	public static class BoundingBoxTest extends LineTestTable {

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
