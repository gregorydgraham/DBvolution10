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

import com.vividsolutions.jts.geom.*;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.*;
import nz.co.gregs.dbvolution.annotations.*;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.datatypes.spatial2D.*;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author gregorygraham
 */
public class MultiPoint2DExpressionTest extends AbstractTest {

	final GeometryFactory geometryFactory = new GeometryFactory();

	public MultiPoint2DExpressionTest(Object testIterationName, DBDatabase db) throws SQLException {
		super(testIterationName, db);
		MultiPoint2DTestTable lineTestTable = new MultiPoint2DTestTable();

		db.preventDroppingOfTables(false);
		db.dropTableNoExceptions(lineTestTable);
		db.createTable(lineTestTable);

		Coordinate coordinate1 = new Coordinate(2, 3);
		Coordinate coordinate2 = new Coordinate(3, 4);
		Coordinate coordinate3 = new Coordinate(4, 5);
		lineTestTable.multipoint.setValue(coordinate1, coordinate2);
		db.insert(lineTestTable);

		lineTestTable = new MultiPoint2DTestTable();
		lineTestTable.multipoint.setValue(geometryFactory.createMultiPoint(new Coordinate[]{coordinate1, coordinate2, coordinate3}));
		db.insert(lineTestTable);

		lineTestTable = new MultiPoint2DTestTable();
		lineTestTable.multipoint.setValue(geometryFactory.createPoint(coordinate2), geometryFactory.createPoint(coordinate3));
		db.insert(lineTestTable);
	}

	public static class MultiPoint2DTestTable extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		public DBInteger line_id = new DBInteger();

		@DBColumn("multipoint_col")
		public DBMultiPoint2D multipoint = new DBMultiPoint2D();
		
		@DBColumn
		public DBString asText = new DBString(this.column(this.multipoint).stringResult());
//		
//		@DBColumn
//		public DBString asPolygon = new DBString(this.column(this.multipoint).polygon2DResult().stringResult());
	}

	@Test
	public void testValue() throws SQLException {
		System.out.println("value");
		MultiPoint point = geometryFactory.createMultiPoint(new Coordinate[]{new Coordinate(2.0, 3.0), new Coordinate(3.0, 4.0)});
		final MultiPoint2DTestTable pointTestTable = new MultiPoint2DTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(MultiPoint2DExpression.value(point).is(pointTestTable.column(pointTestTable.multipoint)));
		List<MultiPoint2DTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
	}

	@Test
	public void testGetQueryableDatatypeForExpressionValue() {
		System.out.println("getQueryableDatatypeForExpressionValue");
		Line2DExpression instance = new Line2DExpression();
		DBLine2D result = instance.getQueryableDatatypeForExpressionValue();
		assertEquals(DBLine2D.class, result.getClass());
	}

	@Test
	public void testCopy() {
		System.out.println("copy");
		Line2DExpression instance = new Line2DExpression();
		Line2DExpression result = instance.copy();
		assertEquals(instance, result);
	}

	@Test
	public void testIsAggregator() {
		System.out.println("isAggregator");
		Line2DExpression instance = new Line2DExpression();
		boolean expResult = false;
		boolean result = instance.isAggregator();
		assertEquals(expResult, result);
	}

	@Test
	public void testGetTablesInvolved() {
		System.out.println("getTablesInvolved");
		final MultiPoint2DTestTable pointTestTable = new MultiPoint2DTestTable();
		Line2DExpression instance = new Line2DExpression(pointTestTable.column(pointTestTable.multipoint));
		Set<DBRow> result = instance.getTablesInvolved();
		Assert.assertThat(result.size(), is(1));
		DBRow[] resultArray = result.toArray(new DBRow[]{});
		DBRow aRow = resultArray[0];
		if (!(aRow instanceof MultiPoint2DTestTable)) {
			fail("Set should include LineTestTable");
		}
	}

	@Test
	public void testIsPurelyFunctional() {
		System.out.println("isPurelyFunctional");
		MultiPoint2DExpression instance = new MultiPoint2DExpression();
		boolean result = instance.isPurelyFunctional();
		Assert.assertThat(result, is(true));

		final MultiPoint2DTestTable pointTestTable = new MultiPoint2DTestTable();
		instance = new MultiPoint2DExpression(pointTestTable.column(pointTestTable.multipoint));
		Assert.assertThat(instance.isPurelyFunctional(), is(false));
	}

	@Test
	public void testGetIncludesNull() {
		System.out.println("getIncludesNull");
		Line2DExpression instance = new Line2DExpression((LineString) null);
		Assert.assertThat(instance.getIncludesNull(), is(true));

		final MultiPoint2DTestTable pointTestTable = new MultiPoint2DTestTable();
		instance = new Line2DExpression(pointTestTable.column(pointTestTable.multipoint));
		Assert.assertThat(instance.getIncludesNull(), is(false));
	}

	@Test
	public void testStringResult() throws SQLException {
		System.out.println("stringResult");
		MultiPoint line = geometryFactory.createMultiPoint(new Coordinate[]{new Coordinate(2.0, 3.0), new Coordinate(3.0, 4.0)});
		final MultiPoint2DTestTable pointTestTable = new MultiPoint2DTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(MultiPoint2DExpression.value(line).stringResult().is(pointTestTable.column(pointTestTable.multipoint).stringResult()));
		List<MultiPoint2DTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
	}

	@Test
	public void testLine2DResult() throws SQLException {
		System.out.println("stringResult");
		LineString line = geometryFactory.createLineString(new Coordinate[]{new Coordinate(2.0, 3.0), new Coordinate(3.0, 4.0)});
		final MultiPoint2DTestTable pointTestTable = new MultiPoint2DTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.setBlankQueryAllowed(true);
		List<MultiPoint2DTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		database.print(allRows);
		dbQuery.addCondition(Line2DExpression.value(line).stringResult().is(pointTestTable.column(pointTestTable.multipoint).line2DResult().stringResult()));
		allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
	}

//	@Test
//	public void testPolygon2DResult() throws SQLException {
//		System.out.println("stringResult");
//		Polygon line = geometryFactory.createPolygon(new Coordinate[]{new Coordinate(2.0, 3.0), new Coordinate(3.0, 4.0), new Coordinate(4.0, 5.0), new Coordinate(2.0, 3.0)});
//		final MultiPoint2DTestTable pointTestTable = new MultiPoint2DTestTable();
//		DBQuery dbQuery = database.getDBQuery(pointTestTable);
//		dbQuery.setBlankQueryAllowed(true);
//		List<MultiPoint2DTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
//		database.print(allRows);
//		dbQuery.addCondition(Polygon2DExpression.value(line).is(pointTestTable.column(pointTestTable.multipoint).polygon2DResult()));
//		allRows = dbQuery.getAllInstancesOf(pointTestTable);
//		Assert.assertThat(allRows.size(), is(1));
//		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
//	}

	@Test
	public void testIs_MultiPoint() throws SQLException {
		System.out.println("is");
		MultiPoint mpoint = geometryFactory.createMultiPoint(new Coordinate[]{new Coordinate(2.0, 3.0), new Coordinate(3.0, 4.0)});
		final MultiPoint2DTestTable pointTestTable = new MultiPoint2DTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(pointTestTable.column(pointTestTable.multipoint).is(mpoint));
		List<MultiPoint2DTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
	}

	@Test
	public void testIs_MultiPoint2DResult() throws SQLException {
		System.out.println("is");
		MultiPoint mpoint = geometryFactory.createMultiPoint(new Coordinate[]{new Coordinate(2.0, 3.0), new Coordinate(3.0, 4.0)});
		final MultiPoint2DTestTable multiPointRow = new MultiPoint2DTestTable();
		DBQuery dbQuery = database.getDBQuery(multiPointRow);
		dbQuery.addCondition(MultiPoint2DExpression.value(mpoint).is(multiPointRow.column(multiPointRow.multipoint)));
		List<MultiPoint2DTestTable> allRows = dbQuery.getAllInstancesOf(multiPointRow);
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
	}

	@Test
	public void testNumberOfPoints() throws SQLException {
		System.out.println("is");
		final MultiPoint2DTestTable multiPointRow = new MultiPoint2DTestTable();
		DBQuery dbQuery = database.getDBQuery(multiPointRow);
		dbQuery.addCondition(multiPointRow.column(multiPointRow.multipoint).numberOfPoints().is(3));
		List<MultiPoint2DTestTable> allRows = dbQuery.getAllInstancesOf(multiPointRow);
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(2));
	}

	@Test
	public void testPointAtIndex() throws SQLException {
		System.out.println("is");
		Point point = geometryFactory.createPoint(new Coordinate(3, 4));
		final MultiPoint2DTestTable multiPointRow = new MultiPoint2DTestTable();
		DBQuery dbQuery = database.getDBQuery(multiPointRow);
		dbQuery.addCondition(multiPointRow.column(multiPointRow.multipoint).getPointAtIndex(2).is(point));
		dbQuery.setSortOrder(multiPointRow.column(multiPointRow.line_id));
		List<MultiPoint2DTestTable> allRows = dbQuery.getAllInstancesOf(multiPointRow);
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(2));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
		Assert.assertThat(allRows.get(1).line_id.intValue(), is(2));
	}

	@Test
	public void testGetMaxX() throws SQLException {
		System.out.println("getMaxX");
//		LineString multipoint = geometryFactory.createLineString(new Coordinate[]{new Coordinate(2.0, 3.0), new Coordinate(3.0, 4.0)});
		final MultiPoint2DTestTable lineTestTable = new MultiPoint2DTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.setSortOrder(lineTestTable.column(lineTestTable.line_id));
		dbQuery.addCondition(lineTestTable.column(lineTestTable.multipoint).getMaxX().is(4));
		List<MultiPoint2DTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(2));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(2));
		Assert.assertThat(allRows.get(1).line_id.intValue(), is(3));
	}

	@Test
	public void testGetMinX() throws SQLException {
		System.out.println("getMaxX");
//		LineString multipoint = geometryFactory.createLineString(new Coordinate[]{new Coordinate(2.0, 3.0), new Coordinate(3.0, 4.0)});
		final MultiPoint2DTestTable lineTestTable = new MultiPoint2DTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.multipoint).getMinX().is(3));
		List<MultiPoint2DTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(3));
	}

	@Test
	public void testGetMaxY() throws SQLException {
		System.out.println("getMaxY");
//		LineString multipoint = geometryFactory.createLineString(new Coordinate[]{new Coordinate(2.0, 3.0), new Coordinate(3.0, 4.0)});
		final MultiPoint2DTestTable lineTestTable = new MultiPoint2DTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.multipoint).getMaxY().is(4));
		List<MultiPoint2DTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
	}

	@Test
	public void testGetMinY() throws SQLException {
		System.out.println("getMinY");
//		LineString multipoint = geometryFactory.createLineString(new Coordinate[]{new Coordinate(2.0, 3.0), new Coordinate(3.0, 4.0)});
		final MultiPoint2DTestTable lineTestTable = new MultiPoint2DTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.multipoint).getMinY().is(3));
		List<MultiPoint2DTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(2));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
		Assert.assertThat(allRows.get(1).line_id.intValue(), is(2));
	}

	@Test
	public void testDimension() throws SQLException {
		System.out.println("dimension");
		final MultiPoint2DTestTable lineTestTable = new MultiPoint2DTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.multipoint).dimension().is(1));
		List<MultiPoint2DTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.multipoint).dimension().is(0));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(3));
	}

	public static class BoundingBoxTest extends MultiPoint2DTestTable {

		private static final long serialVersionUID = 1L;

		@DBColumn
		public DBString stringLine = new DBString(this.column(this.multipoint).stringResult().substringBetween("(", " "));
//		@DBColumn
//		public DBNumber getX = new DBNumber(this.column(this.multipoint).getMaxX());
//		@DBColumn
//		public DBNumber getY = new DBNumber(this.column(this.multipoint).getMaxY());
		@DBColumn
		public DBPolygon2D boundingBox = new DBPolygon2D(this.column(this.multipoint).boundingBox());
//		@DBColumn
//		public DBBoolean getXis2 = new DBBoolean(this.column(this.multipoint).getMaxX().is(2));

	}

	@Test
	public void testBoundingBox() throws SQLException {
		System.out.println("boundingBox");
		final BoundingBoxTest pointTestTable = new BoundingBoxTest();
		DBQuery dbQuery = database.getDBQuery(pointTestTable).setBlankQueryAllowed(true);
		dbQuery.addCondition(pointTestTable.column(pointTestTable.multipoint).getMaxY().is(4));
		List<BoundingBoxTest> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
		final String boundingText = allRows.get(0).boundingBox.jtsPolygonValue().toText();
		Assert.assertThat(boundingText, is("POLYGON ((2 3, 3 3, 3 4, 2 4, 2 3))"));
	}

}
