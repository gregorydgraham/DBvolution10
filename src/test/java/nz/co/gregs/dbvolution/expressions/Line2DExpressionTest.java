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
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBGeometry2D;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBLine2D;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.*;

/**
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
		public DBInteger point_id = new DBInteger();

		@DBColumn("point_col")
		public DBLine2D line = new DBLine2D();
	}

	@Test
	public void testValue() throws SQLException {
		System.out.println("value");
		LineString point = geometryFactory.createLineString(new Coordinate[]{new Coordinate(2.0, 3.0), new Coordinate(3.0, 4.0)});
		final LineTestTable pointTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(Line2DExpression.value(point).is(pointTestTable.column(pointTestTable.line)));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).point_id.intValue(), is(1));
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
		System.out.println("isPurelyFunctional");
		Line2DExpression instance = new Line2DExpression();
		boolean result = instance.isPurelyFunctional();
		Assert.assertThat(result, is(true));

		final LineTestTable pointTestTable = new LineTestTable();
		instance = new Line2DExpression(pointTestTable.column(pointTestTable.line));
		Assert.assertThat(instance.isPurelyFunctional(), is(false));
	}

	@Test
	public void testGetIncludesNull() {
		System.out.println("getIncludesNull");
		Line2DExpression instance = new Line2DExpression((LineString) null);
		Assert.assertThat(instance.getIncludesNull(), is(true));

		final LineTestTable pointTestTable = new LineTestTable();
		instance = new Line2DExpression(pointTestTable.column(pointTestTable.line));
		Assert.assertThat(instance.getIncludesNull(), is(false));
	}

	@Test
	public void testStringResult() throws SQLException {
		System.out.println("stringResult");
		LineString line = geometryFactory.createLineString(new Coordinate[]{new Coordinate(2.0, 3.0), new Coordinate(3.0, 4.0)});
		final LineTestTable pointTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(Line2DExpression.value(line).stringResult().is(pointTestTable.column(pointTestTable.line).stringResult()));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).point_id.intValue(), is(1));
	}

	@Test
	public void testIs_Line() throws SQLException {
		System.out.println("is");
		LineString line = geometryFactory.createLineString(new Coordinate[]{new Coordinate(2.0, 3.0), new Coordinate(3.0, 4.0)});
		final LineTestTable pointTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(pointTestTable.column(pointTestTable.line).is(line));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).point_id.intValue(), is(1));
	}

	@Test
	public void testIs_Line2DResult() throws SQLException {
		System.out.println("is");
		LineString line = geometryFactory.createLineString(new Coordinate[]{new Coordinate(2.0, 3.0), new Coordinate(3.0, 4.0)});
		final LineTestTable lineTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(Line2DExpression.value(line).is(lineTestTable.column(lineTestTable.line)));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).point_id.intValue(), is(1));
	}

//	@Test
//	public void testGetX() throws SQLException {
//		System.out.println("getX");
//		final LineTestTable pointTestTable = new LineTestTable();
//		DBQuery dbQuery = database.getDBQuery(pointTestTable);
//		dbQuery.addCondition(pointTestTable.column(pointTestTable.line).getX().is(2));
//		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
//		Assert.assertThat(allRows.size(), is(1));
//		Assert.assertThat(allRows.get(0).point_id.intValue(), is(1));
//
//		dbQuery = database.getDBQuery(pointTestTable);
//		dbQuery.addCondition(pointTestTable.column(pointTestTable.line).getX().is(4));
//		allRows = dbQuery.getAllInstancesOf(pointTestTable);
//		Assert.assertThat(allRows.size(), is(1));
//		Assert.assertThat(allRows.get(0).point_id.intValue(), is(2));
//	}
//
//	@Test
//	public void testGetY() throws SQLException {
//		System.out.println("getY");
//		final LineTestTable pointTestTable = new LineTestTable();
//		DBQuery dbQuery = database.getDBQuery(pointTestTable);
//		dbQuery.addCondition(pointTestTable.column(pointTestTable.line).getY().is(3));
//		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
//		Assert.assertThat(allRows.size(), is(1));
//		Assert.assertThat(allRows.get(0).point_id.intValue(), is(1));
//
//		dbQuery = database.getDBQuery(pointTestTable);
//		dbQuery.addCondition(pointTestTable.column(pointTestTable.line).getY().is(6));
//		allRows = dbQuery.getAllInstancesOf(pointTestTable);
//		Assert.assertThat(allRows.size(), is(1));
//		Assert.assertThat(allRows.get(0).point_id.intValue(), is(2));
//	}

	@Test
	public void testDimension() throws SQLException {
		System.out.println("dimension");
		final LineTestTable lineTestTable = new LineTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).dimension().is(0));
		List<LineTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.line).dimension().is(1));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(3));
	}

//	public static class BoundingBoxTest extends LineTestTable {
//
//		private static final long serialVersionUID = 1L;
//
//		@DBColumn
//		public DBString stringLine = new DBString(this.column(this.line).stringResult().substringBetween("(", " "));
//		@DBColumn
//		public DBNumber getX = new DBNumber(this.column(this.line).getX());
//		@DBColumn
//		public DBNumber getY = new DBNumber(this.column(this.line).getY());
//		@DBColumn
//		public DBGeometry2D boundingBox = new DBGeometry2D(this.column(this.line).boundingBox());
//		@DBColumn
//		public DBBoolean getXis2 = new DBBoolean(this.column(this.line).getX().is(2));
//
//	}
//
//	@Test
//	public void testBoundingBox() throws SQLException {
//		System.out.println("boundingBox");
//		final BoundingBoxTest pointTestTable = new BoundingBoxTest();
//		DBQuery dbQuery = database.getDBQuery(pointTestTable).setBlankQueryAllowed(true);
//		dbQuery.addCondition(pointTestTable.column(pointTestTable.line).getY().is(3));
//		List<BoundingBoxTest> allRows = dbQuery.getAllInstancesOf(pointTestTable);
//		database.print(allRows);
//		Assert.assertThat(allRows.size(), is(1));
//		Assert.assertThat(allRows.get(0).point_id.intValue(), is(1));
//		final String boundingText = allRows.get(0).boundingBox.getGeometryValue().toText();
//		String[] splits = boundingText.split("[^-0-9.]+");
//		int numbersTested = 0;
//		for (String split : splits) {
//			System.out.println("SPLIT: " + split);
//			if (split.length() > 0) {
//				Assert.assertThat(Math.round(Double.parseDouble(split) * 1000) / 1000.0, isOneOf(2.0, 3.0));
//				numbersTested++;
//			}
//		}
//		Assert.assertThat(numbersTested, is(10));
//	}

}
