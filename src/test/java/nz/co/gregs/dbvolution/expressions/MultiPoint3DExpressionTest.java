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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.*;
import nz.co.gregs.dbvolution.annotations.*;
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
public class MultiPoint3DExpressionTest extends AbstractTest {

	final GeometryFactory3D geometryFactory = new GeometryFactory3D();

	public MultiPoint3DExpressionTest(Object testIterationName, DBDatabase db) throws SQLException {
		super(testIterationName, db);
		MultiPoint3DTestTable lineTestTable = new MultiPoint3DTestTable();

		db.preventDroppingOfTables(false);
		db.dropTableNoExceptions(lineTestTable);
		db.createTable(lineTestTable);

		Coordinate coordinate1 = new Coordinate(2, 3, 0);
		Coordinate coordinate2 = new Coordinate(3, 4, 1);
		Coordinate coordinate3 = new Coordinate(4, 5, 2);
		lineTestTable.multipoint.setValue(coordinate1, coordinate2);
		db.insert(lineTestTable);

		lineTestTable = new MultiPoint3DTestTable();
		lineTestTable.multipoint.setValue(geometryFactory.createMultiPointZ(new Coordinate[]{coordinate1, coordinate2, coordinate3}));
		db.insert(lineTestTable);

		lineTestTable = new MultiPoint3DTestTable();
		lineTestTable.multipoint.setValue(geometryFactory.createPointZ(coordinate2), geometryFactory.createPointZ(coordinate3));
		db.insert(lineTestTable);
	}

	public static class MultiPoint3DTestTable extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		public DBInteger line_id = new DBInteger();

		@DBColumn("multipoint_col")
		public DBMultiPoint3D multipoint = new DBMultiPoint3D();
		
		@DBColumn
		public DBString asText = new DBString(this.column(this.multipoint).stringResult());
//		
//		@DBColumn
//		public DBString asPolygon = new DBString(this.column(this.multipoint).polygon3DResult().stringResult());
	}

	@Test
	public void testValue() throws SQLException {
		MultiPointZ mpoint = geometryFactory.createMultiPointZ(new Coordinate[]{new Coordinate(2.0, 3.0, 4.0), new Coordinate(3.0, 4.0, 5.0)});
		final MultiPoint3DTestTable pointTestTable = new MultiPoint3DTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(MultiPoint3DExpression.value(mpoint).is(pointTestTable.column(pointTestTable.multipoint)));
		List<MultiPoint3DTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
		Assert.assertThat(allRows.get(0).multipoint.multiPointZValue(), is(mpoint));
	}

	@Test
	public void testValueWithPointArray() throws SQLException {
		Coordinate[] coords = new Coordinate[]{new Coordinate(2.0, 3.0, 4.0), new Coordinate(3.0, 4.0, 5.0)};
		MultiPointZ mpoint = geometryFactory.createMultiPointZ(coords);
		List<PointZ> pointList = new ArrayList<PointZ>();
		for (Coordinate coord : coords) {
			PointZ point = geometryFactory.createPointZ(coord);
			pointList.add(point);
		}
		final MultiPoint3DTestTable pointTestTable = new MultiPoint3DTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(MultiPoint3DExpression.value(pointList.toArray(new PointZ[]{}))
				.is(pointTestTable.column(pointTestTable.multipoint)));
		List<MultiPoint3DTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
		Assert.assertThat(allRows.get(0).multipoint.multiPointZValue(), is(mpoint));
	}


	@Test
	public void testValueWithMultiPoint3DResult() throws SQLException {
		Coordinate[] coords = new Coordinate[]{new Coordinate(2.0, 3.0, 4.0), new Coordinate(3.0, 4.0, 5.0)};
		MultiPointZ mpoint = geometryFactory.createMultiPointZ(coords);
		List<PointZ> pointList = new ArrayList<PointZ>();
		for (Coordinate coord : coords) {
			PointZ point = geometryFactory.createPointZ(coord);
			pointList.add(point);
		}
		final MultiPoint3DTestTable pointTestTable = new MultiPoint3DTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(MultiPoint3DExpression.value(MultiPoint3DExpression.value(pointList.toArray(new PointZ[]{})))
				.is(pointTestTable.column(pointTestTable.multipoint)));
		List<MultiPoint3DTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
		Assert.assertThat(allRows.get(0).multipoint.multiPointZValue(), is(mpoint));
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
		final MultiPoint3DTestTable pointTestTable = new MultiPoint3DTestTable();
		Line3DExpression instance = new Line3DExpression(pointTestTable.column(pointTestTable.multipoint).line3DResult());
		Set<DBRow> result = instance.getTablesInvolved();
		Assert.assertThat(result.size(), is(1));
		DBRow[] resultArray = result.toArray(new DBRow[]{});
		DBRow aRow = resultArray[0];
		if (!(aRow instanceof MultiPoint3DTestTable)) {
			fail("Set should include LineTestTable");
		}
	}

	@Test
	public void testIsPurelyFunctional() {
		MultiPoint3DExpression instance = new MultiPoint3DExpression();
		boolean result = instance.isPurelyFunctional();
		Assert.assertThat(result, is(true));

		final MultiPoint3DTestTable pointTestTable = new MultiPoint3DTestTable();
		instance = new MultiPoint3DExpression(pointTestTable.column(pointTestTable.multipoint));
		Assert.assertThat(instance.isPurelyFunctional(), is(false));
	}

	@Test
	public void testGetIncludesNull() {
		Line3DExpression instance = new Line3DExpression((LineStringZ) null);
		Assert.assertThat(instance.getIncludesNull(), is(true));

		final MultiPoint3DTestTable pointTestTable = new MultiPoint3DTestTable();
		instance = new Line3DExpression(pointTestTable.column(pointTestTable.multipoint).line3DResult());
		Assert.assertThat(instance.getIncludesNull(), is(false));
	}

	@Test
	public void testStringResult() throws SQLException {
		MultiPointZ line = geometryFactory.createMultiPointZ(new Coordinate[]{new Coordinate(2.0, 3.0, 4.0), new Coordinate(3.0, 4.0, 5.0)});
		final MultiPoint3DTestTable pointTestTable = new MultiPoint3DTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(MultiPoint3DExpression.value(line).stringResult().is(pointTestTable.column(pointTestTable.multipoint).stringResult()));
		List<MultiPoint3DTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
	}

	@Test
	public void testLine3DResult() throws SQLException {
		LineStringZ line = geometryFactory.createLineStringZ(new Coordinate[]{new Coordinate(2.0, 3.0, 4.0), new Coordinate(3.0, 4.0, 5.0)});
		final MultiPoint3DTestTable pointTestTable = new MultiPoint3DTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.setBlankQueryAllowed(true);
		List<MultiPoint3DTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		dbQuery.addCondition(Line3DExpression.value(line).stringResult().is(pointTestTable.column(pointTestTable.multipoint).line3DResult().stringResult()));
		allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
	}

	@Test
	public void testIs_MultiPoint() throws SQLException {
		MultiPointZ mpoint = geometryFactory.createMultiPointZ(new Coordinate[]{new Coordinate(2.0, 3.0, 4.0), new Coordinate(3.0, 4.0, 5.0)});
		final MultiPoint3DTestTable pointTestTable = new MultiPoint3DTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(pointTestTable.column(pointTestTable.multipoint).is(mpoint));
		List<MultiPoint3DTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
	}

	@Test
	public void testIs_MultiPoint3DResult() throws SQLException {
		MultiPointZ mpoint = geometryFactory.createMultiPointZ(new Coordinate[]{new Coordinate(2.0, 3.0, 4.0), new Coordinate(3.0, 4.0, 5.0)});
		final MultiPoint3DTestTable multiPointRow = new MultiPoint3DTestTable();
		DBQuery dbQuery = database.getDBQuery(multiPointRow);
		dbQuery.addCondition(MultiPoint3DExpression.value(mpoint).is(multiPointRow.column(multiPointRow.multipoint)));
		List<MultiPoint3DTestTable> allRows = dbQuery.getAllInstancesOf(multiPointRow);
		
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
	}

	@Test
	public void testIsNot_MultiPoint3DResult() throws SQLException {
		MultiPointZ mpoint = geometryFactory.createMultiPointZ(new Coordinate[]{new Coordinate(2.0, 3.0, 4.0), new Coordinate(3.0, 4.0, 5.0)});
		final MultiPoint3DTestTable multiPointRow = new MultiPoint3DTestTable();
		DBQuery dbQuery = database.getDBQuery(multiPointRow);
		dbQuery.addCondition(MultiPoint3DExpression.value(mpoint).isNot(multiPointRow.column(multiPointRow.multipoint)));
		List<MultiPoint3DTestTable> allRows = dbQuery.getAllInstancesOf(multiPointRow);
		
		Assert.assertThat(allRows.size(), is(2));
		Assert.assertThat(allRows.get(0).line_id.intValue(), isOneOf(2,3));
	}

	@Test
	public void testIsNot_MultiPoint3D() throws SQLException {
		MultiPointZ mpoint = geometryFactory.createMultiPointZ(new Coordinate[]{new Coordinate(2.0, 3.0, 4.0), new Coordinate(3.0, 4.0, 5.0)});
		final MultiPoint3DTestTable multiPointRow = new MultiPoint3DTestTable();
		DBQuery dbQuery = database.getDBQuery(multiPointRow);
		dbQuery.addCondition(multiPointRow.column(multiPointRow.multipoint).isNot(mpoint));
		List<MultiPoint3DTestTable> allRows = dbQuery.getAllInstancesOf(multiPointRow);
		
		Assert.assertThat(allRows.size(), is(2));
		Assert.assertThat(allRows.get(0).line_id.intValue(), isOneOf(2,3));
	}

	@Test
	public void testNumberOfPoints() throws SQLException {
		final MultiPoint3DTestTable multiPointRow = new MultiPoint3DTestTable();
		DBQuery dbQuery = database.getDBQuery(multiPointRow);
		dbQuery.addCondition(multiPointRow.column(multiPointRow.multipoint).numberOfPoints().is(3));
		List<MultiPoint3DTestTable> allRows = dbQuery.getAllInstancesOf(multiPointRow);
		
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(2));
	}

	@Test
	public void testPointAtIndex_int() throws SQLException {
		PointZ point = geometryFactory.createPointZ(new Coordinate(3, 4, 5));
		final MultiPoint3DTestTable multiPointRow = new MultiPoint3DTestTable();
		DBQuery dbQuery = database.getDBQuery(multiPointRow);
		dbQuery.addCondition(multiPointRow.column(multiPointRow.multipoint).getPointAtIndexZeroBased(1).is(point));
		dbQuery.setSortOrder(multiPointRow.column(multiPointRow.line_id));
		List<MultiPoint3DTestTable> allRows = dbQuery.getAllInstancesOf(multiPointRow);
		
		Assert.assertThat(allRows.size(), is(2));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
		Assert.assertThat(allRows.get(1).line_id.intValue(), is(2));
	}

	@Test
	public void testPointAtIndex_long() throws SQLException {
		final MultiPoint3DTestTable multiPointRow = new MultiPoint3DTestTable();
		DBQuery dbQuery = database.getDBQuery(multiPointRow);
		dbQuery.addCondition(multiPointRow.column(multiPointRow.multipoint).getPointAtIndexZeroBased(0L).getX().is(2));
		List<MultiPoint3DTestTable> allRows = dbQuery.getAllInstancesOf(multiPointRow);
		
		Assert.assertThat(allRows.size(), is(2));
		Assert.assertThat(allRows.get(0).line_id.intValue(), isOneOf(1,2));
	}

	@Test
	public void testHasMagnitude() throws SQLException {
		final MultiPoint3DTestTable multiPointRow = new MultiPoint3DTestTable();
		DBQuery dbQuery = database.getDBQuery(multiPointRow);
		dbQuery.addCondition(multiPointRow.column(multiPointRow.multipoint).hasMagnitude().not());
		List<MultiPoint3DTestTable> allRows = dbQuery.getAllInstancesOf(multiPointRow);
		
		Assert.assertThat(allRows.size(), is(3));
		Assert.assertThat(allRows.get(0).line_id.intValue(), isOneOf(1,2,3));
	}

	@Test
	public void testMagnitude() throws SQLException {
		final MultiPoint3DTestTable multiPointRow = new MultiPoint3DTestTable();
		DBQuery dbQuery = database.getDBQuery(multiPointRow);
		dbQuery.addCondition(multiPointRow.column(multiPointRow.multipoint).magnitude().isNull());
		List<MultiPoint3DTestTable> allRows = dbQuery.getAllInstancesOf(multiPointRow);
		
		Assert.assertThat(allRows.size(), is(3));
		Assert.assertThat(allRows.get(0).line_id.intValue(), isOneOf(1,2,3));
	}

	@Test
	public void testSpatialDimensions() throws SQLException {
		final MultiPoint3DTestTable multiPointRow = new MultiPoint3DTestTable();
		DBQuery dbQuery = database.getDBQuery(multiPointRow);
		dbQuery.addCondition(multiPointRow.column(multiPointRow.multipoint).spatialDimensions().is(2));
		List<MultiPoint3DTestTable> allRows = dbQuery.getAllInstancesOf(multiPointRow);
		
		Assert.assertThat(allRows.size(), is(3));
		Assert.assertThat(allRows.get(0).line_id.intValue(), isOneOf(1,2,3));
	}

	@Test
	public void testGetMaxX() throws SQLException {
		final MultiPoint3DTestTable lineTestTable = new MultiPoint3DTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.setSortOrder(lineTestTable.column(lineTestTable.line_id));
		dbQuery.addCondition(lineTestTable.column(lineTestTable.multipoint).maxX().is(4));
		List<MultiPoint3DTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		
		Assert.assertThat(allRows.size(), is(2));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(2));
		Assert.assertThat(allRows.get(1).line_id.intValue(), is(3));
	}

	@Test
	public void testGetMinX() throws SQLException {
		final MultiPoint3DTestTable lineTestTable = new MultiPoint3DTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.multipoint).minX().is(3));
		List<MultiPoint3DTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(3));
	}

	@Test
	public void testGetMaxY() throws SQLException {
		final MultiPoint3DTestTable lineTestTable = new MultiPoint3DTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.multipoint).maxY().is(4));
		List<MultiPoint3DTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
	}

	@Test
	public void testGetMinY() throws SQLException {
		final MultiPoint3DTestTable lineTestTable = new MultiPoint3DTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.multipoint).minY().is(3));
		List<MultiPoint3DTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		
		Assert.assertThat(allRows.size(), is(2));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
		Assert.assertThat(allRows.get(1).line_id.intValue(), is(2));
	}

	@Test
	public void testDimension() throws SQLException {
		final MultiPoint3DTestTable lineTestTable = new MultiPoint3DTestTable();
		DBQuery dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.multipoint).measurableDimensions().is(1));
		List<MultiPoint3DTestTable> allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(0));
		dbQuery = database.getDBQuery(lineTestTable);
		dbQuery.addCondition(lineTestTable.column(lineTestTable.multipoint).measurableDimensions().is(0));
		allRows = dbQuery.getAllInstancesOf(lineTestTable);
		Assert.assertThat(allRows.size(), is(3));
	}

	public static class BoundingBoxTest extends MultiPoint3DTestTable {

		private static final long serialVersionUID = 1L;

		@DBColumn
		public DBString stringLine = new DBString(this.column(this.multipoint).stringResult().substringBetween("(", " "));
		@DBColumn
		public DBPolygon3D boundingBox = new DBPolygon3D(this.column(this.multipoint).boundingBox());

	}

	@Test
	public void testBoundingBox() throws SQLException {
		final BoundingBoxTest pointTestTable = new BoundingBoxTest();
		DBQuery dbQuery = database.getDBQuery(pointTestTable).setBlankQueryAllowed(true);
		dbQuery.addCondition(pointTestTable.column(pointTestTable.multipoint).maxY().is(4));
		List<BoundingBoxTest> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).line_id.intValue(), is(1));
		final String boundingText = allRows.get(0).boundingBox.jtsPolygonValue().toText();
		Assert.assertThat(boundingText, is("POLYGON ((2 3, 3 3, 3 4, 2 4, 2 3))"));
	}

}
