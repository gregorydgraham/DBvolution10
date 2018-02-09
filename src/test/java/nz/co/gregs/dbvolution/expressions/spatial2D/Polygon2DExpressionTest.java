/*
 * Copyright 2018 gregorygraham.
 *
 * This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. 
 * To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/4.0/ 
 * or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.
 * 
 * You are free to:
 *     Share - copy and redistribute the material in any medium or format
 *     Adapt - remix, transform, and build upon the material
 * 
 *     The licensor cannot revoke these freedoms as long as you follow the license terms.               
 *     Under the following terms:
 *                 
 *         Attribution - 
 *             You must give appropriate credit, provide a link to the license, and indicate if changes were made. 
 *             You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.
 *         NonCommercial - 
 *             You may not use the material for commercial purposes.
 *         ShareAlike - 
 *             If you remix, transform, or build upon the material, 
 *             you must distribute your contributions under the same license as the original.
 *         No additional restrictions - 
 *             You may not apply legal terms or technological measures that legally restrict others from doing anything the 
 *             license permits.
 * 
 * Check the Creative Commons website for any details, legalese, and updates.
 */
package nz.co.gregs.dbvolution.expressions.spatial2D;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBPolygon2D;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author gregorygraham
 */
public class Polygon2DExpressionTest extends AbstractTest {

	final GeometryFactory geometryFactory = new GeometryFactory();

	public Polygon2DExpressionTest(Object testIterationName, DBDatabase db) throws SQLException {
		super(testIterationName, db);

		PolygonTestTable polygonTestTable = new PolygonTestTable();

		db.preventDroppingOfTables(false);
		db.dropTableNoExceptions(polygonTestTable);
		db.createTable(polygonTestTable);

		Coordinate coordinate00 = new Coordinate(0, 0);
		Coordinate coordinate01 = new Coordinate(0, 1);
		Coordinate coordinate11 = new Coordinate(1, 1);
		Coordinate coordinate10 = new Coordinate(1, 0);
		Coordinate coordinate43 = new Coordinate(4, 3);
		Coordinate coordinate32 = new Coordinate(3, 2);
		polygonTestTable.poly.setValue(geometryFactory.createPolygon(
				new Coordinate[]{coordinate00, coordinate11, coordinate10, coordinate00}));
		db.insert(polygonTestTable);

		polygonTestTable = new PolygonTestTable();
		polygonTestTable.poly.setValue(geometryFactory.createPolygon(
				new Coordinate[]{coordinate11, coordinate43, coordinate32, coordinate11}));
		db.insert(polygonTestTable);

		polygonTestTable = new PolygonTestTable();
		polygonTestTable.poly.setValue(geometryFactory.createPolygon(
				new Coordinate[]{coordinate00, coordinate01, coordinate11, coordinate10, coordinate00}));
		db.insert(polygonTestTable);
	}

	@Test
	public void testUnitSquare() throws SQLException {
		final PolygonTestTable testTable = new PolygonTestTable();
		DBQuery dbQuery = database.getDBQuery(testTable);
		dbQuery.addCondition(testTable.column(testTable.poly).is(Polygon2DExpression.unitSquare()));
		List<PolygonTestTable> allRows = dbQuery.getAllInstancesOf(testTable);
		Assert.assertThat(allRows.size(), is(1));

	}

	@Test
	public void testIs() throws SQLException {
		final PolygonTestTable testTable = new PolygonTestTable();
		DBQuery dbQuery = database.getDBQuery(testTable);
		dbQuery.addCondition(testTable.column(testTable.poly).is(
				geometryFactory.createPolygon(
						new Coordinate[]{
							new Coordinate(0, 0),
							new Coordinate(1, 1),
							new Coordinate(1, 0),
							new Coordinate(0, 0)}
				)));
		List<PolygonTestTable> allRows = dbQuery.getAllInstancesOf(testTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).poly_id.intValue(), is(1));

		dbQuery = database.getDBQuery(testTable);
		dbQuery.addCondition(testTable.column(testTable.poly).is(
				Polygon2DExpression.value(
						geometryFactory.createPolygon(
								new Coordinate[]{
									new Coordinate(0, 0),
									new Coordinate(1, 1),
									new Coordinate(1, 0),
									new Coordinate(0, 0)}
						))));
		allRows = dbQuery.getAllInstancesOf(testTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).poly_id.intValue(), is(1));

		dbQuery = database.getDBQuery(testTable);
		dbQuery.addCondition(testTable.column(testTable.poly).is(
				new Polygon2DExpression().expression(
						geometryFactory.createPolygon(
								new Coordinate[]{
									new Coordinate(0, 0),
									new Coordinate(1, 1),
									new Coordinate(1, 0),
									new Coordinate(0, 0)}
						))));
		allRows = dbQuery.getAllInstancesOf(testTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).poly_id.intValue(), is(1));

	}

	@Test
	public void testIsNull() throws SQLException {
		final PolygonTestTable testTable = new PolygonTestTable();
		DBQuery dbQuery = database.getDBQuery(testTable);
		dbQuery.addCondition(testTable.column(testTable.poly).isNull());
		List<PolygonTestTable> allRows = dbQuery.getAllInstancesOf(testTable);
		Assert.assertThat(allRows.size(), is(0));

		PolygonTestTable row = new PolygonTestTable();
		row.poly.setValueToNull();
		database.insert(row);

		dbQuery = database.getDBQuery(testTable);
		dbQuery.addCondition(testTable.column(testTable.poly).isNull());
		allRows = dbQuery.getAllInstancesOf(testTable);
		Assert.assertThat(allRows.size(), is(1));

		dbQuery = database.getDBQuery(testTable);
		dbQuery.addCondition(testTable.column(testTable.poly).isNotNull());
		allRows = dbQuery.getAllInstancesOf(testTable);
		Assert.assertThat(allRows.size(), is(3));

	}

	@Test
	public void testDimensions() throws SQLException {
		final PolygonTestTable testTable = new PolygonTestTable();
		DBQuery dbQuery = database.getDBQuery(testTable);
		dbQuery.addCondition(testTable.column(testTable.poly).measurableDimensions().is(2));
		List<PolygonTestTable> allRows = dbQuery.getAllInstancesOf(testTable);
		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testHasMagnitude() throws SQLException {
		final PolygonTestTable testTable = new PolygonTestTable();
		DBQuery dbQuery = database.getDBQuery(testTable);
		dbQuery.addCondition(testTable.column(testTable.poly).hasMagnitude().isNot(Boolean.TRUE));
		List<PolygonTestTable> allRows = dbQuery.getAllInstancesOf(testTable);
		Assert.assertThat(allRows.size(), is(3));

		dbQuery = database.getDBQuery(testTable);
		dbQuery.addCondition(testTable.column(testTable.poly).hasMagnitude().is(Boolean.FALSE));
		allRows = dbQuery.getAllInstancesOf(testTable);
		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testIntersects() throws SQLException {
		final PolygonTestTable testTable = new PolygonTestTable();
		DBQuery dbQuery = database.getDBQuery(testTable);
		dbQuery.addCondition(
				testTable.column(testTable.poly)
						.intersects(
								geometryFactory
										.createPolygon(
												new Coordinate[]{
													new Coordinate(0, 0),
													new Coordinate(0, 1),
													new Coordinate(1, 0),
													new Coordinate(0, 0)}
										)));
		List<PolygonTestTable> allRows = dbQuery.getAllInstancesOf(testTable);

		Assert.assertThat(allRows.size(), is(2));
		for (PolygonTestTable row : allRows) {
			Assert.assertThat(row.poly_id.intValue(), isOneOf(1, 3));
		}
	}

	@Test
	public void testContains() throws SQLException {
		final PolygonTestTable testTable = new PolygonTestTable();

		DBQuery dbQuery = database.getDBQuery(testTable);
		dbQuery.addCondition(
				Polygon2DExpression.value(-1, -1, -1, 2, 2, 2, 2, -1, -1, -1)
						.contains(
								testTable.column(testTable.poly)
						));
		List<PolygonTestTable> allRows = dbQuery.getAllInstancesOf(testTable);

		Assert.assertThat(allRows.size(), is(2));
		for (PolygonTestTable row : allRows) {
			Assert.assertThat(row.poly_id.intValue(), isOneOf(1, 3));
		}
	}

	@Test
	public void testTouches() throws SQLException {
		final PolygonTestTable testTable = new PolygonTestTable();
		DBQuery dbQuery = database.getDBQuery(testTable);
		dbQuery.addCondition(
				testTable.column(testTable.poly)
						.touches(
								geometryFactory
										.createPolygon(
												new Coordinate[]{
													new Coordinate(0, 0),
													new Coordinate(1, 0),
													new Coordinate(-1, -2),
													new Coordinate(-1, -3),
													new Coordinate(0, 0)}
										)));
		List<PolygonTestTable> allRows = dbQuery.getAllInstancesOf(testTable);

		Assert.assertThat(allRows.size(), is(2));
		for (PolygonTestTable row : allRows) {
			Assert.assertThat(row.poly_id.intValue(), isOneOf(1, 3));
		}
		dbQuery = database.getDBQuery(testTable);
		dbQuery.addCondition(
				testTable.column(testTable.poly)
						.touches(
								Polygon2DExpression
										.value(0, 0, 1, 0, -1, -2, -1, -3, 0, 0)
						));
		allRows = dbQuery.getAllInstancesOf(testTable);

		Assert.assertThat(allRows.size(), is(2));
		for (PolygonTestTable row : allRows) {
			Assert.assertThat(row.poly_id.intValue(), isOneOf(1, 3));
		}
	}

	@Test
	public void testOverlaps() throws SQLException {

		final PolygonTestTable testTable = new PolygonTestTable();

		DBQuery dbQuery = database.getDBQuery(testTable);
		dbQuery.addCondition(
				testTable.column(testTable.poly)
						.overlaps(
								Polygon2DExpression
										.value(0, 0, 0, 1, 1, 0, 0, 0)
						)
		);
		List<PolygonTestTable> allRows = dbQuery.getAllInstancesOf(testTable);

		Assert.assertThat(allRows.size(), is(1));
		for (PolygonTestTable row : allRows) {
			Assert.assertThat(row.poly_id.intValue(), isOneOf(1));
		}
	}

	@Test
	public void testIntersection() throws SQLException {
		final PolygonIntersectionTestTable testTable = new PolygonIntersectionTestTable();

		DBQuery dbQuery = database.getDBQuery(testTable).setBlankQueryAllowed(true);
		dbQuery.addCondition(
				testTable.column(testTable.poly)
						.intersection(
								Polygon2DExpression
										.value(0, 0, 0, 1, 1, 0, 0, 0)
						).is(Polygon2DExpression
								.value(0, 0, 0.5, 0.5, 1, 0, 0, 0))
		);

		List<PolygonIntersectionTestTable> allRows = dbQuery.getAllInstancesOf(testTable);
		Assert.assertThat(allRows.size(), is(1));

		for (PolygonTestTable row : allRows) {
			Assert.assertThat(row.poly_id.intValue(), isOneOf(1));
		}
	}

	public static class PolygonTestTable extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		public DBInteger poly_id = new DBInteger();

		@DBColumn("poly_col")
		public DBPolygon2D poly = new DBPolygon2D();
	}

	public static class PolygonIntersectionTestTable extends PolygonTestTable {

		private static final long serialVersionUID = 1L;

		@DBColumn
		public DBPolygon2D intersection
				= this.column(this.poly)
						.intersection(
								Polygon2DExpression.value(0, 0, 0, 1, 1, 0, 0, 0)
						).asExpressionColumn();
		
		@DBColumn
		public DBPolygon2D test
				= Polygon2DExpression.value(0, 0, 0.5, 0.5, 1, 0, 0, 0).asExpressionColumn();
		
		@DBColumn
		public DBBoolean eq
				= this.column(this.poly)
						.intersection(
								Polygon2DExpression.value(0, 0, 0, 1, 1, 0, 0, 0))
						.is(Polygon2DExpression.value(0, 0, 0.5, 0.5, 1, 0, 0, 0)
						).asExpressionColumn();//        (0  0, 0.5  0.5, 1  0, 0  0)
	}

}
