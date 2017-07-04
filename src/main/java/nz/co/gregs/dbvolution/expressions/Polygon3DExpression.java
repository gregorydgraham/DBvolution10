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

//import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Coordinate;
import nz.co.gregs.dbvolution.results.EqualComparable;
import nz.co.gregs.dbvolution.results.Point3DResult;
import nz.co.gregs.dbvolution.results.Polygon3DResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.spatial3D.DBPolygon3D;
import nz.co.gregs.dbvolution.datatypes.spatial3D.PointZ;
import nz.co.gregs.dbvolution.datatypes.spatial3D.PolygonZ;

/**
 * Creates and transforms Polygon3D values within your database queries.
 *
 * <p>
 * Use these methods to manipulate your Polygon3D columns and results for finer
 * control of the query results.
 *
 * @author gregorygraham
 */
public class Polygon3DExpression implements Polygon3DResult, EqualComparable<Polygon3DResult>, ExpressionColumn<DBPolygon3D>, Spatial3DExpression {

	private Polygon3DResult innerGeometry;
	private boolean nullProtectionRequired;

	/**
	 * Default constructor
	 *
	 */
	protected Polygon3DExpression() {
	}

	/**
	 * Create a Polygon3DExpression that represents the value for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }, and when creating column expressions using {@link DBPolygon3D#DBPolygon3D(nz.co.gregs.dbvolution.expressions.Polygon3DExpression)
	 * } and similar methods.
	 *
	 * @param value
	 */
	public Polygon3DExpression(Polygon3DResult value) {
		innerGeometry = value;
		if (value == null || innerGeometry.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	/**
	 * Create a Polygon3DExpression that represents the value for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }, and when creating column expressions using {@link DBPolygon3D#DBPolygon3D(nz.co.gregs.dbvolution.expressions.Polygon3DExpression)
	 * } and similar methods.
	 *
	 * @param geometry
	 */
	public Polygon3DExpression(PolygonZ geometry) {
		innerGeometry = new DBPolygon3D(geometry);
		if (geometry == null || innerGeometry.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	/**
	 * Create a Polygon3DExpression that represents the value for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }, and when creating column expressions using {@link DBPolygon3D#DBPolygon3D(nz.co.gregs.dbvolution.expressions.Polygon3DExpression)
	 * } and similar methods.
	 *
	 * @param polygon the value of this expression
	 * @return a polygon3D expression
	 */
	public static Polygon3DExpression value(PolygonZ polygon) {
		return new Polygon3DExpression(polygon);
	}

	/**
	 * Create a Polygon3DExpression that represents the value for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }, and when creating column expressions using {@link DBPolygon3D#DBPolygon3D(nz.co.gregs.dbvolution.expressions.Polygon3DExpression)
	 * } and similar methods.
	 *
	 * @param polygon the value of this expression
	 * @return a polygon3D expression
	 */
	public static Polygon3DExpression value(Polygon3DResult polygon) {
		return new Polygon3DExpression(polygon);
	}

	/**
	 * Create a Polygon3DExpression that represents the value for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }, and when creating column expressions using {@link DBPolygon3D#DBPolygon3D(nz.co.gregs.dbvolution.expressions.Polygon3DExpression)
	 * } and similar methods.
	 *
	 * @param pointExpressions the points that define the polygon value of this
	 * expression.
	 * @return a polygon3D expression
	 */
	public static Polygon3DExpression value(Point3DExpression... pointExpressions) {
		return Polygon3DExpression.polygon3DFromPoint3DExpressionArray(pointExpressions);
	}

	/**
	 * Create a Polygon3DExpression that represents the value for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }, and when creating column expressions using {@link DBPolygon3D#DBPolygon3D(nz.co.gregs.dbvolution.expressions.Polygon3DExpression)
	 * } and similar methods.
	 *
	 * @param coordinates  the individual numbers that are converted to
	 * point that define the polygon value of this expression.
	 * @return a polygon3D expression
	 */
	public static Polygon3DExpression value(Number... coordinates) {
		ArrayList<NumberExpression> exprs = new ArrayList<NumberExpression>();
		for (Number coord : coordinates) {
			exprs.add(NumberExpression.value(coord));
		}
		return value(exprs.toArray(new NumberExpression[]{}));
	}

	/**
	 * Create a Polygon3DExpression that represents the value for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }, and when creating column expressions using {@link DBPolygon3D#DBPolygon3D(nz.co.gregs.dbvolution.expressions.Polygon3DExpression)
	 * } and similar methods.
	 *
	 * @param coordinateExpressions the individual numbers that are converted to
	 * point that define the polygon value of this expression.
	 * @return a polygon3D expression
	 */
	public static Polygon3DExpression value(NumberExpression... coordinateExpressions) {
		return Polygon3DExpression.polygon3DFromCoordinateArray(coordinateExpressions);
	}

	/**
	 * Create a Polygon3DExpression that represents the value for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }, and when creating column expressions using {@link DBPolygon3D#DBPolygon3D(nz.co.gregs.dbvolution.expressions.Polygon3DExpression)
	 * } and similar methods.
	 *
	 * @param points the points that define the polygon value of this expression.
	 * @return a polygon3D expression
	 */
	public static Polygon3DExpression value(Coordinate... points) {
		List<Point3DExpression> exprs = new ArrayList<Point3DExpression>();
		for (Coordinate point : points) {
			exprs.add(Point3DExpression.value(point));
		}
		return polygon3DFromPoint3DExpressionArray(exprs.toArray(new Point3DExpression[]{}));
	}

	/**
	 * Create a Polygon3DExpression that represents the value for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }, and when creating column expressions using {@link DBPolygon3D#DBPolygon3D(nz.co.gregs.dbvolution.expressions.Polygon3DExpression)
	 * } and similar methods.
	 *
	 * @param points the points that define the polygon value of this expression.
	 * @return a polygon3D expression
	 */
	public static Polygon3DExpression value(PointZ... points) {
		List<Point3DExpression> exprs = new ArrayList<Point3DExpression>();
		for (PointZ point : points) {
			exprs.add(Point3DExpression.value(point));
		}
		return polygon3DFromPoint3DExpressionArray(exprs.toArray(new Point3DExpression[]{}));
	}

	@Override
	public DBPolygon3D getQueryableDatatypeForExpressionValue() {
		return new DBPolygon3D();
	}

	@Override
	public StringExpression toWKTFormat() {
		return stringResult();
	}

	@Override
	public String toSQLString(DBDatabase db) {
		if (innerGeometry == null) {
			return db.getDefinition().getNull();
		} else {
			return innerGeometry.toSQLString(db);
		}
	}

	@Override
	public Polygon3DExpression copy() {
		return new Polygon3DExpression(innerGeometry);
	}

	@Override
	public boolean isAggregator() {
		return innerGeometry == null ? false : innerGeometry.isAggregator();
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		HashSet<DBRow> hashSet = new HashSet<DBRow>();
		if (innerGeometry != null) {
			hashSet.addAll(innerGeometry.getTablesInvolved());
		}
		return hashSet;
	}

	@Override
	public boolean isPurelyFunctional() {
		return innerGeometry == null ? true : innerGeometry.isPurelyFunctional();
	}

	@Override
	public boolean getIncludesNull() {
		return nullProtectionRequired;
	}

	/**
	 * Create a boolean expression that returns TRUE if the two polygons share any
	 * spatial coordinates.
	 *
	 * @param rightHandSide the polygon to compare against.
	 * @return a boolean expression that is true if the polygons interact in any
	 * way.
	 */
	public BooleanExpression intersects(PolygonZ rightHandSide) {
		return intersects(new DBPolygon3D(rightHandSide));
	}

	/**
	 * Create a boolean expression that returns TRUE if the two polygons share any
	 * spatial coordinates.
	 *
	 * @param rightHandSide the polygon to compare against
	 * @return a boolean expression that is true if the polygons interact in any
	 * way.
	 */
	public BooleanExpression intersects(Polygon3DResult rightHandSide) {
		return new BooleanExpression(new PolygonPolygonWithBooleanResult(this, new Polygon3DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon3DIntersectsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	/**
	 * Provides a expression that represents the multipoint3D value as a polygon3D
	 * value.
	 *
	 * <P>
	 * Points are added to the polygon in index order. If necessary the polygon is
	 * closed by adding the first point to the end.
	 *
	 * <p>
	 * MultiPoint3D values with less than 3 points will return NULL values.
	 *
	 * @return a polygon3D expression
	 */
	/* TODO implement public Polygon3DExpression polygon3DResult() {*/
	public Polygon3DExpression polygon3DResult() {
		throw new UnsupportedOperationException("NOT DONE YET, SORRY.");
	}

	private static Polygon3DExpression polygon3DFromPoint3DExpressionArray(Point3DExpression... pointExpressions) {
		return new Polygon3DExpression(new Point3DArrayFunctionWithPolygon3DResult(pointExpressions) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				Point3DExpression[] allPoints = getAllPoints();
				List<String> pointSQL = new ArrayList<String>();
				for (Point3DExpression pointExpr : allPoints) {
					pointSQL.add(pointExpr.toSQLString(db));
				}
				try {
					return db.getDefinition().transformPoint3DArrayToDatabasePolygon3DFormat(pointSQL);
				} catch (UnsupportedOperationException ex) {
					StringExpression newPolygon = StringExpression.value("POLYGON ((");
					String separator = "";

					for (Point3DExpression point : allPoints) {
						newPolygon = newPolygon.append(separator).append(point.getX()).append(" ").append(point.getY());
						separator = " , ";
					}
					final Point3DExpression firstPoint = allPoints[0];
					newPolygon = newPolygon.append(separator).append(firstPoint.getX()).append(" ").append(firstPoint.getY()).append("))");
					return newPolygon.toSQLString(db);
				}
			}
		});
	}

	private static Polygon3DExpression polygon3DFromCoordinateArray(NumberExpression... coordExpressions) {
		return new Polygon3DExpression(new CoordinateArrayFunctionWithPolygon3DResult(coordExpressions) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				NumberExpression[] allCoords = getAllCoordinates();
				List<String> pointSQL = new ArrayList<String>();
				for (NumberExpression pointExpr : allCoords) {
					pointSQL.add(pointExpr.toSQLString(db));
				}
				try {
					return db.getDefinition().transformCoordinateArrayToDatabasePolygon3DFormat(pointSQL);
				} catch (UnsupportedOperationException ex) {
					StringExpression newPolygon = StringExpression.value("POLYGON ((");
					String separator = "";

					for (NumberExpression coord : allCoords) {
						newPolygon = newPolygon.append(separator).append(coord);
						if (separator.equals("")) {
							separator = " ";
						} else if (separator.equals(" ")) {
							separator = ", ";
						} else if (separator.equals(", ")) {
							separator = " ";
						}
					}
					final StringExpression firstPoint = allCoords[0].append(" ").append(allCoords[1]);
					newPolygon = newPolygon.append(separator).append(firstPoint).append("))");
					return newPolygon.toSQLString(db);
				}
			}
		});
	}

	/**
	 * Creates a {@link BooleanExpression} that compares the 2 values using the
	 * EQUALS operation.
	 *
	 * @param rightHandSide the polygon to compare against
	 * @return a BooleanExpression
	 */
	public BooleanExpression is(PolygonZ rightHandSide) {
		return is(new DBPolygon3D(rightHandSide));
	}

	@Override
	public BooleanExpression is(Polygon3DResult rightHandSide) {
		return new BooleanExpression(new PolygonPolygonWithBooleanResult(this, new Polygon3DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon3DEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	/**
	 * Returns an expression that will evaluate to true if the point is inside
	 * this polygon value.
	 *
	 * @param rightHandSide the polygon to compare against
	 * @return a boolean expression
	 */
	public BooleanExpression contains(Coordinate rightHandSide) {
		return contains(Point3DExpression.value(rightHandSide));
	}

	/**
	 * Returns an expression that will evaluate to true if the point is inside
	 * this polygon value.
	 *
	 * @param rightHandSide the polygon to compare against
	 * @return a boolean expression
	 */
	public BooleanExpression contains(PointZ rightHandSide) {
		return contains(Point3DExpression.value(rightHandSide));
	}

	/**
	 * Returns an expression that will evaluate to true if the point is inside
	 * this polygon value.
	 *
	 * @param rightHandSide the point to compare against
	 * @return a boolean expression
	 */
	public BooleanExpression contains(Point3DResult rightHandSide) {
		return new BooleanExpression(new PolygonPointWithBooleanResult(this, new Point3DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon3DContainsPoint3DTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	/**
	 * Returns an expression that will evaluate to true if the polygon is
	 * completely inside this polygon value.
	 *
	 * <p>
	 * A CONTAINS B implies B WITHIN A.
	 *
	 * @param rightHandSide the polygon to compare against
	 * @return a boolean expression
	 */
	public BooleanExpression contains(PolygonZ rightHandSide) {
		return contains(new DBPolygon3D(rightHandSide));
	}

	/**
	 * Returns an expression that will evaluate to true if the polygon is
	 * completely inside this polygon value.
	 *
	 * <p>
	 * A CONTAINS B when A's exterior is outside the exterior of B and the spatial
	 * intersection of A and B is B. It also implies that there are no
	 * intersection points of the exterior rings of the polygons, that they do NOT
	 * touch and B is smaller than A.
	 *
	 * <p>
	 * This operation is the inverse of within: A CONTAINS B implies B WITHIN A.
	 *
	 * @param rightHandSide the polygon to compare against
	 * @return a boolean expression
	 */
	public BooleanExpression contains(Polygon3DResult rightHandSide) {
		return new BooleanExpression(new PolygonPolygonWithBooleanResult(this, new Polygon3DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon3DContainsPolygon3DTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	/**
	 * Creates an SQL expression that is TRUE when the two polygon3D values do NOT
	 * intersect in anyway.
	 *
	 * @param rightHandSide the polygon to compare against
	 * @return a boolean expression that is TRUE if the 2 polygons do NOT
	 * intersect in anyway, otherwise FALSE.
	 */
	public BooleanExpression doesNotIntersect(PolygonZ rightHandSide) {
		return doesNotIntersect(new DBPolygon3D(rightHandSide));
	}

	/**
	 * Creates an SQL expression that is TRUE when the two polygon3D values do NOT
	 * intersect in anyway.
	 *
	 * @param rightHandSide the polygon to compare against
	 * @return a boolean expression that is TRUE if the 2 polygons do NOT
	 * intersect in anyway, otherwise FALSE.
	 */
	public BooleanExpression doesNotIntersect(Polygon3DResult rightHandSide) {
		return new BooleanExpression(new PolygonPolygonWithBooleanResult(this, new Polygon3DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon3DDoesNotIntersectTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	/**
	 * Creates an SQL expression that is TRUE when the two polygon3D values
	 * intersect but neither contains or is within the other.
	 *
	 * <p>
	 * Overlapping polygons have some shared points but they also have unshared
	 * points. This implies that they are also unequal.
	 *
	 * <p>
	 * Two polygon's overlap when their spatial intersection is non-zero but is
	 * not equal to A or B.
	 *
	 * @param rightHandSide the polygon to compare against
	 * @return a boolean expression that is TRUE if the 2 polygons intersect but
	 * are not contained, within, or equal.
	 */
	public BooleanExpression overlaps(PolygonZ rightHandSide) {
		return overlaps(new DBPolygon3D(rightHandSide));
	}

	/**
	 * Creates an SQL expression that is TRUE when the two polygon3D values
	 * intersect but neither contains or is within the other.
	 *
	 * <p>
	 * Overlapping polygons have some shared points but they also have unshared
	 * points. This implies that they are also unequal.
	 *
	 * <p>
	 * Two polygon's overlap when their spatial intersection is non-zero but is
	 * not equal to A or B.
	 *
	 * @param rightHandSide the polygon to compare against
	 * @return a boolean expression that is TRUE if the 2 polygons intersect but
	 * are not contained, within, or equal.
	 */
	public BooleanExpression overlaps(Polygon3DResult rightHandSide) {
		return new BooleanExpression(new PolygonPolygonWithBooleanResult(this, new Polygon3DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon3DOverlapsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	/**
	 * Tests whether the polygons touch.
	 *
	 * <p>
	 * Checks that a) the polygons have at least on point in common and b) that
	 * their interiors do not overlap.
	 *
	 * @param rightHandSide the polygon to compare against
	 * @return BooleanExpression that returns TRUE if and only if the polygons
	 * touch without overlapping
	 */
	public BooleanExpression touches(PolygonZ rightHandSide) {
		return touches(new DBPolygon3D(rightHandSide));
	}

	/**
	 * Tests whether the polygons touch.
	 *
	 * <p>
	 * Checks that a) the polygons have at least on point in common and b) that
	 * their interiors do not overlap.
	 *
	 * @param rightHandSide the polygon to compare against
	 * @return BooleanExpression that returns TRUE if and only if the polygons
	 * touch without overlapping
	 */
	public BooleanExpression touches(Polygon3DResult rightHandSide) {
		return new BooleanExpression(new PolygonPolygonWithBooleanResult(this, new Polygon3DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon3DTouchesTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	/**
	 * Returns an expression that will evaluate to true if the polygon is
	 * completely envelopes this polygon value.
	 *
	 * <p>
	 * A WITHIN B when A's exterior is inside the exterior of B and the spatial
	 * intersection of A and B is A. It also implies that there are no
	 * intersection points of the exterior rings of the polygons, that they do NOT
	 * touch and A is smaller than B.
	 *
	 * <p>
	 * This operation is the inverse of contains: A CONTAINS B implies B WITHIN A.
	 *
	 * @param rightHandSide the polygon to compare against
	 * @return a boolean expression
	 */
	public BooleanExpression within(PolygonZ rightHandSide) {
		return within(new DBPolygon3D(rightHandSide));
	}

	/**
	 * Returns an expression that will evaluate to true if the polygon is
	 * completely envelopes this polygon value.
	 *
	 * <p>
	 * A WITHIN B when A's exterior is inside the exterior of B and the spatial
	 * intersection of A and B is A. It also implies that there are no
	 * intersection points of the exterior rings of the polygons, that they do NOT
	 * touch and A is smaller than B.
	 *
	 * <p>
	 * This operation is the inverse of contains: A CONTAINS B implies B WITHIN A.
	 *
	 * @param rightHandSide the polygon to compare against
	 * @return a boolean expression
	 */
	public BooleanExpression within(Polygon3DResult rightHandSide) {
		return new BooleanExpression(new PolygonPolygonWithBooleanResult(this, new Polygon3DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon3DWithinTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	@Override
	public NumberExpression measurableDimensions() {
		return new NumberExpression(new Polygon3DFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon3DMeasurableDimensionsTransform(getFirst().toSQLString(db));
			}
		});
	}

	@Override
	public NumberExpression spatialDimensions() {
		return new NumberExpression(new Polygon3DFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doPolygon3DSpatialDimensionsTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return NumberExpression.value(2).toSQLString(db);
				}
			}
		});
	}

	@Override
	public BooleanExpression hasMagnitude() {
		return new BooleanExpression(new PolygonWithBooleanResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doPolygon3DHasMagnitudeTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return BooleanExpression.falseExpression().toSQLString(db);
				}
			}
		});
	}

	@Override
	public NumberExpression magnitude() {
		return new NumberExpression(new Polygon3DFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doPolygon3DGetMagnitudeTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return nullExpression().toSQLString(db);
				}
			}
		});
	}

	/**
	 * Returns the volume of the polygon expressed in units.
	 *
	 * @return the volume covered by the polygon in units.
	 */
	public NumberExpression area() {
		return new NumberExpression(new Polygon3DFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon3DGetAreaTransform(getFirst().toSQLString(db));
			}
		});
	}

	@Override
	public Polygon3DExpression boundingBox() {
		return new Polygon3DExpression(new Polygon3DFunctionWithPolygon3DResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doPolygon3DGetBoundingBoxTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					final Polygon3DExpression first = getFirst();
					final NumberExpression maxX = first.maxX();
					final NumberExpression maxY = first.maxY();
					final NumberExpression minX = first.minX();
					final NumberExpression minY = first.minY();
					return Polygon3DExpression
							.value(minX, minY, maxX, minY, maxX, maxY, minX, maxY, minX, minY)
							.toSQLString(db);
				}
			}
		});
	}

	/**
	 * Return a Line3DExpression representing a line drawn around the outside of
	 * the Polygon3D.
	 *
	 * <p>
	 * The line is coincident with the edge of the polygon but it does not contain
	 * any points within the polygon as it is only a line.
	 *
	 * @return a Line3DExpression
	 */
	public Line3DExpression exteriorRing() {
		Line3DExpression exteriorRingExpr = new Line3DExpression(new Polygon3DFunctionWithLine3DResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon3DGetExteriorRingTransform(getFirst().toSQLString(db));
			}
		});
//		return this.measurableDimensions().is(2).ifThenElse(exteriorRingExpr, this);
		return exteriorRingExpr;
	}

	/**
	 * Creates a {@link BooleanExpression} that compares the 2 values using the
	 * NOT EQUALS operation.
	 *
	 * @param geometry the polygon to compare against
	 * @return a BooleanExpression
	 */
	public BooleanExpression isNot(PolygonZ geometry) {
		return this.isNot(Polygon3DExpression.value(geometry));
	}

	@Override
	public BooleanExpression isNot(Polygon3DResult geometry) {
		return this.is(geometry).not();
	}

	@Override
	public StringExpression stringResult() {
		StringExpression stringResultExpr = new StringExpression(new Polygon3DFunctionWithStringResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon3DAsTextTransform(getFirst().toSQLString(db));
			}
		});
//		return this.measurableDimensions().is(2).ifThenElse(exteriorRingExpr, this);
		return stringResultExpr;
	}

	@Override
	public NumberExpression maxX() {
		NumberExpression expr = new NumberExpression(new Polygon3DFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon3DGetMaxXTransform(getFirst().toSQLString(db));
			}
		});
		return expr;
	}

	@Override
	public NumberExpression maxY() {
		NumberExpression expr = new NumberExpression(new Polygon3DFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon3DGetMaxYTransform(getFirst().toSQLString(db));
			}
		});
		return expr;
	}

	@Override
	public NumberExpression maxZ() {
		NumberExpression expr = new NumberExpression(new Polygon3DFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon3DGetMaxZTransform(getFirst().toSQLString(db));
			}
		});
		return expr;
	}

	@Override
	public NumberExpression minX() {
		NumberExpression expr = new NumberExpression(new Polygon3DFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon3DGetMinXTransform(getFirst().toSQLString(db));
			}
		});
		return expr;
	}

	@Override
	public NumberExpression minY() {
		NumberExpression expr = new NumberExpression(new Polygon3DFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon3DGetMinYTransform(getFirst().toSQLString(db));
			}
		});
		return expr;
	}

	@Override
	public NumberExpression minZ() {
		NumberExpression expr = new NumberExpression(new Polygon3DFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon3DGetMinZTransform(getFirst().toSQLString(db));
			}
		});
		return expr;
	}

	@Override
	public DBPolygon3D asExpressionColumn() {
		return new DBPolygon3D(this);
	}

	private static abstract class PolygonPolygonWithBooleanResult extends BooleanExpression {

		private Polygon3DExpression first;
		private Polygon3DExpression second;
		private boolean requiresNullProtection;

		PolygonPolygonWithBooleanResult(Polygon3DExpression first, Polygon3DExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Polygon3DExpression getFirst() {
			return first;
		}

		Polygon3DResult getSecond() {
			return second;
		}

		@Override
		public final String toSQLString(DBDatabase db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		@Override
		public PolygonPolygonWithBooleanResult copy() {
			PolygonPolygonWithBooleanResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
			newInstance.second = second.copy();
			return newInstance;
		}

		protected abstract String doExpressionTransform(DBDatabase db);

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (first != null) {
				hashSet.addAll(first.getTablesInvolved());
			}
			if (second != null) {
				hashSet.addAll(second.getTablesInvolved());
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return first.isAggregator() || second.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

	private static abstract class PolygonWithBooleanResult extends BooleanExpression {

		private Polygon3DExpression first;
//		private Polygon3DExpression second;
		private boolean requiresNullProtection;

		PolygonWithBooleanResult(Polygon3DExpression first) {
			this.first = first;
//			this.second = second;
//			if (this.second == null || this.second.getIncludesNull()) {
//				this.requiresNullProtection = true;
//			}
		}

		Polygon3DExpression getFirst() {
			return first;
		}

//		Polygon3DResult getSecond() {
//			return second;
//		}
		@Override
		public final String toSQLString(DBDatabase db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		@Override
		public PolygonWithBooleanResult copy() {
			PolygonWithBooleanResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
//			newInstance.second = second.copy();
			return newInstance;
		}

		protected abstract String doExpressionTransform(DBDatabase db);

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (first != null) {
				hashSet.addAll(first.getTablesInvolved());
			}
//			if (second != null) {
//				hashSet.addAll(second.getTablesInvolved());
//			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return first.isAggregator();//|| second.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

	private static abstract class PolygonPointWithBooleanResult extends BooleanExpression {

		private Polygon3DExpression first;
		private Point3DExpression second;
		private boolean requiresNullProtection;

		PolygonPointWithBooleanResult(Polygon3DExpression first, Point3DExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Polygon3DExpression getFirst() {
			return first;
		}

		Point3DExpression getSecond() {
			return second;
		}

		@Override
		public final String toSQLString(DBDatabase db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		@Override
		public PolygonPointWithBooleanResult copy() {
			PolygonPointWithBooleanResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
			newInstance.second = second.copy();
			return newInstance;
		}

		protected abstract String doExpressionTransform(DBDatabase db);

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (first != null) {
				hashSet.addAll(first.getTablesInvolved());
			}
			if (second != null) {
				hashSet.addAll(second.getTablesInvolved());
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return first.isAggregator() || second.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

	private static abstract class Polygon3DFunctionWithNumberResult extends NumberExpression {

		private Polygon3DExpression first;
//		private Polygon3DExpression second;
		private boolean requiresNullProtection;

		Polygon3DFunctionWithNumberResult(Polygon3DExpression first) {
			this.first = first;
//			this.second = second;
//			if (this.second == null || this.second.getIncludesNull()) {
//				this.requiresNullProtection = true;
//			}
		}

		Polygon3DExpression getFirst() {
			return first;
		}

//		Polygon3DResult getSecond() {
//			return second;
//		}
		@Override
		public final String toSQLString(DBDatabase db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		@Override
		public Polygon3DFunctionWithNumberResult copy() {
			Polygon3DFunctionWithNumberResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
//			newInstance.second = second.copy();
			return newInstance;
		}

		protected abstract String doExpressionTransform(DBDatabase db);

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (first != null) {
				hashSet.addAll(first.getTablesInvolved());
			}
//			if (second != null) {
//				hashSet.addAll(second.getTablesInvolved());
//			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return first.isAggregator();//|| second.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

	private static abstract class Polygon3DFunctionWithPolygon3DResult extends Polygon3DExpression {

		private Polygon3DExpression first;
//		private Polygon3DExpression second;
		private boolean requiresNullProtection;

		Polygon3DFunctionWithPolygon3DResult(Polygon3DExpression first) {
			this.first = first;
//			this.second = second;
//			if (this.second == null || this.second.getIncludesNull()) {
//				this.requiresNullProtection = true;
//			}
		}

		Polygon3DExpression getFirst() {
			return first;
		}

//		Polygon3DResult getSecond() {
//			return second;
//		}
		@Override
		public final String toSQLString(DBDatabase db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		@Override
		public Polygon3DFunctionWithPolygon3DResult copy() {
			Polygon3DFunctionWithPolygon3DResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
//			newInstance.second = second.copy();
			return newInstance;
		}

		protected abstract String doExpressionTransform(DBDatabase db);

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (first != null) {
				hashSet.addAll(first.getTablesInvolved());
			}
//			if (second != null) {
//				hashSet.addAll(second.getTablesInvolved());
//			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return first.isAggregator();//|| second.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

	private static abstract class Polygon3DFunctionWithLine3DResult extends Line3DExpression {

		private Polygon3DExpression first;
//		private Polygon3DExpression second;
		private boolean requiresNullProtection;

		Polygon3DFunctionWithLine3DResult(Polygon3DExpression first) {
			this.first = first;
//			this.second = second;
//			if (this.second == null || this.second.getIncludesNull()) {
//				this.requiresNullProtection = true;
//			}
		}

		Polygon3DExpression getFirst() {
			return first;
		}

//		Polygon3DResult getSecond() {
//			return second;
//		}
		@Override
		public final String toSQLString(DBDatabase db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		@Override
		public Polygon3DFunctionWithLine3DResult copy() {
			Polygon3DFunctionWithLine3DResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
//			newInstance.second = second.copy();
			return newInstance;
		}

		protected abstract String doExpressionTransform(DBDatabase db);

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (first != null) {
				hashSet.addAll(first.getTablesInvolved());
			}
//			if (second != null) {
//				hashSet.addAll(second.getTablesInvolved());
//			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return first.isAggregator();//|| second.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

	private static abstract class Polygon3DFunctionWithStringResult extends StringExpression {

		private Polygon3DExpression first;
//		private Polygon3DExpression second;
		private boolean requiresNullProtection;

		Polygon3DFunctionWithStringResult(Polygon3DExpression first) {
			this.first = first;
//			this.second = second;
//			if (this.second == null || this.second.getIncludesNull()) {
//				this.requiresNullProtection = true;
//			}
		}

		Polygon3DExpression getFirst() {
			return first;
		}

//		Polygon3DResult getSecond() {
//			return second;
//		}
		@Override
		public final String toSQLString(DBDatabase db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		@Override
		public Polygon3DFunctionWithStringResult copy() {
			Polygon3DFunctionWithStringResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
//			newInstance.second = second.copy();
			return newInstance;
		}

		protected abstract String doExpressionTransform(DBDatabase db);

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (first != null) {
				hashSet.addAll(first.getTablesInvolved());
			}
//			if (second != null) {
//				hashSet.addAll(second.getTablesInvolved());
//			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return first.isAggregator();//|| second.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

	private static abstract class Point3DArrayFunctionWithPolygon3DResult extends Polygon3DExpression {

		private Point3DExpression[] allPoints;
		private boolean requiresNullProtection;

		Point3DArrayFunctionWithPolygon3DResult(Point3DExpression... all) {
			this.allPoints = all;
			for (Point3DExpression all1 : all) {
				if (all1.getIncludesNull()) {
					this.requiresNullProtection = true;
				}
			}
		}

		Point3DExpression[] getAllPoints() {
			return allPoints;
		}

		@Override
		public final String toSQLString(DBDatabase db) {
			BooleanExpression isNull = BooleanExpression.trueExpression();
			if (this.getIncludesNull()) {
				for (Point3DExpression allPoint : allPoints) {
					isNull = isNull.or(BooleanExpression.isNull(allPoint));
				}
				return isNull.toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		@Override
		public Point3DArrayFunctionWithPolygon3DResult copy() {
			Point3DArrayFunctionWithPolygon3DResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.allPoints = Arrays.copyOf(allPoints, allPoints.length);
//			newInstance.second = second.copy();
			return newInstance;
		}

		protected abstract String doExpressionTransform(DBDatabase db);

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (allPoints != null) {
				for (Point3DExpression point : allPoints) {
					hashSet.addAll(point.getTablesInvolved());
				}
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			boolean aggregator = false;
			for (Point3DExpression allPoint : allPoints) {
				aggregator |= allPoint.isAggregator();
			}
			return aggregator;//|| second.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

	private static abstract class CoordinateArrayFunctionWithPolygon3DResult extends Polygon3DExpression {

		private NumberExpression[] allCoords;
		private boolean requiresNullProtection;

		CoordinateArrayFunctionWithPolygon3DResult(NumberExpression... all) {
			this.allCoords = all;
			for (NumberExpression all1 : all) {
				if (all1.getIncludesNull()) {
					this.requiresNullProtection = true;
				}
			}
		}

		NumberExpression[] getAllCoordinates() {
			return allCoords;
		}

		@Override
		public final String toSQLString(DBDatabase db) {
			BooleanExpression isNull = BooleanExpression.trueExpression();
			if (this.getIncludesNull()) {
				for (NumberExpression allPoint : allCoords) {
					isNull = isNull.or(BooleanExpression.isNull(allPoint));
				}
				return isNull.toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		@Override
		public CoordinateArrayFunctionWithPolygon3DResult copy() {
			CoordinateArrayFunctionWithPolygon3DResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.allCoords = Arrays.copyOf(allCoords, allCoords.length);
//			newInstance.second = second.copy();
			return newInstance;
		}

		protected abstract String doExpressionTransform(DBDatabase db);

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (allCoords != null) {
				for (NumberExpression point : allCoords) {
					hashSet.addAll(point.getTablesInvolved());
				}
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			boolean aggregator = false;
			for (NumberExpression allPoint : allCoords) {
				aggregator |= allPoint.isAggregator();
			}
			return aggregator;//|| second.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

}
