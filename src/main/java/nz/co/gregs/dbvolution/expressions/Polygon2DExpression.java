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
import nz.co.gregs.dbvolution.results.EqualComparable;
import nz.co.gregs.dbvolution.results.Point2DResult;
import nz.co.gregs.dbvolution.results.Polygon2DResult;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBPolygon2D;

/**
 * Creates and transforms Polygon2D values within your database queries.
 *
 * <p>
 * Use these methods to manipulate your Polygon2D columns and results for finer
 * control of the query results.
 *
 * @author gregorygraham
 */
public class Polygon2DExpression implements Spatial2DExpression, Polygon2DResult, EqualComparable<Polygon2DResult>, ExpressionColumn<DBPolygon2D> {

	private Polygon2DResult innerGeometry;
	private boolean nullProtectionRequired;

	/**
	 * Default constructor
	 *
	 */
	protected Polygon2DExpression() {
	}

	/**
	 * Create a Polygon2DExpression that represents the value for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }, and when creating column expressions using {@link DBPolygon2D#DBPolygon2D(nz.co.gregs.dbvolution.expressions.Polygon2DExpression)
	 * } and similar methods.
	 *
	 * @param value
	 */
	public Polygon2DExpression(Polygon2DResult value) {
		innerGeometry = value;
		if (value == null || innerGeometry.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	/**
	 * Create a Polygon2DExpression that represents the value for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }, and when creating column expressions using {@link DBPolygon2D#DBPolygon2D(nz.co.gregs.dbvolution.expressions.Polygon2DExpression)
	 * } and similar methods.
	 *
	 * @param geometry
	 */
	public Polygon2DExpression(Polygon geometry) {
		innerGeometry = new DBPolygon2D(geometry);
		if (geometry == null || innerGeometry.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	/**
	 * Create a Polygon2DExpression that represents the value for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }, and when creating column expressions using {@link DBPolygon2D#DBPolygon2D(nz.co.gregs.dbvolution.expressions.Polygon2DExpression)
	 * } and similar methods.
	 *
	 * @param polygon the value of this expression
	 * @return a polygon2d expression
	 */
	public static Polygon2DExpression value(Polygon polygon) {
		return new Polygon2DExpression(polygon);
	}

	/**
	 * Create a Polygon2DExpression that represents the value for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }, and when creating column expressions using {@link DBPolygon2D#DBPolygon2D(nz.co.gregs.dbvolution.expressions.Polygon2DExpression)
	 * } and similar methods.
	 *
	 * @param polygon the value of this expression
	 * @return a polygon2d expression
	 */
	public static Polygon2DExpression value(Polygon2DResult polygon) {
		return new Polygon2DExpression(polygon);
	}

	/**
	 * Create a Polygon2DExpression that represents the value for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }, and when creating column expressions using {@link DBPolygon2D#DBPolygon2D(nz.co.gregs.dbvolution.expressions.Polygon2DExpression)
	 * } and similar methods.
	 *
	 * @param pointExpressions the points that define the polygon value of this
	 * expression.
	 * @return a polygon2d expression
	 */
	public static Polygon2DExpression value(Point2DExpression... pointExpressions) {
		return Polygon2DExpression.polygon2DFromPoint2DExpressionArray(pointExpressions);
	}

	/**
	 * Create a Polygon2DExpression that represents the value for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }, and when creating column expressions using {@link DBPolygon2D#DBPolygon2D(nz.co.gregs.dbvolution.expressions.Polygon2DExpression)
	 * } and similar methods.
	 *
	 * @param coordinates  the individual numbers that are converted to
	 * point that define the polygon value of this expression.
	 * @return a polygon2d expression
	 */
	public static Polygon2DExpression value(Number... coordinates) {
		ArrayList<NumberExpression> exprs = new ArrayList<NumberExpression>();
		for (Number coord : coordinates) {
			exprs.add(NumberExpression.value(coord));
		}
		return value(exprs.toArray(new NumberExpression[]{}));
	}

	/**
	 * Create a Polygon2DExpression that represents the value for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }, and when creating column expressions using {@link DBPolygon2D#DBPolygon2D(nz.co.gregs.dbvolution.expressions.Polygon2DExpression)
	 * } and similar methods.
	 *
	 * @param coordinateExpressions the individual numbers that are converted to
	 * point that define the polygon value of this expression.
	 * @return a polygon2d expression
	 */
	public static Polygon2DExpression value(NumberExpression... coordinateExpressions) {
		return Polygon2DExpression.polygon2DFromCoordinateArray(coordinateExpressions);
	}

	/**
	 * Create a Polygon2DExpression that represents the value for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }, and when creating column expressions using {@link DBPolygon2D#DBPolygon2D(nz.co.gregs.dbvolution.expressions.Polygon2DExpression)
	 * } and similar methods.
	 *
	 * @param points the points that define the polygon value of this expression.
	 * @return a polygon2d expression
	 */
	public static Polygon2DExpression value(Point... points) {
		List<Point2DExpression> exprs = new ArrayList<Point2DExpression>();
		for (Point point : points) {
			exprs.add(Point2DExpression.value(point));
		}
		return polygon2DFromPoint2DExpressionArray(exprs.toArray(new Point2DExpression[]{}));
	}

	@Override
	public DBPolygon2D getQueryableDatatypeForExpressionValue() {
		return new DBPolygon2D();
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
	public Polygon2DExpression copy() {
		return new Polygon2DExpression(innerGeometry);
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
	public BooleanExpression intersects(Polygon rightHandSide) {
		return intersects(new DBPolygon2D(rightHandSide));
	}

	/**
	 * Create a boolean expression that returns TRUE if the two polygons share any
	 * spatial coordinates.
	 *
	 * @param rightHandSide the polygon to compare against
	 * @return a boolean expression that is true if the polygons interact in any
	 * way.
	 */
	public BooleanExpression intersects(Polygon2DResult rightHandSide) {
		return new BooleanExpression(new PolygonPolygonWithBooleanResult(this, new Polygon2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DIntersectsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	/**
	 * Provides a expression that represents the multipoint2d value as a polygon2d
	 * value.
	 *
	 * <P>
	 * Points are added to the polygon in index order. If necessary the polygon is
	 * closed by adding the first point to the end.
	 *
	 * <p>
	 * MultiPoint2d values with less than 3 points will return NULL values.
	 *
	 * @return a polygon2d expression
	 */
	/* TODO implement public Polygon2DExpression polygon2DResult() {*/
	public Polygon2DExpression polygon2DResult() {
		throw new UnsupportedOperationException("NOT DONE YET, SORRY.");
	}

	private static Polygon2DExpression polygon2DFromPoint2DExpressionArray(Point2DExpression... pointExpressions) {
		return new Polygon2DExpression(new Point2dArrayFunctionWithPolygon2DResult(pointExpressions) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				Point2DExpression[] allPoints = getAllPoints();
				List<String> pointSQL = new ArrayList<String>();
				for (Point2DExpression pointExpr : allPoints) {
					pointSQL.add(pointExpr.toSQLString(db));
				}
				try {
					return db.getDefinition().doPoint2DArrayToPolygon2DTransform(pointSQL);
				} catch (UnsupportedOperationException ex) {
					StringExpression newPolygon = StringExpression.value("POLYGON ((");
					String separator = "";

					for (Point2DExpression point : allPoints) {
						newPolygon = newPolygon.append(separator).append(point.getX()).append(" ").append(point.getY());
						separator = " , ";
					}
					final Point2DExpression firstPoint = allPoints[0];
					newPolygon = newPolygon.append(separator).append(firstPoint.getX()).append(" ").append(firstPoint.getY()).append("))");
					return newPolygon.toSQLString(db);
				}
			}
		});
	}

	private static Polygon2DExpression polygon2DFromCoordinateArray(NumberExpression... coordExpressions) {
		return new Polygon2DExpression(new CoordinateArrayFunctionWithPolygon2DResult(coordExpressions) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				NumberExpression[] allCoords = getAllCoordinates();
				List<String> pointSQL = new ArrayList<String>();
				for (NumberExpression pointExpr : allCoords) {
					pointSQL.add(pointExpr.toSQLString(db));
				}
				try {
					return db.getDefinition().doCoordinateArrayToPolygon2DTransform(pointSQL);
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
	public BooleanExpression is(Polygon rightHandSide) {
		return is(new DBPolygon2D(rightHandSide));
	}

	@Override
	public BooleanExpression is(Polygon2DResult rightHandSide) {
		return new BooleanExpression(new PolygonPolygonWithBooleanResult(this, new Polygon2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
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
	public BooleanExpression contains(Point rightHandSide) {
		return contains(new Point2DExpression(rightHandSide));
	}

	/**
	 * Returns an expression that will evaluate to true if the point is inside
	 * this polygon value.
	 *
	 * @param rightHandSide the point to compare against
	 * @return a boolean expression
	 */
	public BooleanExpression contains(Point2DResult rightHandSide) {
		return contains(new Point2DExpression(rightHandSide));
	}

	/**
	 * Returns an expression that will evaluate to true if the point is inside
	 * this polygon value.
	 *
	 * @param rightHandSide the point to compare against
	 * @return a boolean expression
	 */
	public BooleanExpression contains(Point2DExpression rightHandSide) {
		return new BooleanExpression(new PolygonPointWithBooleanResult(this, rightHandSide) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DContainsPoint2DTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
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
	public BooleanExpression contains(Polygon rightHandSide) {
		return contains(new DBPolygon2D(rightHandSide));
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
	public BooleanExpression contains(Polygon2DResult rightHandSide) {
		return new BooleanExpression(new PolygonPolygonWithBooleanResult(this, new Polygon2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DContainsPolygon2DTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	/**
	 * Creates an SQL expression that is TRUE when the two polygon2d values do NOT
	 * intersect in anyway.
	 *
	 * @param rightHandSide the polygon to compare against
	 * @return a boolean expression that is TRUE if the 2 polygons do NOT
	 * intersect in anyway, otherwise FALSE.
	 */
	public BooleanExpression doesNotIntersect(Polygon rightHandSide) {
		return doesNotIntersect(new DBPolygon2D(rightHandSide));
	}

	/**
	 * Creates an SQL expression that is TRUE when the two polygon2d values do NOT
	 * intersect in anyway.
	 *
	 * @param rightHandSide the polygon to compare against
	 * @return a boolean expression that is TRUE if the 2 polygons do NOT
	 * intersect in anyway, otherwise FALSE.
	 */
	public BooleanExpression doesNotIntersect(Polygon2DResult rightHandSide) {
		return new BooleanExpression(new PolygonPolygonWithBooleanResult(this, new Polygon2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DDoesNotIntersectTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	/**
	 * Creates an SQL expression that is TRUE when the two polygon2d values
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
	public BooleanExpression overlaps(Polygon rightHandSide) {
		return overlaps(new DBPolygon2D(rightHandSide));
	}

	/**
	 * Creates an SQL expression that is TRUE when the two polygon2d values
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
	public BooleanExpression overlaps(Polygon2DResult rightHandSide) {
		return new BooleanExpression(new PolygonPolygonWithBooleanResult(this, new Polygon2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DOverlapsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
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
	public BooleanExpression touches(Polygon rightHandSide) {
		return touches(new DBPolygon2D(rightHandSide));
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
	public BooleanExpression touches(Polygon2DResult rightHandSide) {
		return new BooleanExpression(new PolygonPolygonWithBooleanResult(this, new Polygon2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DTouchesTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
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
	public BooleanExpression within(Polygon rightHandSide) {
		return within(new DBPolygon2D(rightHandSide));
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
	public BooleanExpression within(Polygon2DResult rightHandSide) {
		return new BooleanExpression(new PolygonPolygonWithBooleanResult(this, new Polygon2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DWithinTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	@Override
	public NumberExpression measurableDimensions() {
		return new NumberExpression(new Polygon2DFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DMeasurableDimensionsTransform(getFirst().toSQLString(db));
			}
		});
	}

	@Override
	public NumberExpression spatialDimensions() {
		return new NumberExpression(new Polygon2DFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doPolygon2DSpatialDimensionsTransform(getFirst().toSQLString(db));
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
					return db.getDefinition().doPolygon2DHasMagnitudeTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return BooleanExpression.falseExpression().toSQLString(db);
				}
			}
		});
	}

	@Override
	public NumberExpression magnitude() {
		return new NumberExpression(new Polygon2DFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doPolygon2DGetMagnitudeTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return nullExpression().toSQLString(db);
				}
			}
		});
	}

	/**
	 * Returns the area of the polygon expressed in units.
	 *
	 * @return the area covered by the polygon in units.
	 */
	public NumberExpression area() {
		return new NumberExpression(new Polygon2DFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DGetAreaTransform(getFirst().toSQLString(db));
			}
		});
	}

//	public NumberExpression wibbleX() {
//		return new NumberExpression(new Polygon2DFunctionWithNumberResult(this) {
//
//			@Override
//			public String doExpressionTransform(DBDatabase db) {
//				return db.getDefinition().doPolygon2DGetMaxXTransform(getFirst().toSQLString(db));
//			}
//		});
//	}
//
//	public NumberExpression bibbleX() {
//		return new NumberExpression(new Polygon2DFunctionWithNumberResult(this) {
//
//			@Override
//			public String doExpressionTransform(DBDatabase db) {
//				return db.getDefinition().doPolygon2DGetMinXTransform(getFirst().toSQLString(db));
//			}
//		});
//	}
//
//	public NumberExpression wibbleY() {
//		return new NumberExpression(new Polygon2DFunctionWithNumberResult(this) {
//
//			@Override
//			public String doExpressionTransform(DBDatabase db) {
//				return db.getDefinition().doPolygon2DGetMaxYTransform(getFirst().toSQLString(db));
//			}
//		});
//	}
//
//	public NumberExpression bibbleY() {
//		return new NumberExpression(new Polygon2DFunctionWithNumberResult(this) {
//
//			@Override
//			public String doExpressionTransform(DBDatabase db) {
//				return db.getDefinition().doPolygon2DGetMinYTransform(getFirst().toSQLString(db));
//			}
//		});
//	}
	@Override
	public Polygon2DExpression boundingBox() {
		return new Polygon2DExpression(new Polygon2DFunctionWithPolygon2DResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doPolygon2DGetBoundingBoxTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					final Polygon2DExpression first = getFirst();
					final NumberExpression maxX = first.maxX();
					final NumberExpression maxY = first.maxY();
					final NumberExpression minX = first.minX();
					final NumberExpression minY = first.minY();
					return Polygon2DExpression
							.value(minX, minY, maxX, minY, maxX, maxY, minX, maxY, minX, minY)
							.toSQLString(db);
				}
			}
		});
	}

	/**
	 * Return a Line2DExpression representing a line drawn around the outside of
	 * the Polygon2D.
	 *
	 * <p>
	 * The line is coincident with the edge of the polygon but it does not contain
	 * any points within the polygon as it is only a line.
	 *
	 * @return a Line2DExpression
	 */
	public Line2DExpression exteriorRing() {
		Line2DExpression exteriorRingExpr = new Line2DExpression(new Polygon2DFunctionWithLine2DResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DGetExteriorRingTransform(getFirst().toSQLString(db));
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
	public BooleanExpression isNot(Polygon geometry) {
		return this.isNot(Polygon2DExpression.value(geometry));
	}

	@Override
	public BooleanExpression isNot(Polygon2DResult geometry) {
		return this.is(geometry).not();
	}

	@Override
	public StringExpression stringResult() {
		StringExpression stringResultExpr = new StringExpression(new Polygon2DFunctionWithStringResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DAsTextTransform(getFirst().toSQLString(db));
			}
		});
//		return this.measurableDimensions().is(2).ifThenElse(exteriorRingExpr, this);
		return stringResultExpr;
	}

	@Override
	public NumberExpression maxX() {
		NumberExpression expr = new NumberExpression(new Polygon2DFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DGetMaxXTransform(getFirst().toSQLString(db));
			}
		});
		return expr;
	}

	@Override
	public NumberExpression maxY() {
		NumberExpression expr = new NumberExpression(new Polygon2DFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DGetMaxYTransform(getFirst().toSQLString(db));
			}
		});
		return expr;
	}

	@Override
	public NumberExpression minX() {
		NumberExpression expr = new NumberExpression(new Polygon2DFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DGetMinXTransform(getFirst().toSQLString(db));
			}
		});
		return expr;
	}

	@Override
	public NumberExpression minY() {
		NumberExpression expr = new NumberExpression(new Polygon2DFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DGetMinYTransform(getFirst().toSQLString(db));
			}
		});
		return expr;
	}

	@Override
	public DBPolygon2D asExpressionColumn() {
		return new DBPolygon2D(this);
	}

	private static abstract class PolygonPolygonWithBooleanResult extends BooleanExpression {

		private Polygon2DExpression first;
		private Polygon2DExpression second;
		private boolean requiresNullProtection;

		PolygonPolygonWithBooleanResult(Polygon2DExpression first, Polygon2DExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Polygon2DExpression getFirst() {
			return first;
		}

		Polygon2DResult getSecond() {
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

		private Polygon2DExpression first;
//		private Polygon2DExpression second;
		private boolean requiresNullProtection;

		PolygonWithBooleanResult(Polygon2DExpression first) {
			this.first = first;
//			this.second = second;
//			if (this.second == null || this.second.getIncludesNull()) {
//				this.requiresNullProtection = true;
//			}
		}

		Polygon2DExpression getFirst() {
			return first;
		}

//		Polygon2DResult getSecond() {
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

		private Polygon2DExpression first;
		private Point2DExpression second;
		private boolean requiresNullProtection;

		PolygonPointWithBooleanResult(Polygon2DExpression first, Point2DExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Polygon2DExpression getFirst() {
			return first;
		}

		Point2DExpression getSecond() {
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

	private static abstract class Polygon2DFunctionWithNumberResult extends NumberExpression {

		private Polygon2DExpression first;
//		private Polygon2DExpression second;
		private boolean requiresNullProtection;

		Polygon2DFunctionWithNumberResult(Polygon2DExpression first) {
			this.first = first;
//			this.second = second;
//			if (this.second == null || this.second.getIncludesNull()) {
//				this.requiresNullProtection = true;
//			}
		}

		Polygon2DExpression getFirst() {
			return first;
		}

//		Polygon2DResult getSecond() {
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
		public Polygon2DFunctionWithNumberResult copy() {
			Polygon2DFunctionWithNumberResult newInstance;
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

	private static abstract class Polygon2DFunctionWithPolygon2DResult extends Polygon2DExpression {

		private Polygon2DExpression first;
//		private Polygon2DExpression second;
		private boolean requiresNullProtection;

		Polygon2DFunctionWithPolygon2DResult(Polygon2DExpression first) {
			this.first = first;
//			this.second = second;
//			if (this.second == null || this.second.getIncludesNull()) {
//				this.requiresNullProtection = true;
//			}
		}

		Polygon2DExpression getFirst() {
			return first;
		}

//		Polygon2DResult getSecond() {
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
		public Polygon2DFunctionWithPolygon2DResult copy() {
			Polygon2DFunctionWithPolygon2DResult newInstance;
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

	private static abstract class Polygon2DFunctionWithLine2DResult extends Line2DExpression {

		private Polygon2DExpression first;
//		private Polygon2DExpression second;
		private boolean requiresNullProtection;

		Polygon2DFunctionWithLine2DResult(Polygon2DExpression first) {
			this.first = first;
//			this.second = second;
//			if (this.second == null || this.second.getIncludesNull()) {
//				this.requiresNullProtection = true;
//			}
		}

		Polygon2DExpression getFirst() {
			return first;
		}

//		Polygon2DResult getSecond() {
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
		public Polygon2DFunctionWithLine2DResult copy() {
			Polygon2DFunctionWithLine2DResult newInstance;
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

	private static abstract class Polygon2DFunctionWithStringResult extends StringExpression {

		private Polygon2DExpression first;
//		private Polygon2DExpression second;
		private boolean requiresNullProtection;

		Polygon2DFunctionWithStringResult(Polygon2DExpression first) {
			this.first = first;
//			this.second = second;
//			if (this.second == null || this.second.getIncludesNull()) {
//				this.requiresNullProtection = true;
//			}
		}

		Polygon2DExpression getFirst() {
			return first;
		}

//		Polygon2DResult getSecond() {
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
		public Polygon2DFunctionWithStringResult copy() {
			Polygon2DFunctionWithStringResult newInstance;
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

	private static abstract class Point2dArrayFunctionWithPolygon2DResult extends Polygon2DExpression {

		private Point2DExpression[] allPoints;
		private boolean requiresNullProtection;

		Point2dArrayFunctionWithPolygon2DResult(Point2DExpression... all) {
			this.allPoints = all;
			for (Point2DExpression all1 : all) {
				if (all1.getIncludesNull()) {
					this.requiresNullProtection = true;
				}
			}
		}

		Point2DExpression[] getAllPoints() {
			return allPoints;
		}

		@Override
		public final String toSQLString(DBDatabase db) {
			BooleanExpression isNull = BooleanExpression.trueExpression();
			if (this.getIncludesNull()) {
				for (Point2DExpression allPoint : allPoints) {
					isNull = isNull.or(BooleanExpression.isNull(allPoint));
				}
				return isNull.toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		@Override
		public Point2dArrayFunctionWithPolygon2DResult copy() {
			Point2dArrayFunctionWithPolygon2DResult newInstance;
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
				for (Point2DExpression point : allPoints) {
					hashSet.addAll(point.getTablesInvolved());
				}
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			boolean aggregator = false;
			for (Point2DExpression allPoint : allPoints) {
				aggregator |= allPoint.isAggregator();
			}
			return aggregator;//|| second.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

	private static abstract class CoordinateArrayFunctionWithPolygon2DResult extends Polygon2DExpression {

		private NumberExpression[] allCoords;
		private boolean requiresNullProtection;

		CoordinateArrayFunctionWithPolygon2DResult(NumberExpression... all) {
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
		public CoordinateArrayFunctionWithPolygon2DResult copy() {
			CoordinateArrayFunctionWithPolygon2DResult newInstance;
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
