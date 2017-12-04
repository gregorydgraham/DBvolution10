/*
 * Copyright 2015 greg.
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

import nz.co.gregs.dbvolution.results.EqualComparable;
import nz.co.gregs.dbvolution.results.Line2DResult;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBLine2D;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBMultiPoint2D;
import nz.co.gregs.dbvolution.results.MultiPoint2DResult;

/**
 * Represents SQL expressions that are a 2 dimensional path, as a series of
 * connected line segments with X and Y coordinates.
 *
 *
 *
 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @author Gregory Graham
 */
public class Line2DExpression implements Line2DResult, EqualComparable<Line2DResult>, Spatial2DExpression, ExpressionColumn<DBLine2D> {

	private Line2DResult innerLineString;
	private boolean nullProtectionRequired;

	/**
	 * Default constructor, probably shouldn't be used.
	 *
	 */
	protected Line2DExpression() {
	}

	/**
	 * Create a new Line2DExpression containing the specified value or expression.
	 *
	 * <p>
	 * {@link Line2DResult} classes include {@link DBLine2D} and
	 * {@link Line2DExpression}.
	 *
	 * @param value
	 */
	public Line2DExpression(Line2DResult value) {
		initInnerLine(value, value);
	}

	/**
	 * Perform standard set up and checks when creating the expression.
	 *
	 * @param original the original object
	 * @param value the value derived from the original object
	 */
	protected final void initInnerLine(Object original, Line2DResult value) {
		innerLineString = value;
		if (original == null || innerLineString.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	/**
	 * Create a Line2DExpression representing the line supplied.
	 *
	 * @param line
	 */
	public Line2DExpression(LineString line) {
//		innerLineString = new DBLine2D(line);
		initInnerLine(line, new DBLine2D(line));
	}

	/**
	 * Create a Line2DExpression representing the set of points as a line.
	 *
	 * @param points
	 */
	public Line2DExpression(Point... points) {
		GeometryFactory geometryFactory = new GeometryFactory();
		List<Coordinate> coords = new ArrayList<Coordinate>();
		for (Point point : points) {
			coords.add(point.getCoordinate());
		}
		LineString line = geometryFactory.createLineString(coords.toArray(new Coordinate[]{}));
//		innerLineString = new DBLine2D(line);
		initInnerLine(points, new DBLine2D(line));
	}

	/**
	 * Create a Line2DExpression representing the set of coordinates as a line.
	 *
	 * @param coords
	 */
	public Line2DExpression(Coordinate... coords) {
		GeometryFactory geometryFactory = new GeometryFactory();
		LineString line = geometryFactory.createLineString(coords);
//		innerLineString = new DBLine2D(line);
		initInnerLine(coords, new DBLine2D(line));
	}

	/**
	 * Create a Line2DExpression representing the set of points as a line.
	 *
	 * @param points a series of points that constitute a line.
	 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a Line2DExpression
	 */
	public static Line2DExpression value(Point... points) {
		return new Line2DExpression(points);
	}

	/**
	 * Create a Line2DExpression representing the set of coordinates as a line.
	 *
	 * @param coords a series of number to be interpreted as X and Y points of a line.
	 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a Line2DExpression
	 */
	public static Line2DExpression value(Coordinate... coords) {
		return new Line2DExpression(coords);
	}

	/**
	 * Create a Line2DExpression representing the line.
	 *
	 * @param line create a Line2DExpression from this line.
	 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a Line2DExpression
	 */
	public static Line2DExpression value(LineString line) {
		return new Line2DExpression(line);
	}

	/**
	 * Create a Line2DExpression representing the line.
	 *
	 * @param line create a Line2DExpression from this line.
	 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a Line2DExpression
	 */
	public static Line2DExpression value(Line2DResult line) {
		return new Line2DExpression(line);
	}

	/**
	 * Create a Line2DExpression representing the {@link MultiPoint2DExpression}
	 * or {@link DBMultiPoint2D} as a line.
	 *
	 * @param multipoint2DExpression a series of point that constitute a line
	 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a Line2DExpression
	 */
	public static Line2DExpression value(MultiPoint2DResult multipoint2DExpression) {
		return MultiPoint2DExpression.value(multipoint2DExpression).line2DResult();
	}
	
	@Override
	public StringExpression toWKTFormat(){
		return stringResult();
	}

	@Override
	public DBLine2D getQueryableDatatypeForExpressionValue() {
		return new DBLine2D();
	}

	@Override
	public String toSQLString(DBDatabase db) {
		if (innerLineString == null) {
			return db.getDefinition().getNull();
		} else {
			return innerLineString.toSQLString(db);
		}
	}

	@Override
	public Line2DExpression copy() {
		return new Line2DExpression(innerLineString);
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof Line2DExpression) {
			Line2DExpression otherExpr = (Line2DExpression) other;
			return this.innerLineString == otherExpr.innerLineString;
		}
		return false;
	}

	@Override
	public int hashCode() {
		int hash = 5;
		hash = 37 * hash + (this.innerLineString != null ? this.innerLineString.hashCode() : 0);
		hash = 37 * hash + (this.nullProtectionRequired ? 1 : 0);
		return hash;
	}

	@Override
	public boolean isAggregator() {
		return innerLineString == null ? false : innerLineString.isAggregator();
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		HashSet<DBRow> hashSet = new HashSet<DBRow>();
		if (innerLineString != null) {
			hashSet.addAll(innerLineString.getTablesInvolved());
		}
		return hashSet;
	}

	@Override
	public boolean isPurelyFunctional() {
		return innerLineString == null ? true : innerLineString.isPurelyFunctional();
	}

	@Override
	public boolean getIncludesNull() {
		return nullProtectionRequired;
	}

	/**
	 * Convert the Line2D value into a String expression.
	 *
	 * <p>
	 * This should be the WKT (Well Known Text) version of the line.
	 *
	 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a StringExpression of the Line2D in WKT format.
	 */
	@Override
	public StringExpression stringResult() {
		return new StringExpression(new Line2DExpression.LineFunctionWithStringResult(this) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doLine2DAsTextTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return getFirst().toSQLString(db);
				}
			}
		});
	}

	/**
	 * Compare the value of the given LineString to this expression using the
	 * equivalent of EQUALS.
	 *
	 * <p>
	 * The boolean expression will be TRUE if the two expressions are functionally
	 * equivalent.
	 *
	 * <p>
	 * Due to to the imperfect interpretation of floating point numbers there may
	 * some discrepancies between databases but DBV tries to be as accurate as the
	 * database allows.
	 *
	 * @param rightHandSide the value this expression may equal.
	 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be TRUE when the two expressions are functionally equivalent, otherwise FALSE.
	 */
	public BooleanExpression is(LineString rightHandSide) {
		return is(new DBLine2D(rightHandSide));
	}

	/**
	 * Compare this expression to the exterior ring of the given Polygon2D using the
	 * equivalent of EQUALS.
	 *
	 * <p>
	 * The boolean expression will be TRUE if the exterior ring and the line  are functionally
	 * equivalent.
	 *
	 * <p>
	 * Due to to the imperfect interpretation of floating point numbers there may
	 * some discrepancies between databases but DBV tries to be as accurate as the
	 * database allows.
	 *
	 * @param rightHandSide the polygon whose exterior ring may equal this line
	 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be TRUE when the two expressions are functionally equivalent, otherwise FALSE.
	 */
	public BooleanExpression is(Polygon rightHandSide) {
		return is(rightHandSide.getExteriorRing());
	}

	/**
	 * Compare the value of the given Line2D to this expression using the
	 * equivalent of EQUALS.
	 *
	 * <p>
	 * The boolean expression will be TRUE if the two expressions are functionally
	 * equivalent.
	 *
	 * <p>
	 * Due to to the imperfect interpretation of floating point numbers there may
	 * some discrepancies between databases but DBV tries to be as accurate as the
	 * database allows.
	 *
	 * @param rightHandSide the line that this expression might equal
	 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be TRUE when the two expressions are functionally equivalent, otherwise FALSE.
	 */
	@Override
	public BooleanExpression is(Line2DResult rightHandSide) {
		return new BooleanExpression(new Line2DExpression.LineLineWithBooleanResult(this, new Line2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doLine2DEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return getFirst().stringResult().is(getSecond().stringResult()).toSQLString(db);
				}
			}
		});
	}

	/**
	 * Compare the value of the given LineString to this expression using the
	 * equivalent of NOT EQUALS.
	 *
	 * <p>
	 * The boolean expression will be FALSE if the two expressions are functionally
	 * equivalent.
	 *
	 * <p>
	 * Due to to the imperfect interpretation of floating point numbers there may
	 * some discrepancies between databases but DBV tries to be as accurate as the
	 * database allows.
	 *
	 * @param rightHandSide the line that this expression might equal
	 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be FALSE when the two expressions are functionally equivalent, otherwise TRUE.
	 */
	public BooleanExpression isNot(LineString rightHandSide) {
		return isNot(new DBLine2D(rightHandSide));
	}

	/**
	 * Compare this expression to the exterior ring of the given Polygon2D using the
	 * equivalent of NOT EQUALS.
	 *
	 * <p>
	 * The boolean expression will be FALSE if the exterior ring and the line  are functionally
	 * equivalent.
	 *
	 * <p>
	 * Due to to the imperfect interpretation of floating point numbers there may
	 * some discrepancies between databases but DBV tries to be as accurate as the
	 * database allows.
	 *
	 * @param rightHandSide the polygon whose exterior ring might not equal this expression's value.
	 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be FALSE when the two expressions are functionally equivalent, otherwise TRUE.
	 */
	public BooleanExpression isNot(Polygon rightHandSide) {
		return isNot(rightHandSide.getExteriorRing());
	}

	/**
	 * Compare the value of the given Line2D to this expression using the
	 * equivalent of NOT EQUALS.
	 *
	 * <p>
	 * The boolean expression will be FALSE if the two expressions are functionally
	 * equivalent.
	 *
	 * <p>
	 * Due to to the imperfect interpretation of floating point numbers there may
	 * some discrepancies between databases but DBV tries to be as accurate as the
	 * database allows.
	 *
	 * @param rightHandSide the line to compare to this expression.
	 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be FALSE when the two expressions are functionally equivalent, otherwise TRUE.
	 */
	@Override
	public BooleanExpression isNot(Line2DResult rightHandSide) {
		return new BooleanExpression(new Line2DExpression.LineLineWithBooleanResult(this, new Line2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					final DBDefinition defn = db.getDefinition();
					return defn.doLine2DNotEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return getFirst().stringResult().is(getSecond().stringResult()).not().toSQLString(db);
				}
			}
		});
	}

	@Override
	public NumberExpression measurableDimensions() {
		return new NumberExpression(new LineFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doLine2DMeasurableDimensionsTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return NumberExpression.value(1).toSQLString(db);
				}
			}
		});
	}

	@Override
	public NumberExpression spatialDimensions() {
		return new NumberExpression(new LineFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doLine2DSpatialDimensionsTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return NumberExpression.value(2).toSQLString(db);
				}
			}
		});
	}

	@Override
	public BooleanExpression hasMagnitude() {
		return new BooleanExpression(new LineWithBooleanResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doLine2DHasMagnitudeTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return BooleanExpression.falseExpression().toSQLString(db);
				}
			}
		});
	}

	@Override
	public NumberExpression magnitude() {
		return new NumberExpression(new LineFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doLine2DGetMagnitudeTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return nullExpression().toSQLString(db);
				}
			}
		});
	}

	@Override
	public Polygon2DExpression boundingBox() {
		return new Polygon2DExpression(new LineFunctionWithPolygon2DResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doLine2DGetBoundingBoxTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					final Line2DExpression first = getFirst();
					final NumberExpression maxX = first.maxX();
					final NumberExpression maxY = first.maxY();
					final NumberExpression minX = first.minX();
					final NumberExpression minY = first.minY();
					return Polygon2DExpression.value(
							Point2DExpression.value(minX, minY),
							Point2DExpression.value(maxX, minY),
							Point2DExpression.value(maxX, maxY),
							Point2DExpression.value(minX, maxY),
							Point2DExpression.value(minX, minY)).toSQLString(db);
				}
			}
		});
	}

	/**
	 * Return the maximum X value in the Line2D.
	 *
	 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the numeric value of the largest X coordinate for the Line2D.
	 */
	@Override
	public NumberExpression maxX() {

		return new NumberExpression(new LineFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLine2DGetMaxXTransform(getFirst().toSQLString(db));
			}
		});
	}

	/**
	 * Return the minimum X value in the Line2D.
	 *
	 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the numeric value of the smallest X coordinate for the Line2D.
	 */
	@Override
	public NumberExpression minX() {

		return new NumberExpression(new LineFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLine2DGetMinXTransform(getFirst().toSQLString(db));
			}
		});
	}

	/**
	 * Return the maximum Y value in the Line2D.
	 *
	 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the numeric value of the largest Y coordinate for the Line2D.
	 */
	@Override
	public NumberExpression maxY() {

		return new NumberExpression(new LineFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLine2DGetMaxYTransform(getFirst().toSQLString(db));
			}
		});
	}

	/**
	 * Return the minimum Y value in the Line2D.
	 *
	 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the numeric value of the smallest Y coordinate for the Line2D.
	 */
	@Override
	public NumberExpression minY() {

		return new NumberExpression(new LineFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLine2DGetMinYTransform(getFirst().toSQLString(db));
			}
		});
	}

	/**
	 * Tests whether this line and the line represented by the points ever cross.
	 *
	 * <p>
	 * Multiple line segments means it may cross at several points, however this
	 * method only reports TRUE or FALSE.
	 *
	 * <p>
	 * Use {@link #intersectionPoints(nz.co.gregs.dbvolution.results.Line2DResult) } to find the intersection
	 * points of these lines
	 *
	 * @param points points that constitute a line that might intersect this expression.
	 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public BooleanExpression intersects(Point... points) {
		return this.intersects(value(points));
	}

	/**
	 * Tests whether this line and the line represented by the coordinates ever
	 * cross.
	 *
	 * <p>
	 * Multiple line segments means it may cross at several points, however this
	 * method only reports TRUE or FALSE.
	 *
	 * <p>
	 * Use {@link #intersectionPoints(nz.co.gregs.dbvolution.results.Line2DResult) } to find the intersection
	 * points of these lines
	 *
	 * @param coords a series of X and Y values that constitute a line that might intersect this expression.
	 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public BooleanExpression intersects(Coordinate... coords) {
		return this.intersects(value(coords));
	}

	/**
	 * Tests whether this line and the other line ever cross.
	 *
	 * <p>
	 * Multiple line segments means it may cross at several points, however this
	 * method only reports TRUE or FALSE.
	 *
	 * <p>
	 * Use {@link #intersectionPoints(nz.co.gregs.dbvolution.results.Line2DResult) } to find the intersection
	 * points of these lines
	 *
	 * @param lineString a line that might intersect this expression.
	 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public BooleanExpression intersects(LineString lineString) {
		return this.intersects(value(lineString));
	}

	/**
	 * Tests whether this line and the other line ever cross.
	 *
	 * <p>
	 * Multiple line segments means it may cross at several points, however this
	 * method only reports TRUE or FALSE.
	 *
	 * <p>
	 * Use {@link #intersectionPoints(nz.co.gregs.dbvolution.results.Line2DResult) } to find the intersection
	 * points of these lines
	 *
	 * @param crossingLine a line that might intersect this expression.
	 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public BooleanExpression intersects(Line2DResult crossingLine) {
		return new BooleanExpression(new LineLineWithBooleanResult(this, new Line2DExpression(crossingLine)) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLine2DIntersectsLine2DTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	/**
	 * Find all the points of intersection between this expression and the specified Line2D expression.
	 *
	 * @param crossingLine a line that might intersect this expression.
	 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a MultiPoint2D expression containing all the intersection points of the 2 lines.
	 */
	public MultiPoint2DExpression intersectionPoints(Line2DResult crossingLine) {
		return new MultiPoint2DExpression(new LineLineWithMultiPoint2DResult(this, new Line2DExpression(crossingLine)) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLine2DAllIntersectionPointsWithLine2DTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	/**
	 * Find a point where this line and the other line (represented as a series of
	 * points) cross.
	 *
	 * <p>
	 * Multiple line segments means it may cross at several points, however this
	 * method only reports the first point found.
	 *
	 * <p>
	 * Use {@link #intersectionPoints(nz.co.gregs.dbvolution.results.Line2DResult) } to find the intersection
	 * points of these lines
	 *
	 * @param crossingLine points that constitute a line that might intersect this expression.
	 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public Point2DExpression intersectionWith(Point... crossingLine) {
		return intersectionWith(value(crossingLine));
	}

	/**
	 * Find a point where this line and the other line (represented as a series of
	 * coordinates) cross.
	 *
	 * <p>
	 * Multiple line segments means it may cross at several points, however this
	 * method only reports the first point found.
	 *
	 * <p>
	 * Use {@link #intersectionPoints(nz.co.gregs.dbvolution.results.Line2DResult) } to find the intersection
	 * points of these lines
	 *
	 * @param crossingLine a series of X and Y values that constitute a line that might intersect this expression.
	 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public Point2DExpression intersectionWith(Coordinate... crossingLine) {
		return intersectionWith(value(crossingLine));
	}

	/**
	 * Find a point where this line and the other line cross.
	 *
	 * <p>
	 * Multiple line segments means it may cross at several points, however this
	 * method only reports the first point found.
	 *
	 * <p>
	 * Use {@link #intersectionPoints(nz.co.gregs.dbvolution.results.Line2DResult) } to find the intersection
	 * points of these lines
	 *
	 * @param crossingLine a line that might intersect this expression.
	 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public Point2DExpression intersectionWith(LineString crossingLine) {
		return intersectionWith(value(crossingLine));
	}

	/**
	 * Find a point where this line and the line derived from the MultiPoint
	 * cross.
	 *
	 * <p>
	 * Multiple line segments means it may cross at several points, however this
	 * method only reports the first point found.
	 *
	 * <p>
	 * Use {@link #intersectionPoints(nz.co.gregs.dbvolution.results.Line2DResult) } to find the intersection
	 * points of these lines
	 *
	 * @param crossingLine points that constitute a line that might intersect this expression.
	 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public Point2DExpression intersectionWith(MultiPoint2DExpression crossingLine) {
		return intersectionWith(value(crossingLine));
	}

	/**
	 * Find a point where this line and the other line cross.
	 *
	 * <p>
	 * Multiple line segments means it may cross at several points, however this
	 * method only reports the first point found.
	 *
	 * <p>
	 * Use {@link #intersectionPoints(nz.co.gregs.dbvolution.results.Line2DResult) } to find the intersection
	 * points of these lines
	 *
	 * @param crossingLine a line that might intersect this expression.
	 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public Point2DExpression intersectionWith(Line2DResult crossingLine) {
		return new Point2DExpression(new LineLineWithPoint2DResult(this, new Line2DExpression(crossingLine)) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLine2DIntersectionPointWithLine2DTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	/**
	 * Provides a expression that represents the line2d value as a polygon2d
	 * value.
	 *
	 * <P>
	 * Points are added to the polygon in index order. If necessary the polygon is
	 * closed by adding the first point to the end.
	 *
	 * <p>
	 * Line2D values with less than 3 points will return NULL values.
	 *
	 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a polygon2d expression
	 */
	/* TODO implement public Polygon2DExpression polygon2DResult() {*/
	public Polygon2DExpression polygon2DResult() {
		throw new UnsupportedOperationException("NOT DONE YET, SORRY.");
	}

	@Override
	public DBLine2D asExpressionColumn() {
		return new DBLine2D(this);
	}

	private static abstract class LineLineWithBooleanResult extends BooleanExpression {

		private Line2DExpression first;
		private Line2DExpression second;
		private boolean requiresNullProtection;

		LineLineWithBooleanResult(Line2DExpression first, Line2DExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Line2DExpression getFirst() {
			return first;
		}

		Line2DExpression getSecond() {
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
		public LineLineWithBooleanResult copy() {
			LineLineWithBooleanResult newInstance;
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

	private static abstract class LineWithBooleanResult extends BooleanExpression {

		private Line2DExpression first;
		private boolean requiresNullProtection;

		LineWithBooleanResult(Line2DExpression first) {
			this.first = first;
		}

		Line2DExpression getFirst() {
			return first;
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
		public LineWithBooleanResult copy() {
			LineWithBooleanResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
			return newInstance;
		}

		protected abstract String doExpressionTransform(DBDatabase db);

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (first != null) {
				hashSet.addAll(first.getTablesInvolved());
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return first.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

	private static abstract class LineLineWithPoint2DResult extends Point2DExpression {

		private Line2DExpression first;
		private Line2DExpression second;
		private boolean requiresNullProtection;

		LineLineWithPoint2DResult(Line2DExpression first, Line2DExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Line2DExpression getFirst() {
			return first;
		}

		Line2DExpression getSecond() {
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
		public LineLineWithPoint2DResult copy() {
			LineLineWithPoint2DResult newInstance;
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

	private static abstract class LineLineWithMultiPoint2DResult extends MultiPoint2DExpression {

		private Line2DExpression first;
		private Line2DExpression second;
		private boolean requiresNullProtection;

		LineLineWithMultiPoint2DResult(Line2DExpression first, Line2DExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Line2DExpression getFirst() {
			return first;
		}

		Line2DExpression getSecond() {
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
		public LineLineWithMultiPoint2DResult copy() {
			LineLineWithMultiPoint2DResult newInstance;
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

	private static abstract class LineFunctionWithNumberResult extends NumberExpression {

		private Line2DExpression first;
//		private Point2DExpression second;
		private boolean requiresNullProtection;

		LineFunctionWithNumberResult(Line2DExpression first) {
			this.first = first;
//			this.second = second;
			if (this.first == null) {// || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Line2DExpression getFirst() {
			return first;
		}

//		Point2DExpression getSecond() {
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
		public LineFunctionWithNumberResult copy() {
			LineFunctionWithNumberResult newInstance;
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

	private static abstract class LineFunctionWithStringResult extends StringExpression {

		private Line2DExpression first;
		private boolean requiresNullProtection;

		LineFunctionWithStringResult(Line2DExpression first) {
			this.first = first;
			if (this.first == null) {// || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Line2DExpression getFirst() {
			return first;
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
		public LineFunctionWithStringResult copy() {
			LineFunctionWithStringResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
			return newInstance;
		}

		protected abstract String doExpressionTransform(DBDatabase db);

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (first != null) {
				hashSet.addAll(first.getTablesInvolved());
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return first.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

	private static abstract class LineFunctionWithPolygon2DResult extends Polygon2DExpression {

		private Line2DExpression first;
//		private Point2DExpression second;
		private boolean requiresNullProtection;

		LineFunctionWithPolygon2DResult(Line2DExpression first) {
			this.first = first;
//			this.second = second;
			if (this.first == null) {// || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Line2DExpression getFirst() {
			return first;
		}

//		Point2DExpression getSecond() {
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
		public LineFunctionWithPolygon2DResult copy() {
			LineFunctionWithPolygon2DResult newInstance;
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

}
