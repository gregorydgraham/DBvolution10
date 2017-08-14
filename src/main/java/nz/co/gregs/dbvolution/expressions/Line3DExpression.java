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
import nz.co.gregs.dbvolution.results.Line3DResult;
import com.vividsolutions.jts.geom.Coordinate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.spatial3D.DBLine3D;
import nz.co.gregs.dbvolution.datatypes.spatial3D.DBMultiPoint3D;
import nz.co.gregs.dbvolution.datatypes.spatial3D.GeometryFactory3D;
import nz.co.gregs.dbvolution.datatypes.spatial3D.LineStringZ;
import nz.co.gregs.dbvolution.datatypes.spatial3D.LinearRingZ;
import nz.co.gregs.dbvolution.datatypes.spatial3D.PointZ;
import nz.co.gregs.dbvolution.datatypes.spatial3D.PolygonZ;
import nz.co.gregs.dbvolution.results.MultiPoint3DResult;

/**
 * Represents SQL expressions that are a 2 dimensional path, as a series of
 * connected line segments with X and Y coordinates.
 *
 *
 *
 * @author Gregory Graham
 */
public class Line3DExpression implements Line3DResult, EqualComparable<Line3DResult>, Spatial3DExpression, ExpressionColumn<DBLine3D> {

	private Line3DResult innerLineString;
	private boolean nullProtectionRequired;

	/**
	 * Default constructor, probably shouldn't be used.
	 *
	 */
	protected Line3DExpression() {
	}

	/**
	 * Create a new Line3DExpression containing the specified value or expression.
	 *
	 * <p>
	 * {@link Line3DResult} classes include {@link DBLine3D} and
	 * {@link Line3DExpression}.
	 *
	 * @param value
	 */
	public Line3DExpression(Line3DResult value) {
		initInnerLine(value, value);
	}

	/**
	 * Perform standard set up and checks when creating the expression.
	 *
	 * @param original the original object
	 * @param value the value derived from the original object
	 */
	protected final void initInnerLine(Object original, Line3DResult value) {
		innerLineString = value;
		if (original == null || innerLineString.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	/**
	 * Create a Line3DExpression representing the line supplied.
	 *
	 * @param line
	 */
	public Line3DExpression(LineStringZ line) {
//		innerLineString = new DBLine3D(line);
		initInnerLine(line, new DBLine3D(line));
	}

	/**
	 * Create a Line3DExpression representing the set of points as a line.
	 *
	 * @param points
	 */
	public Line3DExpression(PointZ... points) {
		GeometryFactory3D geometryFactory = new GeometryFactory3D();
		List<Coordinate> coords = new ArrayList<Coordinate>();
		for (PointZ point : points) {
			coords.add(point.getCoordinate());
		}
		LineStringZ line = geometryFactory.createLineStringZ(coords.toArray(new Coordinate[]{}));
//		innerLineString = new DBLine3D(line);
		initInnerLine(points, new DBLine3D(line));
	}

	/**
	 * Create a Line3DExpression representing the set of coordinates as a line.
	 *
	 * @param coords
	 */
	public Line3DExpression(Coordinate... coords) {
		GeometryFactory3D geometryFactory = new GeometryFactory3D();
		LineStringZ line = geometryFactory.createLineStringZ(coords);
//		innerLineString = new DBLine3D(line);
		initInnerLine(coords, new DBLine3D(line));
	}

	/**
	 * Create a Line3DExpression representing the set of points as a line.
	 *
	 * @param points a series of points that constitute a line.
	 * @return a Line3DExpression
	 */
	public static Line3DExpression value(PointZ... points) {
		return new Line3DExpression(points);
	}

	/**
	 * Create a Line3DExpression representing the set of coordinates as a line.
	 *
	 * @param coords a series of number to be interpreted as X and Y points of a line.
	 * @return a Line3DExpression
	 */
	public static Line3DExpression value(Coordinate... coords) {
		return new Line3DExpression(coords);
	}

	/**
	 * Create a Line3DExpression representing the line.
	 *
	 * @param line create a Line3DExpression from this line.
	 * @return a Line3DExpression
	 */
	public static Line3DExpression value(LineStringZ line) {
		return new Line3DExpression(line);
	}

	/**
	 * Create a Line3DExpression representing the line.
	 *
	 * @param line create a Line3DExpression from this line.
	 * @return a Line3DExpression
	 */
	public static Line3DExpression value(Line3DResult line) {
		return new Line3DExpression(line);
	}

	/**
	 * Create a Line3DExpression representing the {@link MultiPoint3DExpression}
	 * or {@link DBMultiPoint3D} as a line.
	 *
	 * @param multipoint3DExpression a series of point that constitute a line
	 * @return a Line3DExpression
	 */
	public static Line3DExpression value(MultiPoint3DResult multipoint3DExpression) {
		return MultiPoint3DExpression.value(multipoint3DExpression).line3DResult();
	}
	
	@Override
	public StringExpression toWKTFormat(){
		return stringResult();
	}

	@Override
	public DBLine3D getQueryableDatatypeForExpressionValue() {
		return new DBLine3D();
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
	public Line3DExpression copy() {
		return new Line3DExpression(innerLineString);
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof Line3DExpression) {
			Line3DExpression otherExpr = (Line3DExpression) other;
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
	 * Convert the Line3D value into a String expression.
	 *
	 * <p>
	 * This should be the WKT (Well Known Text) version of the line.
	 *
	 * @return a StringExpression of the Line3D in WKT format.
	 */
	@Override
	public StringExpression stringResult() {
		return new StringExpression(new Line3DExpression.LineFunctionWithStringResult(this) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doLine3DAsTextTransform(getFirst().toSQLString(db));
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
	 * @return a BooleanExpression that will be TRUE when the two expressions are functionally equivalent, otherwise FALSE.
	 */
	public BooleanExpression is(LineStringZ rightHandSide) {
		return is(new DBLine3D(rightHandSide));
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
	 * @return a BooleanExpression that will be TRUE when the two expressions are functionally equivalent, otherwise FALSE.
	 */
	public BooleanExpression is(LinearRingZ rightHandSide) {
		return is(new DBLine3D(rightHandSide));
	}

	/**
	 * Compare this expression to the exterior ring of the given Polygon3D using the
	 * equivalent of EQUALS.
	 *
	 * <p>
	 * The boolean expression will be TRUE if the exterior ring and the line  are functionally
	 * equivalent.
	 *
	 * <p>
	 * Due to the imperfect interpretation of floating point numbers there may
	 * some discrepancies between databases but DBV tries to be as accurate as the
	 * database allows.
	 *
	 * @param rightHandSide the polygon whose exterior ring may equal this line
	 * @return a BooleanExpression that will be TRUE when the two expressions are functionally equivalent, otherwise FALSE.
	 */
	public BooleanExpression is(PolygonZ rightHandSide) {
		return is(rightHandSide.getExteriorRingZ());
	}

	/**
	 * Compare the value of the given Line3D to this expression using the
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
	 * @return a BooleanExpression that will be TRUE when the two expressions are functionally equivalent, otherwise FALSE.
	 */
	@Override
	public BooleanExpression is(Line3DResult rightHandSide) {
		return new BooleanExpression(new Line3DExpression.LineLineWithBooleanResult(this, new Line3DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doLine3DEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
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
	 * @return a BooleanExpression that will be FALSE when the two expressions are functionally equivalent, otherwise TRUE.
	 */
	public BooleanExpression isNot(LineStringZ rightHandSide) {
		return isNot(new DBLine3D(rightHandSide));
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
	 * @return a BooleanExpression that will be FALSE when the two expressions are functionally equivalent, otherwise TRUE.
	 */
	public BooleanExpression isNot(LinearRingZ rightHandSide) {
		return isNot(new DBLine3D(rightHandSide));
	}

	/**
	 * Compare this expression to the exterior ring of the given Polygon3D using the
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
	 * @return a BooleanExpression that will be FALSE when the two expressions are functionally equivalent, otherwise TRUE.
	 */
	public BooleanExpression isNot(PolygonZ rightHandSide) {
		return isNot(rightHandSide.getExteriorRingZ());
	}

	/**
	 * Compare the value of the given Line3D to this expression using the
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
	 * @return a BooleanExpression that will be FALSE when the two expressions are functionally equivalent, otherwise TRUE.
	 */
	@Override
	public BooleanExpression isNot(Line3DResult rightHandSide) {
		return new BooleanExpression(new Line3DExpression.LineLineWithBooleanResult(this, new Line3DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					final DBDefinition defn = db.getDefinition();
					return defn.doLine3DNotEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
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
					return db.getDefinition().doLine3DMeasurableDimensionsTransform(getFirst().toSQLString(db));
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
					return db.getDefinition().doLine3DSpatialDimensionsTransform(getFirst().toSQLString(db));
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
					return db.getDefinition().doLine3DHasMagnitudeTransform(getFirst().toSQLString(db));
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
					return db.getDefinition().doLine3DGetMagnitudeTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return nullExpression().toSQLString(db);
				}
			}
		});
	}

	@Override
	public Polygon3DExpression boundingBox() {
		return new Polygon3DExpression(new LineFunctionWithPolygon3DResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doLine3DGetBoundingBoxTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					final Line3DExpression first = getFirst();
					final NumberExpression maxX = first.maxX();
					final NumberExpression maxY = first.maxY();
					final NumberExpression maxZ = first.maxZ();
					final NumberExpression minX = first.minX();
					final NumberExpression minY = first.minY();
					final NumberExpression minZ = first.minZ();
					return Polygon3DExpression.value(
							Point3DExpression.value(minX, minY, minZ),
							Point3DExpression.value(maxX, minY, minZ),
							Point3DExpression.value(maxX, maxY, minZ),
							Point3DExpression.value(maxX, maxY, maxZ),
							Point3DExpression.value(minX, maxY, maxZ),
							Point3DExpression.value(minX, minY, maxZ),
							Point3DExpression.value(minX, minY, minZ)).toSQLString(db);
				}
			}
		});
	}

	/**
	 * Return the maximum X value in the Line3D.
	 *
	 * @return the numeric value of the largest X coordinate for the Line3D.
	 */
	@Override
	public NumberExpression maxX() {

		return new NumberExpression(new LineFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLine3DGetMaxXTransform(getFirst().toSQLString(db));
			}
		});
	}

	/**
	 * Return the minimum X value in the Line3D.
	 *
	 * @return the numeric value of the smallest X coordinate for the Line3D.
	 */
	@Override
	public NumberExpression minX() {

		return new NumberExpression(new LineFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLine3DGetMinXTransform(getFirst().toSQLString(db));
			}
		});
	}

	/**
	 * Return the maximum Y value in the Line3D.
	 *
	 * @return the numeric value of the largest Y coordinate for the Line3D.
	 */
	@Override
	public NumberExpression maxY() {

		return new NumberExpression(new LineFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLine3DGetMaxYTransform(getFirst().toSQLString(db));
			}
		});
	}

	/**
	 * Return the maximum Z value in the Line3D.
	 *
	 * @return the numeric value of the largest Z coordinate for the Line3D.
	 */
	@Override
	public NumberExpression maxZ() {

		return new NumberExpression(new LineFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLine3DGetMaxZTransform(getFirst().toSQLString(db));
			}
		});
	}

	/**
	 * Return the minimum Y value in the Line3D.
	 *
	 * @return the numeric value of the smallest Y coordinate for the Line3D.
	 */
	@Override
	public NumberExpression minY() {

		return new NumberExpression(new LineFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLine3DGetMinYTransform(getFirst().toSQLString(db));
			}
		});
	}

	/**
	 * Return the minimum Z value in the Line3D.
	 *
	 * @return the numeric value of the smallest Z coordinate for the Line3D.
	 */
	@Override
	public NumberExpression minZ() {

		return new NumberExpression(new LineFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLine3DGetMinZTransform(getFirst().toSQLString(db));
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
	 * Use {@link #intersectionPoints(nz.co.gregs.dbvolution.results.Line3DResult) } to find the intersection
	 * points of these lines
	 *
	 * @param points points that constitute a line that might intersect this expression.
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public BooleanExpression intersects(PointZ... points) {
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
	 * Use {@link #intersectionPoints(nz.co.gregs.dbvolution.results.Line3DResult) } to find the intersection
	 * points of these lines
	 *
	 * @param coords a series of X and Y values that constitute a line that might intersect this expression.
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
	 * Use {@link #intersectionPoints(nz.co.gregs.dbvolution.results.Line3DResult) } to find the intersection
	 * points of these lines
	 *
	 * @param lineString a line that might intersect this expression.
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public BooleanExpression intersects(LineStringZ lineString) {
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
	 * Use {@link #intersectionPoints(nz.co.gregs.dbvolution.results.Line3DResult) } to find the intersection
	 * points of these lines
	 *
	 * @param crossingLine a line that might intersect this expression.
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public BooleanExpression intersects(Line3DResult crossingLine) {
		return new BooleanExpression(new LineLineWithBooleanResult(this, new Line3DExpression(crossingLine)) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLine3DIntersectsLine3DTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	/**
	 * Find all the points of intersection between this expression and the specified Line3D expression.
	 *
	 * @param crossingLine a line that might intersect this expression.
	 * @return a MultiPoint3D expression containing all the intersection points of the 2 lines.
	 */
	public MultiPoint3DExpression intersectionPoints(Line3DResult crossingLine) {
		return new MultiPoint3DExpression(new LineLineWithMultiPoint3DResult(this, new Line3DExpression(crossingLine)) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLine3DAllIntersectionPointsWithLine3DTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
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
	 * Use {@link #intersectionPoints(nz.co.gregs.dbvolution.results.Line3DResult) } to find the intersection
	 * points of these lines
	 *
	 * @param crossingLine points that constitute a line that might intersect this expression.
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public Point3DExpression intersectionWith(PointZ... crossingLine) {
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
	 * Use {@link #intersectionPoints(nz.co.gregs.dbvolution.results.Line3DResult) } to find the intersection
	 * points of these lines
	 *
	 * @param crossingLine a series of X and Y values that constitute a line that might intersect this expression.
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public Point3DExpression intersectionWith(Coordinate... crossingLine) {
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
	 * Use {@link #intersectionPoints(nz.co.gregs.dbvolution.results.Line3DResult) } to find the intersection
	 * points of these lines
	 *
	 * @param crossingLine a line that might intersect this expression.
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public Point3DExpression intersectionWith(LineStringZ crossingLine) {
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
	 * Use {@link #intersectionPoints(nz.co.gregs.dbvolution.results.Line3DResult) } to find the intersection
	 * points of these lines
	 *
	 * @param crossingLine points that constitute a line that might intersect this expression.
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public Point3DExpression intersectionWith(MultiPoint3DExpression crossingLine) {
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
	 * Use {@link #intersectionPoints(nz.co.gregs.dbvolution.results.Line3DResult) } to find the intersection
	 * points of these lines
	 *
	 * @param crossingLine a line that might intersect this expression.
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public Point3DExpression intersectionWith(Line3DResult crossingLine) {
		return new Point3DExpression(new LineLineWithPoint3DResult(this, new Line3DExpression(crossingLine)) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLine3DIntersectionPointWithLine3DTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	/**
	 * Provides a expression that represents the line3D value as a polygon3D
	 * value.
	 *
	 * <P>
	 * Points are added to the polygon in index order. If necessary the polygon is
	 * closed by adding the first point to the end.
	 *
	 * <p>
	 * Line3D values with less than 3 points will return NULL values.
	 *
	 * @return a polygon3D expression
	 */
	/* TODO implement public Polygon3DExpression polygon3DResult() {*/
	public Polygon3DExpression polygon3DResult() {
		throw new UnsupportedOperationException("NOT DONE YET, SORRY.");
	}

	@Override
	public DBLine3D asExpressionColumn() {
		return new DBLine3D(this);
	}

	private static abstract class LineLineWithBooleanResult extends BooleanExpression {

		private Line3DExpression first;
		private Line3DExpression second;
		private boolean requiresNullProtection;

		LineLineWithBooleanResult(Line3DExpression first, Line3DExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Line3DExpression getFirst() {
			return first;
		}

		Line3DExpression getSecond() {
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
			HashSet<DBRow> hashSet = new HashSet<>();
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

		private Line3DExpression first;
		private boolean requiresNullProtection;

		LineWithBooleanResult(Line3DExpression first) {
			this.first = first;
		}

		Line3DExpression getFirst() {
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
			HashSet<DBRow> hashSet = new HashSet<>();
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

	private static abstract class LineLineWithPoint3DResult extends Point3DExpression {

		private Line3DExpression first;
		private Line3DExpression second;
		private boolean requiresNullProtection;

		LineLineWithPoint3DResult(Line3DExpression first, Line3DExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Line3DExpression getFirst() {
			return first;
		}

		Line3DExpression getSecond() {
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
		public LineLineWithPoint3DResult copy() {
			LineLineWithPoint3DResult newInstance;
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
			HashSet<DBRow> hashSet = new HashSet<>();
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

	private static abstract class LineLineWithMultiPoint3DResult extends MultiPoint3DExpression {

		private Line3DExpression first;
		private Line3DExpression second;
		private boolean requiresNullProtection;

		LineLineWithMultiPoint3DResult(Line3DExpression first, Line3DExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Line3DExpression getFirst() {
			return first;
		}

		Line3DExpression getSecond() {
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
		public LineLineWithMultiPoint3DResult copy() {
			LineLineWithMultiPoint3DResult newInstance;
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
			HashSet<DBRow> hashSet = new HashSet<>();
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

		private Line3DExpression first;
//		private Point3DExpression second;
		private boolean requiresNullProtection;

		LineFunctionWithNumberResult(Line3DExpression first) {
			this.first = first;
//			this.second = second;
			if (this.first == null) {// || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Line3DExpression getFirst() {
			return first;
		}

//		Point3DExpression getSecond() {
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
			HashSet<DBRow> hashSet = new HashSet<>();
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

		private Line3DExpression first;
		private boolean requiresNullProtection;

		LineFunctionWithStringResult(Line3DExpression first) {
			this.first = first;
			if (this.first == null) {// || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Line3DExpression getFirst() {
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
			HashSet<DBRow> hashSet = new HashSet<>();
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

	private static abstract class LineFunctionWithPolygon3DResult extends Polygon3DExpression {

		private Line3DExpression first;
//		private Point3DExpression second;
		private boolean requiresNullProtection;

		LineFunctionWithPolygon3DResult(Line3DExpression first) {
			this.first = first;
//			this.second = second;
			if (this.first == null) {// || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Line3DExpression getFirst() {
			return first;
		}

//		Point3DExpression getSecond() {
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
		public LineFunctionWithPolygon3DResult copy() {
			LineFunctionWithPolygon3DResult newInstance;
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
			HashSet<DBRow> hashSet = new HashSet<>();
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
