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
package nz.co.gregs.dbvolution.expressions.spatial2D;

import nz.co.gregs.dbvolution.results.Line2DResult;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBLine2D;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBMultiPoint2D;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.results.AnyResult;
import nz.co.gregs.dbvolution.results.MultiPoint2DResult;

/**
 * Represents SQL expressions that are a 2 dimensional path, as a series of
 * connected line segments with X and Y coordinates.
 *
 *
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class Line2DExpression extends Spatial2DExpression<LineString, Line2DResult, DBLine2D> implements Line2DResult {

	private final static long serialVersionUID = 1l;

	private final boolean moreNullProtectionRequired;

	/**
	 * Default constructor, probably shouldn't be used.
	 *
	 */
	protected Line2DExpression() {
		super();
		moreNullProtectionRequired = false;
	}

	/**
	 * Create a new Line2DExpression containing the specified value or value.
	 *
	 * <p>
	 * {@link Line2DResult} classes include {@link DBLine2D} and
	 * {@link Line2DExpression}.
	 *
	 * @param value
	 */
	public Line2DExpression(Line2DResult value) {
		super(value);
		moreNullProtectionRequired = value == null;
	}

	/**
	 * Create a new Line2DExpression containing the specified value or value.
	 *
	 * <p>
	 * {@link Line2DResult} classes include {@link DBLine2D} and
	 * {@link Line2DExpression}.
	 *
	 * @param value
	 */
	protected Line2DExpression(AnyResult<?> value) {
		super(value);
		moreNullProtectionRequired = value == null;
	}

	/**
	 * Create a Line2DExpression representing the line supplied.
	 *
	 * @param line
	 */
	public Line2DExpression(LineString line) {
		super(new DBLine2D(line));
		moreNullProtectionRequired = line == null;
	}

	/**
	 * Create a Line2DExpression representing the set of points as a line.
	 *
	 * @param points
	 */
	public Line2DExpression(Point... points) {
		super(new DBLine2D(points));
		boolean nulls = false;
//		for (Point point : points) {
//			nulls = point == null ? true : nulls;
//		}
		moreNullProtectionRequired
				= points == null
				|| points.length == 0
				|| nulls
				|| new DBLine2D(points).getIncludesNull();
	}

	/**
	 * Create a Line2DExpression representing the set of coordinates as a line.
	 *
	 * @param coords
	 */
	public Line2DExpression(Coordinate... coords) {
		super(new DBLine2D(coords));
		boolean nulls = false;
//		for (Coordinate point : coords) {
//			nulls = point == null ? true : nulls;
//		}
		moreNullProtectionRequired
				= coords == null
				|| coords.length == 0
				|| nulls
				|| new DBLine2D(coords).getIncludesNull();
	}

	@Override
	public boolean getIncludesNull() {
		return moreNullProtectionRequired
				|| super.getIncludesNull();
	}

	/**
	 * Creates an expression that will return the most common value of the column
	 * supplied.
	 *
	 * <p>
	 * MODE: The number which appears most often in a set of numbers. For example:
	 * in {6, 3, 9, 6, 6, 5, 9, 3} the Mode is 6.</p>
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a number expression.
	 */
	public Line2DExpression modeSimple() {
		@SuppressWarnings("unchecked")
		Line2DExpression modeExpr = new Line2DExpression(
				new ModeSimpleExpression<>(this));

		return modeExpr;
	}

	/**
	 * Creates an expression that will return the most common value of the column
	 * supplied.
	 *
	 * <p>
	 * MODE: The number which appears most often in a set of numbers. For example:
	 * in {6, 3, 9, 6, 6, 5, 9, 3} the Mode is 6.</p>
	 *
	 * <p>
	 * This version of Mode implements a stricter definition that will return null
	 * if the mode is undefined. The mode can be undefined if there are 2 or more
	 * values with the highest frequency value. </p>
	 *
	 * <p>
	 * For example in the list {0,0,0,0,1,1,2,2,2,2,3,4} both 0 and 2 occur four
	 * times and no other value occurs more frequently so the mode is undefined.
	 * {@link #modeSimple() The modeSimple()} method would return either 0 or 2
	 * randomly for the same set.</p>
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the mode or null if undefined.
	 */
	public Line2DExpression modeStrict() {
		@SuppressWarnings("unchecked")
		Line2DExpression modeExpr = new Line2DExpression(
				new ModeStrictExpression<>(this));

		return modeExpr;
	}

	/**
	 * Create a Line2DExpression representing the set of points as a line.
	 *
	 * @param points a series of points that constitute a line.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a Line2DExpression
	 */
	public Line2DExpression expression(Point... points) {
		return new Line2DExpression(points);
	}

	public static Line2DExpression value(Point... points) {
		return new Line2DExpression(points);
	}

	/**
	 * Create a Line2DExpression representing the set of coordinates as a line.
	 *
	 * @param coords a series of number to be interpreted as X and Y points of a
	 * line.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a Line2DExpression
	 */
	public Line2DExpression expression(Coordinate... coords) {
		return new Line2DExpression(coords);
	}

	public static Line2DExpression value(Coordinate... coords) {
		return new Line2DExpression(coords);
	}

	/**
	 * Create a Line2DExpression representing the line.
	 *
	 * @param line create a Line2DExpression from this line.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a Line2DExpression
	 */
	@Override
	public Line2DExpression expression(LineString line) {
		return new Line2DExpression(line);
	}

	/**
	 * Create a Line2DExpression representing the line.
	 *
	 * @param line create a Line2DExpression from this line.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a Line2DExpression
	 */
	@Override
	public Line2DExpression expression(Line2DResult line) {
		return new Line2DExpression(line);
	}

	public static Line2DExpression value(Line2DResult line) {
		return new Line2DExpression(line);
	}

	/**
	 * Create a Line2DExpression representing the {@link MultiPoint2DExpression}
	 * or {@link DBMultiPoint2D} as a line.
	 *
	 * @param multipoint2DExpression a series of point that constitute a line
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a Line2DExpression
	 */
	public Line2DExpression expression(MultiPoint2DResult multipoint2DExpression) {
		return value(multipoint2DExpression);
	}

	public static Line2DExpression value(MultiPoint2DResult multipoint2DExpression) {
		return MultiPoint2DExpression.value(multipoint2DExpression).line2DResult();
	}

	@Override
	public Line2DExpression nullExpression() {

		return new NullExpression();
	}

	@Override
	public Line2DExpression expression(DBLine2D value) {
		return new Line2DExpression(value);
	}

	@Override
	public StringExpression toWKTFormat() {
		return stringResult();
	}

	@Override
	public DBLine2D getQueryableDatatypeForExpressionValue() {
		return new DBLine2D();
	}

	@Override
	protected boolean isNullSafetyTerminator() {
		return super.isNullSafetyTerminator()
				&& moreNullProtectionRequired == false;
	}

	@Override
	public Line2DExpression copy() {
		return isNullSafetyTerminator()
				? new Line2DExpression()
				: getInnerResult() == null
						? nullLine2D()
						: new Line2DExpression((AnyResult<?>) getInnerResult().copy());
	}

	/**
	 * Convert the Line2D value into a String value.
	 *
	 * <p>
	 * This should be the WKT (Well Known Text) version of the line.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a StringExpression of the Line2D in WKT format.
	 */
	@Override
	public StringExpression stringResult() {
		return new StringExpression(new StringResultExpression(this));
	}

	/**
	 * Compare the value of the given LineString to this value using the
	 * equivalent of EQUALS.
	 *
	 * <p>
	 * The boolean value will be TRUE if the two expressions are functionally
	 * equivalent.
	 *
	 * <p>
	 * Due to to the imperfect interpretation of floating point numbers there may
	 * some discrepancies between databases but DBV tries to be as accurate as the
	 * database allows.
	 *
	 * @param rightHandSide the value this value may equal.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be TRUE when the two expressions are
	 * functionally equivalent, otherwise FALSE.
	 */
	@Override
	public BooleanExpression is(LineString rightHandSide) {
		return is(new DBLine2D(rightHandSide));
	}

	/**
	 * Compare this value to the exterior ring of the given Polygon2D using the
	 * equivalent of EQUALS.
	 *
	 * <p>
	 * The boolean value will be TRUE if the exterior ring and the line are
	 * functionally equivalent.
	 *
	 * <p>
	 * Due to to the imperfect interpretation of floating point numbers there may
	 * some discrepancies between databases but DBV tries to be as accurate as the
	 * database allows.
	 *
	 * @param rightHandSide the polygon whose exterior ring may equal this line
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be TRUE when the two expressions are
	 * functionally equivalent, otherwise FALSE.
	 */
	public BooleanExpression is(Polygon rightHandSide) {
		return is(rightHandSide.getExteriorRing());
	}

	/**
	 * Compare the value of the given Line2D to this value using the equivalent of
	 * EQUALS.
	 *
	 * <p>
	 * The boolean value will be TRUE if the two expressions are functionally
	 * equivalent.
	 *
	 * <p>
	 * Due to to the imperfect interpretation of floating point numbers there may
	 * some discrepancies between databases but DBV tries to be as accurate as the
	 * database allows.
	 *
	 * @param rightHandSide the line that this value might equal
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be TRUE when the two expressions are
	 * functionally equivalent, otherwise FALSE.
	 */
	@Override
	public BooleanExpression is(Line2DResult rightHandSide) {
		return new BooleanExpression(new IsExpression(this, new Line2DExpression(rightHandSide)));
	}

	/**
	 * Compare the value of the given LineString to this value using the
	 * equivalent of NOT EQUALS.
	 *
	 * <p>
	 * The boolean value will be FALSE if the two expressions are functionally
	 * equivalent.
	 *
	 * <p>
	 * Due to to the imperfect interpretation of floating point numbers there may
	 * some discrepancies between databases but DBV tries to be as accurate as the
	 * database allows.
	 *
	 * @param rightHandSide the line that this value might equal
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be FALSE when the two expressions are
	 * functionally equivalent, otherwise TRUE.
	 */
	@Override
	public BooleanExpression isNot(LineString rightHandSide) {
		return isNot(new DBLine2D(rightHandSide));
	}

	/**
	 * Compare this value to the exterior ring of the given Polygon2D using the
	 * equivalent of NOT EQUALS.
	 *
	 * <p>
	 * The boolean value will be FALSE if the exterior ring and the line are
	 * functionally equivalent.
	 *
	 * <p>
	 * Due to to the imperfect interpretation of floating point numbers there may
	 * some discrepancies between databases but DBV tries to be as accurate as the
	 * database allows.
	 *
	 * @param rightHandSide the polygon whose exterior ring might not equal this
	 * value's value.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be FALSE when the two expressions are
	 * functionally equivalent, otherwise TRUE.
	 */
	public BooleanExpression isNot(Polygon rightHandSide) {
		return isNot(rightHandSide.getExteriorRing());
	}

	/**
	 * Compare the value of the given Line2D to this value using the equivalent of
	 * NOT EQUALS.
	 *
	 * <p>
	 * The boolean value will be FALSE if the two expressions are functionally
	 * equivalent.
	 *
	 * <p>
	 * Due to to the imperfect interpretation of floating point numbers there may
	 * some discrepancies between databases but DBV tries to be as accurate as the
	 * database allows.
	 *
	 * @param rightHandSide the line to compare to this value.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be FALSE when the two expressions are
	 * functionally equivalent, otherwise TRUE.
	 */
	@Override
	public BooleanExpression isNot(Line2DResult rightHandSide) {
		return new BooleanExpression(new IsNotExpression(this, new Line2DExpression(rightHandSide)));
	}

	@Override
	public NumberExpression measurableDimensions() {
		return new NumberExpression(new MeasurableDimensionsExpression(this));
	}

	@Override
	public NumberExpression spatialDimensions() {
		return new NumberExpression(new SpatialDimensionsExpression(this));
	}

	@Override
	public BooleanExpression hasMagnitude() {
		return new BooleanExpression(new HasMagnitudeExpression(this));
	}

	@Override
	public NumberExpression magnitude() {
		return new NumberExpression(new MagnitudeExpression(this));
	}

	@Override
	public Polygon2DExpression boundingBox() {
		return new Polygon2DExpression(new BoundingBoxExpression(this));
	}

	/**
	 * Return the maximum X value in the Line2D.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the numeric value of the largest X coordinate for the Line2D.
	 */
	@Override
	public NumberExpression maxX() {

		return new NumberExpression(new MaxXExpression(this));
	}

	/**
	 * Return the minimum X value in the Line2D.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the numeric value of the smallest X coordinate for the Line2D.
	 */
	@Override
	public NumberExpression minX() {

		return new NumberExpression(new MinXExpression(this));
	}

	/**
	 * Return the maximum Y value in the Line2D.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the numeric value of the largest Y coordinate for the Line2D.
	 */
	@Override
	public NumberExpression maxY() {

		return new NumberExpression(new MaxYExpression(this));
	}

	/**
	 * Return the minimum Y value in the Line2D.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the numeric value of the smallest Y coordinate for the Line2D.
	 */
	@Override
	public NumberExpression minY() {

		return new NumberExpression(new MinYExpression(this));
	}

	/**
	 * Tests whether this line and the line represented by the points ever cross.
	 *
	 * <p>
	 * Multiple line segments means it may cross at several points, however this
	 * method only reports TRUE or FALSE.
	 *
	 * <p>
	 * Use {@link #intersectionPoints(nz.co.gregs.dbvolution.results.Line2DResult)
	 * } to find the intersection points of these lines
	 *
	 * @param points points that constitute a line that might intersect this
	 * value.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public BooleanExpression intersects(Point... points) {
		return this.intersects(expression(points));
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
	 * Use {@link #intersectionPoints(nz.co.gregs.dbvolution.results.Line2DResult)
	 * } to find the intersection points of these lines
	 *
	 * @param coords a series of X and Y values that constitute a line that might
	 * intersect this value.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public BooleanExpression intersects(Coordinate... coords) {
		return this.intersects(expression(coords));
	}

	/**
	 * Tests whether this line and the other line ever cross.
	 *
	 * <p>
	 * Multiple line segments means it may cross at several points, however this
	 * method only reports TRUE or FALSE.
	 *
	 * <p>
	 * Use {@link #intersectionPoints(nz.co.gregs.dbvolution.results.Line2DResult)
	 * } to find the intersection points of these lines
	 *
	 * @param lineString a line that might intersect this value.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * Use {@link #intersectionPoints(nz.co.gregs.dbvolution.results.Line2DResult)
	 * } to find the intersection points of these lines
	 *
	 * @param crossingLine a line that might intersect this value.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public BooleanExpression intersects(Line2DResult crossingLine) {
		return new BooleanExpression(new IntersectsExpression(this, new Line2DExpression(crossingLine)));
	}

	/**
	 * Find all the points of intersection between this value and the specified
	 * Line2D value.
	 *
	 * @param crossingLine a line that might intersect this value.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a MultiPoint2D value containing all the intersection points of the
	 * 2 lines.
	 */
	public MultiPoint2DExpression intersectionPoints(Line2DResult crossingLine) {
		return new MultiPoint2DExpression(new InsectionPointsExpression(this, new Line2DExpression(crossingLine)));
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
	 * Use {@link #intersectionPoints(nz.co.gregs.dbvolution.results.Line2DResult)
	 * } to find the intersection points of these lines
	 *
	 * @param crossingLine points that constitute a line that might intersect this
	 * value.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public Point2DExpression intersectionWith(Point... crossingLine) {
		return intersectionWith(expression(crossingLine));
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
	 * Use {@link #intersectionPoints(nz.co.gregs.dbvolution.results.Line2DResult)
	 * } to find the intersection points of these lines
	 *
	 * @param crossingLine a series of X and Y values that constitute a line that
	 * might intersect this value.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public Point2DExpression intersectionWith(Coordinate... crossingLine) {
		return intersectionWith(expression(crossingLine));
	}

	/**
	 * Find a point where this line and the other line cross.
	 *
	 * <p>
	 * Multiple line segments means it may cross at several points, however this
	 * method only reports the first point found.
	 *
	 * <p>
	 * Use {@link #intersectionPoints(nz.co.gregs.dbvolution.results.Line2DResult)
	 * } to find the intersection points of these lines
	 *
	 * @param crossingLine a line that might intersect this value.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * Use {@link #intersectionPoints(nz.co.gregs.dbvolution.results.Line2DResult)
	 * } to find the intersection points of these lines
	 *
	 * @param crossingLine points that constitute a line that might intersect this
	 * value.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public Point2DExpression intersectionWith(MultiPoint2DExpression crossingLine) {
		return intersectionWith(expression(crossingLine));
	}

	/**
	 * Find a point where this line and the other line cross.
	 *
	 * <p>
	 * Multiple line segments means it may cross at several points, however this
	 * method only reports the first point found.
	 *
	 * <p>
	 * Use {@link #intersectionPoints(nz.co.gregs.dbvolution.results.Line2DResult)
	 * } to find the intersection points of these lines
	 *
	 * @param crossingLine a line that might intersect this value.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public Point2DExpression intersectionWith(Line2DResult crossingLine) {
		return new Point2DExpression(new IntersectionWithExpression(this, new Line2DExpression(crossingLine)));
	}

	/**
	 * Provides a value that represents the line2d value as a polygon2d value.
	 *
	 * <P>
	 * Points are added to the polygon in index order. If necessary the polygon is
	 * closed by adding the first point to the end.
	 *
	 * <p>
	 * Line2D values with less than 3 points will return NULL values.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a polygon2d value
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

		private static final long serialVersionUID = 1L;

		private final Line2DExpression first;
		private final Line2DExpression second;
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
		public final String toSQLString(DBDefinition db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		protected abstract String doExpressionTransform(DBDefinition db);

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

		private static final long serialVersionUID = 1L;

		private final Line2DExpression first;
		private boolean requiresNullProtection;

		LineWithBooleanResult(Line2DExpression first) {
			this.first = first;
		}

		Line2DExpression getFirst() {
			return first;
		}

		@Override
		public final String toSQLString(DBDefinition db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		protected abstract String doExpressionTransform(DBDefinition db);

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

		private static final long serialVersionUID = 1L;

		private final Line2DExpression first;
		private final Line2DExpression second;
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
		public final String toSQLString(DBDefinition db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		protected abstract String doExpressionTransform(DBDefinition db);

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

		private static final long serialVersionUID = 1L;

		private final Line2DExpression first;
		private final Line2DExpression second;
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
		public final String toSQLString(DBDefinition db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		protected abstract String doExpressionTransform(DBDefinition db);

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

		private static final long serialVersionUID = 1L;

		private final Line2DExpression first;
		private boolean requiresNullProtection;

		LineFunctionWithNumberResult(Line2DExpression first) {
			this.first = first;
			if (this.first == null) {
				this.requiresNullProtection = true;
			}
		}

		Line2DExpression getFirst() {
			return first;
		}

		@Override
		public final String toSQLString(DBDefinition db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		protected abstract String doExpressionTransform(DBDefinition db);

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

	private static abstract class LineFunctionWithStringResult extends StringExpression {

		private static final long serialVersionUID = 1L;

		private final Line2DExpression first;
		private boolean requiresNullProtection;

		LineFunctionWithStringResult(Line2DExpression first) {
			this.first = first;
			if (this.first == null) {
				this.requiresNullProtection = true;
			}
		}

		Line2DExpression getFirst() {
			return first;
		}

		@Override
		public final String toSQLString(DBDefinition db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		protected abstract String doExpressionTransform(DBDefinition db);

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

		private static final long serialVersionUID = 1L;

		private final Line2DExpression first;
		private boolean requiresNullProtection;

		LineFunctionWithPolygon2DResult(Line2DExpression first) {
			this.first = first;
			if (this.first == null) {
				this.requiresNullProtection = true;
			}
		}

		Line2DExpression getFirst() {
			return first;
		}

		@Override
		public final String toSQLString(DBDefinition db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		protected abstract String doExpressionTransform(DBDefinition db);

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
			return first.isAggregator();//|| second.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

	private static class NullExpression extends Line2DExpression {

		public NullExpression() {
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.getNull();
		}

		@Override
		public Line2DExpression copy() {
			return new NullExpression();
		}
	}

	protected static class StringResultExpression extends LineFunctionWithStringResult {

		public StringResultExpression(Line2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransform(DBDefinition db) {
			try {
				return db.doLine2DAsTextTransform(getFirst().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				return getFirst().toSQLString(db);
			}
		}

		@Override
		public Line2DExpression.StringResultExpression copy() {
			return new Line2DExpression.StringResultExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	protected static class IsExpression extends LineLineWithBooleanResult {

		public IsExpression(Line2DExpression first, Line2DExpression second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doLine2DEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				return getFirst().stringResult().is(getSecond().stringResult()).toSQLString(db);
			}
		}

		@Override
		public Line2DExpression.IsExpression copy() {
			return new Line2DExpression.IsExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}
	}

	protected static class IsNotExpression extends LineLineWithBooleanResult {

		public IsNotExpression(Line2DExpression first, Line2DExpression second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition defn) {
			try {
				return defn.doLine2DNotEqualsTransform(getFirst().toSQLString(defn), getSecond().toSQLString(defn));
			} catch (UnsupportedOperationException unsupported) {
				return getFirst().stringResult().is(getSecond().stringResult()).not().toSQLString(defn);
			}
		}

		@Override
		public Line2DExpression.IsNotExpression copy() {
			return new Line2DExpression.IsNotExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}
	}

	protected static class MeasurableDimensionsExpression extends LineFunctionWithNumberResult {

		public MeasurableDimensionsExpression(Line2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doLine2DMeasurableDimensionsTransform(getFirst().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				return NumberExpression.value(1).toSQLString(db);
			}
		}

		@Override
		public MeasurableDimensionsExpression copy() {
			return new Line2DExpression.MeasurableDimensionsExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	protected static class SpatialDimensionsExpression extends LineFunctionWithNumberResult {

		public SpatialDimensionsExpression(Line2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doLine2DSpatialDimensionsTransform(getFirst().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				return NumberExpression.value(2).toSQLString(db);
			}
		}

		@Override
		public SpatialDimensionsExpression copy() {
			return new Line2DExpression.SpatialDimensionsExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	protected static class HasMagnitudeExpression extends LineWithBooleanResult {

		public HasMagnitudeExpression(Line2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doLine2DHasMagnitudeTransform(getFirst().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				return BooleanExpression.falseExpression().toSQLString(db);
			}
		}

		@Override
		public HasMagnitudeExpression copy() {
			return new Line2DExpression.HasMagnitudeExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	protected static class MagnitudeExpression extends LineFunctionWithNumberResult {

		public MagnitudeExpression(Line2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doLine2DGetMagnitudeTransform(getFirst().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				return nullExpression().toSQLString(db);
			}
		}

		@Override
		public MagnitudeExpression copy() {
			return new Line2DExpression.MagnitudeExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	protected static class BoundingBoxExpression extends LineFunctionWithPolygon2DResult {

		public BoundingBoxExpression(Line2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doLine2DGetBoundingBoxTransform(getFirst().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				final Line2DExpression first = getFirst();
				final NumberExpression maxX = first.maxX();
				final NumberExpression maxY = first.maxY();
				final NumberExpression minX = first.minX();
				final NumberExpression minY = first.minY();

				return Polygon2DExpression.value(Point2DExpression.value(minX, minY),
						Point2DExpression.value(maxX, minY),
						Point2DExpression.value(maxX, maxY),
						Point2DExpression.value(minX, maxY),
						Point2DExpression.value(minX, minY))
						.toSQLString(db);
			}
		}

		@Override
		public Line2DExpression.BoundingBoxExpression copy() {
			return new Line2DExpression.BoundingBoxExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	protected static class MaxXExpression extends LineFunctionWithNumberResult {

		public MaxXExpression(Line2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doLine2DGetMaxXTransform(getFirst().toSQLString(db));
		}

		@Override
		public MaxXExpression copy() {
			return new Line2DExpression.MaxXExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	protected static class MinXExpression extends LineFunctionWithNumberResult {

		public MinXExpression(Line2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doLine2DGetMinXTransform(getFirst().toSQLString(db));
		}

		@Override
		public MinXExpression copy() {
			return new Line2DExpression.MinXExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	protected static class MaxYExpression extends LineFunctionWithNumberResult {

		public MaxYExpression(Line2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doLine2DGetMaxYTransform(getFirst().toSQLString(db));
		}

		@Override
		public MaxYExpression copy() {
			return new Line2DExpression.MaxYExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	protected static class MinYExpression extends LineFunctionWithNumberResult {

		public MinYExpression(Line2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doLine2DGetMinYTransform(getFirst().toSQLString(db));
		}

		@Override
		public MinYExpression copy() {
			return new Line2DExpression.MinYExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	protected static class IntersectsExpression extends LineLineWithBooleanResult {

		public IntersectsExpression(Line2DExpression first, Line2DExpression second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransform(DBDefinition db) {
			return db.doLine2DIntersectsLine2DTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
		}

		@Override
		public IntersectsExpression copy() {
			return new Line2DExpression.IntersectsExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}
	}

	protected static class InsectionPointsExpression extends LineLineWithMultiPoint2DResult {

		public InsectionPointsExpression(Line2DExpression first, Line2DExpression second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransform(DBDefinition db) {
			return db.doLine2DAllIntersectionPointsWithLine2DTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
		}

		@Override
		public InsectionPointsExpression copy() {
			return new Line2DExpression.InsectionPointsExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}
	}

	protected static class IntersectionWithExpression extends LineLineWithPoint2DResult {

		public IntersectionWithExpression(Line2DExpression first, Line2DExpression second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransform(DBDefinition db) {
			return db.doLine2DIntersectionPointWithLine2DTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
		}

		@Override
		public IntersectionWithExpression copy() {
			return new Line2DExpression.IntersectionWithExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}
	}

}
