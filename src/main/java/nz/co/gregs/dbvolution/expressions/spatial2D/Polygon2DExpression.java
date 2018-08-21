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

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import nz.co.gregs.dbvolution.results.Point2DResult;
import nz.co.gregs.dbvolution.results.Polygon2DResult;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBPolygon2D;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.results.AnyResult;

/**
 * Creates and transforms Polygon2D values within your database queries.
 *
 * <p>
 * Use these methods to manipulate your Polygon2D columns and results for finer
 * control of the query results.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class Polygon2DExpression extends Spatial2DExpression<Polygon, Polygon2DResult, DBPolygon2D> implements Polygon2DResult {

	private final static long serialVersionUID = 1l;

	public static Polygon2DExpression unitSquare() {
		return value(
				new Point2DExpression(0, 0),
				new Point2DExpression(1, 0),
				new Point2DExpression(1, 1),
				new Point2DExpression(0, 1)
		);
	}

	private final boolean moreNullProtectionRequired;

	/**
	 * Default constructor
	 *
	 */
	protected Polygon2DExpression() {
		super();
		moreNullProtectionRequired = false;
	}

	/**
	 * Create a Polygon2DExpression that represents the value for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }, and when creating column expressions using {@link DBPolygon2D#DBPolygon2D(nz.co.gregs.dbvolution.results.Polygon2DResult)
	 * } and similar methods.
	 *
	 * @param value
	 */
	public Polygon2DExpression(Polygon2DResult value) {
		super(value);
		moreNullProtectionRequired = value == null;
	}

	/**
	 * Create a Polygon2DExpression that represents the value for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }, and when creating column expressions using {@link DBPolygon2D#DBPolygon2D(nz.co.gregs.dbvolution.results.Polygon2DResult)
	 * } and similar methods.
	 *
	 * @param geometry
	 */
	public Polygon2DExpression(Polygon geometry) {
		super(new DBPolygon2D(geometry));
		moreNullProtectionRequired = geometry == null;
	}

	private Polygon2DExpression(AnyResult<?> innerResult) {
		super(innerResult);
		moreNullProtectionRequired = innerResult == null;
	}

	/**
	 * Create a Polygon2DExpression that represents the value for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }, and when creating column expressions using {@link DBPolygon2D#DBPolygon2D(nz.co.gregs.dbvolution.results.Polygon2DResult)
	 * } and similar methods.
	 *
	 * @param polygon the value of this value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a polygon2d value
	 */
	@Override
	public Polygon2DExpression expression(Polygon polygon) {
		return new Polygon2DExpression(polygon);
	}

	/**
	 * Create a Polygon2DExpression that represents the value for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }, and when creating column expressions using {@link DBPolygon2D#DBPolygon2D(nz.co.gregs.dbvolution.results.Polygon2DResult)
	 * } and similar methods.
	 *
	 * @param polygon the value of this value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a polygon2d value
	 */
	@Override
	public Polygon2DExpression expression(Polygon2DResult polygon) {
		return value(polygon);
	}

	public static Polygon2DExpression value(Polygon2DResult polygon) {
		return new Polygon2DExpression(polygon);
	}

	/**
	 * Create a Polygon2DExpression that represents the value for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }, and when creating column expressions using {@link DBPolygon2D#DBPolygon2D(nz.co.gregs.dbvolution.results.Polygon2DResult)
	 * } and similar methods.
	 *
	 * @param pointExpressions the points that define the polygon value of this
	 * value.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a polygon2d value
	 */
	public Polygon2DExpression expression(Point2DExpression... pointExpressions) {
		return Polygon2DExpression.polygon2DFromPoint2DExpressionArray(pointExpressions);
	}

	public static Polygon2DExpression value(Point2DExpression... pointExpressions) {
		return Polygon2DExpression.polygon2DFromPoint2DExpressionArray(pointExpressions);
	}

	/**
	 * Create a Polygon2DExpression that represents the value for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }, and when creating column expressions using {@link DBPolygon2D#DBPolygon2D(nz.co.gregs.dbvolution.results.Polygon2DResult)
	 * } and similar methods.
	 *
	 * @param coordinates the individual numbers that are converted to point that
	 * define the polygon value of this value.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a polygon2d value
	 */
	public Polygon2DExpression expression(Number... coordinates) {
		return value(coordinates);
	}

	public static Polygon2DExpression value(Number... coordinates) {
		ArrayList<NumberExpression> exprs = new ArrayList<NumberExpression>();
		for (Number coord : coordinates) {
			exprs.add(NumberExpression.value(coord));
		}
		return value(exprs.toArray(new NumberExpression[]{}));
	}

	/**
	 * Create a Polygon2DExpression that represents the value for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }, and when creating column expressions using {@link DBPolygon2D#DBPolygon2D(nz.co.gregs.dbvolution.results.Polygon2DResult)
	 * } and similar methods.
	 *
	 * @param coordinateExpressions the individual numbers that are converted to
	 * point that define the polygon value of this value.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a polygon2d value
	 */
	public Polygon2DExpression expression(NumberExpression... coordinateExpressions) {
		return value(coordinateExpressions);
	}

	public static Polygon2DExpression value(NumberExpression... coordinateExpressions) {
		return Polygon2DExpression.polygon2DFromCoordinateArray(coordinateExpressions);
	}

	/**
	 * Create a Polygon2DExpression that represents the value for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }, and when creating column expressions using {@link DBPolygon2D#DBPolygon2D(nz.co.gregs.dbvolution.results.Polygon2DResult)
	 * } and similar methods.
	 *
	 * @param points the points that define the polygon value of this value.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a polygon2d value
	 */
	public Polygon2DExpression expression(Point... points) {
		return value(points);
	}

	public static Polygon2DExpression value(Point... points) {
		List<Point2DExpression> exprs = new ArrayList<Point2DExpression>();
		for (Point point : points) {
			exprs.add(Point2DExpression.value(point));
		}
		return polygon2DFromPoint2DExpressionArray(exprs.toArray(new Point2DExpression[]{}));
	}

	/**
	 * Create a Polygon2DExpression that represents the value for use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }, and when creating column expressions using {@link DBPolygon2D#DBPolygon2D(nz.co.gregs.dbvolution.results.Polygon2DResult)
	 * } and similar methods.
	 *
	 * @param coords the points that define the polygon value of this value.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a polygon2d value
	 */
	public Polygon2DExpression expression(Coordinate... coords) {
		return value(coords);
	}

	public static Polygon2DExpression value(Coordinate... coordinates) {
		return new Polygon2DExpression(new GeometryFactory().createPolygon(coordinates));
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
	protected boolean isNullSafetyTerminator() {
		return moreNullProtectionRequired
				|| super.isNullSafetyTerminator();
	}

	@Override
	public Polygon2DExpression copy() {
		return isNullSafetyTerminator() ? nullPolygon2D() : new Polygon2DExpression((AnyResult<?>) getInnerResult().copy());
	}

	@Override
	public boolean getIncludesNull() {
		return moreNullProtectionRequired || super.getIncludesNull();
	}

	/**
	 * Create a boolean value that returns TRUE if the two polygons share any
	 * spatial coordinates.
	 *
	 * @param rightHandSide the polygon to compare against.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean value that is true if the polygons interact in any way.
	 */
	public BooleanExpression intersects(Polygon rightHandSide) {
		return intersects(new DBPolygon2D(rightHandSide));
	}

	/**
	 * Create a boolean value that returns TRUE if the two polygons share any
	 * spatial coordinates.
	 *
	 * @param rightHandSide the polygon to compare against
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean value that is true if the polygons interact in any way.
	 */
	public BooleanExpression intersects(Polygon2DResult rightHandSide) {
		return new BooleanExpression(new IntersectsExpression(this, new Polygon2DExpression(rightHandSide)));
	}

	/**
	 * Create a boolean value that returns the polygon that is part of both shapes
	 * or NULL if the polygons do not intersect.
	 *
	 * @param rightHandSide the polygon to compare against.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean value that is true if the polygons interact in any way.
	 */
	public Polygon2DExpression intersection(Polygon rightHandSide) {
		return intersection(new DBPolygon2D(rightHandSide));
	}

	/**
	 * Create a boolean value that returns the polygon that is part of both shapes
	 * or NULL if the polygons do not intersect.
	 *
	 * @param rightHandSide the polygon to compare against
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean value that is true if the polygons interact in any way.
	 */
	public Polygon2DExpression intersection(Polygon2DResult rightHandSide) {
		return new Polygon2DExpression(new IntersectionExpression(this, new Polygon2DExpression(rightHandSide)));
	}

	/**
	 * Provides a value that represents value as a polygon2d value.
	 *
	 * <P>
	 * Points are added to the polygon in index order. If necessary the polygon is
	 * closed by adding the first point to the end.
	 *
	 * <p>
	 * MultiPoint2d values with less than 3 points will return NULL values.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a polygon2d value
	 */
	/* TODO implement public Polygon2DExpression polygon2DResult() {*/
	public Polygon2DExpression polygon2DResult() {
		return this;
	}

	private static Polygon2DExpression polygon2DFromPoint2DExpressionArray(Point2DExpression... pointExpressions) {
		return new Polygon2DExpression(new CreatePolygon2DFromPoint2DArrayExpression(pointExpressions));
	}

	private static Polygon2DExpression polygon2DFromCoordinateArray(NumberExpression... coordExpressions) {
		return new Polygon2DExpression(new CreatePolygon2DFromCoordinateArrayExpression(coordExpressions));
	}

	/**
	 * Creates a {@link BooleanExpression} that compares the 2 values using the
	 * EQUALS operation.
	 *
	 * @param rightHandSide the polygon to compare against
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression is(Polygon rightHandSide) {
		return is(new DBPolygon2D(rightHandSide));
	}

	@Override
	public BooleanExpression is(Polygon2DResult rightHandSide) {
		return new BooleanExpression(new IsExpression(this, new Polygon2DExpression(rightHandSide)));
	}

	/**
	 * Returns an value that will evaluate to true if the point is inside this
	 * polygon value.
	 *
	 * @param rightHandSide the polygon to compare against
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean value
	 */
	public BooleanExpression contains(Point rightHandSide) {
		return contains(Point2DExpression.value(rightHandSide));
	}

	/**
	 * Returns an value that will evaluate to true if the point is inside this
	 * polygon value.
	 *
	 * @param rightHandSide the point to compare against
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean value
	 */
	public BooleanExpression contains(Point2DResult rightHandSide) {
		return new BooleanExpression(new ContainsPoint2DExpression(this, new Point2DExpression(rightHandSide)));
	}

	/**
	 * Returns an value that will evaluate to true if the polygon is completely
	 * inside this polygon value.
	 *
	 * <p>
	 * A CONTAINS B implies B WITHIN A.
	 *
	 * @param rightHandSide the polygon to compare against
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean value
	 */
	public BooleanExpression contains(Polygon rightHandSide) {
		return contains(new DBPolygon2D(rightHandSide));
	}

	/**
	 * Returns an value that will evaluate to true if the polygon is completely
	 * inside this polygon value.
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean value
	 */
	public BooleanExpression contains(Polygon2DResult rightHandSide) {
		return new BooleanExpression(new ContainsPolygon2DExpression(this, new Polygon2DExpression(rightHandSide)));
	}

	/**
	 * Creates an SQL value that is TRUE when the two polygon2d values do NOT
	 * intersect in anyway.
	 *
	 * @param rightHandSide the polygon to compare against
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean value that is TRUE if the 2 polygons do NOT intersect in
	 * anyway, otherwise FALSE.
	 */
	public BooleanExpression doesNotIntersect(Polygon rightHandSide) {
		return doesNotIntersect(new DBPolygon2D(rightHandSide));
	}

	/**
	 * Creates an SQL value that is TRUE when the two polygon2d values do NOT
	 * intersect in anyway.
	 *
	 * @param rightHandSide the polygon to compare against
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean value that is TRUE if the 2 polygons do NOT intersect in
	 * anyway, otherwise FALSE.
	 */
	public BooleanExpression doesNotIntersect(Polygon2DResult rightHandSide) {
		return new BooleanExpression(new DoesNotIntersectPolygon2D(this, new Polygon2DExpression(rightHandSide)));
	}

	/**
	 * Creates an SQL value that is TRUE when the two polygon2d values intersect
	 * but neither contains or is within the other.
	 *
	 * <p>
	 * Overlapping polygons have some shared points but they also have unshared
	 * points. This implies that they are also unequal.
	 *
	 * <p>
	 * Two polygon's overlap when their spatial intersection is non-zero but is
	 * not equal to A or B.
	 *
	 * <p>
	 * If OVERLAPS is not supported natively, DBV will fallback to a test using
	 * INTERSECTS and INTERSECTION.
	 *
	 * @param rightHandSide the polygon to compare against
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean value that is TRUE if the 2 polygons intersect but are
	 * not contained, within, or equal.
	 */
	public BooleanExpression overlaps(Polygon rightHandSide) {
		return overlaps(new DBPolygon2D(rightHandSide));
	}

	/**
	 * Creates an SQL value that is TRUE when the two polygon2d values intersect
	 * but neither contains or is within the other.
	 *
	 * <p>
	 * Overlapping polygons have some shared points but they also have unshared
	 * points. This implies that they are also unequal.
	 *
	 * <p>
	 * Two polygon's overlap when their spatial intersection is non-zero but is
	 * not equal to A or B.
	 *
	 * <p>
	 * If OVERLAPS is not supported natively, DBV will fallback to a test using
	 * INTERSECTS and INTERSECTION.
	 *
	 * @param rightHandSide the polygon to compare against
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean value that is TRUE if the 2 polygons intersect but are
	 * not contained, within, or equal.
	 */
	public BooleanExpression overlaps(Polygon2DResult rightHandSide) {
		return new BooleanExpression(
				new OverlapsPolygon2DExpression(this, new Polygon2DExpression(rightHandSide)));
	}

	/**
	 * Tests whether the polygons touch.
	 *
	 * <p>
	 * Checks that a) the polygons have at least on point in common and b) that
	 * their interiors do not overlap.
	 *
	 * @param rightHandSide the polygon to compare against
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return BooleanExpression that returns TRUE if and only if the polygons
	 * touch without overlapping
	 */
	public BooleanExpression touches(Polygon2DResult rightHandSide) {
		return new BooleanExpression(new TouchesPolygon2DExpression(this, new Polygon2DExpression(rightHandSide)));
	}

	/**
	 * Returns an value that will evaluate to true if the polygon is completely
	 * envelopes this polygon value.
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean value
	 */
	public BooleanExpression within(Polygon rightHandSide) {
		return within(new DBPolygon2D(rightHandSide));
	}

	/**
	 * Returns an value that will evaluate to true if the polygon is completely
	 * envelopes this polygon value.
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean value
	 */
	public BooleanExpression within(Polygon2DResult rightHandSide) {
		return new BooleanExpression(new WithinPolygon2DExpression(this, new Polygon2DExpression(rightHandSide)));
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

	/**
	 * Returns the area of the polygon expressed in units.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the area covered by the polygon in units.
	 */
	public NumberExpression area() {
		return new NumberExpression(new AreaExpression(this));
	}

	@Override
	public Polygon2DExpression boundingBox() {
		return new Polygon2DExpression(new BoundingBoxExpression(this));
	}

	/**
	 * Return a Line2DExpression representing a line drawn around the outside of
	 * the Polygon2D.
	 *
	 * <p>
	 * The line is coincident with the edge of the polygon but it does not contain
	 * any points within the polygon as it is only a line.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a Line2DExpression
	 */
	public Line2DExpression exteriorRing() {
		Line2DExpression exteriorRingExpr = new Line2DExpression(new ExteriorRingExpression(this));
		return exteriorRingExpr;
	}

	/**
	 * Creates a {@link BooleanExpression} that compares the 2 values using the
	 * NOT EQUALS operation.
	 *
	 * @param geometry the polygon to compare against
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isNot(Polygon geometry) {
		return this.isNot(Polygon2DExpression.value(geometry));
	}

	@Override
	public BooleanExpression isNot(Polygon2DResult geometry) {
		return this.is(geometry).not();
	}

	@Override
	public StringExpression stringResult() {
		StringExpression stringResultExpr = new StringExpression(new StringResultExpression(this));
		return stringResultExpr;
	}

	@Override
	public NumberExpression maxX() {
		NumberExpression expr = new NumberExpression(new MaxXExpression(this));
		return expr;
	}

	@Override
	public NumberExpression maxY() {
		NumberExpression expr = new NumberExpression(new MaxYExpression(this));
		return expr;
	}

	@Override
	public NumberExpression minX() {
		NumberExpression expr = new NumberExpression(new MinXExpression(this));
		return expr;
	}

	@Override
	public NumberExpression minY() {
		NumberExpression expr = new NumberExpression(new MinYExpression(this));
		return expr;
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
	public Point2DExpression modeSimple() {
		@SuppressWarnings("unchecked")
		Point2DExpression modeExpr = new Point2DExpression(
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
	public Polygon2DExpression modeStrict() {
		@SuppressWarnings("unchecked")
		Polygon2DExpression modeExpr = new Polygon2DExpression(
				new ModeStrictExpression<>(this));

		return modeExpr;
	}

	@Override
	public DBPolygon2D asExpressionColumn() {
		return new DBPolygon2D(this);
	}

	@Override
	public Polygon2DExpression nullExpression() {

		return new NullExpression();
	}

	public static Polygon2DResult value(DBPolygon2D value) {
		return new Polygon2DExpression(value);
	}

	@Override
	public Polygon2DResult expression(DBPolygon2D value) {
		return value(value);
	}

	private static abstract class PolygonPolygonWithBooleanResult extends BooleanExpression {

		private static final long serialVersionUID = 1L;

		private final Polygon2DExpression first;
		private final Polygon2DExpression second;
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

		Polygon2DExpression getSecond() {
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

	private static abstract class PolygonPolygonWithPolygon2DResult extends Polygon2DExpression {

		private static final long serialVersionUID = 1L;

		private final Polygon2DExpression first;
		private final Polygon2DExpression second;
		private boolean requiresNullProtection;

		PolygonPolygonWithPolygon2DResult(Polygon2DExpression first, Polygon2DExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Polygon2DExpression getFirst() {
			return first;
		}

		Polygon2DExpression getSecond() {
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

	private static abstract class PolygonWithBooleanResult extends BooleanExpression {

		private static final long serialVersionUID = 1L;

		private final Polygon2DExpression first;
		private boolean requiresNullProtection;

		PolygonWithBooleanResult(Polygon2DExpression first) {
			this.first = first;
		}

		Polygon2DExpression getFirst() {
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

	private static abstract class PolygonPointWithBooleanResult extends BooleanExpression {

		private static final long serialVersionUID = 1L;

		private final Polygon2DExpression first;
		private final Point2DExpression second;
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

	private static abstract class Polygon2DFunctionWithNumberResult extends NumberExpression {

		private static final long serialVersionUID = 1L;

		private final Polygon2DExpression first;
		private boolean requiresNullProtection;

		Polygon2DFunctionWithNumberResult(Polygon2DExpression first) {
			this.first = first;
		}

		Polygon2DExpression getFirst() {
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

	private static abstract class Polygon2DFunctionWithPolygon2DResult extends Polygon2DExpression {

		private static final long serialVersionUID = 1L;

		private final Polygon2DExpression first;
		private boolean requiresNullProtection;

		Polygon2DFunctionWithPolygon2DResult(Polygon2DExpression first) {
			this.first = first;
		}

		Polygon2DExpression getFirst() {
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

	private static abstract class Polygon2DFunctionWithLine2DResult extends Line2DExpression {

		private static final long serialVersionUID = 1L;

		private final Polygon2DExpression first;
		private boolean requiresNullProtection;

		Polygon2DFunctionWithLine2DResult(Polygon2DExpression first) {
			this.first = first;
		}

		Polygon2DExpression getFirst() {
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

	private static abstract class Polygon2DFunctionWithStringResult extends StringExpression {

		private static final long serialVersionUID = 1L;

		private final Polygon2DExpression first;
		private boolean requiresNullProtection;

		Polygon2DFunctionWithStringResult(Polygon2DExpression first) {
			this.first = first;
		}

		Polygon2DExpression getFirst() {
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

	private static abstract class Point2dArrayFunctionWithPolygon2DResult extends Polygon2DExpression {

		private static final long serialVersionUID = 1L;

		private final Point2DExpression[] allPoints;
		private boolean requiresNullProtection;

		Point2dArrayFunctionWithPolygon2DResult(Point2DExpression... all) {
			this.allPoints = all;
			for (Point2DResult all1 : all) {
				if (all1.getIncludesNull()) {
					this.requiresNullProtection = true;
				}
			}
		}

		Point2DExpression[] getAllPoints() {
			return allPoints;
		}

		@Override
		public final String toSQLString(DBDefinition db) {
			BooleanExpression isNull = BooleanExpression.trueExpression();
			if (this.getIncludesNull()) {
				for (Point2DResult allPoint : allPoints) {
					isNull = isNull.or(BooleanExpression.isNull(allPoint));
				}
				return isNull.toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		protected abstract String doExpressionTransform(DBDefinition db);

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (allPoints != null) {
				for (Point2DResult point : allPoints) {
					hashSet.addAll(point.getTablesInvolved());
				}
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			boolean aggregator = false;
			for (Point2DResult allPoint : allPoints) {
				aggregator |= allPoint.isAggregator();
			}
			return aggregator;
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

	private static abstract class CoordinateArrayFunctionWithPolygon2DResult extends Polygon2DExpression {

		private static final long serialVersionUID = 1L;

		private final NumberExpression[] allCoords;
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
		public final String toSQLString(DBDefinition db) {
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

		protected abstract String doExpressionTransform(DBDefinition db);

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
			return aggregator;
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

	private static class IntersectsExpression extends PolygonPolygonWithBooleanResult {

		public IntersectsExpression(Polygon2DExpression first, Polygon2DExpression second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doPolygon2DIntersectsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
		}

		@Override
		public IntersectsExpression copy() {
			return new Polygon2DExpression.IntersectsExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy());
		}
	}

	private class IntersectionExpression extends PolygonPolygonWithPolygon2DResult {

		public IntersectionExpression(Polygon2DExpression first, Polygon2DExpression second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doPolygon2DIntersectionTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
		}

		@Override
		public IntersectionExpression copy() {
			return new Polygon2DExpression.IntersectionExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}
	}

	private static class CreatePolygon2DFromPoint2DArrayExpression extends Point2dArrayFunctionWithPolygon2DResult {

		public CreatePolygon2DFromPoint2DArrayExpression(Point2DExpression[] all) {
			super(all);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransform(DBDefinition db) {
			Point2DExpression[] allPoints = getAllPoints();
			List<String> pointSQL = new ArrayList<String>();
			for (Point2DExpression pointExpr : allPoints) {
				pointSQL.add(pointExpr.toSQLString(db));
			}
			try {
				return db.transformPoint2DArrayToDatabasePolygon2DFormat(pointSQL);
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

		@Override
		public CreatePolygon2DFromPoint2DArrayExpression copy() {
			Point2DExpression[] allPoints = getAllPoints();
			List<Point2DExpression> pointCopies = new ArrayList<>();
			for (Point2DExpression pointExpr : allPoints) {
				pointCopies.add(pointExpr.copy());
			}
			return new Polygon2DExpression.CreatePolygon2DFromPoint2DArrayExpression(
					pointCopies.isEmpty() ? null : pointCopies.toArray(new Point2DExpression[]{})
			);
		}
	}

	private static class CreatePolygon2DFromCoordinateArrayExpression extends CoordinateArrayFunctionWithPolygon2DResult {

		public CreatePolygon2DFromCoordinateArrayExpression(NumberExpression[] all) {
			super(all);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public CreatePolygon2DFromCoordinateArrayExpression copy() {
			NumberExpression[] allPoints = getAllCoordinates();
			List<NumberExpression> pointCopies = new ArrayList<>();
			for (NumberExpression pointExpr : allPoints) {
				pointCopies.add(pointExpr.copy());
			}
			return new Polygon2DExpression.CreatePolygon2DFromCoordinateArrayExpression(
					pointCopies.isEmpty() ? null : pointCopies.toArray(new NumberExpression[]{})
			);
		}

		@Override
		protected String doExpressionTransform(DBDefinition db) {
			NumberExpression[] allCoords = getAllCoordinates();
			List<String> pointSQL = new ArrayList<String>();
			for (NumberExpression pointExpr : allCoords) {
				pointSQL.add(pointExpr.toSQLString(db));
			}
			try {
				return db.transformCoordinateArrayToDatabasePolygon2DFormat(pointSQL);
			} catch (UnsupportedOperationException ex) {
				StringExpression newPolygon = StringExpression.value("POLYGON ((");
				StringExpression lastPoint = StringExpression.value("");
				String separator = "";

				for (NumberExpression coord : allCoords) {
					newPolygon = newPolygon.append(separator).append(coord);
					if (separator.equals(" ")) {
						lastPoint = lastPoint.append(separator).append(coord);
					} else {
						lastPoint = lastPoint.expression(coord);
					}
					switch (separator) {
						case " ":
							separator = ", ";
							break;
						default:
							separator = " ";
							break;
					}
				}
				final StringExpression firstPoint = allCoords[0].append(" ").append(allCoords[1]);
				if (!firstPoint.toSQLString(db).equals(lastPoint.toSQLString(db))) {
					newPolygon = newPolygon.append(separator).append(firstPoint).append("))");
				}
				newPolygon = newPolygon.append("))");
				return newPolygon.toSQLString(db);
			}
		}
	}

	private class IsExpression extends PolygonPolygonWithBooleanResult {

		public IsExpression(Polygon2DExpression first, Polygon2DExpression second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doPolygon2DEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
		}

		@Override
		public Polygon2DExpression.IsExpression copy() {
			return new Polygon2DExpression.IsExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}
	}

	private class ContainsPoint2DExpression extends PolygonPointWithBooleanResult {

		public ContainsPoint2DExpression(Polygon2DExpression first, Point2DExpression second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doPolygon2DContainsPoint2DTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
		}

		@Override
		public ContainsPoint2DExpression copy() {
			return new Polygon2DExpression.ContainsPoint2DExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}
	}

	private class ContainsPolygon2DExpression extends PolygonPolygonWithBooleanResult {

		public ContainsPolygon2DExpression(Polygon2DExpression first, Polygon2DExpression second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doPolygon2DContainsPolygon2DTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
		}

		@Override
		public ContainsPolygon2DExpression copy() {
			return new Polygon2DExpression.ContainsPolygon2DExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}
	}

	private class DoesNotIntersectPolygon2D extends PolygonPolygonWithBooleanResult {

		public DoesNotIntersectPolygon2D(Polygon2DExpression first, Polygon2DExpression second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doPolygon2DDoesNotIntersectTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
		}

		@Override
		public DoesNotIntersectPolygon2D copy() {
			return new Polygon2DExpression.DoesNotIntersectPolygon2D(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}
	}

	private class OverlapsPolygon2DExpression extends PolygonPolygonWithBooleanResult {

		public OverlapsPolygon2DExpression(Polygon2DExpression first, Polygon2DExpression second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doPolygon2DOverlapsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			} catch (UnsupportedOperationException ex) {
				return BooleanExpression.allOf(
						this.getFirst().intersects(getSecond()),
						this.getFirst().intersection(getSecond()).isNot(this.getFirst()),
						this.getFirst().intersection(getSecond()).isNot(this.getSecond())
				).toSQLString(db);
			}
		}

		@Override
		public OverlapsPolygon2DExpression copy() {
			return new Polygon2DExpression.OverlapsPolygon2DExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}
	}

	private class TouchesPolygon2DExpression extends PolygonPolygonWithBooleanResult {

		public TouchesPolygon2DExpression(Polygon2DExpression first, Polygon2DExpression second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doPolygon2DTouchesTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
		}

		@Override
		public TouchesPolygon2DExpression copy() {
			return new Polygon2DExpression.TouchesPolygon2DExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}
	}

	private class WithinPolygon2DExpression extends PolygonPolygonWithBooleanResult {

		public WithinPolygon2DExpression(Polygon2DExpression first, Polygon2DExpression second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doPolygon2DWithinTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
		}

		@Override
		public WithinPolygon2DExpression copy() {
			return new Polygon2DExpression.WithinPolygon2DExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}
	}

	private class MeasurableDimensionsExpression extends Polygon2DFunctionWithNumberResult {

		public MeasurableDimensionsExpression(Polygon2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doPolygon2DMeasurableDimensionsTransform(getFirst().toSQLString(db));
		}

		@Override
		public MeasurableDimensionsExpression copy() {
			return new Polygon2DExpression.MeasurableDimensionsExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	private class SpatialDimensionsExpression extends Polygon2DFunctionWithNumberResult {

		public SpatialDimensionsExpression(Polygon2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doPolygon2DSpatialDimensionsTransform(getFirst().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				return NumberExpression.value(2).toSQLString(db);
			}
		}

		@Override
		public SpatialDimensionsExpression copy() {
			return new Polygon2DExpression.SpatialDimensionsExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	private class HasMagnitudeExpression extends PolygonWithBooleanResult {

		public HasMagnitudeExpression(Polygon2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doPolygon2DHasMagnitudeTransform(getFirst().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				return BooleanExpression.falseExpression().toSQLString(db);
			}
		}

		@Override
		public HasMagnitudeExpression copy() {
			return new Polygon2DExpression.HasMagnitudeExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	private class MagnitudeExpression extends Polygon2DFunctionWithNumberResult {

		public MagnitudeExpression(Polygon2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doPolygon2DGetMagnitudeTransform(getFirst().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				return nullExpression().toSQLString(db);
			}
		}

		@Override
		public MagnitudeExpression copy() {
			return new Polygon2DExpression.MagnitudeExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	private class AreaExpression extends Polygon2DFunctionWithNumberResult {

		public AreaExpression(Polygon2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doPolygon2DGetAreaTransform(getFirst().toSQLString(db));
		}

		@Override
		public AreaExpression copy() {
			return new Polygon2DExpression.AreaExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	private class BoundingBoxExpression extends Polygon2DFunctionWithPolygon2DResult {

		public BoundingBoxExpression(Polygon2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public BoundingBoxExpression copy() {
			return new Polygon2DExpression.BoundingBoxExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doPolygon2DGetBoundingBoxTransform(getFirst().toSQLString(db));
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
	}

	private class ExteriorRingExpression extends Polygon2DFunctionWithLine2DResult {

		public ExteriorRingExpression(Polygon2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doPolygon2DGetExteriorRingTransform(getFirst().toSQLString(db));
		}

		@Override
		public ExteriorRingExpression copy() {
			return new Polygon2DExpression.ExteriorRingExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	private class StringResultExpression extends Polygon2DFunctionWithStringResult {

		public StringResultExpression(Polygon2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doPolygon2DAsTextTransform(getFirst().toSQLString(db));
		}

		@Override
		public StringResultExpression copy() {
			return new Polygon2DExpression.StringResultExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	private class MaxXExpression extends Polygon2DFunctionWithNumberResult {

		public MaxXExpression(Polygon2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doPolygon2DGetMaxXTransform(getFirst().toSQLString(db));
		}

		@Override
		public MaxXExpression copy() {
			return new Polygon2DExpression.MaxXExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	private class MaxYExpression extends Polygon2DFunctionWithNumberResult {

		public MaxYExpression(Polygon2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doPolygon2DGetMaxYTransform(getFirst().toSQLString(db));
		}

		@Override
		public MaxYExpression copy() {
			return new Polygon2DExpression.MaxYExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	private class MinXExpression extends Polygon2DFunctionWithNumberResult {

		public MinXExpression(Polygon2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doPolygon2DGetMinXTransform(getFirst().toSQLString(db));
		}

		@Override
		public MinXExpression copy() {
			return new Polygon2DExpression.MinXExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	private class MinYExpression extends Polygon2DFunctionWithNumberResult {

		public MinYExpression(Polygon2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doPolygon2DGetMinYTransform(getFirst().toSQLString(db));
		}

		@Override
		public MinYExpression copy() {
			return new Polygon2DExpression.MinYExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	private static class NullExpression extends Polygon2DExpression {

		public NullExpression() {
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.getNull();
		}

		@Override
		public NullExpression copy() {
			return new Polygon2DExpression.NullExpression();
		}
	}
}
