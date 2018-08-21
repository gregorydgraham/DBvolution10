/*
 * Copyright 2015 gregory.graham.
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

import nz.co.gregs.dbvolution.results.LineSegment2DResult;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineSegment;
import com.vividsolutions.jts.geom.Point;
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.H2DBDefinition;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBLineSegment2D;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.results.AnyResult;

/**
 * Represents expressions that produce a geometry consisting of 2 points and
 * representing a line from the first point to the second.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregory.graham
 */
public class LineSegment2DExpression extends Spatial2DExpression<LineSegment, LineSegment2DResult, DBLineSegment2D> implements LineSegment2DResult {

	private static final long serialVersionUID = 1l;

	private final boolean moreNullProtectionRequired;

	/**
	 * Default constructor.
	 *
	 */
	protected LineSegment2DExpression() {
		super();
		moreNullProtectionRequired = false;
	}

	/**
	 * Create a LineSegment2D value encapsulating the value supplied.
	 *
	 * @param value
	 */
	public LineSegment2DExpression(LineSegment2DResult value) {
		super(value);
		moreNullProtectionRequired = value == null;
	}

	/**
	 * Create a LineSegment2D value encapsulating the value supplied.
	 *
	 * @param line
	 */
	public LineSegment2DExpression(LineSegment line) {
		super(new DBLineSegment2D(line));
		moreNullProtectionRequired = line == null;
	}

	/**
	 * Create a LineSegment2D value encapsulating the values supplied.
	 *
	 * @param point1x
	 * @param point1y
	 * @param point2x
	 * @param point2y
	 */
	public LineSegment2DExpression(double point1x, double point1y, double point2x, double point2y) {
		super(new DBLineSegment2D(point1x, point1y, point2x, point2y));
		moreNullProtectionRequired = false;
	}

	/**
	 * Create a LineSegment2D value encapsulating the value supplied.
	 *
	 * @param point1
	 * @param point2
	 *
	 */
	public LineSegment2DExpression(Point point1, Point point2) {
		super(new DBLineSegment2D(point1, point2));
		moreNullProtectionRequired = point1 == null || point2 == null;
	}

	/**
	 * Create a LineSegment2D value encapsulating the value supplied.
	 *
	 * @param coord1
	 * @param coord2
	 */
	public LineSegment2DExpression(Coordinate coord1, Coordinate coord2) {
		super(new DBLineSegment2D(coord1, coord2));
		moreNullProtectionRequired = coord1 == null || coord2 == null;
	}

	protected LineSegment2DExpression(AnyResult<?> innerResult) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Create an value for the line segment created from the 2 points.
	 *
	 * @param point1 the starting point of this value
	 * @param point2 the end point of this value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LineSegment2D value
	 */
	public LineSegment2DExpression expression(Point point1, Point point2) {
		return new LineSegment2DExpression(point1, point2);
	}

	public static LineSegment2DExpression value(Point point1, Point point2) {
		return new LineSegment2DExpression(point1, point2);
	}

	/**
	 * Create an value for the line segment created from the 2 coordinates.
	 *
	 * @param coord1 the starting point of this value
	 * @param coord2 the end point of this value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LineSegment2D value
	 */
	public LineSegment2DExpression expression(Coordinate coord1, Coordinate coord2) {
		return new LineSegment2DExpression(coord1, coord2);
	}

	public static LineSegment2DExpression value(Coordinate coord1, Coordinate coord2) {
		return new LineSegment2DExpression(coord1, coord2);
	}

	/**
	 * Create an value for the line segment created by combining the 4 numbers
	 * into 2 points.
	 *
	 * @param x1 the first X of this value
	 * @param y1 the first Y of this value
	 * @param x2 the last X of this value
	 * @param y2 the last Y of this value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LineSegment2D value
	 */
	public LineSegment2DExpression expression(Double x1, Double y1, Double x2, Double y2) {
		return new LineSegment2DExpression(new Coordinate(x1, y1), new Coordinate(x2, y2));
	}

	public static LineSegment2DExpression value(Double x1, Double y1, Double x2, Double y2) {
		return new LineSegment2DExpression(new Coordinate(x1, y1), new Coordinate(x2, y2));
	}

	/**
	 * Create an value for the line segment created from the 2 points.
	 *
	 * @param line the value of this line value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LineSegment2D value
	 */
	@Override
	public LineSegment2DExpression expression(LineSegment line) {
		return new LineSegment2DExpression(line);
	}

	/**
	 * Create an value for the line segment created from the 2 points.
	 *
	 * @param line the value of this line value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a LineSegment2D value
	 */
	@Override
	public LineSegment2DExpression expression(LineSegment2DResult line) {
		return new LineSegment2DExpression(line);
	}

	@Override
	public DBLineSegment2D getQueryableDatatypeForExpressionValue() {
		return new DBLineSegment2D();
	}

	@Override
	public LineSegment2DExpression copy() {
		return isNullSafetyTerminator() ? nullLineSegment2D() : new LineSegment2DExpression((AnyResult<?>) getInnerResult().copy());
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof LineSegment2DExpression) {
			LineSegment2DExpression otherExpr = (LineSegment2DExpression) other;
			final H2DBDefinition defn = new H2DBDefinition();
			return this.toSQLString(defn).equals(otherExpr.toSQLString(defn));
		}
		return false;
	}

	@Override
	public int hashCode() {
		int hash = 5;
		hash = 37 * hash + (this.getInnerResult() != null ? this.getInnerResult().hashCode() : 0);
		hash = 37 * hash + (this.getIncludesNull() ? 1 : 0);
		return hash;
	}

	@Override
	protected boolean isNullSafetyTerminator() {
		return moreNullProtectionRequired == false && super.isNullSafetyTerminator();
	}

	@Override
	public boolean getIncludesNull() {
		return moreNullProtectionRequired || super.getIncludesNull();
	}

	@Override
	public StringExpression toWKTFormat() {
		return stringResult();
	}

	@Override
	public LineSegment2DExpression nullExpression() {

		return new NullExpression();
	}

	@Override
	public LineSegment2DExpression expression(DBLineSegment2D value) {
		return value(value);
	}

	public static LineSegment2DExpression value(LineSegment2DResult value) {
		return new LineSegment2DExpression(value);
	}

	/**
	 * Convert this LineSegment2D to a String representation based on the Well
	 * Known Text (WKT) format.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a StringExpression representing this spatial value.
	 */
	@Override
	public StringExpression stringResult() {
		return new StringExpression(new StringResultExpression(this));
	}

	/**
	 * Creates a {@link BooleanExpression} that compares the 2 instances using the
	 * EQUALS operation.
	 *
	 * @param rightHandSide the value to compare against
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression is(LineSegment rightHandSide) {
		return is(new DBLineSegment2D(rightHandSide));
	}

	@Override
	public BooleanExpression is(LineSegment2DResult rightHandSide) {
		return new BooleanExpression(new IsExpression(this, new LineSegment2DExpression(rightHandSide)));
	}

	/**
	 * Creates a {@link BooleanExpression} that compares the 2 values using the
	 * NOT EQUALS operation.
	 *
	 * @param rightHandSide the value to compare against
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression returning TRUE if the two line segments are
	 * different, otherwise FALSE.
	 */
	@Override
	public BooleanExpression isNot(LineSegment rightHandSide) {
		return isNot(new DBLineSegment2D(rightHandSide));
	}

	@Override
	public BooleanExpression isNot(LineSegment2DResult rightHandSide) {
		return is(rightHandSide).not();
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

	@Override
	public NumberExpression maxX() {

		return new NumberExpression(new MaxXExpression(this));
	}

	@Override
	public NumberExpression minX() {

		return new NumberExpression(new MinXExpression(this));
	}

	@Override
	public NumberExpression maxY() {

		return new NumberExpression(new MaxYExpression(this));
	}

	@Override
	public NumberExpression minY() {

		return new NumberExpression(new MinYExpression(this));
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
	public LineSegment2DExpression modeSimple() {
		@SuppressWarnings("unchecked")
		LineSegment2DExpression modeExpr = new LineSegment2DExpression(
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
	public LineSegment2DExpression modeStrict() {
		@SuppressWarnings("unchecked")
		LineSegment2DExpression modeExpr = new LineSegment2DExpression(
				new ModeStrictExpression<>(this));

		return modeExpr;
	}

	/**
	 * Tests whether this line segment and the line segment represented by the
	 * points ever cross.
	 *
	 * <p>
	 * Use {@link #intersectionWith(com.vividsolutions.jts.geom.Point, com.vividsolutions.jts.geom.Point)
	 * } to find the intersection point.
	 *
	 * @param point1 the first point in the line segment to compare against
	 * @param point2 the last point in the line segment to compare against
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public BooleanExpression intersects(Point point1, Point point2) {
		return this.intersects(new LineSegment2DExpression(point1, point2));
	}

	/**
	 * Tests whether this line segment and the line segment represented by the
	 * coordinates ever cross.
	 *
	 * <p>
	 * Use {@link #intersectionWith(com.vividsolutions.jts.geom.Coordinate, com.vividsolutions.jts.geom.Coordinate)
	 * } to find the intersection point.
	 *
	 * @param coord1 the first point in the line segment to compare against
	 * @param coord2 the last point in the line segment to compare against
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public BooleanExpression intersects(Coordinate coord1, Coordinate coord2) {
		return this.intersects(new LineSegment2DExpression(coord1, coord2));
	}

	/**
	 * Tests whether this line segment and the other line segment ever cross.
	 *
	 * <p>
	 * Use {@link #intersectionWith(com.vividsolutions.jts.geom.LineSegment) } to
	 * find the intersection point.
	 *
	 * @param linesegment the value to compare against
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public BooleanExpression intersects(LineSegment linesegment) {
		return this.intersects(new LineSegment2DExpression(linesegment));
	}

	/**
	 * Tests whether this line segment and the other line segment ever cross.
	 *
	 * <p>
	 * Use {@link #intersectionWith(nz.co.gregs.dbvolution.results.LineSegment2DResult)
	 * } to find the intersection point.
	 *
	 * @param crossingLine the value to compare against
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public BooleanExpression intersects(LineSegment2DResult crossingLine) {
		return new BooleanExpression(new IntersectsExpression(this, new LineSegment2DExpression(crossingLine)));
	}

	/**
	 * Returns an value providing the point of intersection between this line
	 * segment and the line segment formed from the two points provided.
	 *
	 * @param point1 the first point of the line segment to compare against
	 * @param point2 the last point of the line segment to compare against
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a Point2DExpression
	 */
	public Point2DExpression intersectionWith(Point point1, Point point2) {
		return this.intersectionWith(new LineSegment2DExpression(point1, point2));
	}

	/**
	 * Returns an value providing the point of intersection between this line
	 * segment and the line segment formed from the four ordinates provided.
	 *
	 * @param point1x the first X of the line segment to compare against
	 * @param point1y the first Y of the line segment to compare against
	 * @param point2x the last X of the line segment to compare against
	 * @param point2y the last Y of the line segment to compare against
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a Point2DExpression
	 */
	public Point2DExpression intersectionWith(Double point1x, Double point1y, Double point2x, Double point2y) {
		return this.intersectionWith(new LineSegment2DExpression(point1x, point1y, point2x, point2y));
	}

	/**
	 * Returns an value providing the point of intersection between this line
	 * segment and the line segment formed from the two coordinates provided.
	 *
	 * @param coord1 the first point of the line segment to compare against
	 * @param coord2 the last point of the line segment to compare against
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a Point2DExpression
	 */
	public Point2DExpression intersectionWith(Coordinate coord1, Coordinate coord2) {
		return this.intersectionWith(new LineSegment2DExpression(coord1, coord2));
	}

	/**
	 * Returns an value providing the point of intersection between this line
	 * segment and the line segment provided.
	 *
	 * @param lineString the line segment to compare against
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a Point2DExpression
	 */
	public Point2DExpression intersectionWith(LineSegment lineString) {
		return this.intersectionWith(new LineSegment2DExpression(lineString));
	}

	/**
	 * Returns an value providing the point of intersection between this line
	 * segment and the {@link LineSegment2DResult}/{@link LineSegment2DExpression}
	 * provided.
	 *
	 * @param crossingLine the line segment to compare against
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a Point2DExpression
	 */
	public Point2DExpression intersectionWith(LineSegment2DResult crossingLine) {
		return new Point2DExpression(new IntersectionWithExpression(this, new LineSegment2DExpression(crossingLine)));
	}

	@Override
	public DBLineSegment2D asExpressionColumn() {
		return new DBLineSegment2D(this);
	}

	private static abstract class LineSegmentLineSegmentWithBooleanResult extends BooleanExpression {

		private static final long serialVersionUID = 1L;

		private final LineSegment2DExpression first;
		private final LineSegment2DExpression second;
		private boolean requiresNullProtection;

		LineSegmentLineSegmentWithBooleanResult(LineSegment2DExpression first, LineSegment2DExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		LineSegment2DExpression getFirst() {
			return first;
		}

		LineSegment2DExpression getSecond() {
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

	private static abstract class LineSegmentLineSegmentWithPointResult extends Point2DExpression {

		private static final long serialVersionUID = 1L;

		private final LineSegment2DExpression first;
		private final LineSegment2DExpression second;
		private boolean requiresNullProtection;

		LineSegmentLineSegmentWithPointResult(LineSegment2DExpression first, LineSegment2DExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		LineSegment2DExpression getFirst() {
			return first;
		}

		LineSegment2DExpression getSecond() {
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

	private static abstract class LineSegmentWithNumberResult extends NumberExpression {

		private static final long serialVersionUID = 1L;

		private final LineSegment2DExpression first;
		private boolean requiresNullProtection;

		LineSegmentWithNumberResult(LineSegment2DExpression first) {
			this.first = first;
			if (this.first == null) {
				this.requiresNullProtection = true;
			}
		}

		LineSegment2DExpression getFirst() {
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

	private static abstract class LineSegmentWithBooleanResult extends BooleanExpression {

		private static final long serialVersionUID = 1L;

		private final LineSegment2DExpression first;
		private boolean requiresNullProtection;

		LineSegmentWithBooleanResult(LineSegment2DExpression first) {
			this.first = first;
			if (this.first == null) {
				this.requiresNullProtection = true;
			}
		}

		LineSegment2DExpression getFirst() {
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

	private static abstract class LineSegmentWithStringResult extends StringExpression {

		private static final long serialVersionUID = 1L;

		private final LineSegment2DExpression first;
		private boolean requiresNullProtection;

		LineSegmentWithStringResult(LineSegment2DExpression first) {
			this.first = first;
			if (this.first == null) {
				this.requiresNullProtection = true;
			}
		}

		LineSegment2DExpression getFirst() {
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

	private static abstract class LineSegmentWithGeometry2DResult extends Polygon2DExpression {

		private static final long serialVersionUID = 1L;

		private final LineSegment2DExpression first;
		private boolean requiresNullProtection;

		LineSegmentWithGeometry2DResult(LineSegment2DExpression first) {
			this.first = first;
			if (this.first == null) {
				this.requiresNullProtection = true;
			}
		}

		LineSegment2DExpression getFirst() {
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

	private static class NullExpression extends LineSegment2DExpression {

		public NullExpression() {
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.getNull();
		}

		@Override
		public NullExpression copy() {
			return new NullExpression();
		}
	}

	private static class StringResultExpression extends LineSegmentWithStringResult {

		public StringResultExpression(LineSegment2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransform(DBDefinition db) {
			try {
				return db.doLineSegment2DAsTextTransform(getFirst().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				return getFirst().toSQLString(db);
			}
		}

		@Override
		public StringResultExpression copy() {
			return new StringResultExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	protected static class IsExpression extends LineSegmentLineSegmentWithBooleanResult {

		public IsExpression(LineSegment2DExpression first, LineSegment2DExpression second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doLineSegment2DEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				return getFirst().stringResult().is(getSecond().stringResult()).toSQLString(db);
			}
		}

		@Override
		public LineSegment2DExpression.IsExpression copy() {
			return new LineSegment2DExpression.IsExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}
	}

	protected static class MeasurableDimensionsExpression extends LineSegmentWithNumberResult {

		public MeasurableDimensionsExpression(LineSegment2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doLineSegment2DDimensionTransform(getFirst().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				return NumberExpression.value(1).toSQLString(db);
			}
		}

		@Override
		public MeasurableDimensionsExpression copy() {
			return new MeasurableDimensionsExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	protected class SpatialDimensionsExpression extends LineSegmentWithNumberResult {

		public SpatialDimensionsExpression(LineSegment2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doLineSegment2DSpatialDimensionsTransform(getFirst().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				return NumberExpression.value(2).toSQLString(db);
			}
		}

		@Override
		public SpatialDimensionsExpression copy() {
			return new SpatialDimensionsExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	protected class HasMagnitudeExpression extends LineSegmentWithBooleanResult {

		public HasMagnitudeExpression(LineSegment2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doLineSegment2DHasMagnitudeTransform(getFirst().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				return BooleanExpression.falseExpression().toSQLString(db);
			}
		}

		@Override
		public HasMagnitudeExpression copy() {
			return new HasMagnitudeExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	protected class MagnitudeExpression extends LineSegmentWithNumberResult {

		public MagnitudeExpression(LineSegment2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doLineSegment2DGetMagnitudeTransform(getFirst().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				return nullExpression().toSQLString(db);
			}
		}

		@Override
		public MagnitudeExpression copy() {
			return new LineSegment2DExpression.MagnitudeExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	protected class BoundingBoxExpression extends LineSegmentWithGeometry2DResult {

		public BoundingBoxExpression(LineSegment2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doLineSegment2DGetBoundingBoxTransform(getFirst().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				final LineSegment2DExpression first = getFirst();
				final NumberExpression maxX = first.maxX();
				final NumberExpression maxY = first.maxY();
				final NumberExpression minX = first.minX();
				final NumberExpression minY = first.minY();
				return Polygon2DExpression.value(
						Point2DExpression.value(minX, minY),
						Point2DExpression.value(maxX, minY),
						Point2DExpression.value(maxX, maxY),
						Point2DExpression.value(minX, maxY),
						Point2DExpression.value(minX, minY))
						.toSQLString(db);
			}
		}

		@Override
		public LineSegment2DExpression.BoundingBoxExpression copy() {
			return new LineSegment2DExpression.BoundingBoxExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	protected class MaxXExpression extends LineSegmentWithNumberResult {

		public MaxXExpression(LineSegment2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doLineSegment2DGetMaxXTransform(getFirst().toSQLString(db));
		}

		@Override
		public MaxXExpression copy() {
			return new LineSegment2DExpression.MaxXExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	protected class MinXExpression extends LineSegmentWithNumberResult {

		public MinXExpression(LineSegment2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doLineSegment2DGetMinXTransform(getFirst().toSQLString(db));
		}

		@Override
		public MinXExpression copy() {
			return new LineSegment2DExpression.MinXExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	protected class MaxYExpression extends LineSegmentWithNumberResult {

		public MaxYExpression(LineSegment2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doLineSegment2DGetMaxYTransform(getFirst().toSQLString(db));
		}

		@Override
		public MaxYExpression copy() {
			return new LineSegment2DExpression.MaxYExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	protected class MinYExpression extends LineSegmentWithNumberResult {

		public MinYExpression(LineSegment2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doLineSegment2DGetMinYTransform(getFirst().toSQLString(db));
		}

		@Override
		public MinYExpression copy() {
			return new LineSegment2DExpression.MinYExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	protected class IntersectsExpression extends LineSegmentLineSegmentWithBooleanResult {

		public IntersectsExpression(LineSegment2DExpression first, LineSegment2DExpression second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransform(DBDefinition db) {
			return db.doLineSegment2DIntersectsLineSegment2DTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
		}

		@Override
		public IntersectsExpression copy() {
			return new LineSegment2DExpression.IntersectsExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}
	}

	protected class IntersectionWithExpression extends LineSegmentLineSegmentWithPointResult {

		public IntersectionWithExpression(LineSegment2DExpression first, LineSegment2DExpression second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransform(DBDefinition db) {
			return db.doLineSegment2DIntersectionPointWithLineSegment2DTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
		}

		@Override
		public IntersectionWithExpression copy() {
			return new LineSegment2DExpression.IntersectionWithExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}
	}

}
