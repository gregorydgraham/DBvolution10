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
import com.vividsolutions.jts.geom.Point;
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBPoint2D;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.IntegerExpression;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.results.AnyResult;

/**
 * Represents SQL expressions that are a 2 dimensional points, that is a
 * physical location in a 2D space with X and Y points.
 *
 * <p>
 * Use these methods to manipulate and transform point2d values and results.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class Point2DExpression extends Spatial2DExpression<Point, Point2DResult, DBPoint2D> implements Point2DResult {

	private final static long serialVersionUID = 1l;

	/**
	 * Default constructor
	 *
	 */
	protected Point2DExpression() {
		super();
	}

	/**
	 * Create a Point2DExpression that represents the point value provided.
	 *
	 * @param value
	 */
	protected Point2DExpression(AnyResult<?> value) {
		super(value);
	}

	/**
	 * Create a Point2DExpression that represents the point value provided.
	 *
	 * @param value
	 */
	public Point2DExpression(Point2DResult value) {
		super(value);
	}

	/**
	 * Create a Point2DExpression that represents the point value provided.
	 *
	 * @param point
	 */
	public Point2DExpression(Point point) {
		super(point == null ? null : new DBPoint2D(point));
	}

	public Point2DExpression(double xValue, double yValue) {
		super(new DBPoint2D(xValue, yValue));
	}

	/**
	 * Create a Point2DExpression that represents the point value provided.
	 *
	 * @param point the value of this value.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a Point2DExpression of the point.
	 */
	@Override
	public Point2DExpression expression(Point point) {
		return new Point2DExpression(point);
	}

	/**
	 * Create a Point2DExpression that represents the point value provided.
	 *
	 * @param point the value of this value.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a Point2DExpression of the point.
	 */
	public static Point2DExpression value(Point2DResult point) {
		return new Point2DExpression(point);
	}

	/**
	 * Create a Point2DExpression that represents the coordinate value provided.
	 *
	 * @param coordinate the value of this value.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a Point2DExpression of the point.
	 */
	public static Point2DExpression value(Coordinate coordinate) {
		Point point = new GeometryFactory().createPoint(coordinate);
		return Point2DExpression.value(point);
	}

	/**
	 * Create a Point2DExpression that represents the values provided.
	 *
	 * @param xValue the X value of this value.
	 * @param yValue the Y value of this value.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a Point2DExpression of the x and y values provided.
	 */
	public Point2DExpression expression(Integer xValue, Integer yValue) {
		return value(NumberExpression.value(xValue), NumberExpression.value(yValue));
	}

	public static Point2DExpression value(Integer xValue, Integer yValue) {
		return value(NumberExpression.value(xValue), NumberExpression.value(yValue));
	}

	/**
	 * Create a Point2DExpression that represents the values provided.
	 *
	 * @param xValue the X value of this value.
	 * @param yValue the Y value of this value.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a Point2DExpression of the x and y values provided.
	 */
	public Point2DExpression expression(Long xValue, Long yValue) {
		return value(NumberExpression.value(xValue), NumberExpression.value(yValue));
	}

	public static Point2DExpression value(Long xValue, Long yValue) {
		return value(NumberExpression.value(xValue), NumberExpression.value(yValue));
	}

	/**
	 * Create a Point2DExpression that represents the values provided.
	 *
	 * @param xValue the X value of this value.
	 * @param yValue the Y value of this value.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a Point2DExpression of the x and y values provided.
	 */
	public Point2DExpression expression(Double xValue, Double yValue) {
		return value(NumberExpression.value(xValue), NumberExpression.value(yValue));
	}

	public static Point2DExpression value(Double xValue, Double yValue) {
		return new Point2DExpression(xValue, yValue);
	}

	/**
	 * Create a Point2DExpression that represents the values provided.
	 *
	 * @param xValue the X value of this value.
	 * @param yValue the Y value of this value.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a Point2DExpression of the x and y values provided.
	 */
	public Point2DExpression expression(NumberExpression xValue, NumberExpression yValue) {
		return value(xValue, yValue);
	}

	public static Point2DExpression value(NumberExpression xValue, NumberExpression yValue) {
		return new Point2DExpression(new NumberToPointExpression(xValue, yValue));
	}

	/**
	 * Create a Point2DExpression that represents the values provided.
	 *
	 * @param xValue the X value of this value.
	 * @param yValue the Y value of this value.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a Point2DExpression of the x and y values provided.
	 */
	public Point2DExpression expression(IntegerExpression xValue, IntegerExpression yValue) {
		return value(xValue.numberResult(), yValue.numberResult());
	}

	public static Point2DExpression value(IntegerExpression xValue, IntegerExpression yValue) {
		return value(xValue.numberResult(), yValue.numberResult());
	}

	@Override
	public Point2DExpression nullExpression() {

		return new NullExpression();
	}

	@Override
	public Point2DExpression expression(Point2DResult value) {
		return new Point2DExpression(value);
	}

	@Override
	public Point2DExpression expression(DBPoint2D value) {
		return new Point2DExpression(value);
	}

	@Override
	public DBPoint2D getQueryableDatatypeForExpressionValue() {
		return new DBPoint2D();
	}

	@Override
	public Point2DExpression copy() {
		return this.isNullSafetyTerminator() ? nullPoint2D() : new Point2DExpression((AnyResult<?>) getInnerResult().copy());
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof Point2DExpression) {
			Point2DExpression otherExpr = (Point2DExpression) other;
			return this.getInnerResult() == otherExpr.getInnerResult();
		}
		return false;
	}

	@Override
	public int hashCode() {
		int hash = 7;
		hash = 97 * hash + (this.getInnerResult() != null ? this.getInnerResult().hashCode() : 0);
		hash = 97 * hash + (this.getIncludesNull() ? 1 : 0);
		return hash;
	}

	@Override
	public StringExpression toWKTFormat() {
		return stringResult();
	}

	@Override
	public StringExpression stringResult() {
		return new StringExpression(new StringResultExpression(this));
	}

	/**
	 * Creates a {@link BooleanExpression} that compares the 2 instances using the
	 * EQUALS operation.
	 *
	 * @param rightHandSide the value to compare against.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression is(Point rightHandSide) {
		return is(new DBPoint2D(rightHandSide));
	}

	@Override
	public BooleanExpression is(Point2DResult rightHandSide) {
		return new BooleanExpression(new IsExpression(this, new Point2DExpression(rightHandSide)));
	}

	/**
	 * Creates a {@link BooleanExpression} that compares the 2 instances using the
	 * NOT EQUALS operation.
	 *
	 * @param rightHandSide the value to compare against.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isNot(Point rightHandSide) {
		return isNot(new DBPoint2D(rightHandSide));
	}

	@Override
	public BooleanExpression isNot(Point2DResult rightHandSide) {
		return this.is(rightHandSide).not();
	}

	/**
	 * Retrieves the X value of this point expression.
	 *
	 * <p>
	 * Provides access to the X value of this point allowing for transforms and
	 * tests.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NumberExpression of the X coordinate.
	 */
	public NumberExpression getX() {
		return new NumberExpression(new GetXExpression(this));
	}

	/**
	 * Retrieves the Y value of this point expression.
	 *
	 * <p>
	 * Provides access to the Y value of this point allowing for transforms and
	 * tests.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a NumberExpression of the Y coordinate.
	 */
	public NumberExpression getY() {
		return new NumberExpression(new GetYExpression(this));
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
	 * Calculate the distance between this point and the other point.
	 *
	 * <p>
	 * Creates an SQL value that will report the distance (in units) between these
	 * two points.
	 *
	 * <p>
	 * Essentially this utilizes a database specific method to calculate
	 * sqrt((x2-x1)^2+(y2-y1)^2).
	 *
	 * @param otherPoint the point from which to derive the distance.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a number value of the distance between the two points in units.
	 */
	public NumberExpression distanceTo(Point2DExpression otherPoint) {
		return new NumberExpression(new DistanceToExpression(this, otherPoint));
	}

	@Override
	public Polygon2DExpression boundingBox() {
		return new Polygon2DExpression(new BoundingBoxExpression(this));
	}

	@Override
	public NumberExpression maxX() {
		return this.getX();
	}

	@Override
	public NumberExpression maxY() {
		return this.getY();
	}

	@Override
	public NumberExpression minX() {
		return this.getX();
	}

	@Override
	public NumberExpression minY() {
		return this.getY();
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
	@SuppressWarnings("unchecked")
	public Point2DExpression modeSimple() {
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
	public Point2DExpression modeStrict() {
		Point2DExpression modeExpr = new Point2DExpression(
				new ModeStrictExpression<>(this));
		return modeExpr;
	}

	@Override
	public DBPoint2D asExpressionColumn() {
		return new DBPoint2D(this);
	}

	private static abstract class PointPointFunctionWithBooleanResult extends BooleanExpression {

		private static final long serialVersionUID = 1L;

		private final Point2DExpression first;
		private final Point2DExpression second;
		private boolean requiresNullProtection;

		PointPointFunctionWithBooleanResult(Point2DExpression first, Point2DExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Point2DExpression getFirst() {
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

	private static abstract class PointFunctionWithBooleanResult extends BooleanExpression {

		private static final long serialVersionUID = 1L;

		private final Point2DExpression first;
		private boolean requiresNullProtection;

		PointFunctionWithBooleanResult(Point2DExpression first) {
			this.first = first;
		}

		Point2DExpression getFirst() {
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

	private static abstract class PointPointFunctionWithNumberResult extends NumberExpression {

		private static final long serialVersionUID = 1L;

		private final Point2DExpression first;
		private final Point2DExpression second;
		private boolean requiresNullProtection;

		PointPointFunctionWithNumberResult(Point2DExpression first, Point2DExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Point2DExpression getFirst() {
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

	private static abstract class NumberNumberFunctionWithPoint2DResult extends Point2DExpression {

		private static final long serialVersionUID = 1L;

		private final NumberExpression first;
		private final NumberExpression second;
		private boolean requiresNullProtection;

		NumberNumberFunctionWithPoint2DResult(NumberExpression first, NumberExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		NumberExpression getFirst() {
			return first;
		}

		NumberExpression getSecond() {
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

	private static abstract class PointFunctionWithNumberResult extends NumberExpression {

		private static final long serialVersionUID = 1L;

		private final Point2DExpression first;
		private boolean requiresNullProtection;

		PointFunctionWithNumberResult(Point2DExpression first) {
			this.first = first;
			if (this.first == null) {
				this.requiresNullProtection = true;
			}
		}

		Point2DExpression getFirst() {
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

	private static abstract class PointFunctionWithStringResult extends StringExpression {

		private static final long serialVersionUID = 1L;

		private final Point2DExpression first;
		private boolean requiresNullProtection;

		PointFunctionWithStringResult(Point2DExpression first) {
			this.first = first;
			if (this.first == null) {
				this.requiresNullProtection = true;
			}
		}

		Point2DExpression getFirst() {
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

	private static abstract class PointFunctionWithGeometry2DResult extends Polygon2DExpression {

		private static final long serialVersionUID = 1L;

		private final Point2DExpression first;
		private boolean requiresNullProtection;

		PointFunctionWithGeometry2DResult(Point2DExpression first) {
			this.first = first;
			if (this.first == null) {
				this.requiresNullProtection = true;
			}
		}

		Point2DExpression getFirst() {
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

	private static class NumberToPointExpression extends NumberNumberFunctionWithPoint2DResult {

		public NumberToPointExpression(NumberExpression first, NumberExpression second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransform(DBDefinition db) {
			return db.transformCoordinatesIntoDatabasePoint2DFormat(getFirst().toSQLString(db), getSecond().toSQLString(db));
		}

		@Override
		public Point2DExpression copy() {
			return new NumberToPointExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}

	}

	private static class NullExpression extends Point2DExpression {

		public NullExpression() {
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.getNull();
		}

		@Override
		public Point2DExpression copy() {
			return new NullExpression();
		}
	}

	private class StringResultExpression extends PointFunctionWithStringResult {

		public StringResultExpression(Point2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransform(DBDefinition db) {
			try {
				return db.doPoint2DAsTextTransform(getFirst().toSQLString(db));
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

	protected static class IsExpression extends PointPointFunctionWithBooleanResult {

		public IsExpression(Point2DExpression first, Point2DExpression second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doPoint2DEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				return BooleanExpression.allOf(
						getFirst().stringResult().substringBetween("(", " ").numberResult()
								.is(getSecond().stringResult().substringBetween("(", " ").numberResult()),
						getFirst().stringResult().substringAfter("(").substringBetween(" ", ")").numberResult()
								.is(getSecond().stringResult().substringAfter("(").substringBetween(" ", ")").numberResult())
				).toSQLString(db);
			}
		}

		@Override
		public Point2DExpression.IsExpression copy() {
			return new Point2DExpression.IsExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}
	}

	protected static class GetXExpression extends PointFunctionWithNumberResult {

		public GetXExpression(Point2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doPoint2DGetXTransform(getFirst().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				return getFirst().stringResult().substringBetween("(", " ").numberResult().toSQLString(db);
			}
		}

		@Override
		public GetXExpression copy() {
			return new GetXExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	protected static class GetYExpression extends PointFunctionWithNumberResult {

		public GetYExpression(Point2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doPoint2DGetYTransform(getFirst().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				return getFirst().stringResult().substringAfter("(").substringBetween(" ", ")").numberResult().toSQLString(db);
			}
		}

		@Override
		public GetYExpression copy() {
			return new GetYExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	protected static class MeasurableDimensionsExpression extends PointFunctionWithNumberResult {

		public MeasurableDimensionsExpression(Point2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doPoint2DMeasurableDimensionsTransform(getFirst().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				return NumberExpression.value(0).toSQLString(db);
			}
		}

		@Override
		public MeasurableDimensionsExpression copy() {
			return new MeasurableDimensionsExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	protected static class SpatialDimensionsExpression extends PointFunctionWithNumberResult {

		public SpatialDimensionsExpression(Point2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doPoint2DSpatialDimensionsTransform(getFirst().toSQLString(db));
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

	protected static class HasMagnitudeExpression extends PointFunctionWithBooleanResult {

		public HasMagnitudeExpression(Point2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doPoint2DHasMagnitudeTransform(getFirst().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				return BooleanExpression.falseExpression().toSQLString(db);
			}
		}

		@Override
		public boolean isBooleanStatement() {
			return true;
		}

		@Override
		public HasMagnitudeExpression copy() {
			return new HasMagnitudeExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	protected static class MagnitudeExpression extends PointFunctionWithNumberResult {

		public MagnitudeExpression(Point2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doPoint2DGetMagnitudeTransform(getFirst().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				return nullExpression().toSQLString(db);
			}
		}

		@Override
		public MagnitudeExpression copy() {
			return new MagnitudeExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	protected static class DistanceToExpression extends PointPointFunctionWithNumberResult {

		public DistanceToExpression(Point2DExpression first, Point2DExpression second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doPoint2DDistanceBetweenTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				return getSecond().getX().minus(getFirst().getX()).bracket().squared().plus(getSecond().getY().minus(getFirst().getY()).bracket().squared()).squareRoot().toSQLString(db);
			}
		}

		@Override
		public DistanceToExpression copy() {
			return new DistanceToExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}
	}

	protected static class BoundingBoxExpression extends PointFunctionWithGeometry2DResult {

		public BoundingBoxExpression(Point2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doPoint2DGetBoundingBoxTransform(getFirst().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				final Point2DExpression first = getFirst();
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
		public Point2DExpression.BoundingBoxExpression copy() {
			return new Point2DExpression.BoundingBoxExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

}
