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
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.H2DBDefinition;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBMultiPoint2D;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.results.AnyResult;
import nz.co.gregs.dbvolution.results.MultiPoint2DResult;

/**
 * Creates and transforms MultiPoint2D values within your database queries.
 *
 * <p>
 * Use these methods to manipulate your MultiPoint2D columns and results for
 * finer control of the query results.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class MultiPoint2DExpression extends Spatial2DExpression<MultiPoint, MultiPoint2DResult, DBMultiPoint2D> implements MultiPoint2DResult {

	private final static long serialVersionUID = 1l;

	private final boolean moreNullProtectionRequired;

	/**
	 * Default constructor
	 *
	 */
	protected MultiPoint2DExpression() {
		super();
		moreNullProtectionRequired = false;
	}

	/**
	 * Create a {@link MultiPoint2DExpression} representing from provided
	 * {@link MultiPoint2DResult}/{@link MultiPoint2DExpression}
	 *
	 * @param value
	 */
	public MultiPoint2DExpression(MultiPoint2DResult value) {
		super(value);
		moreNullProtectionRequired = value == null;
	}

	/**
	 * Create a {@link MultiPoint2DExpression} representing from provided
	 * {@link Point JTS points}.
	 *
	 * @param points
	 */
	public MultiPoint2DExpression(Point... points) {
		super(new DBMultiPoint2D(points));
		boolean nulls = false;
//		for (Point point : points) {
//			nulls = point == null ? true : nulls;
//		}
		moreNullProtectionRequired
				= points == null
				|| points.length == 0
				|| nulls
				|| new DBMultiPoint2D(points).getIncludesNull();
	}

	/**
	 * Create a {@link MultiPoint2DExpression} representing from provided
	 * {@link MultiPoint JTS multipoint}.
	 *
	 * @param points
	 */
	public MultiPoint2DExpression(MultiPoint points) {
		super(new DBMultiPoint2D(points));
		moreNullProtectionRequired = points == null;
	}

	private MultiPoint2DExpression(AnyResult<?> innerResult) {
		super(innerResult);
		moreNullProtectionRequired = innerResult == null;
	}

	/**
	 * Create a {@link MultiPoint2DExpression} representing from provided
	 * {@link Point JTS points}.
	 *
	 * @param points the points to include in this value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a MultiPoint2DExpression.
	 */
	public MultiPoint2DExpression expression(Point... points) {
		return new MultiPoint2DExpression(points);
	}

	public static MultiPoint2DExpression value(Point... points) {
		return new MultiPoint2DExpression(points);
	}

	/**
	 * Create a {@link MultiPoint2DExpression} representing from provided
	 * {@link Coordinate JTS coordinates}.
	 *
	 * @param coords the points to include in this value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a MultiPoint2DExpression.
	 */
	public MultiPoint2DExpression expression(Coordinate... coords) {
		return value(coords);
	}

	public static MultiPoint2DExpression value(Coordinate... coords) {
		GeometryFactory geometryFactory = new GeometryFactory();
		MultiPoint multiPoint = geometryFactory.createMultiPoint(coords);
		return new MultiPoint2DExpression(multiPoint);
	}

	/**
	 * Create a {@link MultiPoint2DExpression} representing from the provided
	 * {@link MultiPoint JTS multipoint}.
	 *
	 * @param points the points to include in this value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a MultiPoint2DExpression representing the points
	 */
	@Override
	public MultiPoint2DExpression expression(MultiPoint points) {
		return value(points);
	}

	/**
	 * Create a {@link MultiPoint2DExpression} representing from the
	 * {@link MultiPoint2DResult}.
	 *
	 * @param points the points to include in this value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a MultiPoint2DExpression representing the points
	 */
	@Override
	public MultiPoint2DExpression expression(MultiPoint2DResult points) {
		return value(points);
	}

	public static MultiPoint2DExpression value(MultiPoint2DResult points) {
		return new MultiPoint2DExpression(points);
	}

	@Override
	public DBMultiPoint2D getQueryableDatatypeForExpressionValue() {
		return new DBMultiPoint2D();
	}

	@Override
	public MultiPoint2DExpression copy() {
		return isNullSafetyTerminator() ? nullMultiPoint2D() : new MultiPoint2DExpression((AnyResult<?>) getInnerResult().copy());
	}

	@Override
	public MultiPoint2DExpression nullExpression() {

		return new NullExpression();
	}

	@Override
	public MultiPoint2DExpression expression(DBMultiPoint2D value) {
		return new MultiPoint2DExpression(value);
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof MultiPoint2DExpression) {
			MultiPoint2DExpression otherExpr = (MultiPoint2DExpression) other;
			final H2DBDefinition defn = new H2DBDefinition();
			return this.toSQLString(defn).equals(otherExpr.toSQLString(defn));
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
	protected boolean isNullSafetyTerminator() {
		return moreNullProtectionRequired == false
				|| super.isNullSafetyTerminator();
	}

	@Override
	public boolean getIncludesNull() {
		return moreNullProtectionRequired || super.getIncludesNull();
	}

	@Override
	public StringExpression toWKTFormat() {
		return stringResult();
	}

	/**
	 * Transform the MultiPoint2D value into the Well Known Text (WKT) version of
	 * the value.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a StringExpression representing the value transformed into a WKT
	 * string.
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
	public BooleanExpression is(MultiPoint rightHandSide) {
		return is(new DBMultiPoint2D(rightHandSide));
	}

	@Override
	public BooleanExpression is(MultiPoint2DResult rightHandSide) {
		return new BooleanExpression(new IsExpression(this, new MultiPoint2DExpression(rightHandSide)));
	}

	/**
	 * Creates a {@link BooleanExpression} that compares the 2 values using the
	 * NOT EQUALS operation.
	 *
	 * @param rightHandSide the {@link MultiPoint} to compare to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@Override
	public BooleanExpression isNot(MultiPoint rightHandSide) {
		return this.is(rightHandSide).not();
	}

	@Override
	public BooleanExpression isNot(MultiPoint2DResult rightHandSide) {
		return is(rightHandSide).not();
	}

	@Override
	public NumberExpression maxX() {
		return new NumberExpression(new MaxXExpression(this));
	}

	@Override
	public NumberExpression maxY() {
		return new NumberExpression(new MaxYExpression(this));
	}

	@Override
	public NumberExpression minX() {
		return new NumberExpression(new MinXExpression(this));
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
	public MultiPoint2DExpression modeSimple() {
		@SuppressWarnings("unchecked")
		MultiPoint2DExpression modeExpr = new MultiPoint2DExpression(
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
	public MultiPoint2DExpression modeStrict() {
		@SuppressWarnings("unchecked")
		MultiPoint2DExpression modeExpr = new MultiPoint2DExpression(
				new ModeStrictExpression<>(this));

		return modeExpr;
	}

	/**
	 * Create a NumberExpression that represents the number of points stored in
	 * the MultiPoint value.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a number value
	 */
	public NumberExpression numberOfPoints() {
		return new NumberExpression(new NumberOfPointsExpression(this));
	}

	/**
	 * Create a point2d value that represents the point at the given index.
	 *
	 * <p>
	 * This method provides array-like access to the points in the multipoint2d
	 * value and uses a Java standard zero-based index.
	 *
	 * <p>
	 * Databases may use a one-based index but DBvolution compensates for that
	 * automatically.
	 *
	 * <p>
	 * Indexes beyond the end of the multipoint2d will return NULL values.
	 *
	 * @param index a zero-based index within the multipoint2d
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a point2d value
	 */
	public Point2DExpression getPointAtIndexZeroBased(Number index) {
		return getPointAtIndexZeroBased(NumberExpression.value(index));
	}

	/**
	 * Create a point2d value that represents the point at the given index.
	 *
	 * <p>
	 * This method provides array-like access to the points in the multipoint2d
	 * value and uses a Java standard zero-based index.
	 *
	 * <p>
	 * Databases may use a one-based index but DBvolution compensates for that
	 * automatically.
	 *
	 * <p>
	 * Indexes beyond the end of the multipoint2d will return NULL values.
	 *
	 * @param index a zero-based index within the multipoint2d
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a point2d value
	 */
	public Point2DExpression getPointAtIndexZeroBased(long index) {
		return getPointAtIndexZeroBased(NumberExpression.value(index).numberResult());
	}

	/**
	 * Create a point2d value that represents the point at the given index.
	 *
	 * <p>
	 * This method provides array-like access to the points in the multipoint2d
	 * value and uses a Java standard zero-based index.
	 *
	 * <p>
	 * Databases may use a one-based index but DBvolution compensates for that
	 * automatically.
	 *
	 * <p>
	 * Indexes beyond the end of the multipoint2d will return NULL values.
	 *
	 * @param index a zero-based index within the multipoint2d
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a point2d value
	 */
	public Point2DExpression getPointAtIndexZeroBased(NumberExpression index) {
		return new Point2DExpression(new GetPointAtIndexExpression(this, index));
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
	 * Provides a value that represents the multipoint2d value as a line2d value.
	 *
	 * <P>
	 * Points are added to the line in index order.
	 *
	 * <p>
	 * MultiPoint2d values with less than 2 points will return NULL values.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a line2d value
	 */
	public Line2DExpression line2DResult() {
		return new Line2DExpression(new Line2DResultExpression(this));
	}

	/**
	 * Provides a value that represents the multipoint2d value as a polygon2d
	 * value.
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
		throw new UnsupportedOperationException("NOT DONE YET, SORRY.");
	}

	@Override
	public DBMultiPoint2D asExpressionColumn() {
		return new DBMultiPoint2D(this);
	}

	private static abstract class MultiPoint2DFunctionLine2DResult extends Line2DExpression {

		private static final long serialVersionUID = 1L;

		private final MultiPoint2DExpression first;
		private boolean requiresNullProtection;

		MultiPoint2DFunctionLine2DResult(MultiPoint2DExpression first) {
			this.first = first;
		}

		MultiPoint2DExpression getFirst() {
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

	private static abstract class SingleArgumentBooleanFunction<A extends DBExpression> extends BooleanExpression {

		private static final long serialVersionUID = 1L;

		private final A first;
		private boolean requiresNullProtection;

		SingleArgumentBooleanFunction(A first) {
			this.first = first;
		}

		A getFirst() {
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

	private static abstract class MultiPoint2DMultiPoint2DFunctionWithBooleanResult extends BooleanExpression {

		private static final long serialVersionUID = 1L;

		private final MultiPoint2DExpression first;
		private final MultiPoint2DExpression second;
		private boolean requiresNullProtection;

		MultiPoint2DMultiPoint2DFunctionWithBooleanResult(MultiPoint2DExpression first, MultiPoint2DExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		MultiPoint2DExpression getFirst() {
			return first;
		}

		MultiPoint2DExpression getSecond() {
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

	private static abstract class MultiPointNumberFunctionWithPoint2DResult extends Point2DExpression {

		private static final long serialVersionUID = 1L;

		private final MultiPoint2DExpression first;
		private final NumberExpression second;
		private boolean requiresNullProtection;

		MultiPointNumberFunctionWithPoint2DResult(MultiPoint2DExpression first, NumberExpression second) {
			this.first = first;
			this.second = second;
			if (this.first == null) {
				this.requiresNullProtection = true;
			}
		}

		MultiPoint2DExpression getFirst() {
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

	private static abstract class MultiPointFunctionWithNumberResult extends NumberExpression {

		private static final long serialVersionUID = 1L;

		private final MultiPoint2DExpression first;
		private boolean requiresNullProtection;

		MultiPointFunctionWithNumberResult(MultiPoint2DExpression first) {
			this.first = first;
			if (this.first == null) {
				this.requiresNullProtection = true;
			}
		}

		MultiPoint2DExpression getFirst() {
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

	private static abstract class MultiPointFunctionWithStringResult extends StringExpression {

		private static final long serialVersionUID = 1L;

		private final MultiPoint2DExpression first;
		private boolean requiresNullProtection;

		MultiPointFunctionWithStringResult(MultiPoint2DExpression first) {
			this.first = first;
			if (this.first == null) {
				this.requiresNullProtection = true;
			}
		}

		MultiPoint2DExpression getFirst() {
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

	private static abstract class MultiPoint2DFunctionWithGeometry2DResult extends Polygon2DExpression {

		private static final long serialVersionUID = 1L;

		private final MultiPoint2DExpression first;
		private boolean requiresNullProtection;

		MultiPoint2DFunctionWithGeometry2DResult(MultiPoint2DExpression first) {
			this.first = first;
			if (this.first == null) {
				this.requiresNullProtection = true;
			}
		}

		MultiPoint2DExpression getFirst() {
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

	private static class NullExpression extends MultiPoint2DExpression {

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

	private class StringResultExpression extends MultiPointFunctionWithStringResult {

		public StringResultExpression(MultiPoint2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransform(DBDefinition db) {
			try {
				return db.doMultiPoint2DAsTextTransform(getFirst().toSQLString(db));
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

	private class IsExpression extends MultiPoint2DMultiPoint2DFunctionWithBooleanResult {

		public IsExpression(MultiPoint2DExpression first, MultiPoint2DExpression second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doMultiPoint2DEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				return getFirst().stringResult().is(getSecond().stringResult()).toSQLString(db);
			}
		}

		@Override
		public MultiPoint2DExpression.IsExpression copy() {
			return new MultiPoint2DExpression.IsExpression(
					getFirst() == null ? null : getFirst().copy(),
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	private class MaxXExpression extends MultiPointFunctionWithNumberResult {

		public MaxXExpression(MultiPoint2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doMultiPoint2DGetMaxXTransform(getFirst().toSQLString(db));
		}

		@Override
		public MaxXExpression copy() {
			return new MultiPoint2DExpression.MaxXExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

private class MaxYExpression extends MultiPointFunctionWithNumberResult {

		public MaxYExpression(MultiPoint2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doMultiPoint2DGetMaxYTransform(getFirst().toSQLString(db));
		}

		@Override
		public MaxYExpression copy() {
			return new MultiPoint2DExpression.MaxYExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

private class MinXExpression extends MultiPointFunctionWithNumberResult {

		public MinXExpression(MultiPoint2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doMultiPoint2DGetMinXTransform(getFirst().toSQLString(db));
		}

		@Override
		public MinXExpression copy() {
			return new MultiPoint2DExpression.MinXExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

private class MinYExpression extends MultiPointFunctionWithNumberResult {

		public MinYExpression(MultiPoint2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doMultiPoint2DGetMinYTransform(getFirst().toSQLString(db));
		}

		@Override
		public MinYExpression copy() {
			return new MultiPoint2DExpression.MinYExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}

	}

	private class NumberOfPointsExpression extends MultiPointFunctionWithNumberResult {

		public NumberOfPointsExpression(MultiPoint2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doMultiPoint2DGetNumberOfPointsTransform(getFirst().toSQLString(db));
		}

		@Override
		public NumberOfPointsExpression copy() {
			return new MultiPoint2DExpression.NumberOfPointsExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}

	}

	private class GetPointAtIndexExpression extends MultiPointNumberFunctionWithPoint2DResult {

		public GetPointAtIndexExpression(MultiPoint2DExpression first, NumberExpression second) {
			super(first, second);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			return db.doMultiPoint2DGetPointAtIndexTransform(
					getFirst().toSQLString(db),
					getSecond().plus(1).toSQLString(db)
			);
		}

		@Override
		public GetPointAtIndexExpression copy() {
			return new MultiPoint2DExpression.GetPointAtIndexExpression(
					getFirst() == null ? null : getFirst().copy(),
					getSecond() == null ? null : getSecond().copy()
			);
		}

	}

	private class MeasurableDimensionsExpression extends MultiPointFunctionWithNumberResult {

		public MeasurableDimensionsExpression(MultiPoint2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doMultiPoint2DMeasurableDimensionsTransform(getFirst().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				return NumberExpression.value(0).toSQLString(db);
			}
		}

		@Override
		public MeasurableDimensionsExpression copy() {
			return new MultiPoint2DExpression.MeasurableDimensionsExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}
	}

	private class SpatialDimensionsExpression extends MultiPointFunctionWithNumberResult {

		public SpatialDimensionsExpression(MultiPoint2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doMultiPoint2DSpatialDimensionsTransform(getFirst().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				return NumberExpression.value(2).toSQLString(db);
			}
		}

		@Override
		public SpatialDimensionsExpression copy() {
			return new MultiPoint2DExpression.SpatialDimensionsExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}

	}

	private class HasMagnitudeExpression extends SingleArgumentBooleanFunction<DBExpression> {

		public HasMagnitudeExpression(DBExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doMultiPoint2DHasMagnitudeTransform(getFirst().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				return BooleanExpression.falseExpression().toSQLString(db);
			}
		}

		@Override
		public HasMagnitudeExpression copy() {
			return new MultiPoint2DExpression.HasMagnitudeExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}

	}

	private class MagnitudeExpression extends MultiPointFunctionWithNumberResult {

		public MagnitudeExpression(MultiPoint2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doMultiPoint2DGetMagnitudeTransform(getFirst().toSQLString(db));
			} catch (UnsupportedOperationException unsupported) {
				return nullExpression().toSQLString(db);
			}
		}

		@Override
		public MagnitudeExpression copy() {
			return new MultiPoint2DExpression.MagnitudeExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}

	}

	private class BoundingBoxExpression extends MultiPoint2DFunctionWithGeometry2DResult {

		public BoundingBoxExpression(MultiPoint2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String doExpressionTransform(DBDefinition db) {
			try {
				return db.doMultiPoint2DGetBoundingBoxTransform(getFirst().toSQLString(db));
			} catch (UnsupportedOperationException exp) {
				final MultiPoint2DExpression first = getFirst();
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
		public BoundingBoxExpression copy() {
			return new MultiPoint2DExpression.BoundingBoxExpression(
					getFirst() == null ? null : getFirst().copy()
			);
		}

	}

	private class Line2DResultExpression extends MultiPoint2DFunctionLine2DResult {

		public Line2DResultExpression(MultiPoint2DExpression first) {
			super(first);
		}
		private final static long serialVersionUID = 1l;

		@Override
		protected String doExpressionTransform(DBDefinition db) {
			return db.doMultiPoint2DToLine2DTransform(getFirst().toSQLString(db));
		}

		@Override
		public Line2DResultExpression copy() {
			return new MultiPoint2DExpression.Line2DResultExpression(
					getFirst() == null ? null : getFirst().copy());
		}

	}
}
