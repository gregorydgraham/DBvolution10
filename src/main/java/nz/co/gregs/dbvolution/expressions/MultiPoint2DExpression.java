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

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBMultiPoint2D;
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

	private final MultiPoint2DResult innerPoint;
	private final boolean nullProtectionRequired;

	/**
	 * Default constructor
	 *
	 */
	protected MultiPoint2DExpression() {
		innerPoint = null;
		nullProtectionRequired = false;
	}

	/**
	 * Create a {@link MultiPoint2DExpression} representing from provided
	 * {@link MultiPoint2DResult}/{@link MultiPoint2DExpression}
	 *
	 * @param value
	 */
	public MultiPoint2DExpression(MultiPoint2DResult value) {
		innerPoint = value;
		nullProtectionRequired = value == null || innerPoint.getIncludesNull();
	}

	/**
	 * Create a {@link MultiPoint2DExpression} representing from provided
	 * {@link Point JTS points}.
	 *
	 * @param points
	 */
	public MultiPoint2DExpression(Point... points) {
		innerPoint = new DBMultiPoint2D(points);
		nullProtectionRequired = points == null || points.length == 0 || innerPoint.getIncludesNull();
	}

	/**
	 * Create a {@link MultiPoint2DExpression} representing from provided
	 * {@link MultiPoint JTS multipoint}.
	 *
	 * @param points
	 */
	public MultiPoint2DExpression(MultiPoint points) {
		innerPoint = new DBMultiPoint2D(points);
		nullProtectionRequired = points == null || innerPoint.getIncludesNull();
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
	public static MultiPoint2DExpression value(MultiPoint points) {
		return new MultiPoint2DExpression(points);
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
	public static MultiPoint2DExpression value(MultiPoint2DResult points) {
		return new MultiPoint2DExpression(points);
	}

	@Override
	public DBMultiPoint2D getQueryableDatatypeForExpressionValue() {
		return new DBMultiPoint2D();
	}

	@Override
	public String toSQLString(DBDefinition db) {
		if (innerPoint == null) {
			return db.getNull();
		} else {
			return innerPoint.toSQLString(db);
		}
	}

	@Override
	public MultiPoint2DExpression copy() {
		return new MultiPoint2DExpression(innerPoint);
	}
	
	@Override
	public MultiPoint2DExpression nullExpression() {

		return new MultiPoint2DExpression() {

			@Override
			public String toSQLString(DBDefinition db) {
				return db.getNull();
			}
		};
	}

	@Override
	public MultiPoint2DExpression expression(MultiPoint value) {
		return new MultiPoint2DExpression(value);
	}

	@Override
	public MultiPoint2DExpression expression(MultiPoint2DResult value) {
		return new MultiPoint2DExpression(value);
	}

	@Override
	public MultiPoint2DExpression expression(DBMultiPoint2D value) {
		return new MultiPoint2DExpression(value);
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof MultiPoint2DExpression) {
			MultiPoint2DExpression otherExpr = (MultiPoint2DExpression) other;
			return this.innerPoint == otherExpr.innerPoint;
		}
		return false;
	}

	@Override
	public int hashCode() {
		int hash = 7;
		hash = 97 * hash + (this.innerPoint != null ? this.innerPoint.hashCode() : 0);
		hash = 97 * hash + (this.nullProtectionRequired ? 1 : 0);
		return hash;
	}

	@Override
	public boolean isAggregator() {
		return innerPoint == null ? false : innerPoint.isAggregator();
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		HashSet<DBRow> hashSet = new HashSet<DBRow>();
		if (innerPoint != null) {
			hashSet.addAll(innerPoint.getTablesInvolved());
		}
		return hashSet;
	}

	@Override
	public boolean isPurelyFunctional() {
		return innerPoint == null ? true : innerPoint.isPurelyFunctional();
	}

	@Override
	public boolean getIncludesNull() {
		return nullProtectionRequired;
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
		return new StringExpression(new MultiPointFunctionWithStringResult(this) {

			@Override
			protected String doExpressionTransform(DBDefinition db) {
				try {
					return db.doMultiPoint2DAsTextTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return getFirst().toSQLString(db);
				}
			}
		});
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
		return new BooleanExpression(new MultiPoint2DMultiPoint2DFunctionWithBooleanResult(this, new MultiPoint2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDefinition db) {
				try {
					return db.doMultiPoint2DEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return getFirst().stringResult().is(getSecond().stringResult()).toSQLString(db);
				}
			}
		});
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
		return new NumberExpression(new MultiPointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDefinition db) {
				return db.doMultiPoint2DGetMaxXTransform(getFirst().toSQLString(db));
			}
		});
	}

	@Override
	public NumberExpression maxY() {
		return new NumberExpression(new MultiPointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDefinition db) {
				return db.doMultiPoint2DGetMaxYTransform(getFirst().toSQLString(db));
			}
		});
	}

	@Override
	public NumberExpression minX() {
		return new NumberExpression(new MultiPointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDefinition db) {
				return db.doMultiPoint2DGetMinXTransform(getFirst().toSQLString(db));
			}
		});
	}

	@Override
	public NumberExpression minY() {
		return new NumberExpression(new MultiPointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDefinition db) {
				return db.doMultiPoint2DGetMinYTransform(getFirst().toSQLString(db));
			}
		});
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
		return new NumberExpression(new MultiPointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDefinition db) {
				return db.doMultiPoint2DGetNumberOfPointsTransform(getFirst().toSQLString(db));
			}
		});
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
		return new Point2DExpression(new MultiPointNumberFunctionWithPoint2DResult(this, index) {

			@Override
			public String doExpressionTransform(DBDefinition db) {
				return db.doMultiPoint2DGetPointAtIndexTransform(
						getFirst().toSQLString(db),
						getSecond().plus(1).toSQLString(db)
				);
			}
		});
	}

	@Override
	public NumberExpression measurableDimensions() {
		return new NumberExpression(new MultiPointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDefinition db) {
				try {
					return db.doMultiPoint2DMeasurableDimensionsTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return NumberExpression.value(0).toSQLString(db);
				}
			}
		});
	}

	@Override
	public NumberExpression spatialDimensions() {
		return new NumberExpression(new MultiPointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDefinition db) {
				try {
					return db.doMultiPoint2DSpatialDimensionsTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return NumberExpression.value(2).toSQLString(db);
				}
			}
		});
	}

	@Override
	public BooleanExpression hasMagnitude() {
		return new BooleanExpression(new SingleArgumentBooleanFunction<DBExpression>(this) {

			@Override
			public String doExpressionTransform(DBDefinition db) {
				try {
					return db.doMultiPoint2DHasMagnitudeTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return BooleanExpression.falseExpression().toSQLString(db);
				}
			}
		});
	}

	@Override
	public NumberExpression magnitude() {
		return new NumberExpression(new MultiPointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDefinition db) {
				try {
					return db.doMultiPoint2DGetMagnitudeTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return nullExpression().toSQLString(db);
				}
			}
		});
	}

	@Override
	public Polygon2DExpression boundingBox() {
		return new Polygon2DExpression(new MultiPoint2DFunctionWithGeometry2DResult(this) {

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
		});
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
		return new Line2DExpression(new MultiPoint2DFunctionLine2DResult(this) {

			@Override
			protected String doExpressionTransform(DBDefinition db) {
				return db.doMultiPoint2DToLine2DTransform(getFirst().toSQLString(db));
			}
		});
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

		private MultiPoint2DExpression first;
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

		@Override
		@SuppressWarnings("unchecked")
		public MultiPoint2DFunctionLine2DResult copy() {
			MultiPoint2DFunctionLine2DResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
			return newInstance;
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
			return first.isAggregator();// || second.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

	private static abstract class SingleArgumentBooleanFunction<A extends DBExpression> extends BooleanExpression {

		private A first;
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

		@Override
		@SuppressWarnings("unchecked")
		public SingleArgumentBooleanFunction<A> copy() {
			SingleArgumentBooleanFunction<A> newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = (A) first.copy();
			return newInstance;
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
			return first.isAggregator();// || second.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

	private static abstract class MultiPoint2DMultiPoint2DFunctionWithBooleanResult extends BooleanExpression {

		private MultiPoint2DExpression first;
		private MultiPoint2DExpression second;
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

		@Override
		public MultiPoint2DMultiPoint2DFunctionWithBooleanResult copy() {
			MultiPoint2DMultiPoint2DFunctionWithBooleanResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
			newInstance.second = second.copy();
			return newInstance;
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

		private MultiPoint2DExpression first;
		private NumberExpression second;
		private boolean requiresNullProtection;

		MultiPointNumberFunctionWithPoint2DResult(MultiPoint2DExpression first, NumberExpression second) {
			this.first = first;
			this.second = second;
			if (this.first == null) {// || this.second.getIncludesNull()) {
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

		@Override
		public MultiPointNumberFunctionWithPoint2DResult copy() {
			MultiPointNumberFunctionWithPoint2DResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
			newInstance.second = second.copy();
			return newInstance;
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

		private MultiPoint2DExpression first;
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

		@Override
		public MultiPointFunctionWithNumberResult copy() {
			MultiPointFunctionWithNumberResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
			return newInstance;
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

		private MultiPoint2DExpression first;
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

		@Override
		public MultiPointFunctionWithStringResult copy() {
			MultiPointFunctionWithStringResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
			return newInstance;
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

		private MultiPoint2DExpression first;
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

		@Override
		public MultiPoint2DFunctionWithGeometry2DResult copy() {
			MultiPoint2DFunctionWithGeometry2DResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException | IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
			return newInstance;
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
}
