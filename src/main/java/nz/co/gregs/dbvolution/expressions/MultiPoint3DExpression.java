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
import nz.co.gregs.dbvolution.results.EqualComparable;
import com.vividsolutions.jts.geom.Point;
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.spatial3D.DBMultiPoint3D;
import nz.co.gregs.dbvolution.results.MultiPoint3DResult;
import nz.co.gregs.dbvolution.results.NumberResult;

/**
 * Creates and transforms MultiPoint3D values within your database queries.
 *
 * <p>
 * Use these methods to manipulate your MultiPoint3D columns and results for
 * finer control of the query results.
 *
 * @author gregorygraham
 */
public class MultiPoint3DExpression implements MultiPoint3DResult, EqualComparable<MultiPoint3DResult>, Spatial3DExpression, ExpressionColumn<DBMultiPoint3D> {

	private MultiPoint3DResult innerPoint;
	private boolean nullProtectionRequired;

	/**
	 * Default constructor
	 *
	 */
	protected MultiPoint3DExpression() {
	}

	/**
	 * Create a {@link MultiPoint3DExpression} representing from provided
	 * {@link MultiPoint3DResult}/{@link MultiPoint3DExpression}
	 *
	 * @param value
	 */
	public MultiPoint3DExpression(MultiPoint3DResult value) {
		innerPoint = value;
		if (value == null || innerPoint.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	/**
	 * Create a {@link MultiPoint3DExpression} representing from provided
	 * {@link Point JTS points}.
	 *
	 * @param points
	 */
	public MultiPoint3DExpression(Point... points) {
		innerPoint = new DBMultiPoint3D(points);
		if (points == null || points.length == 0 || innerPoint.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	/**
	 * Create a {@link MultiPoint3DExpression} representing from provided
	 * {@link MultiPoint JTS multipoint}.
	 *
	 * @param points
	 */
	public MultiPoint3DExpression(MultiPoint points) {
		innerPoint = new DBMultiPoint3D(points);
		if (points == null || innerPoint.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	/**
	 * Create a {@link MultiPoint3DExpression} representing from provided
	 * {@link Point JTS points}.
	 *
	 * @param points the points to include in this value
	 * @return a MultiPoint3DExpression.
	 */
	public static MultiPoint3DExpression value(Point... points) {
		return new MultiPoint3DExpression(points);
	}

	/**
	 * Create a {@link MultiPoint3DExpression} representing from provided
	 * {@link Coordinate JTS coordinates}.
	 *
	 * @param coords the points to include in this value
	 * @return a MultiPoint3DExpression.
	 */
	public static MultiPoint3DExpression value(Coordinate... coords) {
		GeometryFactory geometryFactory = new GeometryFactory();
		MultiPoint multiPoint = geometryFactory.createMultiPoint(coords);
		return new MultiPoint3DExpression(multiPoint);
	}

	/**
	 * Create a {@link MultiPoint3DExpression} representing from the provided
	 * {@link MultiPoint JTS multipoint}.
	 *
	 * @param points the points to include in this value
	 * @return a MultiPoint3DExpression representing the points
	 */
	public static MultiPoint3DExpression value(MultiPoint points) {
		return new MultiPoint3DExpression(points);
	}

	/**
	 * Create a {@link MultiPoint3DExpression} representing from the
	 * {@link MultiPoint3DResult}.
	 *
	 * @param points the points to include in this value
	 * @return a MultiPoint3DExpression representing the points
	 */
	public static MultiPoint3DExpression value(MultiPoint3DResult points) {
		return new MultiPoint3DExpression(points);
	}

	@Override
	public DBMultiPoint3D getQueryableDatatypeForExpressionValue() {
		return new DBMultiPoint3D();
	}

	@Override
	public String toSQLString(DBDatabase db) {
		if (innerPoint == null) {
			return db.getDefinition().getNull();
		} else {
			return innerPoint.toSQLString(db);
		}
	}

	@Override
	public MultiPoint3DExpression copy() {
		return new MultiPoint3DExpression(innerPoint);
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof MultiPoint3DExpression) {
			MultiPoint3DExpression otherExpr = (MultiPoint3DExpression) other;
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
	 * Transform the MultiPoint3D value into the Well Known Text (WKT) version of
	 * the value.
	 *
	 * @return a StringExpression representing the value transformed into a WKT
	 * string.
	 */
	@Override
	public StringExpression stringResult() {
		return new StringExpression(new MultiPointFunctionWithStringResult(this) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doMultiPoint3DAsTextTransform(getFirst().toSQLString(db));
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
	 * @return a BooleanExpression
	 */
	public BooleanExpression is(MultiPoint rightHandSide) {
		return is(new DBMultiPoint3D(rightHandSide));
	}

	@Override
	public BooleanExpression is(MultiPoint3DResult rightHandSide) {
		return new BooleanExpression(new MultiPoint3DMultiPoint3DFunctionWithBooleanResult(this, new MultiPoint3DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doMultiPoint3DEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
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
	 * @param rightHandSide  the {@link MultiPoint} to compare to
	 * @return a BooleanExpression
	 */
	public BooleanExpression isNot(MultiPoint rightHandSide) {
		return this.is(rightHandSide).not();
	}

	@Override
	public BooleanExpression isNot(MultiPoint3DResult rightHandSide) {
		return is(rightHandSide).not();
	}

	@Override
	public NumberExpression maxX() {
		return new NumberExpression(new MultiPointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doMultiPoint3DGetMaxXTransform(getFirst().toSQLString(db));
			}
		});
	}

	@Override
	public NumberExpression maxY() {
		return new NumberExpression(new MultiPointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doMultiPoint3DGetMaxYTransform(getFirst().toSQLString(db));
			}
		});
	}

	@Override
	public NumberExpression maxZ() {
		return new NumberExpression(new MultiPointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doMultiPoint3DGetMaxZTransform(getFirst().toSQLString(db));
			}
		});
	}

	@Override
	public NumberExpression minX() {
		return new NumberExpression(new MultiPointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doMultiPoint3DGetMinXTransform(getFirst().toSQLString(db));
			}
		});
	}

	@Override
	public NumberExpression minY() {
		return new NumberExpression(new MultiPointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doMultiPoint3DGetMinYTransform(getFirst().toSQLString(db));
			}
		});
	}

	@Override
	public NumberExpression minZ() {
		return new NumberExpression(new MultiPointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doMultiPoint3DGetMinZTransform(getFirst().toSQLString(db));
			}
		});
	}

	/**
	 * Create a NumberExpression that represents the number of points stored in
	 * the MultiPoint value.
	 *
	 * @return a number expression
	 */
	public NumberExpression numberOfPoints() {
		return new NumberExpression(new MultiPointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doMultiPoint3DGetNumberOfPointsTransform(getFirst().toSQLString(db));
			}
		});
	}

	/**
	 * Create a point3D expression that represents the point at the given index.
	 *
	 * <p>
	 * This method provides array-like access to the points in the multipoint3D
	 * value and uses a Java standard zero-based index.
	 *
	 * <p>
	 * Databases may use a one-based index but DBvolution compensates for that
	 * automatically.
	 *
	 * <p>
	 * Indexes beyond the end of the multipoint3D will return NULL values.
	 *
	 * @param index a zero-based index within the multipoint3D
	 * @return a point3D expression
	 */
	public Point3DExpression getPointAtIndexZeroBased(int index) {
		return getPointAtIndexZeroBased(NumberExpression.value(index));
	}

	/**
	 * Create a point3D expression that represents the point at the given index.
	 *
	 * <p>
	 * This method provides array-like access to the points in the multipoint3D
	 * value and uses a Java standard zero-based index.
	 *
	 * <p>
	 * Databases may use a one-based index but DBvolution compensates for that
	 * automatically.
	 *
	 * <p>
	 * Indexes beyond the end of the multipoint3D will return NULL values.
	 *
	 * @param index a zero-based index within the multipoint3D
	 * @return a point3D expression
	 */
	public Point3DExpression getPointAtIndexZeroBased(long index) {
		return getPointAtIndexZeroBased(NumberExpression.value(index));
	}

	/**
	 * Create a point3D expression that represents the point at the given index.
	 *
	 * <p>
	 * This method provides array-like access to the points in the multipoint3D
	 * value and uses a Java standard zero-based index.
	 *
	 * <p>
	 * Databases may use a one-based index but DBvolution compensates for that
	 * automatically.
	 *
	 * <p>
	 * Indexes beyond the end of the multipoint3D will return NULL values.
	 *
	 * @param index a zero-based index within the multipoint3D
	 * @return a point3D expression
	 */
	public Point3DExpression getPointAtIndexZeroBased(NumberResult index) {
		return new Point3DExpression(new MultiPointNumberFunctionWithPoint3DResult(this, new NumberExpression(index)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doMultiPoint3DGetPointAtIndexTransform(
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
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doMultiPoint3DMeasurableDimensionsTransform(getFirst().toSQLString(db));
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
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doMultiPoint3DSpatialDimensionsTransform(getFirst().toSQLString(db));
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
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doMultiPoint3DHasMagnitudeTransform(getFirst().toSQLString(db));
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
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doMultiPoint3DGetMagnitudeTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return nullExpression().toSQLString(db);
				}
			}
		});
	}

	@Override
	public Polygon3DExpression boundingBox() {
		return new Polygon3DExpression(new MultiPoint3DFunctionWithGeometry3DResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doMultiPoint3DGetBoundingBoxTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException exp) {
					final MultiPoint3DExpression first = getFirst();
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
							Point3DExpression.value(minX, minY, minZ))
							.toSQLString(db);
				}
			}
		});
	}

	/**
	 * Provides a expression that represents the multipoint3D value as a line3D
	 * value.
	 *
	 * <P>
	 * Points are added to the line in index order.
	 *
	 * <p>
	 * MultiPoint3D values with less than 2 points will return NULL values.
	 *
	 * @return a line3D expression
	 */
	public Line3DExpression line3DResult() {
		return new Line3DExpression(new SingleArgumentLine3DFunction<MultiPoint3DExpression>(this) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doMultiPoint3DToLine3DTransform(getFirst().toSQLString(db));
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

	@Override
	public DBMultiPoint3D asExpressionColumn() {
		return new DBMultiPoint3D(this);
	}

	private static abstract class SingleArgumentLine3DFunction<A extends DBExpression> extends Line3DExpression {

		private A first;
//		private B second;
		private boolean requiresNullProtection;

		SingleArgumentLine3DFunction(A first) {
			this.first = first;
//			this.second = second;
//			if (this.second == null || this.second.getIncludesNull()) {
//				this.requiresNullProtection = true;
//			}
		}

		A getFirst() {
			return first;
		}

//		MultiPoint3DExpression getSecond() {
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
		@SuppressWarnings("unchecked")
		public SingleArgumentLine3DFunction<A> copy() {
			SingleArgumentLine3DFunction<A> newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = (A) first.copy();
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
			return first.isAggregator();// || second.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

	private static abstract class SingleArgumentBooleanFunction<A extends DBExpression> extends BooleanExpression {

		private A first;
//		private B second;
		private boolean requiresNullProtection;

		SingleArgumentBooleanFunction(A first) {
			this.first = first;
//			this.second = second;
//			if (this.second == null || this.second.getIncludesNull()) {
//				this.requiresNullProtection = true;
//			}
		}

		A getFirst() {
			return first;
		}

//		MultiPoint3DExpression getSecond() {
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
		@SuppressWarnings("unchecked")
		public SingleArgumentBooleanFunction<A> copy() {
			SingleArgumentBooleanFunction<A> newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = (A) first.copy();
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
			return first.isAggregator();// || second.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

//	private static abstract class SingleArgumentPolygon3DFunction<A extends DBExpression> extends Polygon3DExpression {
//
//		private A first;
////		private B second;
//		private boolean requiresNullProtection;
//
//		SingleArgumentPolygon3DFunction(A first) {
//			this.first = first;
////			this.second = second;
////			if (this.second == null || this.second.getIncludesNull()) {
////				this.requiresNullProtection = true;
////			}
//		}
//
//		A getFirst() {
//			return first;
//		}
//
////		MultiPoint3DExpression getSecond() {
////			return second;
////		}
//		@Override
//		public final String toSQLString(DBDatabase db) {
//			if (this.getIncludesNull()) {
//				return BooleanExpression.isNull(first).toSQLString(db);
//			} else {
//			return doExpressionTransform(db);
//			}
//		}
//
//		@Override
//		@SuppressWarnings("unchecked")
//		public SingleArgumentPolygon3DFunction<A> copy() {
//			SingleArgumentPolygon3DFunction<A> newInstance;
//			try {
//				newInstance = getClass().newInstance();
//			} catch (InstantiationException ex) {
//				throw new RuntimeException(ex);
//			} catch (IllegalAccessException ex) {
//				throw new RuntimeException(ex);
//			}
//			newInstance.first = (A) first.copy();
////			newInstance.second = second.copy();
//			return newInstance;
//		}
//
//		protected abstract String doExpressionTransform(DBDatabase db);
//
//		@Override
//		public Set<DBRow> getTablesInvolved() {
//			HashSet<DBRow> hashSet = new HashSet<DBRow>();
//			if (first != null) {
//				hashSet.addAll(first.getTablesInvolved());
//			}
////			if (second != null) {
////				hashSet.addAll(second.getTablesInvolved());
////			}
//			return hashSet;
//		}
//
//		@Override
//		public boolean isAggregator() {
//			return first.isAggregator();// || second.isAggregator();
//		}
//
//		@Override
//		public boolean getIncludesNull() {
//			return requiresNullProtection;
//		}
//	}
	private static abstract class MultiPoint3DMultiPoint3DFunctionWithBooleanResult extends BooleanExpression {

		private MultiPoint3DExpression first;
		private MultiPoint3DExpression second;
		private boolean requiresNullProtection;

		MultiPoint3DMultiPoint3DFunctionWithBooleanResult(MultiPoint3DExpression first, MultiPoint3DExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		MultiPoint3DExpression getFirst() {
			return first;
		}

		MultiPoint3DExpression getSecond() {
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
		public MultiPoint3DMultiPoint3DFunctionWithBooleanResult copy() {
			MultiPoint3DMultiPoint3DFunctionWithBooleanResult newInstance;
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

//	private static abstract class MultiPointMultiPointFunctionWithNumberResult extends NumberExpression {
//
//		private MultiPoint3DExpression first;
//		private MultiPoint3DExpression second;
//		private boolean requiresNullProtection;
//
//		MultiPointMultiPointFunctionWithNumberResult(MultiPoint3DExpression first, MultiPoint3DExpression second) {
//			this.first = first;
//			this.second = second;
//			if (this.second == null || this.second.getIncludesNull()) {
//				this.requiresNullProtection = true;
//			}
//		}
//
//		MultiPoint3DExpression getFirst() {
//			return first;
//		}
//
//		MultiPoint3DExpression getSecond() {
//			return second;
//		}
//
//		@Override
//		public final String toSQLString(DBDatabase db) {
//			if (this.getIncludesNull()) {
//				return BooleanExpression.isNull(first).toSQLString(db);
//			} else {
//				return doExpressionTransform(db);
//			}
//		}
//
//		@Override
//		public MultiPointMultiPointFunctionWithNumberResult copy() {
//			MultiPointMultiPointFunctionWithNumberResult newInstance;
//			try {
//				newInstance = getClass().newInstance();
//			} catch (InstantiationException ex) {
//				throw new RuntimeException(ex);
//			} catch (IllegalAccessException ex) {
//				throw new RuntimeException(ex);
//			}
//			newInstance.first = first.copy();
//			newInstance.second = second.copy();
//			return newInstance;
//		}
//
//		protected abstract String doExpressionTransform(DBDatabase db);
//
//		@Override
//		public Set<DBRow> getTablesInvolved() {
//			HashSet<DBRow> hashSet = new HashSet<DBRow>();
//			if (first != null) {
//				hashSet.addAll(first.getTablesInvolved());
//			}
//			if (second != null) {
//				hashSet.addAll(second.getTablesInvolved());
//			}
//			return hashSet;
//		}
//
//		@Override
//		public boolean isAggregator() {
//			return first.isAggregator() || second.isAggregator();
//		}
//
//		@Override
//		public boolean getIncludesNull() {
//			return requiresNullProtection;
//		}
//	}

//	private static abstract class NumberNumberFunctionWithMultiPoint3DResult extends MultiPoint3DExpression {
//
//		private NumberExpression first;
//		private NumberExpression second;
//		private boolean requiresNullProtection;
//
//		NumberNumberFunctionWithMultiPoint3DResult(NumberExpression first, NumberExpression second) {
//			this.first = first;
//			this.second = second;
//			if (this.second == null || this.second.getIncludesNull()) {
//				this.requiresNullProtection = true;
//			}
//		}
//
//		NumberExpression getFirst() {
//			return first;
//		}
//
//		NumberExpression getSecond() {
//			return second;
//		}
//
//		@Override
//		public final String toSQLString(DBDatabase db) {
//			if (this.getIncludesNull()) {
//				return BooleanExpression.isNull(first).toSQLString(db);
//			} else {
//				return doExpressionTransform(db);
//			}
//		}
//
//		@Override
//		public NumberNumberFunctionWithMultiPoint3DResult copy() {
//			NumberNumberFunctionWithMultiPoint3DResult newInstance;
//			try {
//				newInstance = getClass().newInstance();
//			} catch (InstantiationException ex) {
//				throw new RuntimeException(ex);
//			} catch (IllegalAccessException ex) {
//				throw new RuntimeException(ex);
//			}
//			newInstance.first = first.copy();
//			newInstance.second = second.copy();
//			return newInstance;
//		}
//
//		protected abstract String doExpressionTransform(DBDatabase db);
//
//		@Override
//		public Set<DBRow> getTablesInvolved() {
//			HashSet<DBRow> hashSet = new HashSet<DBRow>();
//			if (first != null) {
//				hashSet.addAll(first.getTablesInvolved());
//			}
//			if (second != null) {
//				hashSet.addAll(second.getTablesInvolved());
//			}
//			return hashSet;
//		}
//
//		@Override
//		public boolean isAggregator() {
//			return first.isAggregator() || second.isAggregator();
//		}
//
//		@Override
//		public boolean getIncludesNull() {
//			return requiresNullProtection;
//		}
//	}
//
	private static abstract class MultiPointNumberFunctionWithPoint3DResult extends Point3DExpression {

		private MultiPoint3DExpression first;
		private NumberExpression second;
		private boolean requiresNullProtection;

		MultiPointNumberFunctionWithPoint3DResult(MultiPoint3DExpression first, NumberExpression second) {
			this.first = first;
			this.second = second;
			if (this.first == null) {// || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		MultiPoint3DExpression getFirst() {
			return first;
		}

		NumberExpression getSecond() {
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
		public MultiPointNumberFunctionWithPoint3DResult copy() {
			MultiPointNumberFunctionWithPoint3DResult newInstance;
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

	private static abstract class MultiPointFunctionWithNumberResult extends NumberExpression {

		private MultiPoint3DExpression first;
//		private Point3DExpression second;
		private boolean requiresNullProtection;

		MultiPointFunctionWithNumberResult(MultiPoint3DExpression first) {
			this.first = first;
//			this.second = second;
			if (this.first == null) {// || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		MultiPoint3DExpression getFirst() {
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
		public MultiPointFunctionWithNumberResult copy() {
			MultiPointFunctionWithNumberResult newInstance;
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

	private static abstract class MultiPointFunctionWithStringResult extends StringExpression {

		private MultiPoint3DExpression first;
//		private Point3DExpression second;
		private boolean requiresNullProtection;

		MultiPointFunctionWithStringResult(MultiPoint3DExpression first) {
			this.first = first;
//			this.second = second;
			if (this.first == null) {// || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		MultiPoint3DExpression getFirst() {
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
		public MultiPointFunctionWithStringResult copy() {
			MultiPointFunctionWithStringResult newInstance;
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

	private static abstract class MultiPoint3DFunctionWithGeometry3DResult extends Polygon3DExpression {

		private MultiPoint3DExpression first;
//		private Point3DExpression second;
		private boolean requiresNullProtection;

		MultiPoint3DFunctionWithGeometry3DResult(MultiPoint3DExpression first) {
			this.first = first;
//			this.second = second;
			if (this.first == null) {// || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		MultiPoint3DExpression getFirst() {
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
		public MultiPoint3DFunctionWithGeometry3DResult copy() {
			MultiPoint3DFunctionWithGeometry3DResult newInstance;
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
