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
import nz.co.gregs.dbvolution.results.EqualComparable;
import nz.co.gregs.dbvolution.results.Point3DResult;
import com.vividsolutions.jts.geom.Point;
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.spatial3D.DBPoint3D;
import nz.co.gregs.dbvolution.datatypes.spatial3D.GeometryFactory3D;
import nz.co.gregs.dbvolution.datatypes.spatial3D.PointZ;
import nz.co.gregs.dbvolution.results.PointResult;

/**
 * Represents SQL expressions that are a 3 dimensional points, that is a
 * physical location in a 3D space with X and Y points.
 *
 * <p>
 * Use these methods to manipulate and transform point3d values and results.
 *
 * @author gregorygraham
 */
public class Point3DExpression implements PointResult, Point3DResult, EqualComparable<Point3DResult>, Spatial3DExpression, ExpressionColumn<DBPoint3D> {

	private Point3DResult innerPoint;
	private boolean nullProtectionRequired;

	/**
	 * Default constructor
	 *
	 */
	protected Point3DExpression() {
	}

	/**
	 * Create a Point3DExpression that represents the point value provided.
	 *
	 * @param value
	 */
	public Point3DExpression(Point3DResult value) {
		innerPoint = value;
		if (value == null || innerPoint.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	/**
	 * Create a Point3DExpression that represents the point value provided.
	 *
	 * @param point
	 */
	public Point3DExpression(PointZ point) {
		innerPoint = new DBPoint3D(point);
		if (point == null || innerPoint.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	/**
	 * Create a Point3DExpression that represents the point value provided.
	 *
	 * @param point
	 */
	public Point3DExpression(Coordinate point) {
		innerPoint = new DBPoint3D(new GeometryFactory3D().createPointZ(point));
		if (point == null || innerPoint.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	/**
	 * Create a Point3DExpression that represents the point value provided.
	 *
	 * @param point the value of this expression.
	 * @return a Point3DExpression of the point.
	 */
	public static Point3DExpression value(Coordinate point) {
		return new Point3DExpression(point);
	}

	/**
	 * Create a Point3DExpression that represents the point value provided.
	 *
	 * @param point the value of this expression.
	 * @return a Point3DExpression of the point.
	 */
	public static Point3DExpression value(PointZ point) {
		return new Point3DExpression(point);
	}

	/**
	 * Create a Point3DExpression that represents the point value provided.
	 *
	 * @param point the value of this expression.
	 * @return a Point3DExpression of the point.
	 */
	public static Point3DExpression value(Point3DResult point) {
		return new Point3DExpression(point);
	}

	/**
	 * Create a Point3DExpression that represents the values provided.
	 *
	 * @param xValue the X value of this expression.
	 * @param yValue the Y value of this expression.
	 * @param zValue the Z value of this expression.
	 * @return a Point3DExpression of the x, y, and z values provided.
	 */
	public static Point3DExpression value(Integer xValue, Integer yValue, Integer zValue) {
		return value(NumberExpression.value(xValue), NumberExpression.value(yValue), NumberExpression.value(zValue));
	}

	/**
	 * Create a Point3DExpression that represents the values provided.
	 *
	 * @param xValue the X value of this expression.
	 * @param yValue the Y value of this expression.
	 * @param zValue the Z value of this expression.
	 * @return a Point3DExpression of the x, y, and z values provided.
	 */
	public static Point3DExpression value(Long xValue, Long yValue, Long zValue) {
		return value(NumberExpression.value(xValue), NumberExpression.value(yValue), NumberExpression.value(zValue));
	}

	/**
	 * Create a Point3DExpression that represents the values provided.
	 *
	 * @param xValue the X value of this expression.
	 * @param yValue the Y value of this expression.
	 * @param zValue the Z value of this expression.
	 * @return a Point2DExpression of the x, y and z values provided.
	 */
	public static Point3DExpression value(Double xValue, Double yValue, Double zValue) {
		return value(NumberExpression.value(xValue), NumberExpression.value(yValue), NumberExpression.value(zValue));
	}

	/**
	 * Create a Point3DExpression that represents the values provided.
	 *
	 * @param xValue the X value of this expression.
	 * @param yValue the Y value of this expression.
	 * @param zValue the Z value of this expression.
	 * @return a Point2DExpression of the x, y, and z values provided.
	 */
	public static Point3DExpression value(NumberExpression xValue, NumberExpression yValue, NumberExpression zValue) {
		return new Point3DExpression(new NumberNumberFunctionWithPoint3DResult(xValue, yValue, zValue) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().transformCoordinatesIntoDatabasePoint3DFormat(getFirst().toSQLString(db), getSecond().toSQLString(db), getThird().toSQLString(db));
			}

		});
	}

	@Override
	public DBPoint3D getQueryableDatatypeForExpressionValue() {
		return new DBPoint3D();
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
	public Point3DExpression copy() {
		return new Point3DExpression(innerPoint);
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof Point3DExpression) {
			Point3DExpression otherExpr = (Point3DExpression) other;
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

	@Override
	public StringExpression stringResult() {
		return new StringExpression(new PointFunctionWithStringResult(this) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doPoint3DAsTextTransform(getFirst().toSQLString(db));
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
	 * @param rightHandSide the value to compare against.
	 * @return a BooleanExpression
	 */
	public BooleanExpression is(PointZ rightHandSide) {
		return is(new DBPoint3D(rightHandSide));
	}

	@Override
	public BooleanExpression is(Point3DResult rightHandSide) {
		return new BooleanExpression(new PointPointFunctionWithBooleanResult(this, new Point3DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doPoint3DEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return BooleanExpression.allOf(
							getFirst().stringResult().substringBetween("(", " ").numberResult()
							.is(getSecond().stringResult().substringBetween("(", " ").numberResult()),
							getFirst().stringResult().substringAfter("(").substringBetween(" ", " ").numberResult()
							.is(getSecond().stringResult().substringAfter("(").substringBetween(" ", " ").numberResult()),
							getFirst().stringResult().substringAfter("(").substringAfter(" ").substringBetween(" ", ")").numberResult()
							.is(getSecond().stringResult().substringAfter("(").substringAfter(" ").substringBetween(" ", ")").numberResult())
					).toSQLString(db);
				}
			}
		});
	}

	/**
	 * Creates a {@link BooleanExpression} that compares the 2 instances using the
	 * NOT EQUALS operation.
	 *
	 * @param rightHandSide the value to compare against.
	 * @return a BooleanExpression
	 */
	public BooleanExpression isNot(PointZ rightHandSide) {
		return isNot(new DBPoint3D(rightHandSide));
	}

	@Override
	public BooleanExpression isNot(Point3DResult rightHandSide) {
		return this.is(rightHandSide).not();
//		return new BooleanExpression(new PointPointFunctionWithBooleanResult(this, new Point2DExpression(rightHandSide)) {
//
//			@Override
//			public String doExpressionTransform(DBDatabase db) {
//				try {
//					return db.getDefinition().doPoint2DEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
//				} catch (UnsupportedOperationException unsupported) {
//					return BooleanExpression.notAllOf(
//							getFirst().stringResult().substringBetween("(", " ").numberResult()
//							.is(getSecond().stringResult().substringBetween("(", " ").numberResult()),
//							getFirst().stringResult().substringAfter("(").substringBetween(" ", ")").numberResult()
//							.is(getSecond().stringResult().substringAfter("(").substringBetween(" ", ")").numberResult())
//					).toSQLString(db);
//				}
//			}
//		});
	}

	@Override
	public NumberExpression getX() {
		return new NumberExpression(new PointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doPoint3DGetXTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return getFirst().stringResult().substringBetween("(", " ").numberResult().toSQLString(db);
				}
			}
		});
	}

	@Override
	public NumberExpression getY() {
		return new NumberExpression(new PointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doPoint3DGetYTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return getFirst().stringResult().substringAfter("(").substringBetween(" ", " ").numberResult().toSQLString(db);
				}
			}
		});
	}

	public NumberExpression getZ() {
		return new NumberExpression(new PointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doPoint3DGetZTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return getFirst().stringResult().substringAfter("(").substringAfter(" ").substringBetween(" ", ")").numberResult().toSQLString(db);
				}
			}
		});
	}

	@Override
	public NumberExpression measurableDimensions() {
		return new NumberExpression(new PointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doPoint3DMeasurableDimensionsTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return NumberExpression.value(0).toSQLString(db);
				}
			}
		});
	}

	@Override
	public NumberExpression spatialDimensions() {
		return new NumberExpression(new PointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doPoint3DSpatialDimensionsTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return NumberExpression.value(3).toSQLString(db);
				}
			}
		});
	}

	@Override
	public BooleanExpression hasMagnitude() {
		return new BooleanExpression(new PointFunctionWithBooleanResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doPoint3DHasMagnitudeTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return BooleanExpression.falseExpression().toSQLString(db);
				}
			}

			@Override
			public boolean isBooleanStatement() {
				return true;
			}

		});
	}

	@Override
	public NumberExpression magnitude() {
		return new NumberExpression(new PointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doPoint3DGetMagnitudeTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return nullExpression().toSQLString(db);
				}
			}
		});
	}

	/**
	 * Calculate the distance between this point and the other point.
	 *
	 * <p>
	 * Creates an SQL expression that will report the distance (in units) between
	 * these two points.
	 *
	 * <p>
	 * Essentially this utilizes a database specific method to calculate
	 * sqrt((x2-x1)^2+(y2-y1)^2+(z2-z1)^2).
	 *
	 * @param otherPoint the point from which to derive the distance.
	 * @return a number expression of the distance between the two points in
	 * units.
	 */
	public NumberExpression distanceTo(Point3DExpression otherPoint) {
		return new NumberExpression(new PointPointFunctionWithNumberResult(this, otherPoint) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doPoint3DDistanceBetweenTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return getSecond().getX().minus(getFirst().getX()).bracket().squared()
							.plus(getSecond().getY().minus(getFirst().getY()).bracket().squared())
							.plus(getSecond().getZ().minus(getFirst().getZ()).bracket().squared())
							.squareRoot().toSQLString(db);
				}
			}
		});
	}

	@Override
	public Polygon3DExpression boundingBox() {
		return new Polygon3DExpression(new PointFunctionWithGeometry3DResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doPoint3DGetBoundingBoxTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					final Point3DExpression first = getFirst();
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

	@Override
	public NumberExpression minZ() {
		return this.getX();
	}

	@Override
	public NumberExpression maxZ() {
		return this.getY();
	}

	@Override
	public DBPoint3D asExpressionColumn() {
		return new DBPoint3D(this);
	}

	private static abstract class PointPointFunctionWithBooleanResult extends BooleanExpression {

		private Point3DExpression first;
		private Point3DExpression second;
		private boolean requiresNullProtection;

		PointPointFunctionWithBooleanResult(Point3DExpression first, Point3DExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Point3DExpression getFirst() {
			return first;
		}

		Point3DExpression getSecond() {
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
		public PointPointFunctionWithBooleanResult copy() {
			PointPointFunctionWithBooleanResult newInstance;
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

	private static abstract class PointFunctionWithBooleanResult extends BooleanExpression {

		private Point3DExpression first;
		private boolean requiresNullProtection;

		PointFunctionWithBooleanResult(Point3DExpression first) {
			this.first = first;
		}

		Point3DExpression getFirst() {
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
		public PointFunctionWithBooleanResult copy() {
			PointFunctionWithBooleanResult newInstance;
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

	private static abstract class PointPointFunctionWithNumberResult extends NumberExpression {

		private Point3DExpression first;
		private Point3DExpression second;
		private boolean requiresNullProtection;

		PointPointFunctionWithNumberResult(Point3DExpression first, Point3DExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Point3DExpression getFirst() {
			return first;
		}

		Point3DExpression getSecond() {
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
		public PointPointFunctionWithNumberResult copy() {
			PointPointFunctionWithNumberResult newInstance;
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

	private static abstract class NumberNumberFunctionWithPoint3DResult extends Point3DExpression {

		private NumberExpression first;
		private NumberExpression second;
		private NumberExpression third;
		private boolean requiresNullProtection;

		NumberNumberFunctionWithPoint3DResult(NumberExpression first, NumberExpression second, NumberExpression third) {
			this.first = first;
			this.second = second;
			this.third = third;
			if (this.second == null || this.second.getIncludesNull()||this.third==null|| this.third.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		NumberExpression getFirst() {
			return first;
		}

		NumberExpression getSecond() {
			return second;
		}

		NumberExpression getThird() {
			return third;
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
		public NumberNumberFunctionWithPoint3DResult copy() {
			NumberNumberFunctionWithPoint3DResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
			newInstance.second = second.copy();
			newInstance.third = third.copy();
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
			if (third != null) {
				hashSet.addAll(third.getTablesInvolved());
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return first.isAggregator() || second.isAggregator() || third.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

	private static abstract class PointFunctionWithNumberResult extends NumberExpression {

		private Point3DExpression first;
		private boolean requiresNullProtection;

		PointFunctionWithNumberResult(Point3DExpression first) {
			this.first = first;
//			this.second = second;
			if (this.first == null) {// || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Point3DExpression getFirst() {
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
		public PointFunctionWithNumberResult copy() {
			PointFunctionWithNumberResult newInstance;
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

	private static abstract class PointFunctionWithStringResult extends StringExpression {

		private Point3DExpression first;
		private boolean requiresNullProtection;

		PointFunctionWithStringResult(Point3DExpression first) {
			this.first = first;
			if (this.first == null) {
				this.requiresNullProtection = true;
			}
		}

		Point3DExpression getFirst() {
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
		public PointFunctionWithStringResult copy() {
			PointFunctionWithStringResult newInstance;
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

	private static abstract class PointFunctionWithGeometry3DResult extends Polygon3DExpression {

		private Point3DExpression first;
		private boolean requiresNullProtection;

		PointFunctionWithGeometry3DResult(Point3DExpression first) {
			this.first = first;
			if (this.first == null) {
				this.requiresNullProtection = true;
			}
		}

		Point3DExpression getFirst() {
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
		public PointFunctionWithGeometry3DResult copy() {
			PointFunctionWithGeometry3DResult newInstance;
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
