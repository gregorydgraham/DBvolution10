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

import nz.co.gregs.dbvolution.results.EqualComparable;
import nz.co.gregs.dbvolution.results.Point2DResult;
import com.vividsolutions.jts.geom.Point;
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBPoint2D;

/**
 *
 * @author gregorygraham
 */
public class Point2DExpression implements Point2DResult, EqualComparable<Point2DResult>, Spatial2DExpression {

	private Point2DResult innerPoint;
	private boolean nullProtectionRequired;

	protected Point2DExpression() {
	}

	public Point2DExpression(Point2DResult value) {
		innerPoint = value;
		if (value == null || innerPoint.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	public Point2DExpression(Point point) {
		innerPoint = new DBPoint2D(point);
		if (point == null || innerPoint.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	public static Point2DExpression value(Point point) {
		return new Point2DExpression(point);
	}

	public static Point2DExpression value(Integer xValue, Integer yValue) {
		return value(NumberExpression.value(xValue), NumberExpression.value(yValue));
	}

	public static Point2DExpression value(Long xValue, Long yValue) {
		return value(NumberExpression.value(xValue), NumberExpression.value(yValue));
	}

	public static Point2DExpression value(Double xValue, Double yValue) {
		return value(NumberExpression.value(xValue), NumberExpression.value(yValue));
	}

	public static Point2DExpression value(NumberExpression xValue, NumberExpression yValue) {
		return new Point2DExpression(new NumberNumberFunctionWithPoint2DResult(xValue, yValue) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().transformCoordinatesIntoDatabasePoint2DFormat(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}

		});
	}

	@Override
	public DBPoint2D getQueryableDatatypeForExpressionValue() {
		return new DBPoint2D();
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
	public Point2DExpression copy() {
		return new Point2DExpression(innerPoint);
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof Point2DExpression) {
			Point2DExpression otherExpr = (Point2DExpression) other;
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
	public StringExpression toWKTFormat(){
		return stringResult();
	}

	public StringExpression stringResult() {
		return new StringExpression(new PointFunctionWithStringResult(this) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doPoint2DAsTextTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return getFirst().toSQLString(db);
				}
			}
		});
	}

	public BooleanExpression is(Point rightHandSide) {
		return is(new DBPoint2D(rightHandSide));
	}

	@Override
	public BooleanExpression is(Point2DResult rightHandSide) {
		return new BooleanExpression(new PointPointFunctionWithBooleanResult(this, new Point2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doPoint2DEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return 
							BooleanExpression.allOf(
							getFirst().stringResult().substringBetween("(", " ").numberResult()
							.is(getSecond().stringResult().substringBetween("(", " ").numberResult()),
							getFirst().stringResult().substringAfter("(").substringBetween(" ", ")").numberResult()
							.is(getSecond().stringResult().substringAfter("(").substringBetween(" ", ")").numberResult())
							).toSQLString(db);
				}
			}
		});
	}

	@Override
	public BooleanExpression isNot(Point2DResult rightHandSide) {
		return new BooleanExpression(new PointPointFunctionWithBooleanResult(this, new Point2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doPoint2DEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return 
							BooleanExpression.notAllOf(
							getFirst().stringResult().substringBetween("(", " ").numberResult()
							.is(getSecond().stringResult().substringBetween("(", " ").numberResult()),
							getFirst().stringResult().substringAfter("(").substringBetween(" ", ")").numberResult()
							.is(getSecond().stringResult().substringAfter("(").substringBetween(" ", ")").numberResult())
							).toSQLString(db);
				}
			}
		});
	}

	public NumberExpression getX() {
		return new NumberExpression(new PointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doPoint2DGetXTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return getFirst().stringResult().substringBetween("(", " ").numberResult().toSQLString(db);
				}
			}
		});
	}

	public NumberExpression getY() {
		return new NumberExpression(new PointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doPoint2DGetYTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return getFirst().stringResult().substringAfter("(").substringBetween(" ", ")").numberResult().toSQLString(db);
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
					return db.getDefinition().doPoint2DMeasurableDimensionsTransform(getFirst().toSQLString(db));
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
					return db.getDefinition().doPoint2DSpatialDimensionsTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return NumberExpression.value(2).toSQLString(db);
				}
			}
		});
	}

	@Override
	public BooleanExpression hasMagnitude() {
		return new BooleanExpression(new PointWithBooleanResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doPoint2DHasMagnitudeTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return BooleanExpression.falseExpression().toSQLString(db);
				}
			}
		});
	}

	@Override
	public NumberExpression magnitude() {
		return new NumberExpression(new PointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doPoint2DGetMagnitudeTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return nullExpression().toSQLString(db);
				}
			}
		});
	}

	public NumberExpression distanceBetween(Point2DExpression otherPoint) {
		return new NumberExpression(new PointPointFunctionWithNumberResult(this, otherPoint) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doPoint2DDistanceBetweenTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return getSecond().getX().minus(getFirst().getX()).bracket().squared().plus(getSecond().getY().minus(getFirst().getY()).bracket().squared()).squareRoot().toSQLString(db);
				}
			}
		});
	}

	@Override
	public Polygon2DExpression boundingBox() {
		return new Polygon2DExpression(new PointFunctionWithGeometry2DResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doPoint2DGetBoundingBoxTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					final StringExpression coordinates = getFirst().stringResult().substringBetween("(", ")");
					return StringExpression.value("POLYGON((")
							.append(coordinates)
							.append(",").append(coordinates)
							.append(",").append(coordinates)
							.append(",").append(coordinates)
							.append(",").append(coordinates)
							.append("))")
							.toSQLString(db);
				}
			}
		});
	}

	private static abstract class PointPointFunctionWithBooleanResult extends BooleanExpression {

		private Point2DExpression first;
		private Point2DExpression second;
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

	private static abstract class PointWithBooleanResult extends BooleanExpression {

		private Point2DExpression first;
		private boolean requiresNullProtection;

		PointWithBooleanResult(Point2DExpression first) {
			this.first = first;
		}

		Point2DExpression getFirst() {
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
		public PointWithBooleanResult copy() {
			PointWithBooleanResult newInstance;
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

		private Point2DExpression first;
		private Point2DExpression second;
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

	private static abstract class NumberNumberFunctionWithPoint2DResult extends Point2DExpression {

		private NumberExpression first;
		private NumberExpression second;
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
		public final String toSQLString(DBDatabase db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		@Override
		public NumberNumberFunctionWithPoint2DResult copy() {
			NumberNumberFunctionWithPoint2DResult newInstance;
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

	private static abstract class PointFunctionWithNumberResult extends NumberExpression {

		private Point2DExpression first;
//		private Point2DExpression second;
		private boolean requiresNullProtection;

		PointFunctionWithNumberResult(Point2DExpression first) {
			this.first = first;
//			this.second = second;
			if (this.first == null) {// || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Point2DExpression getFirst() {
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

		private Point2DExpression first;
//		private Point2DExpression second;
		private boolean requiresNullProtection;

		PointFunctionWithStringResult(Point2DExpression first) {
			this.first = first;
//			this.second = second;
			if (this.first == null) {// || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Point2DExpression getFirst() {
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

	private static abstract class PointFunctionWithGeometry2DResult extends Polygon2DExpression {

		private Point2DExpression first;
//		private Point2DExpression second;
		private boolean requiresNullProtection;

		PointFunctionWithGeometry2DResult(Point2DExpression first) {
			this.first = first;
//			this.second = second;
			if (this.first == null) {// || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Point2DExpression getFirst() {
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
		public PointFunctionWithGeometry2DResult copy() {
			PointFunctionWithGeometry2DResult newInstance;
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
