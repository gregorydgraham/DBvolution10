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
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBMultiPoint2D;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBPoint2D;
import nz.co.gregs.dbvolution.results.MultiPoint2DResult;
import nz.co.gregs.dbvolution.results.NumberResult;

/**
 *
 * @author gregorygraham
 */
public class MultiPoint2DExpression implements MultiPoint2DResult, EqualComparable<MultiPoint2DResult>, SpatialExpression {

	private MultiPoint2DResult innerPoint;
	private boolean nullProtectionRequired;

	protected MultiPoint2DExpression() {
	}

	public MultiPoint2DExpression(MultiPoint2DResult value) {
		innerPoint = value;
		if (value == null || innerPoint.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	public MultiPoint2DExpression(Point... points) {
		innerPoint = new DBMultiPoint2D(points);
		if (points == null || points.length == 0 || innerPoint.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	public MultiPoint2DExpression(MultiPoint points) {
		innerPoint = new DBMultiPoint2D(points);
		if (points == null || innerPoint.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	public static MultiPoint2DExpression value(Point... points) {
		return new MultiPoint2DExpression(points);
	}

	public static MultiPoint2DExpression value(Coordinate... coords) {
		GeometryFactory geometryFactory = new GeometryFactory();
		MultiPoint multiPoint = geometryFactory.createMultiPoint(coords);
		return new MultiPoint2DExpression(multiPoint);
	}

	public static MultiPoint2DExpression value(MultiPoint points) {
		return new MultiPoint2DExpression(points);
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
	public MultiPoint2DExpression copy() {
		return new MultiPoint2DExpression(innerPoint);
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

	public StringExpression stringResult() {
		return new StringExpression(new MultiPointFunctionWithStringResult(this) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doMultiPoint2DAsTextTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return getFirst().toSQLString(db);
				}
			}
		});
	}

	public BooleanExpression is(MultiPoint rightHandSide) {
		return is(new DBMultiPoint2D(rightHandSide));
	}

	@Override
	public BooleanExpression is(MultiPoint2DResult rightHandSide) {
		return new BooleanExpression(new MultiPoint2DMultiPoint2DFunctionWithBooleanResult(this, new MultiPoint2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doMultiPoint2DEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return getFirst().stringResult().is(getSecond().stringResult()).toSQLString(db);
				}
			}
		});
	}

	public NumberExpression getMaxX() {
		return new NumberExpression(new MultiPointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doMultiPoint2DGetMaxXTransform(getFirst().toSQLString(db));
			}
		});
	}

	public NumberExpression getMaxY() {
		return new NumberExpression(new MultiPointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doMultiPoint2DGetMaxYTransform(getFirst().toSQLString(db));
			}
		});
	}

	public NumberExpression getMinX() {
		return new NumberExpression(new MultiPointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doMultiPoint2DGetMinXTransform(getFirst().toSQLString(db));
			}
		});
	}

	public NumberExpression getMinY() {
		return new NumberExpression(new MultiPointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doMultiPoint2DGetMinYTransform(getFirst().toSQLString(db));
			}
		});
	}

	public NumberExpression numberOfPoints() {
		return new NumberExpression(new MultiPointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doMultiPoint2DGetNumberOfPointsTransform(getFirst().toSQLString(db));
			}
		});
	}

	public Point2DExpression getPointAtIndex(int index) {
		return getPointAtIndex(NumberExpression.value(index));
	}

	public Point2DExpression getPointAtIndex(long index) {
		return getPointAtIndex(NumberExpression.value(index));
	}

	public Point2DExpression getPointAtIndex(NumberResult index) {
		return new Point2DExpression(new MultiPointNumberFunctionWithPoint2DResult(this, new NumberExpression(index)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doMultiPoint2DGetPointAtIndexTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	@Override
	public NumberExpression dimension() {
		return new NumberExpression(new MultiPointFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doMultiPoint2DDimensionTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return NumberExpression.value(0).toSQLString(db);
				}
			}
		});
	}

	@Override
	public Polygon2DExpression boundingBox() {
		return new Polygon2DExpression(new MultiPoint2DFunctionWithGeometry2DResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doMultiPoint2DGetBoundingBoxTransform(getFirst().toSQLString(db));
			}
		});
	}

	Line2DExpression line2DResult() {
		return new Line2DExpression(new SingleArgumentLine2DFunction<MultiPoint2DExpression>(this) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doMultiPoint2DToLine2DTransform(getFirst().toSQLString(db));
			}
		});
	}

	Polygon2DExpression polygon2DResult() {
		return new Polygon2DExpression(new SingleArgumentPolygon2DFunction<MultiPoint2DExpression>(this) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doMultiPoint2DToPolygon2DTransform(getFirst().toSQLString(db));
			}
		});
	}

	private static abstract class SingleArgumentLine2DFunction<A extends DBExpression> extends Line2DExpression {

		private A first;
//		private B second;
		private boolean requiresNullProtection;

		SingleArgumentLine2DFunction(A first) {
			this.first = first;
//			this.second = second;
//			if (this.second == null || this.second.getIncludesNull()) {
//				this.requiresNullProtection = true;
//			}
		}

		A getFirst() {
			return first;
		}

//		MultiPoint2DExpression getSecond() {
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
		public SingleArgumentLine2DFunction<A> copy() {
			SingleArgumentLine2DFunction<A> newInstance;
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

	private static abstract class SingleArgumentPolygon2DFunction<A extends DBExpression> extends Polygon2DExpression {

		private A first;
//		private B second;
		private boolean requiresNullProtection;

		SingleArgumentPolygon2DFunction(A first) {
			this.first = first;
//			this.second = second;
//			if (this.second == null || this.second.getIncludesNull()) {
//				this.requiresNullProtection = true;
//			}
		}

		A getFirst() {
			return first;
		}

//		MultiPoint2DExpression getSecond() {
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
		public SingleArgumentPolygon2DFunction<A> copy() {
			SingleArgumentPolygon2DFunction<A> newInstance;
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
		public final String toSQLString(DBDatabase db) {
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

	private static abstract class MultiPointMultiPointFunctionWithNumberResult extends NumberExpression {

		private MultiPoint2DExpression first;
		private MultiPoint2DExpression second;
		private boolean requiresNullProtection;

		MultiPointMultiPointFunctionWithNumberResult(MultiPoint2DExpression first, MultiPoint2DExpression second) {
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
		public final String toSQLString(DBDatabase db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		@Override
		public MultiPointMultiPointFunctionWithNumberResult copy() {
			MultiPointMultiPointFunctionWithNumberResult newInstance;
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

//	private static abstract class NumberNumberFunctionWithMultiPoint2DResult extends MultiPoint2DExpression {
//
//		private NumberExpression first;
//		private NumberExpression second;
//		private boolean requiresNullProtection;
//
//		NumberNumberFunctionWithMultiPoint2DResult(NumberExpression first, NumberExpression second) {
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
//		public NumberNumberFunctionWithMultiPoint2DResult copy() {
//			NumberNumberFunctionWithMultiPoint2DResult newInstance;
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
		public final String toSQLString(DBDatabase db) {
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

		private MultiPoint2DExpression first;
//		private Point2DExpression second;
		private boolean requiresNullProtection;

		MultiPointFunctionWithNumberResult(MultiPoint2DExpression first) {
			this.first = first;
//			this.second = second;
			if (this.first == null) {// || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		MultiPoint2DExpression getFirst() {
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

		private MultiPoint2DExpression first;
//		private Point2DExpression second;
		private boolean requiresNullProtection;

		MultiPointFunctionWithStringResult(MultiPoint2DExpression first) {
			this.first = first;
//			this.second = second;
			if (this.first == null) {// || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		MultiPoint2DExpression getFirst() {
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

	private static abstract class MultiPoint2DFunctionWithGeometry2DResult extends Polygon2DExpression {

		private MultiPoint2DExpression first;
//		private Point2DExpression second;
		private boolean requiresNullProtection;

		MultiPoint2DFunctionWithGeometry2DResult(MultiPoint2DExpression first) {
			this.first = first;
//			this.second = second;
			if (this.first == null) {// || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		MultiPoint2DExpression getFirst() {
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
		public MultiPoint2DFunctionWithGeometry2DResult copy() {
			MultiPoint2DFunctionWithGeometry2DResult newInstance;
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
