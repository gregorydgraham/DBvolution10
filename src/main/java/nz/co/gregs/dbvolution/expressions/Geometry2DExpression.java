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

import com.vividsolutions.jts.geom.Geometry;
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBGeometry2D;

/**
 *
 * @author gregorygraham
 */
public class Geometry2DExpression implements Geometry2DResult, EqualComparable<Geometry2DResult> {

	private Geometry2DResult innerGeometry;
	private boolean nullProtectionRequired;

	protected Geometry2DExpression() {
	}

	public Geometry2DExpression(Geometry2DResult value) {
		innerGeometry = value;
		if (value == null || innerGeometry.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	public Geometry2DExpression(Geometry geometry) {
		innerGeometry = new DBGeometry2D(geometry);
		if (geometry == null || innerGeometry.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	public static Geometry2DExpression value(Geometry geometry) {
		return new Geometry2DExpression(geometry);
	}

	@Override
	public DBGeometry2D getQueryableDatatypeForExpressionValue() {
		return new DBGeometry2D();
	}

	@Override
	public String toSQLString(DBDatabase db) {
		if (innerGeometry == null) {
			return db.getDefinition().getNull();
		} else {
			return innerGeometry.toSQLString(db);
		}
	}

	@Override
	public Geometry2DExpression copy() {
		return new Geometry2DExpression(innerGeometry);
	}

	@Override
	public boolean isAggregator() {
		return innerGeometry == null ? false : innerGeometry.isAggregator();
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		HashSet<DBRow> hashSet = new HashSet<DBRow>();
		if (innerGeometry != null) {
			hashSet.addAll(innerGeometry.getTablesInvolved());
		}
		return hashSet;
	}

	@Override
	public boolean isPurelyFunctional() {
		return innerGeometry == null ? true : innerGeometry.isPurelyFunctional();
	}

	@Override
	public boolean getIncludesNull() {
		return nullProtectionRequired;
	}

	public BooleanExpression intersects(Geometry rightHandSide) {
		return intersects(new DBGeometry2D(rightHandSide));
	}

	public BooleanExpression intersects(Geometry2DResult rightHandSide) {
		return new BooleanExpression(new GeometryGeometryWithBooleanResult(this, new Geometry2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doGeometry2DIntersectionTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	public BooleanExpression is(Geometry rightHandSide) {
		return is(new DBGeometry2D(rightHandSide));
	}

	@Override
	public BooleanExpression is(Geometry2DResult rightHandSide) {
		return new BooleanExpression(new GeometryGeometryWithBooleanResult(this, new Geometry2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doGeometry2DEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	public BooleanExpression contains(Geometry rightHandSide) {
		return contains(new DBGeometry2D(rightHandSide));
	}

	public BooleanExpression contains(Geometry2DResult rightHandSide) {
		return new BooleanExpression(new GeometryGeometryWithBooleanResult(this, new Geometry2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doGeometry2DContainsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	public BooleanExpression doesNotIntersect(Geometry rightHandSide) {
		return doesNotIntersect(new DBGeometry2D(rightHandSide));
	}

	public BooleanExpression doesNotIntersect(Geometry2DResult rightHandSide) {
		return new BooleanExpression(new GeometryGeometryWithBooleanResult(this, new Geometry2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doGeometry2DDoesNotIntersectTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	public BooleanExpression overlaps(Geometry rightHandSide) {
		return overlaps(new DBGeometry2D(rightHandSide));
	}

	public BooleanExpression overlaps(Geometry2DResult rightHandSide) {
		return new BooleanExpression(new GeometryGeometryWithBooleanResult(this, new Geometry2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doGeometry2DOverlapsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	public BooleanExpression touches(Geometry rightHandSide) {
		return touches(new DBGeometry2D(rightHandSide));
	}

	public BooleanExpression touches(Geometry2DResult rightHandSide) {
		return new BooleanExpression(new GeometryGeometryWithBooleanResult(this, new Geometry2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doGeometry2DTouchesTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	public BooleanExpression within(Geometry rightHandSide) {
		return within(new DBGeometry2D(rightHandSide));
	}

	public BooleanExpression within(Geometry2DResult rightHandSide) {
		return new BooleanExpression(new GeometryGeometryWithBooleanResult(this, new Geometry2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doGeometry2DWithinTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	public NumberExpression dimension() {
		return new NumberExpression(new GeometryWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doGeometry2DGetDimensionTransform(getFirst().toSQLString(db));
			}
		});
	}

	public NumberExpression area() {
		return new NumberExpression(new GeometryWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doGeometry2DGetAreaTransform(getFirst().toSQLString(db));
			}
		});
	}

	public Geometry2DExpression boundingBox() {
		return new Geometry2DExpression(new GeometryWithGeometryResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doGeometry2DGetBoundingBoxTransform(getFirst().toSQLString(db));
			}
		});
	}

	public Geometry2DExpression exteriorRing() {
		Geometry2DExpression exteriorRingExpr = new Geometry2DExpression(new GeometryWithGeometryResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doGeometry2DGetExteriorRingTransform(getFirst().toSQLString(db));
			}
		});
		return this.dimension().is(2).ifThenElse(exteriorRingExpr, this);
	}

	public BooleanExpression isNot(Geometry geometry) {
		return this.is(geometry).not();
	}

	public BooleanExpression isNot(Geometry2DResult geometry) {
		return this.is(geometry).not();
	}

	private static abstract class GeometryGeometryWithBooleanResult extends BooleanExpression {

		private Geometry2DExpression first;
		private Geometry2DExpression second;
		private boolean requiresNullProtection;

		GeometryGeometryWithBooleanResult(Geometry2DExpression first, Geometry2DExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Geometry2DExpression getFirst() {
			return first;
		}

		Geometry2DResult getSecond() {
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
		public GeometryGeometryWithBooleanResult copy() {
			GeometryGeometryWithBooleanResult newInstance;
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

	private static abstract class GeometryWithNumberResult extends NumberExpression {

		private Geometry2DExpression first;
//		private Geometry2DExpression second;
		private boolean requiresNullProtection;

		GeometryWithNumberResult(Geometry2DExpression first) {
			this.first = first;
//			this.second = second;
//			if (this.second == null || this.second.getIncludesNull()) {
//				this.requiresNullProtection = true;
//			}
		}

		Geometry2DExpression getFirst() {
			return first;
		}

//		Geometry2DResult getSecond() {
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
		public GeometryWithNumberResult copy() {
			GeometryWithNumberResult newInstance;
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

	private static abstract class GeometryWithGeometryResult extends Geometry2DExpression {

		private Geometry2DExpression first;
//		private Geometry2DExpression second;
		private boolean requiresNullProtection;

		GeometryWithGeometryResult(Geometry2DExpression first) {
			this.first = first;
//			this.second = second;
//			if (this.second == null || this.second.getIncludesNull()) {
//				this.requiresNullProtection = true;
//			}
		}

		Geometry2DExpression getFirst() {
			return first;
		}

//		Geometry2DResult getSecond() {
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
		public GeometryWithGeometryResult copy() {
			GeometryWithGeometryResult newInstance;
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
