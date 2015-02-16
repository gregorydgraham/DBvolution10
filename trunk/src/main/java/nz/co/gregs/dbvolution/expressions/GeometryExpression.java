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
import nz.co.gregs.dbvolution.datatypes.spatial.DBGeometry;

/**
 *
 * @author gregorygraham
 */
public class GeometryExpression implements GeometryResult, EqualComparable<GeometryResult> {

	private GeometryResult innerGeometry;
	private boolean nullProtectionRequired;

	protected GeometryExpression() {
	}

	public GeometryExpression(GeometryResult value) {
		innerGeometry = value;
		if (value == null || innerGeometry.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	private GeometryExpression(Geometry geometry) {
		innerGeometry = new DBGeometry(geometry);
		if (geometry == null || innerGeometry.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	public static GeometryExpression value(Geometry geometry) {
		return new GeometryExpression(geometry);
	}

	@Override
	public DBGeometry getQueryableDatatypeForExpressionValue() {
		return new DBGeometry();
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
	public GeometryExpression copy() {
		return new GeometryExpression(innerGeometry);
	}

	@Override
	public boolean isAggregator() {
		return innerGeometry.isAggregator();
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
		return innerGeometry.isPurelyFunctional();
	}

	@Override
	public boolean getIncludesNull() {
		return nullProtectionRequired;
	}

	public BooleanExpression intersects(Geometry rightHandSide) {
		return intersects(new DBGeometry(rightHandSide));
	}

	public BooleanExpression intersects(GeometryResult rightHandSide) {
		return new BooleanExpression(new GeometryGeometryWithBooleanResult(this, new GeometryExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doGeometryIntersectionTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	public BooleanExpression is(Geometry rightHandSide) {
		return is(new DBGeometry(rightHandSide));
	}

	@Override
	public BooleanExpression is(GeometryResult rightHandSide) {
		return new BooleanExpression(new GeometryGeometryWithBooleanResult(this, new GeometryExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doGeometryEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	public BooleanExpression contains(Geometry rightHandSide) {
		return contains(new DBGeometry(rightHandSide));
	}

	public BooleanExpression contains(GeometryResult rightHandSide) {
		return new BooleanExpression(new GeometryGeometryWithBooleanResult(this, new GeometryExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doGeometryContainsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	public BooleanExpression doesNotIntersect(Geometry rightHandSide) {
		return doesNotIntersect(new DBGeometry(rightHandSide));
	}

	public BooleanExpression doesNotIntersect(GeometryResult rightHandSide) {
		return new BooleanExpression(new GeometryGeometryWithBooleanResult(this, new GeometryExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doGeometryDoesNotIntersectTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	public BooleanExpression overlaps(Geometry rightHandSide) {
		return overlaps(new DBGeometry(rightHandSide));
	}

	public BooleanExpression overlaps(GeometryResult rightHandSide) {
		return new BooleanExpression(new GeometryGeometryWithBooleanResult(this, new GeometryExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doGeometryOverlapsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	public BooleanExpression touches(Geometry rightHandSide) {
		return touches(new DBGeometry(rightHandSide));
	}

	public BooleanExpression touches(GeometryResult rightHandSide) {
		return new BooleanExpression(new GeometryGeometryWithBooleanResult(this, new GeometryExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doGeometryTouchesTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	public BooleanExpression within(Geometry rightHandSide) {
		return within(new DBGeometry(rightHandSide));
	}

	public BooleanExpression within(GeometryResult rightHandSide) {
		return new BooleanExpression(new GeometryGeometryWithBooleanResult(this, new GeometryExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doGeometryWithinTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	private static abstract class GeometryGeometryWithBooleanResult extends BooleanExpression {

		private GeometryExpression first;
		private GeometryExpression second;
		private boolean requiresNullProtection;

		GeometryGeometryWithBooleanResult(GeometryExpression first, GeometryExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		GeometryExpression getFirst() {
			return first;
		}

		GeometryResult getSecond() {
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

}
