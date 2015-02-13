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

import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.spatial.DBGeometry;

/**
 *
 * @author gregorygraham
 */
public class GeometryExpression implements GeometryResult{

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
	
	@Override
	public DBGeometry getQueryableDatatypeForExpressionValue() {
		return new DBGeometry();
	}

	@Override
	public String toSQLString(DBDatabase db) {
		return innerGeometry.toSQLString(db);
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
		return innerGeometry.getTablesInvolved();
	}

	@Override
	public boolean isPurelyFunctional() {
		return innerGeometry.isPurelyFunctional();
	}

	@Override
	public boolean getIncludesNull() {
		return nullProtectionRequired;
	}
	
	public BooleanExpression intersects(GeometryResult rightHandSide){
		return new BooleanExpression(new GeometryGeometryWithBooleanResult(this, new GeometryExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
					return db.getDefinition().doGeometryIntersectionTransform(db, getFirst().toSQLString(db), getSecond().toSQLString(db));
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
