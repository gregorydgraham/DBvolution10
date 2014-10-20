/*
 * Copyright 2014 gregory.graham.
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
import nz.co.gregs.dbvolution.datatypes.DBBits;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 *
 * @author gregory.graham
 */
public class BitsExpression implements BitsResult {

	private final BitsResult innerBitResult;

	public BitsExpression(BitsResult bitResult) {
		this.innerBitResult = bitResult;
	}

	@Override
	public BitsExpression copy() {
		return new BitsExpression(this.getInnerBitResult());
	}

	@Override
	public QueryableDatatype getQueryableDatatypeForExpressionValue() {
		return new DBBits();
	}

	@Override
	public String toSQLString(DBDatabase db) {
		if (innerBitResult != null) {
			return innerBitResult.toSQLString(db);
		} else {
			return "";
		}
	}

	@Override
	public boolean isAggregator() {
		if (innerBitResult != null) {
			return innerBitResult.isAggregator();
		} else {
			return false;
		}
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		if (innerBitResult != null) {
			return innerBitResult.getTablesInvolved();
		} else {
			return new HashSet<DBRow>();
		}
	}

	@Override
	public boolean getIncludesNull() {
		if (innerBitResult != null) {
			return innerBitResult.getIncludesNull();
		} else {
			return false;
		}
	}

	protected BitsResult getInnerBitResult() {
		return innerBitResult;
	}

	public BooleanExpression is(int i) {
		return new BooleanExpression(new DBBinaryBooleanArithmetic(this, NumberExpression.value(i).convertToBits()) {
			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " = ";
			}
		});
	}

	private static abstract class DBBinaryBooleanArithmetic implements BooleanResult {

		private BitsExpression first;
		private BitsResult second;
		private boolean requiresNullProtection;

		DBBinaryBooleanArithmetic(BitsExpression first, BitsResult second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		@Override
		public DBBoolean getQueryableDatatypeForExpressionValue() {
			return new DBBoolean();
		}

		@Override
		public String toSQLString(DBDatabase db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return first.toSQLString(db) + this.getEquationOperator(db) + second.toSQLString(db);
			}
		}

		@Override
		public DBBinaryBooleanArithmetic copy() {
			DBBinaryBooleanArithmetic newInstance;
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

		protected abstract String getEquationOperator(DBDatabase db);

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
