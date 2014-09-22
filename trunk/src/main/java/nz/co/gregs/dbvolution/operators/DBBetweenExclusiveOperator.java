/*
 * Copyright 2014 Gregory Graham.
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
package nz.co.gregs.dbvolution.operators;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer;
import nz.co.gregs.dbvolution.exceptions.InappropriateRelationshipOperator;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.DateExpression;
import nz.co.gregs.dbvolution.expressions.DateResult;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.expressions.NumberResult;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.expressions.StringResult;

/**
 *
 * @author Gregory Graham
 */
public class DBBetweenExclusiveOperator extends DBOperator {

	public static final long serialVersionUID = 1L;
	DBExpression lowest;
	DBExpression highest;

	@SuppressWarnings("unchecked")
	public DBBetweenExclusiveOperator(DBExpression lowValue, DBExpression highValue) {
		super();
		lowest = lowValue == null ? lowValue : lowValue.copy();
		highest = highValue == null ? highValue : highValue.copy();
	}

//	@Override
//	public String generateWhereLine(DBDatabase db, String columnName) {
//		String lowerSQLValue = firstValue.toSQLString(db);
//		String upperSQLValue = secondValue.toSQLString(db);
//		String beginWhereLine = "";
//		return beginWhereLine + (invertOperator ? " not(" : "(") + columnName + " > " + lowerSQLValue + " and " + columnName + " < " + upperSQLValue + ")";
//	}

	@Override
	public DBOperator getInverseOperator() {
		throw new InappropriateRelationshipOperator(this);
	}

	@Override
	public DBBetweenExclusiveOperator copyAndAdapt(QueryableDatatypeSyncer.DBSafeInternalQDTAdaptor typeAdaptor) {
		DBBetweenExclusiveOperator op = new DBBetweenExclusiveOperator(typeAdaptor.convert(firstValue), typeAdaptor.convert(secondValue));
		op.invertOperator = this.invertOperator;
		op.includeNulls = this.includeNulls;
		return op;
	}

	@Override
	public BooleanExpression generateWhereExpression(DBDatabase db, DBExpression column) {
		DBExpression genericExpression = column;
		BooleanExpression betweenOp = BooleanExpression.trueExpression();
		if (genericExpression instanceof StringExpression) {
			StringExpression stringExpression = (StringExpression) genericExpression;
			betweenOp = stringExpression.bracket().isBetweenExclusive((StringResult) firstValue, (StringResult) secondValue);
		} else if (genericExpression instanceof NumberExpression) {
			NumberExpression numberExpression = (NumberExpression) genericExpression;
			betweenOp = numberExpression.isBetweenExclusive((NumberResult) firstValue, (NumberResult) secondValue);
		} else if (genericExpression instanceof DateExpression) {
			DateExpression dateExpression = (DateExpression) genericExpression;
			betweenOp = dateExpression.isBetweenExclusive((DateResult) firstValue, (DateResult) secondValue);
		}
		return this.invertOperator ? betweenOp.not() : betweenOp;
	}
}
