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
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.DateExpression;
import nz.co.gregs.dbvolution.results.DateResult;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.results.NumberResult;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.results.StringResult;

/**
 *
 * @author Gregory Graham
 */
public class DBBetweenExclusiveOperator extends DBOperator {

	private static final long serialVersionUID = 1L;
//	DBExpression lowest;
//	DBExpression highest;

	public DBBetweenExclusiveOperator(DBExpression lowValue, DBExpression highValue) {
		super(lowValue == null ? lowValue : lowValue.copy(),
				highValue == null ? highValue : highValue.copy());
	}

	@Override
	public DBBetweenExclusiveOperator copyAndAdapt(QueryableDatatypeSyncer.DBSafeInternalQDTAdaptor typeAdaptor) {
		DBBetweenExclusiveOperator op = new DBBetweenExclusiveOperator(typeAdaptor.convert(getFirstValue()), typeAdaptor.convert(getSecondValue()));
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
			StringResult firstStringExpr = null;
			StringResult secondStringExpr = null;
			if (getFirstValue() instanceof NumberResult) {
				NumberResult numberResult = (NumberResult) getFirstValue();
				firstStringExpr = new NumberExpression(numberResult).stringResult();
			} else if (getFirstValue() instanceof StringResult) {
				firstStringExpr = (StringResult) getFirstValue();
			}
			if (getSecondValue() instanceof NumberResult) {
				NumberResult numberResult = (NumberResult) getSecondValue();
				secondStringExpr = new NumberExpression(numberResult).stringResult();
			} else if (getSecondValue() instanceof StringResult) {
				secondStringExpr = (StringResult) getSecondValue();
			}
			if (firstStringExpr != null && secondStringExpr != null) {
				betweenOp = stringExpression.bracket().isBetweenExclusive(firstStringExpr, secondStringExpr);
			}
		} else if ((genericExpression instanceof NumberExpression)
				&& (getFirstValue() instanceof NumberResult)
				&& (getSecondValue() instanceof NumberResult)) {
			NumberExpression numberExpression = (NumberExpression) genericExpression;
			betweenOp = numberExpression.isBetweenExclusive((NumberResult) getFirstValue(), (NumberResult) getSecondValue());
		} else if ((genericExpression instanceof DateExpression)
				&& (getFirstValue() instanceof DateResult)
				&& (getSecondValue() instanceof DateResult)) {
			DateExpression dateExpression = (DateExpression) genericExpression;
			betweenOp = dateExpression.isBetweenExclusive((DateResult) getFirstValue(), (DateResult) getSecondValue());
		} else {
			throw new DBRuntimeException("whoops");
		}
		return this.invertOperator ? betweenOp.not() : betweenOp;
	}
}
