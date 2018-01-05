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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.DateExpression;
import nz.co.gregs.dbvolution.expressions.IntegerExpression;
import nz.co.gregs.dbvolution.results.DateResult;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.results.NumberResult;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.results.IntegerResult;
import nz.co.gregs.dbvolution.results.StringResult;

/**
 * Implements a type agnostic comparison that finds items between the 2 values
 * including the values themselves.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBBetweenInclusiveOperator extends DBOperator {

	private static final long serialVersionUID = 1L;

	/**
	 * Implements a type agnostic comparison that finds items between the 2 values
	 * including the values themselves.
	 *
	 * @param lowValue
	 * @param highValue
	 */
	@SuppressFBWarnings(
			value = "NP_LOAD_OF_KNOWN_NULL_VALUE",
			justification = "Null is a valid value in databases")
	public DBBetweenInclusiveOperator(DBExpression lowValue, DBExpression highValue) {
		super(lowValue == null ? lowValue : lowValue.copy(),
				highValue == null ? highValue : highValue.copy());
	}

	@Override
	public DBBetweenInclusiveOperator copyAndAdapt(QueryableDatatypeSyncer.DBSafeInternalQDTAdaptor typeAdaptor) {
		DBBetweenInclusiveOperator op = new DBBetweenInclusiveOperator(typeAdaptor.convert(getFirstValue()), typeAdaptor.convert(getSecondValue()));
		op.invertOperator = this.invertOperator;
		op.includeNulls = this.includeNulls;
		return op;
	}

	@Override
	public BooleanExpression generateWhereExpression(DBDefinition db, DBExpression column) {
		DBExpression genericExpression = column;
		BooleanExpression betweenOp = BooleanExpression.trueExpression();
		if (genericExpression instanceof StringExpression) {
			StringExpression stringExpression = (StringExpression) genericExpression;
			StringResult firstStringExpr = null;
			StringResult secondStringExpr = null;
			if (getFirstValue() instanceof NumberResult) {
				NumberResult numberResult = (NumberResult) getFirstValue();
				firstStringExpr = new NumberExpression(numberResult).stringResult();
			} else if (getFirstValue() instanceof IntegerResult) {
				NumberResult numberResult = (NumberResult) getFirstValue();
				firstStringExpr = new NumberExpression(numberResult).stringResult();
			} else if (getFirstValue() instanceof StringResult) {
				firstStringExpr = (StringResult) getFirstValue();
			}
			if (getSecondValue() instanceof NumberResult) {
				NumberResult numberResult = (NumberResult) getSecondValue();
				secondStringExpr = new NumberExpression(numberResult).stringResult();
			} else if (getSecondValue() instanceof IntegerResult) {
				IntegerResult numberResult = (IntegerResult) getSecondValue();
				secondStringExpr = new IntegerExpression(numberResult).stringResult();
			} else if (getSecondValue() instanceof StringResult) {
				secondStringExpr = (StringResult) getSecondValue();
			}
			if (firstStringExpr != null && secondStringExpr != null) {
				betweenOp = stringExpression.bracket().isBetweenInclusive(firstStringExpr, secondStringExpr);
			}
		} else if ((genericExpression instanceof NumberExpression)
				&& (getFirstValue() instanceof NumberResult)
				&& (getSecondValue() instanceof NumberResult)) {
			NumberExpression numberExpression = (NumberExpression) genericExpression;
			betweenOp = numberExpression.isBetweenInclusive((NumberResult) getFirstValue(), (NumberResult) getSecondValue());
		} else if ((genericExpression instanceof IntegerExpression)
				&& (getFirstValue() instanceof IntegerResult)
				&& (getSecondValue() instanceof IntegerResult)) {
			IntegerExpression numberExpression = (IntegerExpression) genericExpression;
			betweenOp = numberExpression.isBetweenInclusive((IntegerResult) getFirstValue(), (IntegerResult) getSecondValue());
		} else if ((genericExpression instanceof DateExpression)
				&& (getFirstValue() instanceof DateResult)
				&& (getSecondValue() instanceof DateResult)) {
			DateExpression dateExpression = (DateExpression) genericExpression;
			betweenOp = dateExpression.isBetweenInclusive((DateResult) getFirstValue(), (DateResult) getSecondValue());
		}
		return this.invertOperator ? betweenOp.not() : betweenOp;
	}
}
