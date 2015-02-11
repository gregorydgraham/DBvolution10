/*
 * Copyright 2013 Gregory Graham.
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

import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer.DBSafeInternalQDTAdaptor;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.expressions.BooleanArrayExpression;
import nz.co.gregs.dbvolution.expressions.BooleanArrayResult;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.BooleanResult;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.DateExpression;
import nz.co.gregs.dbvolution.expressions.DateResult;
import nz.co.gregs.dbvolution.expressions.EqualComparable;
import nz.co.gregs.dbvolution.expressions.IntervalExpression;
import nz.co.gregs.dbvolution.expressions.IntervalResult;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.expressions.NumberResult;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.expressions.StringResult;

/**
 *
 * @author Gregory Graham
 */
public class DBEqualsOperator extends DBOperator {

	private static final long serialVersionUID = 1L;

	/**
	 *
	 */
	public DBEqualsOperator() {
		super();
	}

	public DBEqualsOperator(DBExpression equalTo) {
		this.firstValue = (equalTo == null ? equalTo : equalTo.copy());
	}

	public DBEqualsOperator(Object equalTo) {
		QueryableDatatype first = QueryableDatatype.getQueryableDatatypeForObject(equalTo);
		this.firstValue = (first == null ? first : first.copy());
	}

	@Override
	public DBEqualsOperator copyAndAdapt(DBSafeInternalQDTAdaptor typeAdaptor) {
		DBEqualsOperator op = new DBEqualsOperator(typeAdaptor.convert(firstValue));
		op.invertOperator = this.invertOperator;
		op.includeNulls = this.includeNulls;
		return op;
	}

	@Override
	public BooleanExpression generateWhereExpression(DBDatabase db, DBExpression column) {
		DBExpression genericExpression = column;
		BooleanExpression op = BooleanExpression.trueExpression();
		if (genericExpression instanceof EqualComparable) {
			if (genericExpression instanceof StringExpression) {
				StringExpression stringExpression = (StringExpression) genericExpression;
				if ((firstValue instanceof StringResult)||firstValue==null) {
					op = stringExpression.bracket().is((StringResult) firstValue);
				} else if (firstValue instanceof NumberResult) {
					op = stringExpression.bracket().is(new NumberExpression((NumberResult) firstValue).stringResult());
				} else {
					throw new nz.co.gregs.dbvolution.exceptions.ComparisonBetweenTwoDissimilarTypes(db, genericExpression, firstValue);
				}
			} else if ((genericExpression instanceof NumberExpression) && ((firstValue instanceof NumberResult)||firstValue==null)) {
				NumberExpression numberExpression = (NumberExpression) genericExpression;
				op = numberExpression.is((NumberResult) firstValue);
			} else if ((genericExpression instanceof DateExpression) && ((firstValue instanceof DateResult)||firstValue==null)) {
				DateExpression dateExpression = (DateExpression) genericExpression;
				op = dateExpression.is((DateResult) firstValue);
			} else if ((genericExpression instanceof BooleanExpression) && ((firstValue instanceof BooleanResult)||firstValue==null)) {
				BooleanExpression boolExpr = (BooleanExpression) genericExpression;
				op = boolExpr.is((BooleanResult) firstValue);
			} else if ((genericExpression instanceof BooleanArrayExpression) && ((firstValue instanceof BooleanArrayResult)||firstValue==null)) {
				BooleanArrayExpression boolExpr = (BooleanArrayExpression) genericExpression;
				op = boolExpr.is((BooleanArrayResult) firstValue);
			} else if ((genericExpression instanceof IntervalExpression) && ((firstValue instanceof IntervalResult)||firstValue==null)) {
				IntervalExpression intervalExpr = (IntervalExpression) genericExpression;
				op = intervalExpr.is((IntervalResult) firstValue);
			} else {
				throw new nz.co.gregs.dbvolution.exceptions.ComparisonBetweenTwoDissimilarTypes(db, genericExpression, firstValue);
			}
		} else {
			throw new nz.co.gregs.dbvolution.exceptions.IncomparableTypeUsedInComparison(db, genericExpression);
		}
		return this.invertOperator ? op.not() : op;
	}
}
