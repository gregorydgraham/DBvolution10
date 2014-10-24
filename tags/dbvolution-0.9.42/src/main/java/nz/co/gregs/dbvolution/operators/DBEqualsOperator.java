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
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
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
public class DBEqualsOperator extends DBOperator {

	public static final long serialVersionUID = 1L;
//    protected DBDefinition defn;

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
		if (genericExpression instanceof StringExpression) {
			StringExpression stringExpression = (StringExpression) genericExpression;
			if (firstValue instanceof StringResult) {
				op = stringExpression.bracket().is((StringResult) firstValue);
			} else if (firstValue instanceof NumberResult) {
				op = stringExpression.bracket().is(new NumberExpression((NumberResult) firstValue).stringResult());
			}
		} else if (genericExpression instanceof NumberExpression) {
			NumberExpression numberExpression = (NumberExpression) genericExpression;
			op = numberExpression.is((NumberResult) firstValue);
		} else if (genericExpression instanceof DateExpression) {
			DateExpression dateExpression = (DateExpression) genericExpression;
			op = dateExpression.is((DateResult) firstValue);
		}
		return this.invertOperator ? op.not() : op;
	}
}
