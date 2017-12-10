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

import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.results.BooleanResult;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.results.NumberResult;

/**
 * Creates a bitwise comparison for boolean or number expressions
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class DBBitwiseEqualsOperator extends DBEqualsOperator {

	private static final long serialVersionUID = 1L;

	/**
	 * Creates a bitwise comparison for boolean or number expressions
	 *
	 * @param equalTo
	 */
	public DBBitwiseEqualsOperator(BooleanExpression equalTo) {
		super(equalTo);
	}

	@Override
	public BooleanExpression generateWhereExpression(DBDefinition db, DBExpression column) {
		DBExpression genericExpression = column;
		BooleanExpression op = BooleanExpression.trueExpression();
		if (genericExpression instanceof BooleanExpression) {
			BooleanExpression expr = (BooleanExpression) genericExpression;
			final DBExpression firstValue = getFirstValue();
			if (firstValue instanceof BooleanResult) {
				op = expr.is((BooleanResult) firstValue);
			} else if (firstValue instanceof NumberResult) {
				op = expr.is(new NumberExpression((NumberResult) firstValue).is(1));
			}
		}
		return this.invertOperator ? op.not() : op;
	}
}
