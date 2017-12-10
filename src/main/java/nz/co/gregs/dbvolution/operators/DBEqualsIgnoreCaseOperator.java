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
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
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
 * Implements the EQUALS operator for DBStrings, but case-insensitive.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBEqualsIgnoreCaseOperator extends DBEqualsOperator {

	private static final long serialVersionUID = 1L;

	/**
	 * Implements the EQUALS operator for DBStrings, but case-insensitive.
	 *
	 * @param equalTo
	 */
	public DBEqualsIgnoreCaseOperator(DBExpression equalTo) {
		super(equalTo);
	}

	@Override
	public DBEqualsIgnoreCaseOperator copyAndAdapt(DBSafeInternalQDTAdaptor typeAdaptor) {
		DBEqualsIgnoreCaseOperator op = new DBEqualsIgnoreCaseOperator(typeAdaptor.convert(getFirstValue()));
		op.invertOperator = this.invertOperator;
		op.includeNulls = this.includeNulls;
		return op;
	}

	@Override
	public BooleanExpression generateWhereExpression(DBDefinition db, DBExpression column) {
		DBExpression genericExpression = column;
		BooleanExpression op = BooleanExpression.trueExpression();
		if (genericExpression instanceof StringExpression) {
			StringExpression stringExpression = (StringExpression) genericExpression;
			op = stringExpression.bracket().isIgnoreCase((StringResult) getFirstValue());
		} else if (genericExpression instanceof NumberExpression) {
			NumberExpression numberExpression = (NumberExpression) genericExpression;
			op = numberExpression.is((NumberResult) getFirstValue());
		} else if (genericExpression instanceof IntegerExpression) {
			IntegerExpression numberExpression = (IntegerExpression) genericExpression;
			op = numberExpression.is((IntegerResult) getFirstValue());
		} else if (genericExpression instanceof DateExpression) {
			DateExpression dateExpression = (DateExpression) genericExpression;
			op = dateExpression.is((DateResult) getFirstValue());
		}
		return this.invertOperator ? op.not() : op;
	}
}
