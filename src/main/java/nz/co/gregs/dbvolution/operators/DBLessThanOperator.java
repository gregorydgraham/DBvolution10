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
 * Implements LESSTHAN for all types that support it.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBLessThanOperator extends DBOperator {

	private static final long serialVersionUID = 1L;

	/**
	 * Implements LESSTHAN for all types that support it.
	 *
	 * @param lessThanThis the expression to compare to.
	 */
	public DBLessThanOperator(DBExpression lessThanThis) {
		super((lessThanThis != null ? lessThanThis.copy() : null));
	}

	/**
	 * Default Constructor
	 *
	 */
	public DBLessThanOperator() {
		super();
	}

	@Override
	public DBLessThanOperator copyAndAdapt(DBSafeInternalQDTAdaptor typeAdaptor) {
		DBLessThanOperator op = new DBLessThanOperator(typeAdaptor.convert(getFirstValue()));
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
			op = stringExpression.bracket().isLessThan((StringResult) getFirstValue());
		} else if (genericExpression instanceof NumberExpression) {
			NumberExpression numberExpression = (NumberExpression) genericExpression;
			op = numberExpression.isLessThan((NumberResult) getFirstValue());
		} else if (genericExpression instanceof IntegerExpression) {
			IntegerExpression numberExpression = (IntegerExpression) genericExpression;
			op = numberExpression.isLessThan((IntegerResult) getFirstValue());
		} else if (genericExpression instanceof DateExpression) {
			DateExpression dateExpression = (DateExpression) genericExpression;
			op = dateExpression.isLessThan((DateResult) getFirstValue());
		}
		return this.invertOperator ? op.not() : op;
	}
}
