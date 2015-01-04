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
public class DBLessThanOperator extends DBOperator {

    private static final long serialVersionUID = 1L;
//    protected final QueryableDatatype firstValue;

    /**
     *
     * @param lessThanThis
     */
    public DBLessThanOperator(DBExpression lessThanThis) {
        super();
        if (lessThanThis != null) {
            this.firstValue = lessThanThis.copy();
        } else {
            this.firstValue = null;
        }
    }

    public DBLessThanOperator() {
        super();
        this.firstValue = null;
    }
    
    @Override
    public DBLessThanOperator copyAndAdapt(DBSafeInternalQDTAdaptor typeAdaptor) {
    	DBLessThanOperator op = new DBLessThanOperator(typeAdaptor.convert(firstValue));
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
			op = stringExpression.bracket().isLessThan((StringResult) firstValue);
		} else if (genericExpression instanceof NumberExpression) {
			NumberExpression numberExpression = (NumberExpression) genericExpression;
			op = numberExpression.isLessThan((NumberResult) firstValue);
		} else if (genericExpression instanceof DateExpression) {
			DateExpression dateExpression = (DateExpression) genericExpression;
			op = dateExpression.isLessThan((DateResult) firstValue);
		}
		return this.invertOperator ? op.not() : op;
	}
}
