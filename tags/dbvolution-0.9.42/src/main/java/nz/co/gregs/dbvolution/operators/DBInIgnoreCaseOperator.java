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

import java.util.ArrayList;
import java.util.List;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer.DBSafeInternalQDTAdaptor;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.DateExpression;
import nz.co.gregs.dbvolution.expressions.DateResult;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.expressions.NumberResult;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.expressions.StringResult;

public class DBInIgnoreCaseOperator extends DBInOperator {

    public static final long serialVersionUID = 1L;

    public DBInIgnoreCaseOperator(List<DBExpression> listOfPossibleValues) {
        super(listOfPossibleValues);
    }

    public DBInIgnoreCaseOperator() {
        super();
    }
    
    @Override
    public DBInIgnoreCaseOperator copyAndAdapt(DBSafeInternalQDTAdaptor typeAdaptor) {
    	ArrayList<DBExpression> list = new ArrayList<DBExpression>();
    	for (DBExpression item: listOfPossibleValues) {
    		list.add(typeAdaptor.convert(item));
    	}
    	DBInIgnoreCaseOperator op = new DBInIgnoreCaseOperator(list);
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
			op = stringExpression.bracket().isInIgnoreCase(listOfPossibleStrings.toArray(new StringResult[]{}));
		}
		return this.invertOperator ? op.not() : op;
	}
}
