/*
 * Copyright 2013 gregory.graham.
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

import nz.co.gregs.dbvolution.generators.DataGenerator;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer.DBSafeInternalQDTAdaptor;

public class DBLessThanOrEqualOperator extends DBLessThanOperator {

    public static final long serialVersionUID = 1L;

    public DBLessThanOrEqualOperator(DataGenerator lessThanThis) {
        super(lessThanThis);
    }

    public DBLessThanOrEqualOperator() {
        super();
    }

    @Override
    public String getInverse() {
        return " > ";
    }

    @Override
    public String getOperator() {
        return " <= ";
    }
    
    @Override
    public DBOperator getInverseOperator() {
        return new DBGreaterThanOperator(firstValue);
    }
    
    @Override
    public DBLessThanOrEqualOperator copyAndAdapt(DBSafeInternalQDTAdaptor typeAdaptor) {
    	DBLessThanOrEqualOperator op = new DBLessThanOrEqualOperator(typeAdaptor.convert(firstValue));
    	op.invertOperator = this.invertOperator;
    	op.includeNulls = this.includeNulls;
    	return op;
    }
}
