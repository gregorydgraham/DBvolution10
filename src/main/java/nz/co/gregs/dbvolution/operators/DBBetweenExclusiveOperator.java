/*
 * Copyright 2014 gregory.graham.
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
import nz.co.gregs.dbvolution.exceptions.InappropriateRelationshipOperator;
import nz.co.gregs.dbvolution.variables.DBExpression;

/**
 *
 * @author gregory.graham
 */
public class DBBetweenExclusiveOperator  extends DBOperator{
    public static final long serialVersionUID = 1L;

//    private final QueryableDatatype firstValue;
//    private final QueryableDatatype secondValue;
    
    public DBBetweenExclusiveOperator(DBExpression lowValue, DBExpression highValue){
        super();
        this.firstValue = lowValue==null?lowValue:lowValue.copy();
        this.secondValue = highValue==null?highValue:highValue.copy();
    }
    
    @Override
    public String generateWhereLine(DBDatabase db, String columnName) {
//        lowValue.setDatabase(database);
        String lowerSQLValue = firstValue.toSQLString(db);
//        highValue.setDatabase(db);
        String upperSQLValue = secondValue.toSQLString(db);
        String beginWhereLine = db.getDefinition().beginAndLine();
        return beginWhereLine + (invertOperator?"!(":"(")+columnName + " > " + lowerSQLValue + " and "+columnName + " < " + upperSQLValue+")";
    }

    @Override
    public String generateRelationship(DBDatabase database, String columnName, String otherColumnName) {
        throw new InappropriateRelationshipOperator(this);
    }

    @Override
    public DBOperator getInverseOperator() {
        throw new InappropriateRelationshipOperator(this);
    }
    
    @Override
    public DBBetweenOperator copyAndAdapt(QueryableDatatypeSyncer.DBSafeInternalQDTAdaptor typeAdaptor) {
    	DBBetweenOperator op = new DBBetweenOperator(typeAdaptor.convert(firstValue), typeAdaptor.convert(secondValue));
    	op.invertOperator = this.invertOperator;
    	op.includeNulls = this.includeNulls;
    	return op;
    }
}
