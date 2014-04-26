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

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer;

/**
 *
 * @author Gregory Graham
 */
abstract class DBMetaOperator extends DBOperator{
    private static final long serialVersionUID = 1L;
    
    protected DBOperator operator;

    @Override
    public DBOperator copyAndAdapt(QueryableDatatypeSyncer.DBSafeInternalQDTAdaptor typeAdaptor) {
        return operator.copyAndAdapt(typeAdaptor);
    }

    @Override
    public String generateRelationship(DBDatabase database, String columnName, String otherColumnName) {
        return operator.generateRelationship(database, columnName, otherColumnName);
    }

    @Override
    public String generateWhereLine(DBDatabase database, String columnName) {
        return operator.generateWhereLine(database, columnName);
    }

    @Override
    public DBOperator getInverseOperator() {
        return this.operator.getInverseOperator();
    }

    @Override
    public void invertOperator(Boolean invertOperator) {
        operator.invertOperator(invertOperator);
    }

    @Override
    public void not() {
        operator.not();
    }
}
