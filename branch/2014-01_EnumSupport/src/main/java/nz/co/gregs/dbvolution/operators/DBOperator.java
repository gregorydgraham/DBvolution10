/*
 * Copyright 2013 gregorygraham.
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

import java.io.Serializable;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer.DBSafeInternalQDTAdaptor;

/**
 *
 * @author gregorygraham
 */
abstract public class DBOperator implements Serializable {

    Boolean invertOperator = false;
    Boolean includeNulls = false;
    protected DBExpression firstValue;
    protected DBExpression secondValue;
    protected DBExpression thirdValue;

    public DBOperator() {
        firstValue = null;
        secondValue = null;
        thirdValue = null;
    }

    /**
     * Formats the operator into SQL for comparing a column to pre-supplied values
     *
     * <p>
     * Within this function you need to transform the column name, operator and
     * values into a where clause line.
     * <p>
     * The line should formatted as " this = that " and should make use of
     * the DBDefinition with DBDatabase to ensure compatibility with all
     * databases.
     *
     * @param database
     * @param columnName
     * @return the column name, operator and values as a where clause snippet
     */
    abstract public String generateWhereLine(DBDatabase database, String columnName);

    /**
     * Formats the operator into SQL for comparing 2 columns
     *
     * <p>
     * Within this function you need to transform the column names and operator
     * into a where clause line.
     * <p>
     * The line should formatted as " this = that " and should make use of
     * the DBDefinition with DBDatabase to ensure compatibility with all
     * databases.
     *
     * @param database
     * @param columnName
     * @param otherColumnName
     * @return the column name, operator and values as a where clause snippet
     */
    abstract public String generateRelationship(DBDatabase database, String columnName, String otherColumnName);

    public void invertOperator(Boolean invertOperator) {
        this.invertOperator = invertOperator;
    }

    public void not() {
        invertOperator = true;
    }

    abstract public DBOperator getInverseOperator();

    // TODO
    public void includeNulls() {
        includeNulls = true;
    }

    public boolean equals(DBOperator other) {
        return this.getClass() == other.getClass()
                && this.invertOperator == other.invertOperator
                && this.includeNulls == other.includeNulls
                && firstValue.equals(other.firstValue)
                && secondValue.equals(other.secondValue)
                && thirdValue.equals(other.thirdValue);
    }

    abstract public DBOperator copyAndAdapt(DBSafeInternalQDTAdaptor typeAdaptor);
}
