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

package nz.co.gregs.dbvolution.expressions;

import nz.co.gregs.dbvolution.operators.DBOperator;

/**
 * SOON TO BE REMOVED.
 *
 * @author Gregory Graham
 * @deprecated Replaced by {@link DBExpression} such as {@link NumberExpression}, {@link StringExpression}, and {@link DateExpression}.  For most instances you should change the addComparison method to the addCondition method, remove the operator and use the tests provided by the expression.
 */
@Deprecated
public class DBDataComparison {
    private final DBOperator operator;
    private final DBExpression leftHandSide;

    public DBDataComparison(DBExpression transformForLeftHandSide, DBOperator operatorWithRightHandSideValues) {
        this.operator = operatorWithRightHandSideValues;
        this.leftHandSide = transformForLeftHandSide;
    }
    
    

    public DBOperator getOperator() {
        return operator;
    }

    public DBExpression getLeftHandSide() {
        return leftHandSide;
    }
    
}
