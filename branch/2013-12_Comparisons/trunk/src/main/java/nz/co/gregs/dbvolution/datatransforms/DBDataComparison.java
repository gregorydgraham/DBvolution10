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

package nz.co.gregs.dbvolution.datatransforms;

import nz.co.gregs.dbvolution.operators.DBOperator;

/**
 *
 * @author gregory.graham
 */
public class DBDataComparison {
    private final DBOperator operator;
    private final DataTransform transformForQDT;

    public DBDataComparison(DataTransform transformForQDT, DBOperator operator) {
        this.operator = operator;
        this.transformForQDT = transformForQDT;
    }
    
    

    public DBOperator getOperator() {
        return operator;
    }

    public DataTransform getTransform() {
        return transformForQDT;
    }
    
}
