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

import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;


public class DBPermittedRangeOperator extends DBMetaOperator {

    public static final long serialVersionUID = 1L;
    
    public DBPermittedRangeOperator(Object lowerBound, Object upperBound) {
        if (lowerBound != null && upperBound != null) {
            operator = new DBBetweenInclusiveExclusiveOperator(
                    QueryableDatatype.getQueryableDatatypeForObject(lowerBound), 
                    QueryableDatatype.getQueryableDatatypeForObject(upperBound));
        } else if (lowerBound == null && upperBound != null) {
            QueryableDatatype qdt = QueryableDatatype.getQueryableDatatypeForObject(upperBound);
//            qdt.setLiteralValue(upperBound);
            operator = new DBLessThanOrEqualOperator(qdt);
        } else if (lowerBound != null && upperBound == null) {
            final QueryableDatatype qdt = QueryableDatatype.getQueryableDatatypeForObject(lowerBound);
//            qdt.setLiteralValue(lowerBound);
            operator = new DBGreaterThanOperator(qdt);
        }
    }

}
