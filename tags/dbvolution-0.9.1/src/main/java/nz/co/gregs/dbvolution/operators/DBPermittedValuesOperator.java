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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import static nz.co.gregs.dbvolution.datatypes.QueryableDatatype.getQueryableDatatypeForObject;


public class DBPermittedValuesOperator extends DBMetaOperator {

    public static final long serialVersionUID = 1L;

    @SuppressWarnings("unchecked")
    public DBPermittedValuesOperator(Object... permitted) {
        ArrayList<QueryableDatatype> qdts = new ArrayList<QueryableDatatype>();
        for (Object obj : permitted) {
            if (obj instanceof List) {
                List<Object> myList = (List) obj;
                for (Object obj1 : myList) {
                    qdts.add(getQueryableDatatypeForObject(obj1));
                }
            } else if (obj instanceof Set) {
                Set<Object> myList = (Set) obj;
                for (Object obj1 : myList) {
                    qdts.add(getQueryableDatatypeForObject(obj1));
                }
            } else {
                qdts.add(getQueryableDatatypeForObject(obj));
            }
        }
        if (permitted == null) {
            operator = new DBIsNullOperator();
        } else if (qdts.isEmpty())  {
            operator = new DBIsNullOperator();
        } else if (qdts.size() == 1) {
                operator = new DBEqualsOperator(qdts.get(0));
        } else {
            operator = new DBInOperator(qdts);
        }
    }

//    public DBPermittedValuesOperator(List<QueryableDatatype> qdts) {
//        if (qdts == null) {
//            operator = new DBIsNullOperator();
//        } else if (qdts.size() == 1) {
//                operator = new DBEqualsOperator(qdts.get(0));
//        } else {
//            operator = new DBInOperator(qdts);
//        }
//    }
    
}
