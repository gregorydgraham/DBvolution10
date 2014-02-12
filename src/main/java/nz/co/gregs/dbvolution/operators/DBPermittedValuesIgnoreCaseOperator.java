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
import nz.co.gregs.dbvolution.expressions.StringExpression;

public class DBPermittedValuesIgnoreCaseOperator extends DBMetaOperator {

    public static final long serialVersionUID = 1L;

    public DBPermittedValuesIgnoreCaseOperator(String... permitted) {
        ArrayList<QueryableDatatype> qdts = new ArrayList<QueryableDatatype>();
        for (String obj : permitted) {
                qdts.add(getQueryableDatatypeForObject(obj));
        }
        if (permitted == null) {
            operator = new DBIsNullOperator();
        } else if (qdts.isEmpty()) {
            operator = new DBIsNullOperator();
        } else if (qdts.size() == 1) {
            operator = new DBEqualsIgnoreCaseOperator(qdts.get(0));
        } else {
            operator = new DBInIgnoreCaseOperator(qdts);
        }
    }

    public DBPermittedValuesIgnoreCaseOperator(StringExpression... permitted) {
        ArrayList<QueryableDatatype> qdts = new ArrayList<QueryableDatatype>();
        for (StringExpression obj : permitted) {
                qdts.add(getQueryableDatatypeForObject(obj));
        }
        if (permitted == null) {
            operator = new DBIsNullOperator();
        } else if (qdts.isEmpty()) {
            operator = new DBIsNullOperator();
        } else if (qdts.size() == 1) {
            operator = new DBEqualsIgnoreCaseOperator(qdts.get(0));
        } else {
            operator = new DBInIgnoreCaseOperator(qdts);
        }
    }

    public DBPermittedValuesIgnoreCaseOperator(List<String> permitted) {
        ArrayList<QueryableDatatype> qdts = new ArrayList<QueryableDatatype>();
        for (String obj : permitted) {
            qdts.add(getQueryableDatatypeForObject(obj));
        }
        if (permitted == null) {
            operator = new DBIsNullOperator();
        } else if (qdts.isEmpty()) {
            operator = new DBIsNullOperator();
        } else if (qdts.size() == 1) {
            operator = new DBEqualsIgnoreCaseOperator(qdts.get(0));
        } else {
            operator = new DBInIgnoreCaseOperator(qdts);
        }
    }

    public DBPermittedValuesIgnoreCaseOperator(Set<String> permitted) {
        ArrayList<QueryableDatatype> qdts = new ArrayList<QueryableDatatype>();
        for (String obj : permitted) {
            qdts.add(getQueryableDatatypeForObject(obj));
        }
        if (permitted == null) {
            operator = new DBIsNullOperator();
        } else if (qdts.isEmpty()) {
            operator = new DBIsNullOperator();
        } else if (qdts.size() == 1) {
            operator = new DBEqualsIgnoreCaseOperator(qdts.get(0));
        } else {
            operator = new DBInIgnoreCaseOperator(qdts);
        }
    }

}
