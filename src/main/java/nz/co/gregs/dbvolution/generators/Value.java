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
package nz.co.gregs.dbvolution.generators;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

public class Value implements DataGenerator {

    private final QueryableDatatype qdt;
    private Object object;

    public Value(Object obj) {
        this.object = obj;
        qdt = QueryableDatatype.getQueryableDatatypeForObject(obj);
        qdt.setValue(object);
    }

    @Override
    public String toSQLString(DBDatabase db) {
        return qdt.toSQLString(db);
    }

    @Override
    public DataGenerator copy() {
        return new Value(object);
    }

    @Override
    public boolean isNull() {
        return false;
    }
    
}
