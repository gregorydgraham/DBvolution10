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

package nz.co.gregs.dbvolution.variables;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.DBString;


public class StringValue implements DBValue, StringVariable{

    private final DBString qdt;

    public StringValue(String obj) {
        qdt = new DBString(obj);
    }
    
    public StringValue(DBString str) {
        this(str.stringValue());
    }

    @Override
    public String toSQLString(DBDatabase db) {
        return qdt.toSQLString(db);
    }

    @Override
    public StringValue copy() {
        return new StringValue(this.qdt);
    }
    
}
