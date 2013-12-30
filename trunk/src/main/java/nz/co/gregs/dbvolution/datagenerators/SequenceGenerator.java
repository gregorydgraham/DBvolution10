/*
 * Copyright 2013 greg.
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
package nz.co.gregs.dbvolution.datagenerators;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.datagenerators.NumberGenerator;

public class SequenceGenerator implements NumberGenerator {
    
    public static final long serialVersionUID = 1L;
    
    private final String sequenceName;
    private String schemaName = null;
    
    public SequenceGenerator(String sequenceName) {
        this.sequenceName = sequenceName;
    }
    
    public SequenceGenerator(String schemaName, String sequenceName) {
        this.schemaName = schemaName;
        this.sequenceName = sequenceName;
    }
    
    @Override
    public String toSQLString(DBDatabase db) {
        return " NEXTVAL( " + (schemaName == null ? "" : schemaName + ", ") + sequenceName + " ) ";
    }
    
    @Override
    public DataGenerator copy() {
        return new SequenceGenerator(schemaName, sequenceName);
    }
    
    @Override
    public boolean isNull() {
        return false;
    }
    
}
