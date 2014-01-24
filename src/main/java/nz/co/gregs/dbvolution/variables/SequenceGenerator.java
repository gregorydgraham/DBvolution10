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
package nz.co.gregs.dbvolution.variables;

import nz.co.gregs.dbvolution.DBDatabase;

public class SequenceGenerator implements NumberResult {

    public static final long serialVersionUID = 1L;

    private final String sequenceName;
    private String schemaName = null;

    public SequenceGenerator(String sequenceName) {
        if (sequenceName == null) {
            throw new NullPointerException("SequenceName Cannot Be Null: please supply a non-null value for the sequence name.");
        }
        this.sequenceName = sequenceName;
    }

    public SequenceGenerator(String schemaName, String sequenceName) {
        this(sequenceName);
        this.schemaName = schemaName;
    }

    @Override
    public String toSQLString(DBDatabase db) {
        return db.getDefinition().getNextSequenceValue(schemaName, sequenceName);
    }

    @Override
    public SequenceGenerator copy() {
        return new SequenceGenerator(schemaName, sequenceName);
    }

}
