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
package nz.co.gregs.dbvolution.datatypes;

import nz.co.gregs.dbvolution.DBDatabase;


/**
 * 
 * Class to allow some functionality for data types that DBvolution does not yet support
 *
 * @author gregory.graham
 */
public class DBUnknownDatatype extends QueryableDatatype {
    public static final long serialVersionUID = 1L;

    @Override
    public String getSQLDatatype() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String formatValueForSQLStatement(DBDatabase db) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public DBUnknownDatatype() {
        super();
    }

    public DBUnknownDatatype(String str) {
        super(str);
    }

    public DBUnknownDatatype(Object str) {
        super(str);
    }
    
}
