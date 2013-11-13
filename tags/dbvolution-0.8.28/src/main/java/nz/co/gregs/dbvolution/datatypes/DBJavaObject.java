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

import java.sql.ResultSet;
import java.sql.SQLException;
import nz.co.gregs.dbvolution.DBDatabase;

/**
 *
 * Implements the abstractions required for handling Java Objects stored in the
 * database
 *
 * @author gregory.graham
 */
public class DBJavaObject extends QueryableDatatype {

    public static final long serialVersionUID = 1;

    // TODO
    @Override
    public String getSQLDatatype() {
        return "JAVA_OBJECT";
    }

    @Override
    public void setFromResultSet(ResultSet resultSet, String fullColumnName) {
        if (resultSet == null || fullColumnName == null) {
            this.useNullOperator();
        } else {
            Object dbValue;
            try {
                dbValue = resultSet.getObject(fullColumnName);
                if (resultSet.wasNull()){
                    dbValue = null;
                }
            } catch (SQLException ex) {
                dbValue = null;
            }
            if (dbValue == null) {
                this.useNullOperator();
            } else {
                this.useEqualsOperator(dbValue);
            }
        }
    }

    @Override
    public String formatValueForSQLStatement(DBDatabase db) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
