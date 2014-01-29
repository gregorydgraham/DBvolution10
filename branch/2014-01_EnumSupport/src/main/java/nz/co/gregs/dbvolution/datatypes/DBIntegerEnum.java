/*
 * Copyright 2014 gregorygraham.
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

public class DBIntegerEnum<E extends Enum<E> & DBEnumValue<Integer>> extends DBEnum<E> {

    private static final long serialVersionUID = 1L;

    public DBIntegerEnum() {
    }

    public DBIntegerEnum(String value) {
        super(value);
    }
    
    public DBIntegerEnum(E value) {
        super(value);
    }

    @Override
    protected void validateLiteralValue(E enumValue) {
    	Object literalValue = enumValue.getLiteralValue();
    	if (literalValue != null) {
    		if (!(literalValue instanceof Integer || literalValue instanceof Long)) {
	    		String enumMethodRef = enumValue.getClass().getName()+"."+enumValue.name()+".getLiteralValue()";
	    		String literalValueTypeRef = literalValue.getClass().getName();
	            throw new IncompatibleClassChangeError("Enum literal type is not valid: "+
	            		enumMethodRef+" returned a "+literalValueTypeRef+", which is not valid for a "+this.getClass().getSimpleName());
    		}
    	}
    }

    @Override
    public String getSQLDatatype() {
        return new DBInteger().getSQLDatatype();
    }

    @Override
    public void setFromResultSet(ResultSet resultSet, String fullColumnName) {
        if (resultSet == null || fullColumnName == null) {
            this.setToNull();
        } else {
            Long dbValue;
            try {
                dbValue = resultSet.getLong(fullColumnName);
                if (resultSet.wasNull()) {
                    dbValue = null;
                }
            } catch (SQLException ex) {
                dbValue = null;
            }
            if (dbValue == null) {
                this.setToNull();
            } else {
                this.setValue(dbValue);
            }
        }
    }
}
