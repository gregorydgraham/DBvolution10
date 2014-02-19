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
import nz.co.gregs.dbvolution.expressions.NumberResult;

/**
 * Like {@link DBInteger} except that the database value can be easily
 * interpreted as an enumeration with {@code Integer} or {@code Long} codes.
 *
 * @param <E> type of enumeration class
 */
public class DBIntegerEnum<E extends Enum<E> & DBEnumValue<? extends Number>> extends DBEnum<E> {

    private static final long serialVersionUID = 1L;

    public DBIntegerEnum() {
    }

    public DBIntegerEnum(Integer value) {
        super(value.longValue());
    }

    public DBIntegerEnum(Long value) {
        super(value);
    }

    public DBIntegerEnum(NumberResult numberExpression) {
        super(numberExpression);
    }

    public DBIntegerEnum(E value) {
        super(value);
    }

    @Override
    public void setValue(Object newLiteralValue) {
        if (newLiteralValue instanceof Long) {
            setValue((Long) newLiteralValue);
        } else if (newLiteralValue instanceof Integer) {
            setValue((Integer)newLiteralValue);
        } else if (newLiteralValue instanceof DBIntegerEnum) {
            setValue(((DBIntegerEnum)newLiteralValue).literalValue);
        } else {
            throw new ClassCastException(this.getClass().getSimpleName() + ".setValue() Called With A Non-Long: Use only Long with this class");
        }
    }
    
    public void setValue(Long newLiteralValue){
        super.setLiteralValue(newLiteralValue);
    }

    public void setValue(Integer newLiteralValue){
        super.setLiteralValue(newLiteralValue);
    }

    @Override
    protected void validateLiteralValue(E enumValue) {
        Object literalValue = enumValue.getCode();
        if (literalValue != null) {
            if (!(literalValue instanceof Integer || literalValue instanceof Long)) {
                String enumMethodRef = enumValue.getClass().getName() + "." + enumValue.name() + ".getLiteralValue()";
                String literalValueTypeRef = literalValue.getClass().getName();
                throw new IncompatibleClassChangeError("Enum literal type is not valid: "
                        + enumMethodRef + " returned a " + literalValueTypeRef + ", which is not valid for a " + this.getClass().getSimpleName());
            }
        }
    }

    @Override
    public String getSQLDatatype() {
        return new DBInteger().getSQLDatatype();
    }

    @Override
    public void setFromResultSet(ResultSet resultSet, String fullColumnName) {
        blankQuery();
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
                this.setLiteralValue(dbValue);
            }
        }
        setUnchanged();
        setDefined(true);
    }

    @Override
    public DBInteger getQueryableDatatypeForExpressionValue() {
        return new DBInteger();
    }
}
