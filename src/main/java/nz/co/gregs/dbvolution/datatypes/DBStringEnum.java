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

import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.expressions.StringResult;

/**
 * Like {@link DBInteger} except that the database value can be easily
 * interpreted as an enumeration with integer codes.
 *
 * @param <E> type of enumeration class
 */
public class DBStringEnum<E extends Enum<E> & DBEnumValue<String>> extends DBEnum<E> {

    private static final long serialVersionUID = 1L;

    public DBStringEnum() {
    }

    public DBStringEnum(String value) {
        super(value);
    }

    public DBStringEnum(StringResult stringExpression) {
        super(stringExpression);
    }

    public DBStringEnum(E value) {
        super(value);
    }

    @Override
    protected void validateLiteralValue(E enumValue) {
        Object localValue = enumValue.getCode();
        if (localValue != null) {
            if (!(localValue instanceof String)) {
                String enumMethodRef = enumValue.getClass().getName() + "." + enumValue.name() + ".getLiteralValue()";
                String literalValueTypeRef = localValue.getClass().getName();
                throw new IncompatibleClassChangeError("Enum literal type is not valid: "
                        + enumMethodRef + " returned a " + literalValueTypeRef + ", which is not valid for a " + this.getClass().getSimpleName());
            }
        }
    }

    @Override
    public String getSQLDatatype() {
        return new DBString().getSQLDatatype();
    }

    @Override
    public void setValue(Object newLiteralValue) {
        if (newLiteralValue instanceof String) {
            setValue((String) newLiteralValue);
        } else if (newLiteralValue instanceof DBString) {
            setValue(((DBString) newLiteralValue).getValue());
        } else {
            throw new ClassCastException(this.getClass().getSimpleName() + ".setValue() Called With A Non-String: Use only Strings with this class");
        }
    }

    public void setValue(String newLiteralValue) {
        super.setLiteralValue(newLiteralValue);
    }

    @Override
    public DBString getQueryableDatatypeForExpressionValue() {
        return new DBString();
    }

    @Override
    public boolean isAggregator() {
        return false;
    }

    @Override
    public Set<DBRow> getTablesInvolved() {
        return new HashSet<DBRow>();
    }

}
