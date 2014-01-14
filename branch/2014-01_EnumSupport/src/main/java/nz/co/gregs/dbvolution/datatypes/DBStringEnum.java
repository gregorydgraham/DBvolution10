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

public class DBStringEnum<E extends DBEnumValue<String>> extends DBEnum<E> {

    private static final long serialVersionUID = 1L;

    public DBStringEnum() {
    }

    public DBStringEnum(E value) {
        super(value);
    }

    @Override
    public String getEnumLiteralValue() {
        DBEnumValue<?> value = getDBEnumValue();
        if (value == null) {
            return null;
        } else {
            Object literalValue1 = value.getLiteralValue();
            if (literalValue1 instanceof String) {
                return (String) literalValue1;
            } else {
                throw new IncompatibleClassChangeError("Enum Literal Type Is Not String: getLiteralValue() needs to return an String but it returned a " + literalValue1.getClass().getSimpleName() + " instead.");
            }
        }
    }

    @Override
    public String getSQLDatatype() {
        return new DBString().getSQLDatatype();
    }
}
