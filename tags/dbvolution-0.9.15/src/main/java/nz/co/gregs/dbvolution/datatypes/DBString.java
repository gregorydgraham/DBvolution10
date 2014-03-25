/*
 * Copyright 2013 Gregory Graham.
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

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.expressions.StringResult;

/**
 *
 * @author Gregory Graham
 */
public class DBString extends QueryableDatatype implements StringResult {

    private static final long serialVersionUID = 1L;

    public DBString() {
        super();
    }

    public DBString(String string) {
        super(string);
    }

    public DBString(StringResult stringExpression) {
        super(stringExpression);
    }

    @Override
    public void setValue(Object newLiteralValue) {
        if (newLiteralValue instanceof String) {
            setValue((String) newLiteralValue);
        } else {
            throw new ClassCastException(this.getClass().getSimpleName() + ".setValue() Called With A Non-String: Use only Strings with this class");
        }
    }

    public void setValue(String str) {
        super.setLiteralValue(str);
    }

    @Override
    public String getValue() {
        final Object value = super.getValue();
        if (value == null) {
            return (String) null;
        } else if (value instanceof String) {
            return (String) value;
        } else {
            return value.toString();
        }
    }

    @Override
    public String getSQLDatatype() {
        return "VARCHAR(1000)";
    }

    @Override
    public String formatValueForSQLStatement(DBDatabase db) {
        DBDefinition defn = db.getDefinition();

        if (literalValue instanceof Date) {
            return defn.getDateFormattedForQuery((Date) literalValue);
        } else {
            String unsafeValue = literalValue.toString();
            return defn.beginStringValue() + defn.safeString(unsafeValue) + defn.endStringValue();
        }
//    }
    }

    @Override
    public DBString copy() {
        return (DBString) super.copy();
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
