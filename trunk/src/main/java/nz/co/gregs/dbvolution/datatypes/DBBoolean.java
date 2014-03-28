/*
 * Copyright 2013 gregorygraham.
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
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.BooleanResult;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.operators.DBPermittedPatternOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeExclusiveOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeInclusiveOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedValuesIgnoreCaseOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedValuesOperator;

public class DBBoolean extends QueryableDatatype implements BooleanResult {

    private static final long serialVersionUID = 1L;

    public DBBoolean() {
    }

    public DBBoolean(Boolean bool) {
        super(bool);
    }

    public DBBoolean(BooleanResult bool) {
        super(bool);
    }

    @Override
    public String getSQLDatatype() {
        return "BIT(1)";
    }

    @Override
    public void setValue(Object newLiteralValue) {
        if (newLiteralValue instanceof Boolean) {
            setValue((Boolean) newLiteralValue);
        } else if (newLiteralValue instanceof DBBoolean) {
            setValue(((DBBoolean) newLiteralValue).getValue());
        } else {
            throw new ClassCastException(this.getClass().getSimpleName() + ".setValue() Called With A Non-Boolean: Use only Booleans with this class");
        }
    }

    public void setValue(Boolean newLiteralValue) {
        super.setLiteralValue(newLiteralValue);
    }

    @Override
    public String formatValueForSQLStatement(DBDatabase db) {
        DBDefinition defn = db.getDefinition();
//        if (this.isDBNull || literalValue == null) {
//            defn.getNull();
//        } else 
        if (literalValue instanceof Boolean) {
            Boolean boolValue = (Boolean) literalValue;
            return defn.beginNumberValue() + (boolValue == Boolean.TRUE ? 1 : 0) + defn.endNumberValue();
        }
        return defn.getNull();
    }

    public Boolean booleanValue() {
        if (this.literalValue instanceof Boolean) {
            return (Boolean) this.literalValue;
        } else {
            return null;
        }
    }

    @Override
    public DBBoolean copy() {
        return (DBBoolean) (BooleanResult) super.copy();
    }

    @Override
    public Boolean getValue() {
        return booleanValue();
    }

    @Override
    public DBBoolean getQueryableDatatypeForExpressionValue() {
        return new DBBoolean();
    }

    @Override
    public boolean isAggregator() {
        return false;
    }

    @Override
    public Set<DBRow> getTablesInvolved() {
        return new HashSet<DBRow>();
    }

    /**
     *
     * reduces the rows to only the object, Set, List, Array, or vararg of
     * objects
     *
     * @param permitted
     */
    public void permittedValues(Boolean permitted) {
        this.setOperator(new DBPermittedValuesOperator(permitted));
    }

    /**
     *
     * excludes the object, Set, List, Array, or vararg of objects
     *
     *
     * @param excluded
     */
    public void excludedValues(Boolean excluded) {
        this.setOperator(new DBPermittedValuesOperator(excluded));
        negateOperator();
    }

    /**
     *
     * reduces the rows to only the object, Set, List, Array, or vararg of
     * objects
     *
     * @param permitted
     */
    public void permittedValues(BooleanExpression permitted) {
        this.setOperator(new DBPermittedValuesOperator(permitted));
    }

    /**
     *
     * excludes the object, Set, List, Array, or vararg of objects
     *
     *
     * @param excluded
     */
    public void excludedValues(BooleanExpression excluded) {
        this.setOperator(new DBPermittedValuesOperator(excluded));
        negateOperator();
    }
}
