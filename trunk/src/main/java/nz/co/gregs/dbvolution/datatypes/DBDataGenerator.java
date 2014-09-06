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

import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.DateExpression;
import nz.co.gregs.dbvolution.expressions.DateResult;
import nz.co.gregs.dbvolution.expressions.LargeObjectExpression;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.expressions.NumberResult;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.expressions.StringResult;

/**
 *
 * @author Gregory Graham
 */
@Deprecated
public class DBDataGenerator extends QueryableDatatype {

    private static final long serialVersionUID = 1L;

	/**
	 *
	 * @param dataGenerator
	 */
	public DBDataGenerator(DBExpression dataGenerator) {
        super(dataGenerator);
    }

	/**
	 * Default constructor.
	 *
	 */
	public DBDataGenerator() {
    }

    @Override
    public String getSQLDatatype() {
        if (getLiteralValue() instanceof DateResult) {
            return new DBDate().getSQLDatatype();
        } else if (getLiteralValue() instanceof NumberResult) {
            return new DBNumber().getSQLDatatype();
        } else if (getLiteralValue() instanceof StringResult) {
            return new DBString().getSQLDatatype();
        } else {
            return new DBUnknownDatatype().getSQLDatatype();
        }
    }

    @Override
    protected String formatValueForSQLStatement(DBDatabase db) {
        return ((DBExpression) getLiteralValue()).toSQLString(db);
    }

    @Override
    public void setValue(Object newLiteralValue) {
        if (newLiteralValue instanceof DBExpression) {
            setValue((DBExpression) newLiteralValue);
        } else if (newLiteralValue instanceof DBDataGenerator) {
            setValue(((DBDataGenerator) newLiteralValue).getLiteralValue());
        } else {
            throw new ClassCastException(this.getClass().getSimpleName() + ".setValue() Called With A " + newLiteralValue.getClass().getSimpleName() + ": Use only Dates with this class");
        }
    }

	/**
	 *
	 * @param newLiteralValue
	 */
	public void setValue(DBExpression newLiteralValue) {
        setLiteralValue(newLiteralValue);
    }

    @Override
    public QueryableDatatype getQueryableDatatypeForExpressionValue() {
        if (getLiteralValue() instanceof DateExpression) {
            return new DBDate();
        } else if (getLiteralValue() instanceof NumberExpression) {
            return new DBNumber();
        } else if (getLiteralValue() instanceof StringExpression) {
            return new DBString();
        } else if (getLiteralValue() instanceof BooleanExpression) {
            return new DBBoolean();
        } else if (getLiteralValue() instanceof LargeObjectExpression) {
            return new DBByteArray();
        } else {
            return new DBUnknownDatatype();
        }
    }

    @Override
    public boolean isAggregator() {
        return this.getValue().isAggregator();
    }

    @Override
    public Set<DBRow> getTablesInvolved() {
        HashSet<DBRow> hashSet = new HashSet<DBRow>();
        hashSet.addAll(((DBExpression) getLiteralValue()).getTablesInvolved());
        return hashSet;
    }

    @Override
    public DBExpression getValue() {
        return (DBExpression)super.getLiteralValue();
    }

}
