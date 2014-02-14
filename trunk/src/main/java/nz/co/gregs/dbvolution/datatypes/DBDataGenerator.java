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
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.DateResult;
import nz.co.gregs.dbvolution.expressions.NumberResult;
import nz.co.gregs.dbvolution.expressions.StringResult;

/**
 *
 * @author Gregory Graham
 */
public class DBDataGenerator extends QueryableDatatype {

    public static final long serialVersionUID = 1L;

    public DBDataGenerator(DBExpression dataGenerator) {
        super(dataGenerator);
    }

    public DBDataGenerator() {
    }

    @Override
    public String getSQLDatatype() {
        if (literalValue instanceof DateResult) {
            return new DBDate().getSQLDatatype();
        } else if (literalValue instanceof NumberResult) {
            return new DBNumber().getSQLDatatype();
        } else if (literalValue instanceof StringResult) {
            return new DBString().getSQLDatatype();
        } else {
            return new DBUnknownDatatype().getSQLDatatype();
        }
    }

    @Override
    protected String formatValueForSQLStatement(DBDatabase db) {
        return ((DBExpression) literalValue).toSQLString(db);
    }

    @Override
    public void setValue(Object newLiteralValue) {
        if (newLiteralValue instanceof DBExpression) {
            setValue((DBExpression) newLiteralValue);
        } else if (newLiteralValue instanceof DBDataGenerator) {
            setValue(((DBDataGenerator) newLiteralValue).literalValue);
        } else {
            throw new ClassCastException(this.getClass().getSimpleName()+".setValue() Called With A "+newLiteralValue.getClass().getSimpleName()+": Use only Dates with this class");
        }
    }
    
    public void setValue(DBExpression newLiteralValue){
        setLiteralValue(newLiteralValue);
    }
}
