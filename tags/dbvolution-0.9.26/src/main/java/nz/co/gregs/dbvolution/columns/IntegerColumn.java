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
package nz.co.gregs.dbvolution.columns;

import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.query.RowDefinition;

public class IntegerColumn extends NumberExpression implements ColumnProvider {

    private AbstractColumn column;

    private IntegerColumn() {
    }

    public IntegerColumn(RowDefinition row, Long field) {
        this.column = new AbstractColumn(row, field);
    }

    public IntegerColumn(RowDefinition row, Integer field) {
        this.column = new AbstractColumn(row, field);
    }

    public IntegerColumn(RowDefinition row, DBInteger field) {
        this.column = new AbstractColumn(row, field);
    }

    @Override
    public String toSQLString(DBDatabase db) {
        return column.toSQLString(db);
    }

    @Override
    public synchronized IntegerColumn copy() {
        try {
            IntegerColumn newInstance = this.getClass().newInstance();
            newInstance.column = this.column;
            return newInstance;
        } catch (InstantiationException ex) {
            throw new RuntimeException(ex);
        } catch (IllegalAccessException ex) {
            throw new RuntimeException(ex);
        }

    }

    @Override
    public AbstractColumn getColumn() {
        return column;
    }

    @Override
    public Set<DBRow> getTablesInvolved() {
        return column.getTablesInvolved();
    }
}
