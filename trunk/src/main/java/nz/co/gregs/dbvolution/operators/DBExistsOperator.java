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
package nz.co.gregs.dbvolution.operators;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer.DBSafeInternalQDTAdaptor;
import nz.co.gregs.dbvolution.exceptions.InappropriateRelationshipOperator;
import nz.co.gregs.dbvolution.exceptions.IncorrectDBRowInstanceSuppliedException;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;

/**
 *
 * @author Gregory Graham
 * @param <E>
 */
public class  DBExistsOperator<E extends DBRow> extends DBOperator {
    public static final long serialVersionUID = 1L;

    E tableRow;
    private final String referencedColumnName;

    /**
     * Creates an exists operator on the given table row
     * instance and column identified by the given property's
     * object reference (field or method).
     * 
     * <p> For example the following code snippet will create
     * an exists operator on the uid column:
     * <pre>
     * Customer customer = ...;
     * new DBExistsOperator(customer, customer.uid);
     * </pre>
     *
     * <p> Requires that {@literal qdtOfTheRow} is from theliteralode tableRow}
     * instance for this to work.
     * @param tableRow
     * @param qdtOfTheRow
     * @throws IncorrectDBRowInstanceSuppliedException if the {@code qdtOfTheRow}
     * is not from the {@code tableRow} instance
     */
    public DBExistsOperator(E tableRow, Object qdtOfTheRow) throws IncorrectDBRowInstanceSuppliedException{
        this.tableRow = DBRow.copyDBRow(tableRow);
        PropertyWrapper qdtField = tableRow.getPropertyWrapperOf(qdtOfTheRow);
        if (qdtField == null) {
            throw new IncorrectDBRowInstanceSuppliedException(tableRow, qdtOfTheRow);
        }
        this.referencedColumnName = qdtField.columnName();
    }
    
    @Override
    public String generateWhereLine(DBDatabase database, String columnName) {
        DBDefinition defn = database.getDefinition();
        DBTable<E> table = DBTable.getInstance(database, tableRow);
        String subSelect;
        try {
            subSelect = table.getSQLSelectAndFromForQuery() + table.getSQLWhereClauseWithExampleAndRawSQL(tableRow, defn.beginWhereClauseLine()+ columnName + defn.getEqualsComparator() + defn.formatColumnName(referencedColumnName));
        } catch (IllegalArgumentException ex) {
            throw new RuntimeException("Error In DBExistsOperator", ex);
        }

        return (invertOperator?" not ":"")+" exists (" + subSelect + ") ";
    }

    @Override
    public String generateRelationship(DBDatabase database, String columnName, String otherColumnName) {
        throw new InappropriateRelationshipOperator(this);
    }

    @Override
    public DBOperator getInverseOperator() {
        throw new InappropriateRelationshipOperator(this);
    }

    @Override
    public DBExistsOperator<E> copyAndAdapt(DBSafeInternalQDTAdaptor typeAdaptor) {
    	return this;
    }
}
