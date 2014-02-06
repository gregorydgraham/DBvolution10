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
package nz.co.gregs.dbvolution.query;

import java.io.Serializable;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;

import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.IncorrectDBRowInstanceSuppliedException;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import nz.co.gregs.dbvolution.operators.DBEqualsOperator;
import nz.co.gregs.dbvolution.operators.DBOperator;

/**
 * Represents a relationship between 2 columns on 2 tables.
 * 
 * <p>This is a generalisation of {@link DBForeignKey} that can be added to DBRows.
 * 
 * <p>Use {@link DBRow#addRelationship(nz.co.gregs.dbvolution.datatypes.QueryableDatatype, nz.co.gregs.dbvolution.DBRow, nz.co.gregs.dbvolution.datatypes.QueryableDatatype) DBRow's AddRelationship methods} to add a relationship to a DBRow.
 * 
 * <p>For a DBQuery, you may be better using the {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) addCondition method}.
 *
 * @author gregorygraham
 */
public class DBRelationship implements Serializable {

    private static final long serialVersionUID = 1L;
    private DBRow firstTable;
    private DBRow secondTable;
    private PropertyWrapper firstColumnPropertyWrapper;
    private PropertyWrapper secondColumnPropertyWrapper;
    private DBOperator operation;

    /**
     * Creates a new DBRelationship representing the connection between to tables
     *
     * <p>Relationships connect 2 columns in 2 tables. DBRelationship represents
     * a columnA = columnB relationship like a classic foreign key relation.
     * 
     * <p>Use this constructor to create a equal new relationship or use {@link DBRow#addRelationship(nz.co.gregs.dbvolution.datatypes.QueryableDatatype, nz.co.gregs.dbvolution.DBRow, nz.co.gregs.dbvolution.datatypes.QueryableDatatype) }
     * 
     * <p>More complex relationships a possible with the longer constructor or  {@link DBRow#addRelationship(nz.co.gregs.dbvolution.datatypes.QueryableDatatype, nz.co.gregs.dbvolution.DBRow, nz.co.gregs.dbvolution.datatypes.QueryableDatatype, nz.co.gregs.dbvolution.operators.DBOperator)  }
     * 
     * @param thisTable
     * @param thisTableField
     * @param otherTable
     * @param otherTableField
     */
    public DBRelationship(DBRow thisTable, QueryableDatatype thisTableField, DBRow otherTable, Object otherTableField) {
        this(thisTable, thisTableField, otherTable, otherTableField, new DBEqualsOperator(thisTableField));
    }

    /**
     * Creates a relationship between the first table's column and the second
     * tables' column, identified by the object references of column's fields
     * and/or methods.
     *
     * <p> For example the following code snippet will create a relationship
     * between the customer's fkAddress column and the address's uid column:
     * <pre>
     * Customer customer = ...;
     * Address address = ...;
     * DBOperator operator = ...;
     * new DBRelationship(customer, customer.fkAddress, address, address.uid, operator);
     * </pre>
     *
     * <p> Requires that {@code thisTableField} is from the {@code thisTable}
     * instance, and {@code otherTableField} is from the {@code otherTable}
     * instance.
     *
     * @param thisTable
     * @param thisTableField
     * @param otherTable
     * @param otherTableField
     * @param operator
     * @throws IncorrectDBRowInstanceSuppliedException if {@code thisTableField}
     * is not from the {@code thisTable} instance or if {@code otherTableField}
     * is not from the {@code otherTable} instance
     */
    public DBRelationship(DBRow thisTable, Object thisTableField, DBRow otherTable, Object otherTableField, DBOperator operator) {
        this.firstTable = DBRow.copyDBRow(thisTable);
        this.secondTable = DBRow.copyDBRow(otherTable);
        this.operation = operator;

        this.firstColumnPropertyWrapper = thisTable.getPropertyWrapperOf(thisTableField);
        if (firstColumnPropertyWrapper == null) {
            throw new IncorrectDBRowInstanceSuppliedException(thisTable, thisTableField);
        }

        this.secondColumnPropertyWrapper = otherTable.getPropertyWrapperOf(otherTableField);
        if (secondColumnPropertyWrapper == null) {
            throw new IncorrectDBRowInstanceSuppliedException(otherTable, otherTableField);
        }
    }

    public String toSQLString(DBDatabase database) {
        final DBDefinition definition = database.getDefinition();
        return getOperation().generateRelationship(database,
                definition.formatTableAliasAndColumnName(firstTable, firstColumnPropertyWrapper.columnName()),
                definition.formatTableAliasAndColumnName(secondTable, secondColumnPropertyWrapper.columnName()));
    }

    public static String toSQLString(DBDatabase database, DBRow firstTable, PropertyWrapper firstColumnProp, DBOperator operation, DBRow secondTable, PropertyWrapper secondColumnProp) {
        final DBDefinition definition = database.getDefinition();
        return operation.generateRelationship(database,
                definition.formatTableAliasAndColumnName(firstTable, firstColumnProp.columnName()),
                definition.formatTableAliasAndColumnName(secondTable, secondColumnProp.columnName()));
    }

    /**
     * @return the firstTable
     */
    public DBRow getFirstTable() {
        return firstTable;
    }

    /**
     * @return the secondTable
     */
    public DBRow getSecondTable() {
        return secondTable;
    }

    /**
     * @return the firstColumn's PropertyWrapper
     */
    public PropertyWrapper getFirstColumnPropertyWrapper() {
        return firstColumnPropertyWrapper;
    }

    /**
     * @return the secondColumn's PropertyWrapper
     */
    public PropertyWrapper getSecondColumnPropertyWrapper() {
        return secondColumnPropertyWrapper;
    }

    /**
     * @return the operation
     */
    public DBOperator getOperation() {
        return operation;
    }
}
