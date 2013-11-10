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
package nz.co.gregs.dbvolution.operators;

import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer.DBSafeInternalQDTAdaptor;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;

/**
 *
 * @author gregorygraham
 */
public class DBLikeCaseInsensitiveOperator extends DBOperator {

    public static final long serialVersionUID = 1L;
    private final QueryableDatatype likeableValue;

    public DBLikeCaseInsensitiveOperator(QueryableDatatype likeableValue) {
        super();
        this.likeableValue = likeableValue;
    }

    public DBLikeCaseInsensitiveOperator() {
        super();
        this.likeableValue = null;
    }

    @Override
    public String generateWhereLine(DBDatabase db, String columnName) {
//        likeableValue.setDatabase(db);
        if (db == null) {
            throw new RuntimeException("Database Cannot Be NULL: Please supply a proper DBDatabase instance.");
        } else if (likeableValue.toSQLString(db) == null) {
            throw new RuntimeException("Actual Comparison Is Required: please supply an actual object to compare against");
        } else if (columnName == null) {
            throw new RuntimeException("MalFormed DBRow: please supply a column name using the DBColumn annotation");
        } else if (invertOperator == null) {
            throw new RuntimeException("Invert Operator Missing: somehow you have removed the invertOperator instance, whatever you did with it, stop it.");
        } else if (getOperator() == null) {
            throw new RuntimeException("Get Operator Returns NULL: the getOperator() method returned null when it should return a String of the database's operator.");
        } else {
            DBDefinition defn = db.getDefinition();
            return defn.beginAndLine() + (invertOperator ? "!(" : "(") + defn.toLowerCase(defn.formatColumnName(columnName)) + getOperator() + " " + defn.toLowerCase(likeableValue.toSQLString(db)) + ")";
        }
    }

    private String getOperator() {
        return " like ";
    }

    @Override
    public String generateRelationship(DBDatabase database, String columnName, String otherColumnName) {
        DBDefinition defn = database.getDefinition();
        return (invertOperator ? "!(" : "(") + defn.toLowerCase(defn.formatColumnName(columnName)) + getOperator() + " " + defn.toLowerCase(otherColumnName) + ")";
    }

    @Override
    public DBOperator getInverseOperator() {
        return this;
    }

    @Override
    public DBLikeCaseInsensitiveOperator copyAndAdapt(DBSafeInternalQDTAdaptor typeAdaptor) {
    	DBLikeCaseInsensitiveOperator op = new DBLikeCaseInsensitiveOperator(typeAdaptor.convert(likeableValue));
    	op.invertOperator = this.invertOperator;
    	return op;
    }
}
