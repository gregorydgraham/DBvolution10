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

import nz.co.gregs.dbvolution.QueryableDatatype;
import nz.co.gregs.dbvolution.databases.DBDatabase;

/**
 *
 * @author gregorygraham
 */
public class DBLikeOperator extends DBOperator {
    private final QueryableDatatype likeableValue;

    public DBLikeOperator(QueryableDatatype likeableValue) {
        this.likeableValue = likeableValue;
    }

    @Override
    public String generateWhereLine(DBDatabase database, String columnName) {
        likeableValue.setDatabase(database);
        return database.beginAndLine() +(invertOperator?"!(":"(")+ database.toLowerCase(database.formatColumnName(columnName)) + getOperator()+" "+database.toLowerCase(likeableValue.getSQLValue())+")";
    }

    private String getOperator() {
        return " like ";
    }
    
}
