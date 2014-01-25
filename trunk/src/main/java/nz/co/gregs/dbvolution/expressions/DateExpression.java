/*
 * Copyright 2014 gregory.graham.
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

package nz.co.gregs.dbvolution.expressions;

import java.util.Date;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.DBDate;


public class DateExpression implements DateResult {

    private DateResult date1;

    protected DateExpression() {
    }
    
    public DateExpression(DateResult dateVariable) {
        date1 = dateVariable;
    }
    
    public DateExpression(Date date) {
        date1 = new DBDate(date);
    }
    
    @Override
    public String toSQLString(DBDatabase db) {
        return date1.toSQLString(db);
    }

    @Override
    public DateExpression copy() {
        return new DateExpression(this.date1);
    }
    
    public static DateExpression currentDate() {
        return new DateExpression(
                new DBNonaryFunction() {
                    @Override
                    String getFunctionName(DBDatabase db) {
                        return db.getDefinition().getCurrentDateFunctionName();
                    }
                });
    }

    public static DateExpression currentDateTime() {
        return new DateExpression(
                new DBNonaryFunction() {
                    @Override
                    String getFunctionName(DBDatabase db) {
                        return db.getDefinition().getCurrentTimestampFunction();
                    }
                });
    }

    public static DateExpression currentTime() {
        return new DateExpression(
                new DBNonaryFunction() {
                    @Override
                    String getFunctionName(DBDatabase db) {
                        return db.getDefinition().getCurrentTimeFunction();
                    }
                });
    }

    private static abstract class DBNonaryFunction implements DateResult {

        public DBNonaryFunction() {
        }

        abstract String getFunctionName(DBDatabase db);

        protected String beforeValue(DBDatabase db) {
            return " " + getFunctionName(db) + "";
        }

        protected String afterValue(DBDatabase db) {
            return " ";
        }

        @Override
        public String toSQLString(DBDatabase db) {
            return this.beforeValue(db) + this.afterValue(db);
        }

        @Override
        public DateExpression.DBNonaryFunction copy() {
            DateExpression.DBNonaryFunction newInstance;
            try {
                newInstance = getClass().newInstance();
            } catch (    InstantiationException | IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
            return newInstance;
        }
    }

}
