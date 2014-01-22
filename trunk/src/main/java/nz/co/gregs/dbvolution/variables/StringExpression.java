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
package nz.co.gregs.dbvolution.variables;

import java.util.Arrays;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.DBString;

public class StringExpression implements StringVariable {

    private StringVariable string1;

    protected StringExpression() {
    }

    public StringExpression(StringVariable stringVariable) {
        string1 = stringVariable;
    }

    public StringExpression(String stringVariable) {
        string1 = new DBString(stringVariable);
    }

    public StringExpression(DBString stringVariable) {
        string1 = stringVariable.copy();
    }

    @Override
    public String toSQLString(DBDatabase db) {
        return string1.toSQLString(db);
    }

    @Override
    public StringExpression copy() {
        return new StringExpression(this);
    }

    public StringExpression append(StringVariable string2) {
        return new StringExpression(new DBBinaryStringArithmetic(this, string2) {

            @Override
            protected String getEquationOperator(DBDatabase db) {
                return db.getDefinition().getConcatOperator();
            }
        });
    }

    public StringExpression append(String string2) {
        return new StringExpression(new DBBinaryStringArithmetic(this, new StringExpression(string2)) {

            @Override
            protected String getEquationOperator(DBDatabase db) {
                return db.getDefinition().getConcatOperator();
            }
        });
    }

    public StringExpression replace(String findString, String replaceWith) {
        return new StringExpression(
                new DBTrinaryStringFunction(this, new StringExpression(findString), new StringExpression(replaceWith)) {
                    @Override
                    String getFunctionName(DBDatabase db) {
                        return db.getDefinition().getReplaceFunctionName();
                    }
                });
    }

    public StringExpression replace(StringVariable findString, String replaceWith) {
        return new StringExpression(
                new DBTrinaryStringFunction(this, findString, new StringExpression(replaceWith)) {
                    @Override
                    String getFunctionName(DBDatabase db) {
                        return db.getDefinition().getReplaceFunctionName();
                    }
                });
    }

    public StringExpression replace(String findString, StringVariable replaceWith) {
        return new StringExpression(
                new DBTrinaryStringFunction(this, new StringExpression(findString), replaceWith) {
                    @Override
                    String getFunctionName(DBDatabase db) {
                        return db.getDefinition().getReplaceFunctionName();
                    }
                });
    }

    public StringExpression replace(StringVariable findString, StringVariable replaceWith) {
        return new StringExpression(
                new DBTrinaryStringFunction(this, findString, replaceWith) {
                    @Override
                    String getFunctionName(DBDatabase db) {
                        return db.getDefinition().getReplaceFunctionName();
                    }
                });
    }

    public StringExpression trim() {
        return new StringExpression(
                new DBUnaryStringFunction(this) {

                    @Override
                    public String toSQLString(DBDatabase db) {
                        return db.getDefinition().doTrimFunction(this.only.toSQLString(db));
                    }

                    @Override
                    String getFunctionName(DBDatabase db) {
                        return "NOT USED BECAUSE SQLSERVER DOESN'T IMPLEMENT TRIM";
                    }
                });
    }

    public StringExpression leftTrim() {
        return new StringExpression(
                new DBUnaryStringFunction(this) {
                    @Override
                    String getFunctionName(DBDatabase db) {
                        return db.getDefinition().getLeftTrimFunctionName();
                    }
                });
    }

    public StringExpression rightTrim() {
        return new StringExpression(
                new DBUnaryStringFunction(this) {
                    @Override
                    String getFunctionName(DBDatabase db) {
                        return db.getDefinition().getRightTrimFunctionName();
                    }
                });
    }

    public StringExpression lowercase() {
        return new StringExpression(
                new DBUnaryStringFunction(this) {
                    @Override
                    String getFunctionName(DBDatabase db) {
                        return db.getDefinition().getLowercaseFunctionName();
                    }
                });
    }

    public DBExpression uppercase() {
        return new StringExpression(
                new DBUnaryStringFunction(this) {
                    @Override
                    String getFunctionName(DBDatabase db) {
                        return db.getDefinition().getUppercaseFunctionName();
                    }
                });
    }

    public NumberExpression length() {
        return new NumberExpression(
                new DBUnaryNumberFunction(this) {
                    @Override
                    String getFunctionName(DBDatabase db) {
                        return db.getDefinition().getStringLengthFunctionName();
                    }
                });
    }

    public static StringExpression currentUser() {
        return new StringExpression(
                new DBNonaryStringFunction() {
                    @Override
                    String getFunctionName(DBDatabase db) {
                        return db.getDefinition().getCurrentUserFunctionName();
                    }
                });
    }

    private static abstract class DBBinaryStringArithmetic implements StringVariable {

        private StringVariable first;
        private StringVariable second;

        public DBBinaryStringArithmetic(StringVariable first, StringVariable second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public String toSQLString(DBDatabase db) {
            return first.toSQLString(db) + this.getEquationOperator(db) + second.toSQLString(db);
        }

        @Override
        public DBBinaryStringArithmetic copy() {
            DBBinaryStringArithmetic newInstance;
            try {
                newInstance = getClass().newInstance();
            } catch (InstantiationException ex) {
                throw new RuntimeException(ex);
            } catch (IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
            newInstance.first = first.copy();
            newInstance.second = second.copy();
            return newInstance;
        }

        protected abstract String getEquationOperator(DBDatabase db);
    }

    private static abstract class DBNonaryStringFunction implements StringVariable {

        public DBNonaryStringFunction() {
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
        public DBNonaryStringFunction copy() {
            DBNonaryStringFunction newInstance;
            try {
                newInstance = getClass().newInstance();
            } catch (InstantiationException ex) {
                throw new RuntimeException(ex);
            } catch (IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
            return newInstance;
        }
    }

    private static abstract class DBUnaryStringFunction implements StringVariable {

        protected StringVariable only;

        public DBUnaryStringFunction() {
            this.only = null;
        }

        public DBUnaryStringFunction(StringVariable only) {
            this.only = only;
        }

        abstract String getFunctionName(DBDatabase db);

        protected String beforeValue(DBDatabase db) {
            return "" + getFunctionName(db) + "( ";
        }

        protected String afterValue(DBDatabase db) {
            return ") ";
        }

        @Override
        public String toSQLString(DBDatabase db) {
            return this.beforeValue(db) + (only == null ? "" : only.toSQLString(db)) + this.afterValue(db);
        }

        @Override
        public DBUnaryStringFunction copy() {
            DBUnaryStringFunction newInstance;
            try {
                newInstance = getClass().newInstance();
            } catch (InstantiationException ex) {
                throw new RuntimeException(ex);
            } catch (IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
            newInstance.only = only.copy();
            return newInstance;
        }
    }

    private static abstract class DBUnaryNumberFunction implements NumberVariable {

        protected StringVariable only;

        public DBUnaryNumberFunction() {
            this.only = null;
        }

        public DBUnaryNumberFunction(StringVariable only) {
            this.only = only;
        }

        abstract String getFunctionName(DBDatabase db);

        protected String beforeValue(DBDatabase db) {
            return "" + getFunctionName(db) + "( ";
        }

        protected String afterValue(DBDatabase db) {
            return ") ";
        }

        @Override
        public String toSQLString(DBDatabase db) {
            return this.beforeValue(db) + (only == null ? "" : only.toSQLString(db)) + this.afterValue(db);
        }

        @Override
        public DBUnaryNumberFunction copy() {
            DBUnaryNumberFunction newInstance;
            try {
                newInstance = getClass().newInstance();
            } catch (InstantiationException ex) {
                throw new RuntimeException(ex);
            } catch (IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
            newInstance.only = only.copy();
            return newInstance;
        }
    }

    private static abstract class DBBinaryStringFunction implements StringVariable {

        private DBExpression first;
        private DBExpression second;

        public DBBinaryStringFunction(StringVariable first) {
            this.first = first;
            this.second = null;
        }

        public DBBinaryStringFunction(StringVariable first, StringVariable second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public String toSQLString(DBDatabase db) {
            return this.beforeValue(db) + first.toSQLString(db) + this.getSeparator(db) + (second == null ? "" : second.toSQLString(db)) + this.afterValue(db);
        }

        @Override
        public DBBinaryStringFunction copy() {
            DBBinaryStringFunction newInstance;
            try {
                newInstance = getClass().newInstance();
            } catch (InstantiationException ex) {
                throw new RuntimeException(ex);
            } catch (IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
            newInstance.first = first.copy();
            newInstance.second = second.copy();
            return newInstance;
        }

        abstract String getFunctionName(DBDatabase db);

        protected String beforeValue(DBDatabase db) {
            return " " + getFunctionName(db) + "( ";
        }

        protected String getSeparator(DBDatabase db) {
            return ", ";
        }

        protected String afterValue(DBDatabase db) {
            return ") ";
        }

    }


    private static abstract class DBTrinaryStringFunction implements StringVariable {

        private DBExpression first;
        private DBExpression second;
        private DBExpression third;

        public DBTrinaryStringFunction(DBExpression first) {
            this.first = first;
            this.second = null;
            this.third = null;
        }

        public DBTrinaryStringFunction(DBExpression first, DBExpression second) {
            this.first = first;
            this.second = second;
        }

        public DBTrinaryStringFunction(DBExpression first, DBExpression second, DBExpression third) {
            this.first = first;
            this.second = second;
            this.third = third;
        }

        @Override
        public String toSQLString(DBDatabase db) {
            return this.beforeValue(db) + first.toSQLString(db) 
                    + this.getSeparator(db) + (second == null ? "" : second.toSQLString(db))  
                    + this.getSeparator(db) + (third == null ? "" : third.toSQLString(db)) 
                    + this.afterValue(db);
        }

        @Override
        public DBTrinaryStringFunction copy() {
            DBTrinaryStringFunction newInstance;
            try {
                newInstance = getClass().newInstance();
            } catch (InstantiationException ex) {
                throw new RuntimeException(ex);
            } catch (IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
            newInstance.first = first.copy();
            newInstance.second = second.copy();
            return newInstance;
        }

        abstract String getFunctionName(DBDatabase db);

        protected String beforeValue(DBDatabase db) {
            return " " + getFunctionName(db) + "( ";
        }

        protected String getSeparator(DBDatabase db) {
            return ", ";
        }

        protected String afterValue(DBDatabase db) {
            return ") ";
        }

    }

    /**
     * Implemented to support STDDEV and VARIANCE but they're aggregators so now
     * it's unused
     *
     */
    private static abstract class DBNnaryStringFunction implements StringVariable {

        protected DBExpression[] values;

        public DBNnaryStringFunction(DBExpression... values) {
            this.values = values;
        }

        @Override
        public String toSQLString(DBDatabase db) {
            StringBuilder str = new StringBuilder();
            str.append(this.beforeValue(db));
            String sep = "";
            for (DBExpression dg : values) {
                str.append(sep).append(dg.toSQLString(db));
                sep = getSeparator(db);
            }
            str.append(this.afterValue(db));
            return str.toString();
        }

        @Override
        public StringVariable copy() {
            DBNnaryStringFunction newInstance;
            try {
                newInstance = getClass().newInstance();
            } catch (InstantiationException ex) {
                throw new RuntimeException(ex);
            } catch (IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
            newInstance.values = Arrays.copyOf(values, values.length);
            return newInstance;
        }

        abstract String getFunctionName(DBDatabase db);

        protected String beforeValue(DBDatabase db) {
            return " " + getFunctionName(db) + "( ";
        }

        protected String getSeparator(DBDatabase db) {
            return ", ";
        }

        protected String afterValue(DBDatabase db) {
            return ") ";
        }
    }
}
