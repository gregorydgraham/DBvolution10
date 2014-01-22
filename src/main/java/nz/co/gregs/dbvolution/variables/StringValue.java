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

public class StringValue implements StringVariable {

    private final StringVariable string1;

    public StringValue(StringVariable stringVariable) {
        string1 = stringVariable;
    }

    public StringValue(String stringVariable) {
        string1 = new DBString(stringVariable);
    }

    @Override
    public String toSQLString(DBDatabase db) {
        return string1.toSQLString(db);
    }

    @Override
    public StringValue copy() {
        return new StringValue(this.string1);
    }

    public StringValue append(StringVariable string2) {
        return new StringValue(new DBBinaryStringArithmetic(this.string1, string2) {

            @Override
            protected String getEquationOperator(DBDatabase db) {
                return db.getDefinition().getConcatOperator();
            }
        });
    }

    public StringValue append(String string2) {
        return new StringValue(new DBBinaryStringArithmetic(this.string1, new StringValue(string2)) {

            @Override
            protected String getEquationOperator(DBDatabase db) {
                return db.getDefinition().getConcatOperator();
            }
        });
    }

    public StringValue replace(String findString, String replaceWith) {
        return new StringValue(
                new DBNnaryStringFunction(new StringVariable[]{this.string1, new StringValue(findString), new StringValue(replaceWith)}) {
                    @Override
                    String getFunctionName(DBDatabase db) {
                        return db.getDefinition().getReplaceFunctionName();
                    }
                });
    }

    public StringValue replace(StringVariable findString, String replaceWith) {
        return new StringValue(
                new DBNnaryStringFunction(new StringVariable[]{this.string1, findString, new StringValue(replaceWith)}) {
                    @Override
                    String getFunctionName(DBDatabase db) {
                        return db.getDefinition().getReplaceFunctionName();
                    }
                });
    }

    public StringValue replace(String findString, StringVariable replaceWith) {
        return new StringValue(
                new DBNnaryStringFunction(new StringVariable[]{this.string1, new StringValue(findString), replaceWith}) {
                    @Override
                    String getFunctionName(DBDatabase db) {
                        return db.getDefinition().getReplaceFunctionName();
                    }
                });
    }

    public StringValue replace(StringVariable findString, StringVariable replaceWith) {
        return new StringValue(
                new DBNnaryStringFunction(new StringVariable[]{this.string1, findString, replaceWith}) {
                    @Override
                    String getFunctionName(DBDatabase db) {
                        return db.getDefinition().getReplaceFunctionName();
                    }
                });
    }

    public StringValue trim() {
        return new StringValue(
                new DBUnaryStringFunction(this.string1) {

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

    public StringValue leftTrim() {
        return new StringValue(
                new DBUnaryStringFunction(this.string1) {
                    @Override
                    String getFunctionName(DBDatabase db) {
                        return db.getDefinition().getLeftTrimFunctionName();
                    }
                });
    }

    public StringValue rightTrim() {
        return new StringValue(
                new DBUnaryStringFunction(this.string1) {
                    @Override
                    String getFunctionName(DBDatabase db) {
                        return db.getDefinition().getRightTrimFunctionName();
                    }
                });
    }

    public StringValue lowercase() {
        return new StringValue(
                new DBUnaryStringFunction(this.string1) {
                    @Override
                    String getFunctionName(DBDatabase db) {
                        return db.getDefinition().getLowercaseFunctionName();
                    }
                });
    }

    public DBValue uppercase() {
        return new StringValue(
                new DBUnaryStringFunction(this.string1) {
                    @Override
                    String getFunctionName(DBDatabase db) {
                        return db.getDefinition().getUppercaseFunctionName();
                    }
                });
    }

    public NumberValue length() {
        return new NumberValue(
                new DBUnaryNumberFunction(this.string1) {
                    @Override
                    String getFunctionName(DBDatabase db) {
                        return db.getDefinition().getStringLengthFunctionName();
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

        private DBValue first;
        private DBValue second;

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

    /**
     * Implemented to support STDDEV and VARIANCE but they're aggregators so now
     * it's unused
     *
     */
    private static abstract class DBNnaryStringFunction implements StringVariable {

        protected DBValue[] values;

        public DBNnaryStringFunction(DBValue... values) {
            this.values = values;
        }

        @Override
        public String toSQLString(DBDatabase db) {
            StringBuilder str = new StringBuilder();
            str.append(this.beforeValue(db));
            String sep = "";
            for (DBValue dg : values) {
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
