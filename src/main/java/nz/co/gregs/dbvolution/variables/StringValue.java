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


public class StringValue implements DBValue, StringVariable{

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
    
    public StringValue append(StringVariable string2){
        return new StringValue(new DBBinaryArithmetic(this.string1, string2) {
            
            @Override
            protected String getEquationOperator(DBDatabase db) {
                return db.getDefinition().getConcatOperator();
            }
        });
    }
    
    public StringValue append(String string2){
        return new StringValue(new DBBinaryArithmetic(this.string1, new StringValue(string2)) {
            
            @Override
            protected String getEquationOperator(DBDatabase db) {
                return db.getDefinition().getConcatOperator();
            }
        });
    }

    public StringValue replace(String findString, String replaceWith) {
        return new StringValue(
                new DBNnaryFunction(new StringVariable[]{this.string1, new StringValue(findString), new StringValue(replaceWith)}) {
                    @Override
                    String getFunctionName(DBDatabase db) {
                        return db.getDefinition().getReplaceFunctionName();
                    }
                });
    }

    public StringValue replace(StringVariable findString, String replaceWith) {
        return new StringValue(
                new DBNnaryFunction(new StringVariable[]{this.string1, findString, new StringValue(replaceWith)}) {
                    @Override
                    String getFunctionName(DBDatabase db) {
                        return db.getDefinition().getReplaceFunctionName();
                    }
                });
    }

    public StringValue replace(String findString, StringVariable replaceWith) {
        return new StringValue(
                new DBNnaryFunction(new StringVariable[]{this.string1, new StringValue(findString), replaceWith}) {
                    @Override
                    String getFunctionName(DBDatabase db) {
                        return db.getDefinition().getReplaceFunctionName();
                    }
                });
    }

    public StringValue replace(StringVariable findString, StringVariable replaceWith) {
        return new StringValue(
                new DBNnaryFunction(new StringVariable[]{this.string1, findString, replaceWith}) {
                    @Override
                    String getFunctionName(DBDatabase db) {
                        return db.getDefinition().getReplaceFunctionName();
                    }
                });
    }

    private static abstract class DBBinaryArithmetic implements StringVariable {

        private StringVariable first;
        private StringVariable second;

        public DBBinaryArithmetic(StringVariable first, StringVariable second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public String toSQLString(DBDatabase db) {
            return first.toSQLString(db) + this.getEquationOperator(db) + second.toSQLString(db);
        }

        @Override
        public DBBinaryArithmetic copy() {
            DBBinaryArithmetic newInstance;
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

    private static abstract class DBUnaryFunction implements StringVariable{
        
        private StringVariable only;

        public DBUnaryFunction() {
            this.only = null;
        }

        public DBUnaryFunction(StringVariable only) {
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
            return this.beforeValue(db) + (only==null?"":only.toSQLString(db)) + this.afterValue(db);
        }

        @Override
        public DBUnaryFunction copy() {
            DBUnaryFunction newInstance;
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

    private static abstract class DBBinaryFunction implements StringVariable {

        private StringVariable first;
        private StringVariable second;

        public DBBinaryFunction(StringVariable first) {
            this.first = first;
            this.second = null;
        }

        public DBBinaryFunction(StringVariable first, StringVariable second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public String toSQLString(DBDatabase db) {
            return this.beforeValue(db)+first.toSQLString(db) + this.getSeparator(db) + (second==null?"":second.toSQLString(db))+this.afterValue(db);
        }

        @Override
        public DBBinaryFunction copy() {
            DBBinaryFunction newInstance;
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
     * Implemented to support STDDEV and VARIANCE but they're aggregators so now it's unused
     * 
     */
    private static abstract class DBNnaryFunction implements StringVariable {

        private StringVariable[] nums;

        public DBNnaryFunction(StringVariable... nums) {
            this.nums = nums;
        }

        @Override
        public String toSQLString(DBDatabase db) {
            StringBuilder str = new StringBuilder();
            str.append(this.beforeValue(db));
            String sep = "";
            for(StringVariable dg : nums){
                    str.append(sep).append(dg.toSQLString(db));
                    sep = getSeparator(db);
            }
            str.append(this.afterValue(db));
            return str.toString();
        }

        @Override
        public StringVariable copy() {
            DBNnaryFunction newInstance;
            try {
                newInstance = getClass().newInstance();
            } catch (InstantiationException ex) {
                throw new RuntimeException(ex);
            } catch (IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
            newInstance.nums = Arrays.copyOf(nums, nums.length);
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
