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
package nz.co.gregs.dbvolution.math;

import java.util.Arrays;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.generators.Column;
import nz.co.gregs.dbvolution.generators.DataGenerator;
import nz.co.gregs.dbvolution.generators.NumberGenerator;
import nz.co.gregs.dbvolution.generators.Value;

public class DBMath implements NumberGenerator {

    private final DataGenerator innerGenerator;

    private DBMath(DataGenerator dataGenerator) {
        super();
        innerGenerator = dataGenerator;
    }

    @Override
    public String toSQLString(DBDatabase db) {
        return innerGenerator.toSQLString(db);
    }

    @Override
    public DataGenerator copy() {
        return new DBMath(innerGenerator);
    }

    public static DBMath column(DBRow row, QueryableDatatype qdt) {
        return new DBMath(new Column(row, qdt));
    }

    public static DBMath value(Object value) {
        return new DBMath(new Value(value));
    }

    public static DBMath bracket(DBMath equation) {
        return new DBMath(new DBUnaryFunction(equation) {
            @Override
            String getFunctionName(DBDatabase db) {
                return "";
            }           
        });
    }

    public static DBMath exp(DBMath equation) {
        return new DBMath(new DBUnaryFunction(equation) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "exp";
            }
        });
    }

    public static DBMath cos(DBMath equation) {
        return new DBMath(new DBUnaryFunction(equation) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "cos";
            }
        });
    }

    public static DBMath cosh(DBMath equation) {
        return new DBMath(new DBUnaryFunction(equation) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "cosh";
            }
        });
    }

    public static DBMath sin(DBMath equation) {
        return new DBMath(new DBUnaryFunction(equation) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "sin";
            }
        });
    }

    public static DBMath sinh(DBMath equation) {
        return new DBMath(new DBUnaryFunction(equation) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "sinh";
            }
        });
    }

    public static DBMath tan(DBMath equation) {
        return new DBMath(new DBUnaryFunction(equation) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "tan";
            }
        });
    }

    public static DBMath tanh(DBMath equation) {
        return new DBMath(new DBUnaryFunction(equation) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "tanh";
            }
        });
    }

    public static DBMath abs(DBMath equation) {
        return new DBMath(new DBUnaryFunction(equation) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "abs";
            }
        });
    }

    public static DBMath arccos(DBMath equation) {
        return new DBMath(new DBUnaryFunction(equation) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "acos";
            }
        });
    }

    public static DBMath arcsin(DBMath equation) {
        return new DBMath(new DBUnaryFunction(equation) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "asin";
            }
        });
    }

    public static DBMath arctan(DBMath equation) {
        return new DBMath(new DBUnaryFunction(equation) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "atan";
            }
        });
    }

    public static DBMath arctan2(DBMath m, DBMath n) {
        return new DBMath(new DBBinaryFunction(m, n) {
            @Override
            String getFunctionName(DBDatabase db) {
                return "atn2";
            }
        });
    }

    public static DBMath cotangent(DBMath equation) {
        return new DBMath(new DBUnaryFunction(equation) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "cot";
            }
        });
    }

    public static DBMath degrees(DBMath equation) {
        return new DBMath(new DBUnaryFunction(equation) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "degrees";
            }
        });
    }

    public static DBMath radians(DBMath equation) {
        return new DBMath(new DBUnaryFunction(equation) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "radians";
            }
        });
    }

    public static DBMath log(DBMath equation) {
        return new DBMath(new DBUnaryFunction(equation) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "log";
            }
        });
    }

    public static DBMath logBase10(DBMath equation) {
        return new DBMath(new DBUnaryFunction(equation) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "log10";
            }
        });
    }

    public static DBMath power(DBMath m, DBMath n) {
        return new DBMath(new DBBinaryFunction(m,n) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "power";
            }
        });
    }

    public static DBMath random(DBMath equation) {
        return new DBMath(new DBUnaryFunction(equation) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "rand";
            }
        });
    }

    public static DBMath sign(DBMath equation) {
        return new DBMath(new DBUnaryFunction(equation) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "sign";
            }
        });
    }

    public static DBMath squareRoot(DBMath equation) {
        return new DBMath(new DBUnaryFunction(equation) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "sqrt";
            }
        });
    }

    public static DBMath standardDev(DBMath num) {
        return new DBMath(new DBUnaryFunction(num) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "stddev";
            }
        });
    }

    public static DBMath variance(DBMath num) {
        return new DBMath(new DBUnaryFunction(num) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "var";
            }
        });
    }

    /**
     * Implements support for CEIL() 
     * 
     * <p>Note: CEIL(-1.5) == -1
     * 
     * @param equation
     * @return the value of the equation rounded up to the nearest integer.
     */
    public static DBMath roundUp(DBMath equation) {
        return new DBMath(new DBUnaryFunction(equation) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "ceil";
            }
        });
    }

    /**
     * Implements support for ROUND()
     * 
     * @param equation
     * @return the equation rounded to the nearest integer.
     */
    public static DBMath round(DBMath equation) {
        return new DBMath(new DBUnaryFunction(equation) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "round";
            }
        });
    }

    /**
     * Implements support for FLOOR() 
     * 
     * <p>note that this is not the same as trunc() as 
     * roundDown(-1.5) == -2 and trunc(-1.5) == -1
     *
     * @param equation
     * @return the value of the equation rounded down to the nearest integer.
     */
    public static DBMath roundDown(DBMath equation) {
        return new DBMath(new DBUnaryFunction(equation) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "floor";
            }
        });
    }

    /**
     * Implements support for TRUNC() 
     * 
     * <p>note that this is not the same as roundDown() as 
     * roundDown(-1.5) == -2 and trunc(-1.5) == -1
     *
     * @param equation
     * @return the value of the equation with the decimal part removed.
     */
    public static DBMath trunc(DBMath equation) {
        return new DBMath(new DBUnaryFunction(equation) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "trunc";
            }
        });
    }

    public DBMath minus(DataGenerator dataGenerator) {
        return new DBMath(new DBBinaryArithmetic(innerGenerator, dataGenerator) {

            @Override
            protected String getEquationOperator(DBDatabase db) {
                return " - ";
            }
        });
    }

    public DBMath minus(Number num) {
        return new DBMath(new DBBinaryArithmetic(innerGenerator, new Value(num)) {

            @Override
            protected String getEquationOperator(DBDatabase db) {
                return " - ";
            }
        });
    }

    public DBMath plus(DataGenerator dataGenerator) {
        return new DBMath(new DBBinaryArithmetic(innerGenerator, dataGenerator) {

            @Override
            protected String getEquationOperator(DBDatabase db) {
                return " + ";
            }
        });
    }

    public DBMath plus(Number num) {
        return new DBMath(new DBBinaryArithmetic(innerGenerator, new Value(num)) {

            @Override
            protected String getEquationOperator(DBDatabase db) {
                return " + ";
            }
        });
    }

    public DBMath times(DataGenerator dataGenerator) {
        return new DBMath(new DBBinaryArithmetic(innerGenerator, dataGenerator) {

            @Override
            protected String getEquationOperator(DBDatabase db) {
                return " * ";
            }
        });
    }

    public DBMath times(Number num) {
        return new DBMath(new DBBinaryArithmetic(innerGenerator, new Value(num)) {

            @Override
            protected String getEquationOperator(DBDatabase db) {
                return " * ";
            }
        });
    }

    public DBMath dividedBy(DataGenerator dataGenerator) {
        return new DBMath(new DBBinaryArithmetic(innerGenerator, dataGenerator) {

            @Override
            protected String getEquationOperator(DBDatabase db) {
                return " / ";
            }
        });
    }

    public DBMath dividedBy(Number num) {
        return new DBMath(new DBBinaryArithmetic(innerGenerator, new Value(num)) {

            @Override
            protected String getEquationOperator(DBDatabase db) {
                return " / ";
            }
        });
    }

    public DBMath mod(DataGenerator dataGenerator) {
        return new DBMath(new DBBinaryArithmetic(innerGenerator, dataGenerator) {

            @Override
            protected String getEquationOperator(DBDatabase db) {
                return " % ";
            }
        });
    }

    public DBMath mod(Number num) {
        return new DBMath(new DBBinaryArithmetic(innerGenerator, new Value(num)) {

            @Override
            protected String getEquationOperator(DBDatabase db) {
                return "%";
            }
        });
    }

    private static abstract class DBBinaryArithmetic implements DataGenerator {

        private DataGenerator first;
        private DataGenerator second;

        public DBBinaryArithmetic(DataGenerator first, DataGenerator second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public String toSQLString(DBDatabase db) {
            return first.toSQLString(db) + this.getEquationOperator(db) + second.toSQLString(db);
        }

        @Override
        public DataGenerator copy() {
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

    private static abstract class DBUnaryFunction implements DataGenerator{
        
        private DataGenerator only;

        public DBUnaryFunction() {
            this.only = null;
        }

        public DBUnaryFunction(DataGenerator only) {
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
        public DataGenerator copy() {
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

    private static abstract class DBBinaryFunction implements DataGenerator {

        private DataGenerator first;
        private DataGenerator second;

        public DBBinaryFunction(DataGenerator first) {
            this.first = first;
            this.second = null;
        }

        public DBBinaryFunction(DataGenerator first, DataGenerator second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public String toSQLString(DBDatabase db) {
            return this.beforeValue(db)+first.toSQLString(db) + this.getSeparator(db) + (second==null?"":second.toSQLString(db))+this.afterValue(db);
        }

        @Override
        public DataGenerator copy() {
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
    private static abstract class DBNnaryFunction implements DataGenerator {

        private DataGenerator[] nums;

        public DBNnaryFunction(DataGenerator... nums) {
            this.nums = nums;
        }

        @Override
        public String toSQLString(DBDatabase db) {
            StringBuilder str = new StringBuilder();
            str.append(this.beforeValue(db));
            String sep = "";
            for(DataGenerator dg : nums){
                    str.append(sep).append(dg.toSQLString(db));
                    sep = getSeparator(db);
            }
            str.append(this.afterValue(db));
            return str.toString();
        }

        @Override
        public DataGenerator copy() {
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
