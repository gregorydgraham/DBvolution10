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
package nz.co.gregs.dbvolution.generators;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

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

    public static DBMath sin(DBMath equation) {
        return new DBMath(new DBUnaryFunction(equation) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "sin";
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
                return " % ";
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

        public DBUnaryFunction(DataGenerator only) {
            this.only = only;
        }

        abstract String getFunctionName(DBDatabase db);

        protected String beforeValue(DBDatabase db) {
            return " " + getFunctionName(db) + "( ";
        }
        
        protected String afterValue(DBDatabase db) {
            return " ) ";
        }

        @Override
        public String toSQLString(DBDatabase db) {
            return this.beforeValue(db) + only.toSQLString(db) + this.afterValue(db);
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
}
