/*
 * Copyright 2014 gregorygraham.
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

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.DBNumber;

public class NumberExpression implements NumberResult {

    private NumberResult innerNumberResult;

    protected NumberExpression() {
    }

    public NumberExpression(Number value) {
        innerNumberResult = new DBNumber(value);
    }

    public NumberExpression(NumberResult value) {
        innerNumberResult = value;
    }

    @Override
    public String toSQLString(DBDatabase db) {
        return getInputNumber().toSQLString(db);
    }

    protected NumberResult getInputNumber() {
        return innerNumberResult;
    }

    @Override
    public NumberExpression copy() {
        return new NumberExpression(getInputNumber());
    }

    public NumberExpression bracket() {
        return new NumberExpression(
                new DBUnaryFunction(this) {
                    @Override
                    String getFunctionName(DBDatabase db) {
                        return "";
                    }
                }
        );
    }

    public NumberExpression exp() {
        return new NumberExpression(new DBUnaryFunction(this) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "exp";
            }
        });
    }

    public NumberExpression cos() {
        return new NumberExpression(new DBUnaryFunction(this) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "cos";
            }
        });
    }

    public NumberExpression cosh() {
        return new NumberExpression(new DBUnaryFunction(this) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "cosh";
            }
        });
    }

    public NumberExpression sin() {
        return new NumberExpression(new DBUnaryFunction(this) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "sin";
            }
        });
    }

    public NumberExpression sinh() {
        return new NumberExpression(new DBUnaryFunction(this) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "sinh";
            }
        });
    }

    public NumberExpression tan() {
        return new NumberExpression(new DBUnaryFunction(this) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "tan";
            }
        });
    }

    public NumberExpression tanh() {
        return new NumberExpression(new DBUnaryFunction(this) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "tanh";
            }
        });
    }

    public NumberExpression abs() {
        return new NumberExpression(new DBUnaryFunction(this) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "abs";
            }
        });
    }

    public NumberExpression arccos() {
        return new NumberExpression(new DBUnaryFunction(this) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "acos";
            }
        });
    }

    public NumberExpression arcsin() {
        return new NumberExpression(new DBUnaryFunction(this) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "asin";
            }
        });
    }

    public NumberExpression arctan() {
        return new NumberExpression(new DBUnaryFunction(this) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "atan";
            }
        });
    }

    public NumberExpression arctan2(NumberExpression n) {
        return new NumberExpression(new DBBinaryFunction(this, n) {
            @Override
            String getFunctionName(DBDatabase db) {
                return "atn2";
            }
        });
    }

    public NumberExpression cotangent() {
        return new NumberExpression(new DBUnaryFunction(this) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "cot";
            }
        });
    }

    public NumberExpression degrees() {
        return new NumberExpression(new DBUnaryFunction(this) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "degrees";
            }
        });
    }

    public NumberExpression radians() {
        return new NumberExpression(new DBUnaryFunction(this) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "radians";
            }
        });
    }

    public NumberExpression log() {
        return new NumberExpression(new DBUnaryFunction(this) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "log";
            }
        });
    }

    public NumberExpression logBase10() {
        return new NumberExpression(new DBUnaryFunction(this) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "log10";
            }
        });
    }

    public NumberExpression power(NumberExpression n) {
        return new NumberExpression(new DBBinaryFunction(this, n) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "power";
            }
        });
    }

    public NumberExpression random() {
        return new NumberExpression(new DBUnaryFunction(this) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "rand";
            }
        });
    }

    public NumberExpression sign() {
        return new NumberExpression(new DBUnaryFunction(this) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "sign";
            }
        });
    }

    public NumberExpression squareRoot() {
        return new NumberExpression(new DBUnaryFunction(this) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "sqrt";
            }
        });
    }

    public NumberExpression standardDev() {
        return new NumberExpression(new DBUnaryFunction(this) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "stddev";
            }
        });
    }

    public NumberExpression variance() {
        return new NumberExpression(new DBUnaryFunction(this) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "var";
            }
        });
    }

    /**
     * Implements support for CEIL()
     *
     * <p>
     * Note: (new DBNumber(-1.5)).ceil() == -1
     *
     * @return the value of the equation rounded up to the nearest integer.
     */
    public NumberExpression roundUp() {
        return new NumberExpression(new DBUnaryFunction(this) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "ceil";
            }
        });
    }

    /**
     * Implements support for ROUND()
     *
     * @return the equation rounded to the nearest integer.
     */
    public NumberExpression round() {
        return new NumberExpression(new DBUnaryFunction(this) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "round";
            }
        });
    }

    /**
     * Implements support for FLOOR()
     *
     * <p>
     * note that this is not the same as {@code trunc()} as
     * {@code roundDown(-1.5) == -2} and {@code trunc(-1.5) == -1}
     *
     * @return the value of the equation rounded down to the nearest integer.
     */
    public NumberExpression roundDown() {
        return new NumberExpression(new DBUnaryFunction(this) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "floor";
            }
        });
    }

    /**
     * Implements support for TRUNC()
     *
     * <p>
     * note that this is not the same as roundDown() as
     * {@code roundDown(-1.5) == -2} and {@code trunc(-1.5) == -1}
     *
     * @return the value of the equation with the decimal part removed.
     */
    public NumberExpression trunc() {
        return new NumberExpression(new DBUnaryFunction(this) {

            @Override
            String getFunctionName(DBDatabase db) {
                return "trunc";
            }
        });
    }

    public NumberExpression minus(NumberExpression equation) {
        return new NumberExpression(new DBBinaryArithmetic(this, equation) {

            @Override
            protected String getEquationOperator(DBDatabase db) {
                return " - ";
            }
        });
    }

    public NumberExpression minus(Number num) {
        final NumberExpression minusThisExpression = new NumberExpression(num);
        final DBBinaryArithmetic minusExpression = new DBBinaryArithmetic(this, minusThisExpression) {

            @Override
            protected String getEquationOperator(DBDatabase db) {
                return " - ";
            }
        };
        return new NumberExpression(minusExpression);
    }

    public NumberExpression plus(NumberResult number) {
        return new NumberExpression(new DBBinaryArithmetic(this, new NumberExpression(number)) {

            @Override
            protected String getEquationOperator(DBDatabase db) {
                return " + ";
            }
        });
    }

    public NumberExpression plus(Number num) {
        return new NumberExpression(new DBBinaryArithmetic(this, new NumberExpression(num)) {

            @Override
            protected String getEquationOperator(DBDatabase db) {
                return " + ";
            }
        });
    }

    public NumberExpression times(NumberResult number) {
        return new NumberExpression(new DBBinaryArithmetic(this, new NumberExpression(number)) {

            @Override
            protected String getEquationOperator(DBDatabase db) {
                return " * ";
            }
        });
    }

    public NumberExpression times(Number num) {
        return new NumberExpression(new DBBinaryArithmetic(this, new NumberExpression(num)) {

            @Override
            protected String getEquationOperator(DBDatabase db) {
                return " * ";
            }
        });
    }

    public NumberExpression dividedBy(NumberResult number) {
        return new NumberExpression(new DBBinaryArithmetic(this, new NumberExpression(number)) {

            @Override
            protected String getEquationOperator(DBDatabase db) {
                return " / ";
            }
        });
    }

    public NumberExpression dividedBy(Number num) {
        return new NumberExpression(new DBBinaryArithmetic(this, new NumberExpression(num)) {

            @Override
            protected String getEquationOperator(DBDatabase db) {
                return " / ";
            }
        });
    }

    public NumberExpression mod(NumberResult number) {
        return new NumberExpression(new DBBinaryArithmetic(this, new NumberExpression(number)) {

            @Override
            protected String getEquationOperator(DBDatabase db) {
                return " % ";
            }
        });
    }

    public NumberExpression mod(Number num) {
        return new NumberExpression(new DBBinaryArithmetic(this, new DBNumber(num)) {
            @Override
            protected String getEquationOperator(DBDatabase db) {
                return "%";
            }
        });
    }

    public static abstract class DBBinaryArithmetic implements NumberResult {

        public NumberResult first;
        public NumberResult second;

        public DBBinaryArithmetic() {
            this.first = null;
            this.second = null;
        }

        public DBBinaryArithmetic(NumberResult first, NumberResult second) {
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
            } catch (InstantiationException | IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
            newInstance.first = first.copy();
            newInstance.second = second.copy();
            return newInstance;
        }

        protected abstract String getEquationOperator(DBDatabase db);
    }

    private static abstract class DBNonaryFunction implements NumberResult {

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
        public DBNonaryFunction copy() {
            DBNonaryFunction newInstance;
            try {
                newInstance = getClass().newInstance();
            } catch (InstantiationException | IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
            return newInstance;
        }
    }

    private static abstract class DBUnaryFunction implements NumberResult {

        protected NumberExpression only;

        public DBUnaryFunction() {
            this.only = null;
        }

        public DBUnaryFunction(NumberExpression only) {
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
        public DBUnaryFunction copy() {
            DBUnaryFunction newInstance;
            try {
                newInstance = getClass().newInstance();
            } catch (InstantiationException | IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
            newInstance.only = only.copy();
            return newInstance;
        }
    }

    private static abstract class DBBinaryFunction implements NumberResult {

        private DBExpression first;
        private DBExpression second;

        public DBBinaryFunction(NumberExpression first) {
            this.first = first;
            this.second = null;
        }

        public DBBinaryFunction(NumberExpression first, NumberExpression second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public String toSQLString(DBDatabase db) {
            return this.beforeValue(db) + first.toSQLString(db) + this.getSeparator(db) + (second == null ? "" : second.toSQLString(db)) + this.afterValue(db);
        }

        @Override
        public DBBinaryFunction copy() {
            DBBinaryFunction newInstance;
            try {
                newInstance = getClass().newInstance();
            } catch (InstantiationException | IllegalAccessException ex) {
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

    private static abstract class DBTrinaryFunction implements NumberResult {

        private DBExpression first;
        private DBExpression second;
        private DBExpression third;

        public DBTrinaryFunction(DBExpression first) {
            this.first = first;
            this.second = null;
            this.third = null;
        }

        public DBTrinaryFunction(DBExpression first, DBExpression second) {
            this.first = first;
            this.second = second;
        }

        public DBTrinaryFunction(DBExpression first, DBExpression second, DBExpression third) {
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
        public DBTrinaryFunction copy() {
            DBTrinaryFunction newInstance;
            try {
                newInstance = getClass().newInstance();
            } catch (InstantiationException | IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
            newInstance.first = first == null ? null : first.copy();
            newInstance.second = second == null ? null : second.copy();
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
