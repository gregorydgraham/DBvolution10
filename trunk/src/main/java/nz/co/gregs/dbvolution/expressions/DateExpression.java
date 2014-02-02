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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
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

    /**
     * Create An Appropriate Expression Object For This Object
     *
     * <p>The expression framework requires a *Expression to work with. The
     * easiest way to get that is the {@code DBRow.column()} method.
     *
     * <p>However if you wish your expression to start with a literal value it
     * is a little trickier.
     *
     * <p>This method provides the easy route to a *Expression from a literal
     * value. Just call, for instance,
     * {@code StringExpression.value("STARTING STRING")} to get a
     * StringExpression and start the expression chain.
     *
     * <ul>
     * <li>Only object classes that are appropriate need to be handle by the
     * DBExpression subclass.<li>
     * <li>The implementation should be {@code static}</li>
     *
     * @param date
     * @return a DBExpression instance that is appropriate to the subclass and
     * the value supplied.
     */
    public static DateExpression value(Date date) {
        return new DateExpression(date);
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
    
    public NumberExpression year(){
        return new NumberExpression(
                new UnaryComplicatedNumberFunction(this) {
            @Override
            public String toSQLString(DBDatabase db) {
                return db.getDefinition().getYearFunction(this.only.toSQLString(db));
            }
        });
    }

    public NumberExpression month(){
        return new NumberExpression(
                new UnaryComplicatedNumberFunction(this) {
            @Override
            public String toSQLString(DBDatabase db) {
                return db.getDefinition().getMonthFunction(this.only.toSQLString(db));
            }
        });
    }

    public NumberExpression day(){
        return new NumberExpression(
                new UnaryComplicatedNumberFunction(this) {
            @Override
            public String toSQLString(DBDatabase db) {
                return db.getDefinition().getDayFunction(this.only.toSQLString(db));
            }
        });
    }

    public NumberExpression hour(){
        return new NumberExpression(
                new UnaryComplicatedNumberFunction(this) {
            @Override
            public String toSQLString(DBDatabase db) {
                return db.getDefinition().getHourFunction(this.only.toSQLString(db));
            }
        });
    }

    public NumberExpression minute(){
        return new NumberExpression(
                new UnaryComplicatedNumberFunction(this) {
            @Override
            public String toSQLString(DBDatabase db) {
                return db.getDefinition().getMinuteFunction(this.only.toSQLString(db));
            }
        });
    }

    public NumberExpression second(){
        return new NumberExpression(
                new UnaryComplicatedNumberFunction(this) {
            @Override
            public String toSQLString(DBDatabase db) {
                return db.getDefinition().getSecondFunction(this.only.toSQLString(db));
            }
        });
    }
    
        public BooleanExpression is(Date date) {
        return is(value(date));
    }

    public BooleanExpression is(DateResult dateExpression) {
        return new BooleanExpression(new DBBinaryBooleanArithmetic(this, dateExpression) {
            @Override
            protected String getEquationOperator(DBDatabase db) {
                return " = ";
            }
        });
    }

    public BooleanExpression isLessThan(Date date) {
        return isLessThan(value(date));
    }

    public BooleanExpression isLessThan(DateResult dateExpression) {
        return new BooleanExpression(new DateExpression.DBBinaryBooleanArithmetic(this, dateExpression) {
            @Override
            protected String getEquationOperator(DBDatabase db) {
                return " < ";
            }
        });
    }

    public BooleanExpression isLessThanOrEqual(Date date) {
        return isLessThanOrEqual(value(date));
    }

    public BooleanExpression isLessThanOrEqual(DateResult dateExpression) {
        return new BooleanExpression(new DBBinaryBooleanArithmetic(this, dateExpression) {
            @Override
            protected String getEquationOperator(DBDatabase db) {
                return " <= ";
            }
        });
    }

    public BooleanExpression isGreaterThan(Date date) {
        return isGreaterThan(value(date));
    }

    public BooleanExpression isGreaterThan(DateResult dateExpression) {
        return new BooleanExpression(new DBBinaryBooleanArithmetic(this, dateExpression) {
            @Override
            protected String getEquationOperator(DBDatabase db) {
                return " > ";
            }
        });
    }

    public BooleanExpression isGreaterThanOrEqual(Date date) {
        return isGreaterThanOrEqual(value(date));
    }

    public BooleanExpression isGreaterThanOrEqual(DateResult dateExpression) {
        return new BooleanExpression(new DBBinaryBooleanArithmetic(this, dateExpression) {
            @Override
            protected String getEquationOperator(DBDatabase db) {
                return " >= ";
            }
        });
    }

    public BooleanExpression isIn(Date... possibleValues) {
        List<DateExpression> possVals = new ArrayList<DateExpression>();
        for (Date num : possibleValues) {
            possVals.add(value(num));
        }
        return isIn(possVals.toArray(new DateExpression[]{}));
    }

    public BooleanExpression isIn(Collection<? extends Date> possibleValues) {
        List<DateExpression> possVals = new ArrayList<DateExpression>();
        for (Date num : possibleValues) {
            possVals.add(value(num));
        }
        return isIn(possVals.toArray(new DateExpression[]{}));
    }

    public BooleanExpression isIn(DateResult... possibleValues) {
        return new BooleanExpression(new DBNnaryBooleanFunction(this, possibleValues) {
            @Override
            protected String getFunctionName(DBDatabase db) {
                return " IN ";
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
            } catch (InstantiationException ex) {
                throw new RuntimeException(ex);
            } catch (IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
            return newInstance;
        }
    }

    private static abstract class UnaryComplicatedNumberFunction implements NumberResult {

        protected DateExpression only;

        public UnaryComplicatedNumberFunction() {
            this.only = null;
        }

        public UnaryComplicatedNumberFunction(DateExpression only) {
            this.only = only;
        }

        @Override
        public abstract String toSQLString(DBDatabase db);

        @Override
        public DateExpression.UnaryComplicatedNumberFunction copy() {
            DateExpression.UnaryComplicatedNumberFunction newInstance;
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

    private static abstract class DBBinaryBooleanArithmetic implements BooleanResult {

        private DateExpression first;
        private DateResult second;

        public DBBinaryBooleanArithmetic(DateExpression first, DateResult second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public String toSQLString(DBDatabase db) {
            return first.toSQLString(db) + this.getEquationOperator(db) + second.toSQLString(db);
        }

        @Override
        public DBBinaryBooleanArithmetic copy() {
            DBBinaryBooleanArithmetic newInstance;
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

    private static abstract class DBNnaryBooleanFunction implements BooleanResult {

        protected DateExpression column;
        protected DateResult[] values;

        public DBNnaryBooleanFunction() {
            this.values = null;
        }

        public DBNnaryBooleanFunction(DateExpression leftHandSide, DateResult[] rightHandSide) {
            this.values = new DateResult[rightHandSide.length];
            this.column = leftHandSide;
            System.arraycopy(rightHandSide, 0, this.values, 0, rightHandSide.length);
        }

        abstract String getFunctionName(DBDatabase db);

        protected String beforeValue(DBDatabase db) {
            return "( ";
        }

        protected String afterValue(DBDatabase db) {
            return ") ";
        }

        @Override
        public String toSQLString(DBDatabase db) {
            StringBuilder builder = new StringBuilder();
            builder
                    .append(column.toSQLString(db))
                    .append(this.getFunctionName(db))
                    .append(this.beforeValue(db));
            String separator = "";
            for (DateResult val : values) {
                if (val != null) {
                    builder.append(separator).append(val.toSQLString(db));
                }
                separator = ", ";
            }
            builder.append(this.afterValue(db));
            return builder.toString();
        }

        @Override
        public DBNnaryBooleanFunction copy() {
            DBNnaryBooleanFunction newInstance;
            try {
                newInstance = getClass().newInstance();
            } catch (InstantiationException ex) {
                throw new RuntimeException(ex);
            } catch (IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
            newInstance.column = this.column.copy();
            newInstance.values = this.values;
            return newInstance;
        }
    }
}
