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

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;

public class StringExpression implements StringResult {

    private StringResult string1;

    protected StringExpression() {
    }

    public StringExpression(StringResult stringVariable) {
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
        return getStringInput().toSQLString(db);
    }

    @Override
    public StringExpression copy() {
        return new StringExpression(this);
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
     * @param string
     * @return a DBExpression instance that is appropriate to the subclass and
     * the value supplied.
     */
    public static StringExpression value(String string) {
        return new StringExpression(string);
    }

    public StringExpression append(StringResult string2) {
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

    public StringExpression replace(StringResult findString, String replaceWith) {
        return new StringExpression(
                new DBTrinaryStringFunction(this, findString, new StringExpression(replaceWith)) {
            @Override
            String getFunctionName(DBDatabase db) {
                return db.getDefinition().getReplaceFunctionName();
            }
        });
    }

    public StringExpression replace(String findString, StringResult replaceWith) {
        return new StringExpression(
                new DBTrinaryStringFunction(this, new StringExpression(findString), replaceWith) {
            @Override
            String getFunctionName(DBDatabase db) {
                return db.getDefinition().getReplaceFunctionName();
            }
        });
    }

    public StringExpression replace(StringResult findString, StringResult replaceWith) {
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

    public StringExpression uppercase() {
        return new StringExpression(
                new DBUnaryStringFunction(this) {
            @Override
            String getFunctionName(DBDatabase db) {
                return db.getDefinition().getUppercaseFunctionName();
            }
        });
    }

    /*endIndex0Based*/
    public StringExpression substring(Number startingIndex0Based) {
        return new Substring(this, startingIndex0Based);
    }

    public StringExpression substring(NumberExpression startingIndex0Based) {
        return new Substring(this, startingIndex0Based);
    }

    public StringExpression substring(Number startingIndex0Based, Number endIndex0Based) {
        return new Substring(this, startingIndex0Based, endIndex0Based);
    }

    public StringExpression substring(NumberExpression startingIndex0Based, Number endIndex0Based) {
        return new Substring(this, startingIndex0Based, new NumberExpression(endIndex0Based));
    }

    public StringExpression substring(Number startingIndex0Based, NumberExpression endIndex0Based) {
        return new Substring(this, new NumberExpression(startingIndex0Based), endIndex0Based);
    }

    public StringExpression substring(NumberExpression startingIndex0Based, NumberExpression endIndex0Based) {
        return new Substring(this, startingIndex0Based, endIndex0Based);
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

    /**
     * @return the string1
     */
    protected StringResult getStringInput() {
        return string1;
    }

    public NumberExpression locationOf(String searchString) {
        return new NumberExpression(new BinaryComplicatedNumberFunction(this, value(searchString)) {
            @Override
            public String toSQLString(DBDatabase db) {
                return db.getDefinition().getPositionFunction(this.first.toSQLString(db), this.second.toSQLString(db));
            }
        });
    }

    private static abstract class DBBinaryStringArithmetic implements StringResult {

        private StringResult first;
        private StringResult second;

        public DBBinaryStringArithmetic(StringResult first, StringResult second) {
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
            } catch (InstantiationException | IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
            newInstance.first = first.copy();
            newInstance.second = second.copy();
            return newInstance;
        }

        protected abstract String getEquationOperator(DBDatabase db);
    }

    private static abstract class DBNonaryStringFunction implements StringResult {

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
            } catch (InstantiationException | IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
            return newInstance;
        }
    }

    private static abstract class DBUnaryStringFunction implements StringResult {

        protected StringExpression only;

        public DBUnaryStringFunction() {
            this.only = null;
        }

        public DBUnaryStringFunction(StringExpression only) {
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
            } catch (InstantiationException | IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
            newInstance.only = only.copy();
            return newInstance;
        }
    }

    private static abstract class DBUnaryNumberFunction implements NumberResult {

        protected StringExpression only;

        public DBUnaryNumberFunction() {
            this.only = null;
        }

        public DBUnaryNumberFunction(StringExpression only) {
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
            } catch (InstantiationException | IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
            newInstance.only = (only == null ? null : only.copy());
            return newInstance;
        }
    }

    private static abstract class DBTrinaryStringFunction implements StringResult {

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

    private static abstract class BinaryComplicatedNumberFunction implements NumberResult {

        protected StringExpression first;
        protected StringExpression second;

        public BinaryComplicatedNumberFunction() {
            this.first = null;
        }

        public BinaryComplicatedNumberFunction(StringExpression first, StringExpression second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public abstract String toSQLString(DBDatabase db);

        @Override
        public StringExpression.BinaryComplicatedNumberFunction copy() {
            StringExpression.BinaryComplicatedNumberFunction newInstance;
            try {
                newInstance = getClass().newInstance();
            } catch (InstantiationException | IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
            newInstance.first = first.copy();
            newInstance.second = second.copy();
            return newInstance;
        }
    }

    private class Substring extends StringExpression implements StringResult {

        private final NumberResult startingPosition;
        private final NumberResult length;

        public Substring(StringResult stringInput, Number startingIndex0Based) {
            super(stringInput);
            this.startingPosition = new DBNumber(startingIndex0Based);
            this.length = null;
        }

        public Substring(StringResult stringInput, NumberResult startingIndex0Based) {
            super(stringInput);
            this.startingPosition = startingIndex0Based.copy();
            this.length = null;
        }

        public Substring(StringResult stringInput, Number startingIndex0Based, Number endIndex0Based) {
            super(stringInput);
            this.startingPosition = new DBNumber(startingIndex0Based);
            this.length = new DBNumber(endIndex0Based);
        }

        public Substring(StringResult stringInput, NumberResult startingIndex0Based, NumberResult endIndex0Based) {
            super(stringInput);
            this.startingPosition = startingIndex0Based.copy();
            this.length = endIndex0Based.copy();
        }

        @Override
        public Substring copy() {
            return (Substring) super.copy();
        }

        @Override
        public String toSQLString(DBDatabase db) {
            if (getStringInput() == null) {
                return "";
            } else {
                return doSubstringTransform(db, getStringInput(), startingPosition, length);
            }
        }

        public String doSubstringTransform(DBDatabase db, StringResult enclosedValue, NumberResult startingPosition, NumberResult substringLength) {
            return " SUBSTRING("
                    + enclosedValue.toSQLString(db)
                    + " FROM "
                    + (startingPosition.toSQLString(db) + " + 1")
                    + (substringLength != null ? " for " + (substringLength.toSQLString(db) + " - " + startingPosition.toSQLString(db)) : "")
                    + ") ";
        }
    }
}
