/*
 * Copyright 2014 Gregory Graham.
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
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
     * <p>
     * The expression framework requires a *Expression to work with. The easiest
     * way to get that is the {@code DBRow.column()} method.
     *
     * <p>
     * However if you wish your expression to start with a literal value it is a
     * little trickier.
     *
     * <p>
     * This method provides the easy route to a *Expression from a literal
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

    public StringExpression ifDBNull(String alternative) {
        return new StringExpression(
                new StringExpression.DBBinaryStringFunction(this, new StringExpression(alternative)) {

                    @Override
                    String getFunctionName(DBDatabase db) {
                        return db.getDefinition().getIfNullFunctionName();
                    }
                });
    }

    public StringExpression ifDBNull(StringResult alternative) {
        return new StringExpression(
                new StringExpression.DBBinaryStringFunction(this, new StringExpression(alternative)) {

                    @Override
                    String getFunctionName(DBDatabase db) {
                        return db.getDefinition().getIfNullFunctionName();
                    }
                });
    }

    public BooleanExpression isLike(String sqlPattern) {
        return isLike(value(sqlPattern));
    }

    public BooleanExpression isLike(StringResult sqlPattern) {
        return new BooleanExpression(new DBBinaryBooleanArithmetic(this, sqlPattern) {
            @Override
            protected String getEquationOperator(DBDatabase db) {
                return " LIKE ";
            }
        });
    }

    public BooleanExpression is(String equivalentString) {
        return is(value(equivalentString));
    }

    public BooleanExpression is(StringResult equivalentString) {
        return new BooleanExpression(new DBBinaryBooleanArithmetic(this, equivalentString) {
            @Override
            protected String getEquationOperator(DBDatabase db) {
                return " = ";
            }
        });
    }

	/**
     * Performs searches based on a range.
     *
     * if both ends of the range are specified the lower-bound will be included
     * in the search and the upper-bound excluded. I.e permittedRange(1,3) will
     * return 1 and 2.
     *
     * <p>
     * if the upper-bound is null the range will be open ended and inclusive.
     * <br>
     * I.e permittedRange(1,null) will return 1,2,3,4,5, etc.
     *
     * <p>
     * if the lower-bound is null the range will be open ended and exclusive.
     * <br>
     * I.e permittedRange(null, 5) will return 4,3,2,1, etc.
     *
     * @param lowerBound
     * @param upperBound
	 * @return a boolean expression representing the required comparison
     */
	public BooleanExpression isBetween(StringResult lowerBound, StringResult upperBound) {
        return BooleanExpression.allOf(
				this.isGreaterThan(lowerBound),
				this.isLessThanOrEqual(upperBound)
		);
    }

	/**
     * Performs searches based on a range.
     *
     * if both ends of the range are specified the lower-bound will be included
     * in the search and the upper-bound excluded. I.e permittedRange(1,3) will
     * return 1 and 2.
     *
     * <p>
     * if the upper-bound is null the range will be open ended and inclusive.
     * <br>
     * I.e permittedRange(1,null) will return 1,2,3,4,5, etc.
     *
     * <p>
     * if the lower-bound is null the range will be open ended and exclusive.
     * <br>
     * I.e permittedRange(null, 5) will return 4,3,2,1, etc.
     *
     * @param lowerBound
     * @param upperBound
	 * @return a boolean expression representing the required comparison
     */
	public BooleanExpression isBetween(String lowerBound, StringResult upperBound) {
        return BooleanExpression.allOf(
				this.isGreaterThan(lowerBound),
				this.isLessThanOrEqual(upperBound)
		);
    }

	/**
     * Performs searches based on a range.
     *
     * if both ends of the range are specified the lower-bound will be included
     * in the search and the upper-bound excluded. I.e permittedRange(1,3) will
     * return 1 and 2.
     *
     * <p>
     * if the upper-bound is null the range will be open ended and inclusive.
     * <br>
     * I.e permittedRange(1,null) will return 1,2,3,4,5, etc.
     *
     * <p>
     * if the lower-bound is null the range will be open ended and exclusive.
     * <br>
     * I.e permittedRange(null, 5) will return 4,3,2,1, etc.
     *
     * @param lowerBound
     * @param upperBound
	 * @return a boolean expression representing the required comparison
     */
	public BooleanExpression isBetween(StringResult lowerBound, String upperBound) {
        return BooleanExpression.allOf(
				this.isGreaterThan(lowerBound),
				this.isLessThanOrEqual(upperBound)
		);
    }

	/**
     * Performs searches based on a range.
     *
     * if both ends of the range are specified the lower-bound will be included
     * in the search and the upper-bound excluded. I.e permittedRange(1,3) will
     * return 1 and 2.
     *
     * <p>
     * if the upper-bound is null the range will be open ended and inclusive.
     * <br>
     * I.e permittedRange(1,null) will return 1,2,3,4,5, etc.
     *
     * <p>
     * if the lower-bound is null the range will be open ended and exclusive.
     * <br>
     * I.e permittedRange(null, 5) will return 4,3,2,1, etc.
     *
     * @param lowerBound
     * @param upperBound
	 * @return a boolean expression representing the required comparison
     */
	public BooleanExpression isBetween(String lowerBound, String upperBound) {
        return BooleanExpression.allOf(
				this.isGreaterThan(lowerBound),
				this.isLessThanOrEqual(upperBound)
		);
    }

    /**
     * Performs searches based on a range.
     *
     * if both ends of the range are specified both the lower- and upper-bound
     * will be included in the search. I.e permittedRangeInclusive(1,3) will
     * return 1, 2, and 3.
     *
     * <p>
     * if the upper-bound is null the range will be open ended and inclusive.
     * <br>
     * I.e permittedRangeInclusive(1,null) will return 1,2,3,4,5, etc.
     *
     * <p>
     * if the lower-bound is null the range will be open ended and inclusive.
     * <br>
     * I.e permittedRangeInclusive(null, 5) will return 5,4,3,2,1, etc.
     *
     * @param lowerBound
     * @param upperBound
	 * @return a boolean expression representing the required comparison
     */
	public BooleanExpression isBetweenInclusive(StringResult lowerBound, StringResult upperBound) {
        return BooleanExpression.allOf(
				this.isGreaterThanOrEqual(lowerBound),
				this.isLessThanOrEqual(upperBound)
		);
    }

    /**
     * Performs searches based on a range.
     *
     * if both ends of the range are specified both the lower- and upper-bound
     * will be included in the search. I.e permittedRangeInclusive(1,3) will
     * return 1, 2, and 3.
     *
     * <p>
     * if the upper-bound is null the range will be open ended and inclusive.
     * <br>
     * I.e permittedRangeInclusive(1,null) will return 1,2,3,4,5, etc.
     *
     * <p>
     * if the lower-bound is null the range will be open ended and inclusive.
     * <br>
     * I.e permittedRangeInclusive(null, 5) will return 5,4,3,2,1, etc.
     *
     * @param lowerBound
     * @param upperBound
	 * @return a boolean expression representing the required comparison
     */
	public BooleanExpression isBetweenInclusive(String lowerBound, StringResult upperBound) {
        return BooleanExpression.allOf(
				this.isGreaterThanOrEqual(lowerBound),
				this.isLessThanOrEqual(upperBound)
		);
    }

    /**
     * Performs searches based on a range.
     *
     * if both ends of the range are specified both the lower- and upper-bound
     * will be included in the search. I.e permittedRangeInclusive(1,3) will
     * return 1, 2, and 3.
     *
     * <p>
     * if the upper-bound is null the range will be open ended and inclusive.
     * <br>
     * I.e permittedRangeInclusive(1,null) will return 1,2,3,4,5, etc.
     *
     * <p>
     * if the lower-bound is null the range will be open ended and inclusive.
     * <br>
     * I.e permittedRangeInclusive(null, 5) will return 5,4,3,2,1, etc.
     *
     * @param lowerBound
     * @param upperBound
	 * @return a boolean expression representing the required comparison
     */
	public BooleanExpression isBetweenInclusive(StringResult lowerBound, String upperBound) {
        return BooleanExpression.allOf(
				this.isGreaterThanOrEqual(lowerBound),
				this.isLessThanOrEqual(upperBound)
		);
    }

    /**
     * Performs searches based on a range.
     *
     * if both ends of the range are specified both the lower- and upper-bound
     * will be included in the search. I.e permittedRangeInclusive(1,3) will
     * return 1, 2, and 3.
     *
     * <p>
     * if the upper-bound is null the range will be open ended and inclusive.
     * <br>
     * I.e permittedRangeInclusive(1,null) will return 1,2,3,4,5, etc.
     *
     * <p>
     * if the lower-bound is null the range will be open ended and inclusive.
     * <br>
     * I.e permittedRangeInclusive(null, 5) will return 5,4,3,2,1, etc.
     *
     * @param lowerBound
     * @param upperBound
	 * @return a boolean expression representing the required comparison
     */
	public BooleanExpression isBetweenInclusive(String lowerBound, String upperBound) {
        return BooleanExpression.allOf(
				this.isGreaterThanOrEqual(lowerBound),
				this.isLessThanOrEqual(upperBound)
		);
    }

	/**
     * Performs searches based on a range.
     *
     * if both ends of the range are specified both the lower- and upper-bound
     * will be excluded in the search. I.e permittedRangeExclusive(1,3) will
     * return 2.
     *
     * <p>
     * if the upper-bound is null the range will be open ended upwards and exclusive.
     * <br>
     * I.e permittedRangeExclusive(1,null) will return 2,3,4,5, etc.
     *
     * <p>
     * if the lower-bound is null the range will be open ended downwards and exclusive.
     * <br>
     * I.e permittedRangeExclusive(null, 5) will return 4,3,2,1, etc.
     *
     * @param lowerBound
     * @param upperBound
	 * @return a boolean expression representing the required comparison
     */
	public BooleanExpression isBetweenExclusive(StringResult lowerBound, StringResult upperBound) {
        return BooleanExpression.allOf(
				this.isGreaterThan(lowerBound),
				this.isLessThan(upperBound)
		);
    }

	/**
     * Performs searches based on a range.
     *
     * if both ends of the range are specified both the lower- and upper-bound
     * will be excluded in the search. I.e permittedRangeExclusive(1,3) will
     * return 2.
     *
     * <p>
     * if the upper-bound is null the range will be open ended upwards and exclusive.
     * <br>
     * I.e permittedRangeExclusive(1,null) will return 2,3,4,5, etc.
     *
     * <p>
     * if the lower-bound is null the range will be open ended downwards and exclusive.
     * <br>
     * I.e permittedRangeExclusive(null, 5) will return 4,3,2,1, etc.
     *
     * @param lowerBound
     * @param upperBound
	 * @return a boolean expression representing the required comparison
     */
	public BooleanExpression isBetweenExclusive(String lowerBound, StringResult upperBound) {
        return BooleanExpression.allOf(
				this.isGreaterThan(lowerBound),
				this.isLessThan(upperBound)
		);
    }

	/**
     * Performs searches based on a range.
     *
     * if both ends of the range are specified both the lower- and upper-bound
     * will be excluded in the search. I.e permittedRangeExclusive(1,3) will
     * return 2.
     *
     * <p>
     * if the upper-bound is null the range will be open ended upwards and exclusive.
     * <br>
     * I.e permittedRangeExclusive(1,null) will return 2,3,4,5, etc.
     *
     * <p>
     * if the lower-bound is null the range will be open ended downwards and exclusive.
     * <br>
     * I.e permittedRangeExclusive(null, 5) will return 4,3,2,1, etc.
     *
     * @param lowerBound
     * @param upperBound
	 * @return a boolean expression representing the required comparison
     */
	public BooleanExpression isBetweenExclusive(StringResult lowerBound, String upperBound) {
        return BooleanExpression.allOf(
				this.isGreaterThan(lowerBound),
				this.isLessThan(upperBound)
		);
    }

	/**
     * Performs searches based on a range.
     *
     * if both ends of the range are specified both the lower- and upper-bound
     * will be excluded in the search. I.e permittedRangeExclusive(1,3) will
     * return 2.
     *
     * <p>
     * if the upper-bound is null the range will be open ended upwards and exclusive.
     * <br>
     * I.e permittedRangeExclusive(1,null) will return 2,3,4,5, etc.
     *
     * <p>
     * if the lower-bound is null the range will be open ended downwards and exclusive.
     * <br>
     * I.e permittedRangeExclusive(null, 5) will return 4,3,2,1, etc.
     *
     * @param lowerBound
     * @param upperBound
	 * @return a boolean expression representing the required comparison
     */
	public BooleanExpression isBetweenExclusive(String lowerBound, String upperBound) {
        return BooleanExpression.allOf(
				this.isGreaterThan(lowerBound),
				this.isLessThan(upperBound)
		);
    }
	
    public BooleanExpression isLessThan(String equivalentString) {
        return isLessThan(value(equivalentString));
    }

    public BooleanExpression isLessThan(StringResult equivalentString) {
        return new BooleanExpression(new DBBinaryBooleanArithmetic(this, equivalentString) {
            @Override
            protected String getEquationOperator(DBDatabase db) {
                return " < ";
            }
        });
    }

    public BooleanExpression isLessThanOrEqual(String equivalentString) {
        return isLessThanOrEqual(value(equivalentString));
    }

    public BooleanExpression isLessThanOrEqual(StringResult equivalentString) {
        return new BooleanExpression(new DBBinaryBooleanArithmetic(this, equivalentString) {
            @Override
            protected String getEquationOperator(DBDatabase db) {
                return " <= ";
            }
        });
    }

    public BooleanExpression isGreaterThan(String equivalentString) {
        return isGreaterThan(value(equivalentString));
    }

    public BooleanExpression isGreaterThan(StringResult equivalentString) {
        return new BooleanExpression(new DBBinaryBooleanArithmetic(this, equivalentString) {
            @Override
            protected String getEquationOperator(DBDatabase db) {
                return " > ";
            }
        });
    }

    public BooleanExpression isGreaterThanOrEqual(String equivalentString) {
        return isGreaterThanOrEqual(value(equivalentString));
    }

    public BooleanExpression isGreaterThanOrEqual(StringResult equivalentString) {
        return new BooleanExpression(new DBBinaryBooleanArithmetic(this, equivalentString) {
            @Override
            protected String getEquationOperator(DBDatabase db) {
                return " >= ";
            }
        });
    }

    public BooleanExpression isIn(String... possibleValues) {
        List<StringExpression> possVals = new ArrayList<StringExpression>();
        for (String str : possibleValues) {
            possVals.add(StringExpression.value(str));
        }
        return isIn(possVals.toArray(new StringExpression[]{}));
    }

    public BooleanExpression isIn(Collection<String> possibleValues) {
        List<StringExpression> possVals = new ArrayList<StringExpression>();
        for (String str : possibleValues) {
            possVals.add(StringExpression.value(str));
        }
        return isIn(possVals.toArray(new StringExpression[]{}));
    }

    public BooleanExpression isIn(StringResult... possibleValues) {
        return new BooleanExpression(new DBNnaryBooleanFunction(this, possibleValues) {
            @Override
            protected String getFunctionName(DBDatabase db) {
                return " IN ";
            }
        });
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

    public StringExpression append(NumberResult number1) {
        return new StringExpression(new DBBinaryStringNumberArithmetic(this, number1) {
            @Override
            protected String getEquationOperator(DBDatabase db) {
                return db.getDefinition().getConcatOperator();
            }
        });
    }

    public StringExpression append(Number number1) {
        return new StringExpression(new DBBinaryStringNumberArithmetic(this, new NumberExpression(number1)) {
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

    public NumberExpression count(){        
        return new NumberExpression(new DBUnaryNumberFunction(this) {

            @Override
            String getFunctionName(DBDatabase db) {
                return db.getDefinition().getCountFunctionName();
            }

            @Override
            public boolean isAggregator() {
                return true;
            }
        });
    }

    public StringExpression max() {
        return new StringExpression(new DBUnaryStringFunction(this){
            @Override
            String getFunctionName(DBDatabase db) {
                return db.getDefinition().getMaxFunctionName();
            }

            @Override
            public boolean isAggregator() {
                return true;
            }
        });
    }

    public StringExpression min() {
        return new StringExpression(new DBUnaryStringFunction(this){
            @Override
            String getFunctionName(DBDatabase db) {
                return db.getDefinition().getMinFunctionName();
            }

            @Override
            public boolean isAggregator() {
                return true;
            }
        });
    }

    @Override
    public DBString getQueryableDatatypeForExpressionValue() {
        return new DBString();
    }

    @Override
    public Set<DBRow> getTablesInvolved() {
        HashSet<DBRow> hashSet = new HashSet<DBRow>();
        if (string1 != null) {
            hashSet.addAll(string1.getTablesInvolved());
        }
        return hashSet;
    }

    @Override
    public boolean isAggregator() {
        return string1 == null ? false : string1.isAggregator();
    }

    private static abstract class DBBinaryStringArithmetic implements StringResult {

        private StringResult first;
        private StringResult second;

        public DBBinaryStringArithmetic(StringResult first, StringResult second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public DBString getQueryableDatatypeForExpressionValue() {
            return new DBString();
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

        @Override
        public Set<DBRow> getTablesInvolved() {
            HashSet<DBRow> hashSet = new HashSet<DBRow>();
            if (first != null) {
                hashSet.addAll(first.getTablesInvolved());
            }
            if (second != null) {
                hashSet.addAll(second.getTablesInvolved());
            }
            return hashSet;
        }

        @Override
        public boolean isAggregator() {
            return this.first.isAggregator() || second.isAggregator();
        }

    }

    private static abstract class DBNonaryStringFunction implements StringResult {

        public DBNonaryStringFunction() {
        }

        @Override
        public DBString getQueryableDatatypeForExpressionValue() {
            return new DBString();
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

        @Override
        public Set<DBRow> getTablesInvolved() {
            HashSet<DBRow> hashSet = new HashSet<DBRow>();
            return hashSet;
        }

        @Override
        public boolean isAggregator() {
            return false;
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

        @Override
        public DBString getQueryableDatatypeForExpressionValue() {
            return new DBString();
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

        @Override
        public Set<DBRow> getTablesInvolved() {
            HashSet<DBRow> hashSet = new HashSet<DBRow>();
            if (only != null) {
                hashSet.addAll(only.getTablesInvolved());
            }
            return hashSet;
        }

        @Override
        public boolean isAggregator() {
            return only.isAggregator();
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

        @Override
        public DBNumber getQueryableDatatypeForExpressionValue() {
            return new DBNumber();
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
            newInstance.only = (only == null ? null : only.copy());
            return newInstance;
        }

        @Override
        public Set<DBRow> getTablesInvolved() {
            HashSet<DBRow> hashSet = new HashSet<DBRow>();
            if (only != null) {
                hashSet.addAll(only.getTablesInvolved());
            }
            return hashSet;
        }

        @Override
        public boolean isAggregator() {
            return only.isAggregator();
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
        public DBString getQueryableDatatypeForExpressionValue() {
            return new DBString();
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
            newInstance.first = first == null ? null : first.copy();
            newInstance.second = second == null ? null : second.copy();
            newInstance.third = third == null ? null : third.copy();
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

        @Override
        public Set<DBRow> getTablesInvolved() {
            HashSet<DBRow> hashSet = new HashSet<DBRow>();
            if (first != null) {
                hashSet.addAll(first.getTablesInvolved());
            }
            if (second != null) {
                hashSet.addAll(second.getTablesInvolved());
            }
            if (third != null) {
                hashSet.addAll(third.getTablesInvolved());
            }
            return hashSet;
        }

        @Override
        public boolean isAggregator() {
            return first.isAggregator() || second.isAggregator() || third.isAggregator();
        }
    }

    private static abstract class DBBinaryStringFunction implements StringResult {

        private StringResult first;
        private StringResult second;

        public DBBinaryStringFunction(StringResult first) {
            this.first = first;
            this.second = null;
        }

        public DBBinaryStringFunction(StringResult first, StringResult second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public DBString getQueryableDatatypeForExpressionValue() {
            return new DBString();
        }

        @Override
        public String toSQLString(DBDatabase db) {
            return this.beforeValue(db) + first.toSQLString(db)
                    + this.getSeparator(db) + (second == null ? "" : second.toSQLString(db))
                    + this.afterValue(db);
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

        @Override
        public Set<DBRow> getTablesInvolved() {
            HashSet<DBRow> hashSet = new HashSet<DBRow>();
            if (first != null) {
                hashSet.addAll(first.getTablesInvolved());
            }
            if (second != null) {
                hashSet.addAll(second.getTablesInvolved());
            }
            return hashSet;
        }

        @Override
        public boolean isAggregator() {
            return first.isAggregator() || second.isAggregator();
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
        public DBNumber getQueryableDatatypeForExpressionValue() {
            return new DBNumber();
        }

        @Override
        public abstract String toSQLString(DBDatabase db);

        @Override
        public StringExpression.BinaryComplicatedNumberFunction copy() {
            StringExpression.BinaryComplicatedNumberFunction newInstance;
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

        @Override
        public Set<DBRow> getTablesInvolved() {
            HashSet<DBRow> hashSet = new HashSet<DBRow>();
            if (first != null) {
                hashSet.addAll(first.getTablesInvolved());
            }
            if (second != null) {
                hashSet.addAll(second.getTablesInvolved());
            }
            return hashSet;
        }

        @Override
        public boolean isAggregator() {
            return first.isAggregator() || second.isAggregator();
        }
    }

    private class Substring extends StringExpression implements StringResult {

        private NumberResult startingPosition;
        private NumberResult length;

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
            return new Substring(getStringInput(), startingPosition, length);
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

        @Override
        public DBString getQueryableDatatypeForExpressionValue() {
            return new DBString();
        }
    }

    private static abstract class DBBinaryBooleanArithmetic implements BooleanResult {

        private StringResult first;
        private StringResult second;

        public DBBinaryBooleanArithmetic(StringResult first, StringResult second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public DBString getQueryableDatatypeForExpressionValue() {
            return new DBString();
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

        @Override
        public Set<DBRow> getTablesInvolved() {
            HashSet<DBRow> hashSet = new HashSet<DBRow>();
            if (first != null) {
                hashSet.addAll(first.getTablesInvolved());
            }
            if (second != null) {
                hashSet.addAll(second.getTablesInvolved());
            }
            return hashSet;
        }

        @Override
        public boolean isAggregator() {
            return first.isAggregator() || second.isAggregator();
        }
    }

    private static abstract class DBNnaryBooleanFunction implements BooleanResult {

        protected StringExpression column;
        protected StringResult[] values;

        public DBNnaryBooleanFunction() {
            this.values = null;
        }

        public DBNnaryBooleanFunction(StringExpression leftHandSide, StringResult[] rightHandSide) {
            this.values = new StringResult[rightHandSide.length];
            this.column = leftHandSide;
            System.arraycopy(rightHandSide, 0, this.values, 0, rightHandSide.length);
        }

        @Override
        public DBString getQueryableDatatypeForExpressionValue() {
            return new DBString();
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
            for (StringResult val : values) {
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

        @Override
        public Set<DBRow> getTablesInvolved() {
            HashSet<DBRow> hashSet = new HashSet<DBRow>();
            if (column != null) {
                hashSet.addAll(column.getTablesInvolved());
            }
            for (StringResult second : values) {
                if (second != null) {
                    hashSet.addAll(second.getTablesInvolved());
                }
            }
            return hashSet;
        }

        @Override
        public boolean isAggregator() {
            boolean result = column.isAggregator();
            for (StringResult numer : values) {
                result = result || numer.isAggregator();
            }
            return result;
        }
    }

    private static abstract class DBBinaryStringNumberArithmetic implements StringResult {

        private StringResult first;
        private NumberResult second;

        public DBBinaryStringNumberArithmetic(StringResult first, NumberResult second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public DBString getQueryableDatatypeForExpressionValue() {
            return new DBString();
        }

        @Override
        public String toSQLString(DBDatabase db) {
            return first.toSQLString(db) + this.getEquationOperator(db) + second.toSQLString(db);
        }

        @Override
        public DBBinaryStringNumberArithmetic copy() {
            DBBinaryStringNumberArithmetic newInstance;
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

        @Override
        public Set<DBRow> getTablesInvolved() {
            HashSet<DBRow> hashSet = new HashSet<DBRow>();
            if (first != null) {
                hashSet.addAll(first.getTablesInvolved());
            }
            if (second != null) {
                hashSet.addAll(second.getTablesInvolved());
            }
            return hashSet;
        }

        @Override
        public boolean isAggregator() {
            return first.isAggregator() || second.isAggregator();
        }

    }

}
