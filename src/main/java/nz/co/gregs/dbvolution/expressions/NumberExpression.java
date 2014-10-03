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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

public class NumberExpression implements NumberResult {

	private NumberResult innerNumberResult;
	private boolean nullProtectionRequired;

	protected NumberExpression() {
	}

	public NumberExpression(Number value) {
		innerNumberResult = new DBNumber(value);
		if (value == null || innerNumberResult.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	public NumberExpression(NumberResult value) {
		innerNumberResult = value;
		if (value == null || innerNumberResult.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	@Override
	public String toSQLString(DBDatabase db) {
		return getInputNumber().toSQLString(db);
	}

	protected NumberResult getInputNumber() {
		return getInnerNumberResult();
	}

	@Override
	public NumberExpression copy() {
		return new NumberExpression(getInputNumber());
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
	 * </ul>
	 *
	 * @param object
	 * @return a DBExpression instance that is appropriate to the subclass and
	 * the value supplied.
	 */
	public static NumberExpression value(Number object) {
		return new NumberExpression(object);
	}

	public StringExpression stringResult() {
		return new StringExpression(new DBBinaryStringNumberFunction(StringExpression.value(""), this) {
			@Override
			public String toSQLString(DBDatabase db) {
				return db.getDefinition().doConcatTransform(super.first.toSQLString(db), super.second.toSQLString(db));
			}
		});
	}

	public StringExpression append(String string) {
		return this.stringResult().append(string);
	}

	public StringExpression append(StringResult string) {
		return this.stringResult().append(string);
	}

	public BooleanExpression is(Number number) {
		return is(value(number));
	}

	public BooleanExpression is(NumberResult numberExpression) {
		return new BooleanExpression(new DBBinaryBooleanArithmetic(this, numberExpression) {
			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " = ";
			}
		});
	}

	public BooleanExpression isNotNull() {
		return BooleanExpression.isNotNull(this);
	}

	public BooleanExpression isNull() {
		return BooleanExpression.isNull(this);
	}

	public BooleanExpression isNot(Number number) {
		return is(value(number)).not();
	}

	public BooleanExpression isNot(NumberResult number) {
		return is(number).not();
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
	public BooleanExpression isBetween(NumberResult lowerBound, NumberResult upperBound) {
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
	public BooleanExpression isBetween(Number lowerBound, NumberResult upperBound) {
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
	public BooleanExpression isBetween(NumberResult lowerBound, Number upperBound) {
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
	public BooleanExpression isBetween(Number lowerBound, Number upperBound) {
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
	public BooleanExpression isBetweenInclusive(NumberResult lowerBound, NumberResult upperBound) {
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
	public BooleanExpression isBetweenInclusive(Number lowerBound, NumberResult upperBound) {
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
	public BooleanExpression isBetweenInclusive(NumberResult lowerBound, Number upperBound) {
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
	public BooleanExpression isBetweenInclusive(Number lowerBound, Number upperBound) {
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
	 * if the upper-bound is null the range will be open ended upwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(1,null) will return 2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended downwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound
	 * @param upperBound
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetweenExclusive(NumberResult lowerBound, NumberResult upperBound) {
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
	 * if the upper-bound is null the range will be open ended upwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(1,null) will return 2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended downwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound
	 * @param upperBound
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetweenExclusive(Number lowerBound, NumberResult upperBound) {
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
	 * if the upper-bound is null the range will be open ended upwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(1,null) will return 2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended downwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound
	 * @param upperBound
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetweenExclusive(NumberResult lowerBound, Number upperBound) {
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
	 * if the upper-bound is null the range will be open ended upwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(1,null) will return 2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended downwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound
	 * @param upperBound
	 * @return a boolean expression representing the required comparison
	 */
	public BooleanExpression isBetweenExclusive(Number lowerBound, Number upperBound) {
		return BooleanExpression.allOf(
				this.isGreaterThan(lowerBound),
				this.isLessThan(upperBound)
		);
	}

	public BooleanExpression isLessThan(Number number) {
		return isLessThan(value(number));
	}

	public BooleanExpression isLessThan(NumberResult numberExpression) {
		return new BooleanExpression(new DBBinaryBooleanArithmetic(this, numberExpression) {
			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " < ";
			}

			@Override
			public boolean getIncludesNull() {
				return false;
			}
		});
	}

	public BooleanExpression isLessThanOrEqual(Number number) {
		return isLessThanOrEqual(value(number));
	}

	public BooleanExpression isLessThanOrEqual(NumberResult numberExpression) {
		return new BooleanExpression(new DBBinaryBooleanArithmetic(this, numberExpression) {
			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " <= ";
			}

			@Override
			public boolean getIncludesNull() {
				return false;
			}

//			@Override
//			public void setIncludesNull(boolean nullsAreIncluded) {
//				throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//			}
		});
	}

	public BooleanExpression isGreaterThan(Number number) {
		return isGreaterThan(value(number));
	}

	public BooleanExpression isGreaterThan(NumberResult number) {
		return new BooleanExpression(new DBBinaryBooleanArithmetic(this, number) {
			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " > ";
			}

			@Override
			public boolean getIncludesNull() {
				return false;
			}

//			@Override
//			public void setIncludesNull(boolean nullsAreIncluded) {
//				throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//			}
		});
	}

	public BooleanExpression isGreaterThanOrEqual(Number number) {
		return isGreaterThanOrEqual(value(number));
	}

	public BooleanExpression isGreaterThanOrEqual(NumberResult number) {
		return new BooleanExpression(new DBBinaryBooleanArithmetic(this, number) {
			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " >= ";
			}

			@Override
			public boolean getIncludesNull() {
				return false;
			}

//			@Override
//			public void setIncludesNull(boolean nullsAreIncluded) {
//				throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//			}
		});
	}

	public BooleanExpression isIn(Number... possibleValues) {
		List<NumberExpression> possVals = new ArrayList<NumberExpression>();
		for (Number num : possibleValues) {
			possVals.add(value(num));
		}
		return isIn(possVals.toArray(new NumberExpression[]{}));
	}

	public BooleanExpression isIn(Collection<? extends Number> possibleValues) {
		List<NumberExpression> possVals = new ArrayList<NumberExpression>();
		for (Number num : possibleValues) {
			possVals.add(value(num));
		}
		return isIn(possVals.toArray(new NumberExpression[]{}));
	}

	public BooleanExpression isIn(NumberResult... possibleValues) {
		BooleanExpression isinExpr
				= new BooleanExpression(new DBNnaryBooleanFunction(this, possibleValues) {
					@Override
					protected String getFunctionName(DBDatabase db) {
						return " IN ";
					}
				});
		if (isinExpr.getIncludesNull()) {
			return BooleanExpression.anyOf(BooleanExpression.isNull(this), isinExpr);
		} else {
			return isinExpr;
		}
	}

	/**
	 * Returns the least/smallest value from the list.
	 *
	 * <p>
	 * Similar to {@link #min() } but this operates on the list provided, rather
	 * than aggregating a column.
	 *
	 * @param possibleValues
	 * @return the least/smallest value from the list.
	 */
	public static NumberExpression leastOf(Number... possibleValues) {
		List<NumberExpression> possVals = new ArrayList<NumberExpression>();
		for (Number num : possibleValues) {
			possVals.add(value(num));
		}
		return leastOf(possVals.toArray(new NumberExpression[]{}));
	}

	/**
	 * Returns the least/smallest value from the list.
	 *
	 * <p>
	 * Similar to {@link #min() } but this operates on the list provided, rather
	 * than aggregating a column.
	 *
	 * @param possibleValues
	 * @return the least/smallest value from the list.
	 */
	public static NumberExpression leastOf(Collection<? extends Number> possibleValues) {
		List<NumberExpression> possVals = new ArrayList<NumberExpression>();
		for (Number num : possibleValues) {
			possVals.add(value(num));
		}
		return leastOf(possVals.toArray(new NumberExpression[]{}));
	}

	/**
	 * Returns the least/smallest value from the list.
	 *
	 * <p>
	 * Similar to {@link #min() } but this operates on the list provided, rather
	 * than aggregating a column.
	 *
	 * @param possibleValues
	 * @return the least/smallest value from the list.
	 */
	public static NumberExpression leastOf(NumberResult... possibleValues) {
		NumberExpression leastExpr
				= new NumberExpression(new DBNnaryNumberFunction(possibleValues) {
					@Override
					protected String getFunctionName(DBDatabase db) {
						return db.getDefinition().getLeastOfFunctionName();
					}
				});
		return leastExpr;
	}

	/**
	 * Returns the greatest/largest value from the list.
	 *
	 * <p>
	 * Similar to {@link #max() } but this operates on the list provided, rather
	 * than aggregating a column.
	 *
	 * @param possibleValues
	 * @return the greatest/largest value from the list.
	 */
	public static NumberExpression greatestOf(Number... possibleValues) {
		List<NumberExpression> possVals = new ArrayList<NumberExpression>();
		for (Number num : possibleValues) {
			possVals.add(value(num));
		}
		return greatestOf(possVals.toArray(new NumberExpression[]{}));
	}

	/**
	 * Returns the greatest/largest value from the list.
	 *
	 * <p>
	 * Similar to {@link #max() } but this operates on the list provided, rather
	 * than aggregating a column.
	 *
	 * @param possibleValues
	 * @return the greatest/largest value from the list.
	 */
	public static NumberExpression greatestOf(Collection<? extends Number> possibleValues) {
		List<NumberExpression> possVals = new ArrayList<NumberExpression>();
		for (Number num : possibleValues) {
			possVals.add(value(num));
		}
		return greatestOf(possVals.toArray(new NumberExpression[]{}));
	}

	/**
	 * Returns the greatest/largest value from the list.
	 *
	 * <p>
	 * Similar to {@link #max() } but this operates on the list provided, rather
	 * than aggregating a column.
	 *
	 * @param possibleValues
	 * @return the greatest/largest value from the list.
	 */
	public static NumberExpression greatestOf(NumberResult... possibleValues) {
		NumberExpression greatestExpr
				= new NumberExpression(new DBNnaryNumberFunction(possibleValues) {
					@Override
					protected String getFunctionName(DBDatabase db) {
						return db.getDefinition().getGreatestOfFunctionName();
					}
				});
		return greatestExpr;
	}

	public static NumberExpression getNextSequenceValue(String sequenceName) {
		return getNextSequenceValue(null, sequenceName);
	}

	public static NumberExpression getNextSequenceValue(String schemaName, String sequenceName) {
		if (schemaName != null) {
			return new NumberExpression(
					new DBBinaryFunction(StringExpression.value(schemaName), StringExpression.value(sequenceName)) {
						@Override
						String getFunctionName(DBDatabase db) {
							return db.getDefinition().getNextSequenceValueFunctionName();
						}
					});
		} else {
			return new NumberExpression(
					new DBUnaryFunction(StringExpression.value(sequenceName)) {
						@Override
						String getFunctionName(DBDatabase db) {
							return db.getDefinition().getNextSequenceValueFunctionName();
						}
					});
		}
	}

	public NumberExpression ifDBNull(Number alternative) {
		return new NumberExpression(
				new DBBinaryFunction(this, new NumberExpression(alternative)) {
					@Override
					String getFunctionName(DBDatabase db) {
						return db.getDefinition().getIfNullFunctionName();
					}
				});
	}

	public NumberExpression ifDBNull(NumberResult alternative) {
		return new NumberExpression(
				new DBBinaryFunction(this, alternative) {
					@Override
					String getFunctionName(DBDatabase db) {
						return db.getDefinition().getIfNullFunctionName();
					}
				});
	}

	public NumberExpression bracket() {
		return new NumberExpression(
				new BracketUnaryFunction(this));
	}

	public NumberExpression exp() {
		return new NumberExpression(new DBUnaryFunction(this) {

			@Override
			public String toSQLString(DBDatabase db) {
				if (!db.getDefinition().supportsExpFunction() && (this.only instanceof NumberExpression)) {
					return (new NumberExpression(2.718281828)).power((NumberExpression) this.only).toSQLString(db);
				} else {
					return super.toSQLString(db); //To change body of generated methods, choose Tools | Templates.
				}
			}

			@Override
			String getFunctionName(DBDatabase db) {
				return db.getDefinition().getExpFunctionName();
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
			public String toSQLString(DBDatabase db) {
				if (db.getDefinition().supportsDegreesFunction()) {
					return super.toSQLString(db);
				} else {
					return db.getDefinition().doDegreesTransform(this.only.toSQLString(db));
				}
			}

			@Override
			String getFunctionName(DBDatabase db) {
				return "degrees";
			}
		});
	}

	public NumberExpression radians() {
		return new NumberExpression(new DBUnaryFunction(this) {
			@Override
			public String toSQLString(DBDatabase db) {
				if (db.getDefinition().supportsRadiansFunction()) {
					return super.toSQLString(db);
				} else {
					return db.getDefinition().doRadiansTransform(this.only.toSQLString(db));
				}
			}

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

	/**
	 * Implements support for CEIL().
	 *
	 * <p>
	 * Returns the smallest integer that is larger than the expression
	 *
	 * <p>
	 * Note:<br>
	 * (new DBNumber( 1.5)).ceil() == 2<br>
	 * (new DBNumber(-1.5)).ceil() == -1
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
	 * Implements support for FLOOR().
	 *
	 * <p>
	 * Returns the largest integer that is smaller than the expression
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
	 * Implements support for TRUNC().
	 *
	 * <p>
	 * Removes the decimal part of the expression, leaving an integer.
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
			public String toSQLString(DBDatabase db) {
				return db.getDefinition().doTruncTransform(only.toSQLString(db), "0");
			}

			@Override
			String getFunctionName(DBDatabase db) {
				return db.getDefinition().getTruncFunctionName();
			}

			@Override
			protected String afterValue(DBDatabase db) {
				return ", 0) ";
			}

		});
	}

	public NumberExpression convertToBits() {
		return new NumberExpression(new DBUnaryFunction(this) {

			@Override
			public String toSQLString(DBDatabase db) {
				return db.getDefinition().doIntegerToBitTransform(only.toSQLString(db));
			}

			@Override
			String getFunctionName(DBDatabase db) {
				return "";
			}
		});
	}

	public NumberExpression minus(NumberExpression equation) {
		return new NumberExpression(new MinusBinaryArithmetic(this, equation));
	}

	public NumberExpression minus(Number num) {
		final NumberExpression minusThisExpression = new NumberExpression(num);
		final DBBinaryArithmetic minusExpression = new MinusBinaryArithmetic(this, minusThisExpression);
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

	/**
	 * MOD returns the remainder from integer division.
	 *
	 * <p>
	 * DBvolution implements mod as a function. The two arguments to the
	 * function are evaluated before MOD is applied.
	 *
	 * <p>
	 * This differs from some implementations where MOD is the "%" operator and
	 * is considered equivalent to "*" and "/". However databases vary in their
	 * implementation and Wikipedia, as of 11 Sept 2014, does not include "%" in
	 * Arithmetic. So I have decided to err on the side of consistency between
	 * databases and implement it so that mod() will return the same result for
	 * all databases.
	 *
	 * @param num 
	 * @return
	 */
	public NumberExpression dividedBy(Number num) {
		return new NumberExpression(new DivisionBinaryArithmetic(this, new NumberExpression(num)));
	}

	/**
	 * MOD returns the remainder from integer division.
	 *
	 * <p>
	 * DBvolution implements mod as a function. The two arguments to the
	 * function are evaluated before MOD is applied.
	 *
	 * <p>
	 * This differs from some implementations where MOD is the "%" operator and
	 * is considered analogous to "*" and "/". However databases vary in their
	 * implementation and Wikipedia, as of 11 Sept 2014, does not include "%" in
	 * Arithmetic. So I have decided to err on the side of consistency between
	 * databases and implement it so that mod() will return the same result for
	 * all databases.
	 *
	 * @param number
	 * @return
	 */
	public NumberExpression mod(NumberResult number) {
		return new NumberExpression(new DBBinaryFunction(this, number) {

			@Override
			public String toSQLString(DBDatabase db) {
				if (db.getDefinition().supportsModulusFunction()) {
					return db.getDefinition().doModulusTransform(first.toSQLString(db), second.toSQLString(db));
				} else {
					return "((" + first.toSQLString(db) + ") % (" + second.toSQLString(db) + "))";
				}
			}

			@Override
			String getFunctionName(DBDatabase db) {
				return "MOD";
			}
		}).trunc();
	}

	public NumberExpression mod(Number num) {
		return this.mod(new NumberExpression(num));
	}

	public NumberExpression average() {
		return new NumberExpression(new DBUnaryFunction(this) {
			@Override
			String getFunctionName(DBDatabase db) {
				return db.getDefinition().getAverageFunctionName();
			}

			@Override
			public boolean isAggregator() {
				return true;
			}
		});
	}

	/**
	 * Synonym for {@link NumberExpression#standardDeviation() }.
	 *
	 * @return A number expression representing the standard deviation of the
	 * grouped rows.
	 */
	public NumberExpression stddev() {
		return standardDeviation();
	}

	public NumberExpression standardDeviation() {
		return new NumberExpression(new DBUnaryFunction(this) {

			@Override
			public String toSQLString(DBDatabase db) {
				if (db.getDefinition().supportsStandardDeviationFunction()) {
					return super.toSQLString(db);
				} else {
					if (this.only instanceof NumberExpression) {
						NumberExpression numb = (NumberExpression) this.only;
						return new NumberExpression(numb).max().minus(new NumberExpression(numb).min()).bracket().dividedBy(6).toSQLString(db);
					} else {
						return null;
					}
				}
			}

			@Override
			String getFunctionName(DBDatabase db) {
				return db.getDefinition().getStandardDeviationFunctionName();
			}

			@Override
			public boolean isAggregator() {
				return true;
			}
		});
	}

	public NumberExpression count() {
		return new NumberExpression(new DBUnaryFunction(this) {
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

	/**
	 * Returns the greatest/largest value from the column.
	 *
	 * <p>
	 * Similar to
	 * {@link #greatestOf(nz.co.gregs.dbvolution.expressions.NumberResult...)}
	 * but this aggregates the column or expression provided, rather than
	 * scanning a list.
	 *
	 * @return the greatest/largest value from the column.
	 */
	public NumberExpression max() {
		return new NumberExpression(new MaxUnaryFunction(this));
	}

	/**
	 * Returns the least/smallest value from the column.
	 *
	 * <p>
	 * Similar to {@link #leastOf(nz.co.gregs.dbvolution.expressions.NumberResult...)
	 * } but this aggregates the column or expression provided, rather than
	 * scanning a list.
	 *
	 * @return the least/smallest value from the column.
	 */
	public NumberExpression min() {
		return new NumberExpression(new MinUnaryFunction(this));
	}

	/**
	 * Returns the sum of all the values from the column.
	 *
	 * @return the sum of all the values from the column.
	 */
	public NumberExpression sum() {
		return new NumberExpression(new DBUnaryFunction(this) {
			@Override
			String getFunctionName(DBDatabase db) {
				return db.getDefinition().getSumFunctionName();
			}

			@Override
			public boolean isAggregator() {
				return true;
			}
		});
	}

	/**
	 * Returns the count of all the values from the column.
	 *
	 * @return the count of all the values from the column.
	 */
	public static NumberExpression countAll() {
		return new NumberExpression(new DBNonaryFunction() {
			@Override
			String getFunctionName(DBDatabase db) {
				return db.getDefinition().getCountFunctionName();
			}

			@Override
			protected String afterValue(DBDatabase db) {
				return "(*)";
			}

			@Override
			public boolean isAggregator() {
				return true;
			}
		});
	}

	@Override
	public DBNumber getQueryableDatatypeForExpressionValue() {
		return new DBNumber();
	}

	@Override
	public boolean isAggregator() {
		return getInnerNumberResult() == null ? false : getInnerNumberResult().isAggregator();
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		HashSet<DBRow> hashSet = new HashSet<DBRow>();
		if (getInnerNumberResult() != null) {
			hashSet.addAll(getInnerNumberResult().getTablesInvolved());
		}
		return hashSet;
	}

	/**
	 * @return the innerNumberResult
	 */
	public NumberResult getInnerNumberResult() {
		return innerNumberResult;
	}

	/**
	 * @param innerNumberResult the innerNumberResult to set
	 */
	public void setInnerNumberResult(NumberResult innerNumberResult) {
		this.innerNumberResult = innerNumberResult;
	}

	@Override
	public boolean getIncludesNull() {
		return nullProtectionRequired;
	}

	private static abstract class DBBinaryArithmetic implements NumberResult {

		public NumberResult first;
		public NumberResult second;

		DBBinaryArithmetic() {
			this.first = null;
			this.second = null;
		}

		DBBinaryArithmetic(NumberResult first, NumberResult second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public DBNumber getQueryableDatatypeForExpressionValue() {
			return new DBNumber();
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

		protected abstract String getEquationOperator(DBDatabase db);

		@Override
		public boolean isAggregator() {
			return first.isAggregator() || second.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}
	}

	private static abstract class DBNonaryFunction implements NumberResult {

		DBNonaryFunction() {
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
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			return newInstance;
		}

		@Override
		public QueryableDatatype getQueryableDatatypeForExpressionValue() {
			return new DBNumber();
		}

		@Override
		public boolean isAggregator() {
			return false;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			return new HashSet<DBRow>();
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}
	}

	private static abstract class DBUnaryFunction implements NumberResult {

		protected DBExpression only;

		DBUnaryFunction() {
			this.only = null;
		}

		DBUnaryFunction(NumberExpression only) {
			this.only = only;
		}

		DBUnaryFunction(DBExpression only) {
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

		@Override
		public boolean getIncludesNull() {
			return false;
		}
	}

	private static abstract class DBBinaryFunction implements NumberResult {

		protected DBExpression first;
		protected DBExpression second;

		DBBinaryFunction(NumberExpression first) {
			this.first = first;
			this.second = null;
		}

		DBBinaryFunction(DBExpression first, DBExpression second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public DBNumber getQueryableDatatypeForExpressionValue() {
			return new DBNumber();
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

		@Override
		public boolean getIncludesNull() {
			return false;
		}
	}

	private static abstract class DBBinaryStringNumberFunction implements StringResult {

		private DBExpression first;
		private DBExpression second;

		DBBinaryStringNumberFunction(StringResult first, NumberResult second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public DBNumber getQueryableDatatypeForExpressionValue() {
			return new DBNumber();
		}

		@Override
		abstract public String toSQLString(DBDatabase db);

		@Override
		public DBBinaryStringNumberFunction copy() {
			DBBinaryStringNumberFunction newInstance;
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

		@Override
		public boolean getIncludesNull() {
			return false;
		}
	}

	private static abstract class DBTrinaryFunction implements NumberResult {

		private DBExpression first;
		private DBExpression second;
		private DBExpression third;

		DBTrinaryFunction(DBExpression first) {
			this.first = first;
			this.second = null;
			this.third = null;
		}

		DBTrinaryFunction(DBExpression first, DBExpression second) {
			this.first = first;
			this.second = second;
		}

		DBTrinaryFunction(DBExpression first, DBExpression second, DBExpression third) {
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
		public boolean isAggregator() {
			return first.isAggregator() || second.isAggregator() || third.isAggregator();
		}
	}

	private static abstract class DBBinaryBooleanArithmetic implements BooleanResult {

		private NumberExpression first;
		private NumberResult second;
		private boolean requiresNullProtection;

		DBBinaryBooleanArithmetic(NumberExpression first, NumberResult second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		@Override
		public DBBoolean getQueryableDatatypeForExpressionValue() {
			return new DBBoolean();
		}

		@Override
		public String toSQLString(DBDatabase db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return first.toSQLString(db) + this.getEquationOperator(db) + second.toSQLString(db);
			}
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

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

	private static abstract class DBNnaryBooleanFunction implements BooleanResult {

		protected NumberExpression column;
		protected final List<NumberResult> values = new ArrayList<NumberResult>();
		boolean nullProtectionRequired = false;

		DBNnaryBooleanFunction() {
		}

		DBNnaryBooleanFunction(NumberExpression leftHandSide, NumberResult[] rightHandSide) {
			this.column = leftHandSide;
			for (NumberResult numberResult : rightHandSide) {
				if (numberResult == null) {
					this.nullProtectionRequired = true;
				} else {
					if (numberResult.getIncludesNull()) {
						this.nullProtectionRequired = true;
					}
					values.add(numberResult);
				}
			}
		}

		@Override
		public DBBoolean getQueryableDatatypeForExpressionValue() {
			return new DBBoolean();
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
			for (NumberResult val : values) {
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
			Collections.copy(this.values, newInstance.values);
			return newInstance;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (column != null) {
				hashSet.addAll(column.getTablesInvolved());
			}
			for (NumberResult second : values) {
				if (second != null) {
					hashSet.addAll(second.getTablesInvolved());
				}
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			boolean result = column.isAggregator();
			for (NumberResult numer : values) {
				result = result || numer.isAggregator();
			}
			return result;
		}

		@Override
		public boolean getIncludesNull() {
			return nullProtectionRequired;
		}

//		@Override
//		public void setIncludesNull(boolean nullsAreIncluded) {
//			this.nullProtectionRequired = nullsAreIncluded;
//		}
	}

	private static abstract class DBNnaryNumberFunction implements NumberResult {

		protected NumberExpression column;
		protected final List<NumberResult> values = new ArrayList<NumberResult>();
		boolean nullProtectionRequired = false;

		DBNnaryNumberFunction() {
		}

		DBNnaryNumberFunction(NumberResult[] rightHandSide) {
			for (NumberResult numberResult : rightHandSide) {
				if (numberResult == null) {
					this.nullProtectionRequired = true;
				} else {
					if (numberResult.getIncludesNull()) {
						this.nullProtectionRequired = true;
					}
					values.add(numberResult);
				}
			}
		}

		@Override
		public DBNumber getQueryableDatatypeForExpressionValue() {
			return new DBNumber();
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
					.append(this.getFunctionName(db))
					.append(this.beforeValue(db));
			String separator = "";
			for (NumberResult val : values) {
				if (val != null) {
					builder.append(separator).append(val.toSQLString(db));
				}
				separator = ", ";
			}
			builder.append(this.afterValue(db));
			return builder.toString();
		}

		@Override
		public DBNnaryNumberFunction copy() {
			DBNnaryNumberFunction newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.column = this.column.copy();
			Collections.copy(this.values, newInstance.values);
			return newInstance;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (column != null) {
				hashSet.addAll(column.getTablesInvolved());
			}
			for (NumberResult second : values) {
				if (second != null) {
					hashSet.addAll(second.getTablesInvolved());
				}
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			boolean result = column.isAggregator();
			for (NumberResult numer : values) {
				result = result || numer.isAggregator();
			}
			return result;
		}

		@Override
		public boolean getIncludesNull() {
			return nullProtectionRequired;
		}

//		@Override
//		public void setIncludesNull(boolean nullsAreIncluded) {
//			this.nullProtectionRequired = nullsAreIncluded;
//		}
	}

	private static abstract class DBUnaryStringFunction implements StringResult {

		protected DBExpression only;

		DBUnaryStringFunction() {
			this.only = null;
		}

		DBUnaryStringFunction(DBExpression only) {
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

	private static class MaxUnaryFunction extends DBUnaryFunction {

		public MaxUnaryFunction(DBExpression only) {
			super(only);
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return db.getDefinition().getMaxFunctionName();
		}

		@Override
		public boolean isAggregator() {
			return true;
		}
	}

	private static class MinUnaryFunction extends DBUnaryFunction {

		MinUnaryFunction(DBExpression only) {
			super(only);
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return db.getDefinition().getMinFunctionName();
		}

		@Override
		public boolean isAggregator() {
			return true;
		}
	}

	private static class MinusBinaryArithmetic extends DBBinaryArithmetic {

		MinusBinaryArithmetic(NumberResult first, NumberResult second) {
			super(first, second);
		}

		@Override
		protected String getEquationOperator(DBDatabase db) {
			return " - ";
		}
	}

	private static class BracketUnaryFunction extends DBUnaryFunction {

		BracketUnaryFunction(DBExpression only) {
			super(only);
		}

		@Override
		String getFunctionName(DBDatabase db) {
			return "";
		}
	}

	private static class DivisionBinaryArithmetic extends DBBinaryArithmetic {

		DivisionBinaryArithmetic(NumberResult first, NumberResult second) {
			super(first, second);
		}

		@Override
		protected String getEquationOperator(DBDatabase db) {
			return " / ";
		}
	}
}
