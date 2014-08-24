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
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;

public class DateExpression implements DateResult {

	private DateResult date1;
	private boolean needsNullProtection = false;

	protected DateExpression() {
	}

	public DateExpression(DateResult dateVariable) {
		date1 = dateVariable;
		if (date1 == null || date1.getIncludesNull()) {
			needsNullProtection = true;
		}
	}

	public DateExpression(Date date) {
		date1 = new DBDate(date);
		if (date == null || date1.getIncludesNull()) {
			needsNullProtection = true;
		}
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
	 * <p>
	 * The expression framework requires a *Expression to work with. The easiest
	 * way to get that is the {@code DBRow.column()} method.
	 *
	 * <p>
	 * However if you wish your expression to start with a literal value it is a
	 * little trickier.
	 *
	 * <p>
	 * This method provides the easy route to a *Expression from a literal value.
	 * Just call, for instance, {@code StringExpression.value("STARTING STRING")}
	 * to get a StringExpression and start the expression chain.
	 *
	 * <ul>
	 * <li>Only object classes that are appropriate need to be handle by the
	 * DBExpression subclass.<li>
	 * <li>The implementation should be {@code static}</li>
	 * </ul>
	 *
	 * @param date
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
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

	public NumberExpression year() {
		return new NumberExpression(
				new UnaryComplicatedNumberFunction(this) {
					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doYearTransform(this.only.toSQLString(db));
					}
				});
	}

	public NumberExpression month() {
		return new NumberExpression(
				new UnaryComplicatedNumberFunction(this) {
					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doMonthTransform(this.only.toSQLString(db));
					}
				});
	}

	/**
	 * Returns the day part of the date.
	 *
	 * <p>
	 * Day in this sense is the number of the day within the month: that is the 23
	 * part of Monday 25th of August 2014
	 *
	 * @return a NumberExpression that will provide the day of this date.
	 */
	public NumberExpression day() {
		return new NumberExpression(
				new UnaryComplicatedNumberFunction(this) {
					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doDayTransform(this.only.toSQLString(db));
					}
				});
	}

	public NumberExpression hour() {
		return new NumberExpression(
				new UnaryComplicatedNumberFunction(this) {
					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doHourTransform(this.only.toSQLString(db));
					}
				});
	}

	public NumberExpression minute() {
		return new NumberExpression(
				new UnaryComplicatedNumberFunction(this) {
					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doMinuteTransform(this.only.toSQLString(db));
					}
				});
	}

	public NumberExpression second() {
		return new NumberExpression(
				new UnaryComplicatedNumberFunction(this) {
					@Override
					public String toSQLString(DBDatabase db) {
						return db.getDefinition().doSecondTransform(this.only.toSQLString(db));
					}
				});
	}

	public BooleanExpression is(Date date) {
		return is(value(date));
	}

	public BooleanExpression is(DateResult dateExpression) {
		BooleanExpression isExpr = new BooleanExpression(new DBBinaryBooleanArithmetic(this, dateExpression) {
			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " = ";
			}
		});
		if (isExpr.getIncludesNull()) {
			return BooleanExpression.isNull(this);
		} else {
			return isExpr;
		}
	}

	public BooleanExpression isNotNull() {
		return BooleanExpression.isNotNull(this);
	}

	public BooleanExpression isNull() {
		return BooleanExpression.isNull(this);
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified the lower-bound will be included in
	 * the search and the upper-bound excluded. I.e permittedRange(1,3) will
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
	public BooleanExpression isBetween(DateResult lowerBound, DateResult upperBound) {
		return BooleanExpression.allOf(
				this.isGreaterThan(lowerBound),
				this.isLessThanOrEqual(upperBound)
		);
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified the lower-bound will be included in
	 * the search and the upper-bound excluded. I.e permittedRange(1,3) will
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
	public BooleanExpression isBetween(Date lowerBound, DateResult upperBound) {
		return BooleanExpression.allOf(
				this.isGreaterThan(lowerBound),
				this.isLessThanOrEqual(upperBound)
		);
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified the lower-bound will be included in
	 * the search and the upper-bound excluded. I.e permittedRange(1,3) will
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
	public BooleanExpression isBetween(DateResult lowerBound, Date upperBound) {
		return BooleanExpression.allOf(
				this.isGreaterThan(lowerBound),
				this.isLessThanOrEqual(upperBound)
		);
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified the lower-bound will be included in
	 * the search and the upper-bound excluded. I.e permittedRange(1,3) will
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
	public BooleanExpression isBetween(Date lowerBound, Date upperBound) {
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
	public BooleanExpression isBetweenInclusive(DateResult lowerBound, DateResult upperBound) {
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
	public BooleanExpression isBetweenInclusive(Date lowerBound, DateResult upperBound) {
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
	public BooleanExpression isBetweenInclusive(DateResult lowerBound, Date upperBound) {
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
	public BooleanExpression isBetweenInclusive(Date lowerBound, Date upperBound) {
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
	public BooleanExpression isBetweenExclusive(DateResult lowerBound, DateResult upperBound) {
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
	public BooleanExpression isBetweenExclusive(Date lowerBound, DateResult upperBound) {
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
	public BooleanExpression isBetweenExclusive(DateResult lowerBound, Date upperBound) {
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
	public BooleanExpression isBetweenExclusive(Date lowerBound, Date upperBound) {
		return BooleanExpression.allOf(
				this.isGreaterThan(lowerBound),
				this.isLessThan(upperBound)
		);
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

			@Override
			public boolean getIncludesNull() {
				return false;
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

			@Override
			public boolean getIncludesNull() {
				return false;
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

			@Override
			public boolean getIncludesNull() {
				return false;
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

			@Override
			public boolean getIncludesNull() {
				return false;
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
		BooleanExpression isInExpr = new BooleanExpression(new DBNnaryBooleanFunction(this, possibleValues) {
			@Override
			protected String getFunctionName(DBDatabase db) {
				return " IN ";
			}
		});
		if (isInExpr.getIncludesNull()) {
			return BooleanExpression.anyOf(BooleanExpression.isNull(this), isInExpr);
		} else {
			return isInExpr;
		}
	}

	public DateExpression ifDBNull(Date alternative) {
		return new DateExpression(
				new DateExpression.DBBinaryFunction(this, new DateExpression(alternative)) {
					@Override
					String getFunctionName(DBDatabase db) {
						return db.getDefinition().getIfNullFunctionName();
					}

					@Override
					public boolean getIncludesNull() {
						return false;
					}
				});
	}

	public DateExpression ifDBNull(DateResult alternative) {
		return new DateExpression(
				new DateExpression.DBBinaryFunction(this, alternative) {
					@Override
					String getFunctionName(DBDatabase db) {
						return db.getDefinition().getIfNullFunctionName();
					}

					@Override
					public boolean getIncludesNull() {
						return false;
					}
				});
	}

	public NumberExpression count() {
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

	public DateExpression max() {
		return new DateExpression(new DBUnaryDateFunction(this) {
			@Override
			String getFunctionName(DBDatabase db) {
				return db.getDefinition().getMaxFunctionName();
			}

			@Override
			public boolean isAggregator() {
				return true;
			}

			@Override
			public boolean getIncludesNull() {
				return false;
			}
		});
	}

	public DateExpression min() {
		return new DateExpression(new DBUnaryDateFunction(this) {
			@Override
			String getFunctionName(DBDatabase db) {
				return db.getDefinition().getMinFunctionName();
			}

			@Override
			public boolean isAggregator() {
				return true;
			}

			@Override
			public boolean getIncludesNull() {
				return false;
			}
		});
	}

	@Override
	public DBDate getQueryableDatatypeForExpressionValue() {
		return new DBDate();
	}

	@Override
	public boolean isAggregator() {
		return date1 == null ? false : date1.isAggregator();
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		return date1 == null ? new HashSet<DBRow>() : date1.getTablesInvolved();
	}

	@Override
	public boolean getIncludesNull() {
		return needsNullProtection;
	}

	private static abstract class DBNonaryFunction implements DateResult {

		DBNonaryFunction() {
		}

		abstract String getFunctionName(DBDatabase db);

		@Override
		public DBDate getQueryableDatatypeForExpressionValue() {
			return new DBDate();
		}

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

		@Override
		public Set<DBRow> getTablesInvolved() {
			return new HashSet<DBRow>();
		}

		@Override
		public boolean isAggregator() {
			return false;
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}
	}

	private static abstract class UnaryComplicatedNumberFunction implements NumberResult {

		protected DateExpression only;

		UnaryComplicatedNumberFunction() {
			this.only = null;
		}

		UnaryComplicatedNumberFunction(DateExpression only) {
			this.only = only;
		}

		@Override
		public DBNumber getQueryableDatatypeForExpressionValue() {
			return new DBNumber();
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

	private static abstract class DBBinaryBooleanArithmetic implements BooleanResult {

		private DateExpression first;
		private DateResult second;
		private boolean requiresNullProtection = false;

		DBBinaryBooleanArithmetic(DateExpression first, DateResult second) {
			this.first = first;
			this.second = second;
			if (second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		@Override
		public DBBoolean getQueryableDatatypeForExpressionValue() {
			return new DBBoolean();
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
			return requiresNullProtection;
		}
	}

	private static abstract class DBNnaryDateFunction implements DateResult {

		protected DateExpression column;
		protected DateResult[] values;

		DBNnaryDateFunction() {
			this.values = null;
		}

		DBNnaryDateFunction(DateExpression leftHandSide, DateResult[] rightHandSide) {
			this.values = new DateResult[rightHandSide.length];
			this.column = leftHandSide;
			System.arraycopy(rightHandSide, 0, this.values, 0, rightHandSide.length);
		}

		@Override
		public DBDate getQueryableDatatypeForExpressionValue() {
			return new DBDate();
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
					.append(column.toSQLString(db)).append(" ")
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
		public DBNnaryDateFunction copy() {
			DBNnaryDateFunction newInstance;
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
		public boolean isAggregator() {
			boolean result = false || column.isAggregator();
			for (DateResult dater : values) {
				result = result || dater.isAggregator();
			}
			return result;
		}
	}

	private static abstract class DBNnaryBooleanFunction implements BooleanResult {

		protected DateExpression column;
		protected List<DateResult> values = new ArrayList<DateResult>();
		boolean nullProtectionRequired = false;

		DBNnaryBooleanFunction() {
		}

		DBNnaryBooleanFunction(DateExpression leftHandSide, DateResult[] rightHandSide) {
			this.column = leftHandSide;
			for (DateResult dateResult : rightHandSide) {
				if (dateResult == null) {
					this.nullProtectionRequired = true;
				} else {
					if (dateResult.getIncludesNull()) {
						this.nullProtectionRequired = true;
					} else {
						values.add(dateResult);
					}
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
			Collections.copy(this.values, newInstance.values);
			return newInstance;
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (column != null) {
				hashSet.addAll(column.getTablesInvolved());
			}
			for (DateResult val : values) {
				if (val != null) {
					hashSet.addAll(val.getTablesInvolved());
				}
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			boolean result = false || column.isAggregator();
			for (DateResult dater : values) {
				result = result || dater.isAggregator();
			}
			return result;
		}

		@Override
		public boolean getIncludesNull() {
			return nullProtectionRequired;
		}
	}

	private static abstract class DBBinaryFunction implements DateResult {

		private DateExpression first;
		private DateResult second;

		DBBinaryFunction(DateExpression first) {
			this.first = first;
			this.second = null;
		}

		DBBinaryFunction(DateExpression first, DateResult second) {
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
			return first.isAggregator() || second.isAggregator();
		}
	}

	private static abstract class DBUnaryNumberFunction implements NumberResult {

		protected DateExpression only;

		DBUnaryNumberFunction() {
			this.only = null;
		}

		DBUnaryNumberFunction(DateExpression only) {
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
		public boolean isAggregator() {
			return only.isAggregator();
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			return only.getTablesInvolved();
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}
	}

	private static abstract class DBUnaryDateFunction implements DateResult {

		protected DateExpression only;

		DBUnaryDateFunction() {
			this.only = null;
		}

		DBUnaryDateFunction(DateExpression only) {
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
		public DBUnaryDateFunction copy() {
			DBUnaryDateFunction newInstance;
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
		public boolean isAggregator() {
			return only.isAggregator();
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			return only.getTablesInvolved();
		}
	}
}
