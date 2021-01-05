/*
 * Copyright 2019 Gregory Graham.
 *
 * Commercial licenses are available, please contact info@gregs.co.nz for details.
 * 
 * This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. 
 * To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/4.0/ 
 * or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.
 * 
 * You are free to:
 *     Share - copy and redistribute the material in any medium or format
 *     Adapt - remix, transform, and build upon the material
 * 
 *     The licensor cannot revoke these freedoms as long as you follow the license terms.               
 *     Under the following terms:
 *                 
 *         Attribution - 
 *             You must give appropriate credit, provide a link to the license, and indicate if changes were made. 
 *             You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.
 *         NonCommercial - 
 *             You may not use the material for commercial purposes.
 *         ShareAlike - 
 *             If you remix, transform, or build upon the material, 
 *             you must distribute your contributions under the same license as the original.
 *         No additional restrictions - 
 *             You may not apply legal terms or technological measures that legally restrict others from doing anything the 
 *             license permits.
 * 
 * Check the Creative Commons website for any details, legalese, and updates.
 */
package nz.co.gregs.dbvolution.expressions;

import java.lang.reflect.Constructor;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBInstant;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBLocalDate;
import nz.co.gregs.dbvolution.datatypes.DBLocalDateTime;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.results.AnyResult;
import nz.co.gregs.dbvolution.results.BooleanResult;
import nz.co.gregs.dbvolution.results.InstantResult;
import nz.co.gregs.dbvolution.results.IntegerResult;
import nz.co.gregs.dbvolution.results.LocalDateResult;
import nz.co.gregs.dbvolution.results.LocalDateTimeResult;
import nz.co.gregs.dbvolution.results.NumberResult;
import nz.co.gregs.dbvolution.results.StringResult;
import nz.co.gregs.separatedstring.SeparatedString;
import nz.co.gregs.separatedstring.SeparatedStringBuilder;

/**
 *
 * @author gregorygraham
 */
public class CaseExpression {

	public static WhenExpression<Boolean, BooleanResult, DBBoolean, BooleanExpression> when(BooleanExpression aThis, Boolean i) {
		return when(aThis, AnyExpression.value(i));
	}

	public static WhenExpression<Instant, InstantResult, DBInstant, InstantExpression> when(BooleanExpression aThis, Instant i) {
		return new WhenExpression<>(aThis, InstantExpression.value(i));
	}

	public static WhenExpression<Long, IntegerResult, DBInteger, IntegerExpression> when(BooleanExpression aThis, Long i) {
		return new WhenExpression<Long, IntegerResult, DBInteger, IntegerExpression>(aThis, AnyExpression.value(i));
	}

	public static WhenExpression<LocalDate, LocalDateResult, DBLocalDate, LocalDateExpression> when(BooleanExpression aThis, LocalDate i) {
		return new WhenExpression<LocalDate, LocalDateResult, DBLocalDate, LocalDateExpression>(aThis, AnyExpression.value(i));
	}

	public static WhenExpression<LocalDateTime, LocalDateTimeResult, DBLocalDateTime, LocalDateTimeExpression> when(BooleanExpression aThis, LocalDateTime i) {
		return new WhenExpression<LocalDateTime, LocalDateTimeResult, DBLocalDateTime, LocalDateTimeExpression>(aThis, AnyExpression.value(i));
	}

	public static WhenExpression<Number, NumberResult, DBNumber, NumberExpression> when(BooleanExpression aThis, Number i) {
		return new WhenExpression<Number, NumberResult, DBNumber, NumberExpression>(aThis, AnyExpression.value(i));
	}

	public static WhenExpression<String, StringResult, DBString, StringExpression> when(BooleanExpression aThis, String i) {
		return new WhenExpression<String, StringResult, DBString, StringExpression>(aThis, AnyExpression.value(i));
	}

	public static WhenExpression<Boolean, BooleanResult, DBBoolean, BooleanExpression> when(BooleanExpression aThis, BooleanExpression i) {
		return new WhenExpression<>(	aThis,new BooleanExpressionNonStatement(i));
	}

	public static <B extends Object, R extends AnyResult<B>, D extends QueryableDatatype<B>, E extends AnyExpression<B, R, D>> WhenExpression<B, R, D, E> when(BooleanExpression aThis, E i) {
		return new WhenExpression<>(aThis, i);
	}

	protected CaseExpression() {
	}

	public static class WhenExpression<B extends Object, R extends AnyResult<B>, D extends QueryableDatatype<B>, E extends AnyExpression<B, R, D>> implements AnyResult<B> {

		protected final List<WhenClause<B, R>> clauses = new ArrayList<WhenClause<B, R>>();
		protected R defaultValue;
		protected final E firstValue;

		public WhenExpression(BooleanExpression test, E value) {
			firstValue = value;
			clauses.add(new WhenClause<B, R>(test, value.asResult()));
		}

		public WhenExpression<B, R, D, E> when(BooleanExpression test, R value) {
			clauses.add(new WhenClause<B, R>(test, value));
			return this;
		}

		public WhenExpression<B, R, D, E> when(BooleanExpression test, B value) {
			clauses.add(new WhenClause<B, R>(test, firstValue.expression(value)));
			return this;
		}

		public E defaultValue(B value) {
			this.defaultValue = firstValue.expression(value);
			return this.build();
		}

		public E defaultValue(R value) {
			this.defaultValue = value;
			return this.build();
		}

		private E build() {
			try {
					Class<?> clazz = firstValue.getClass();
					do {
						Constructor<?>[] constructors = clazz.getDeclaredConstructors();
						for (Constructor<?> constructor : constructors) {
							if (constructor.getParameterTypes().length == 1 && constructor.getParameterTypes()[0].equals(AnyResult.class)) {
								constructor.setAccessible(true);
								@SuppressWarnings("unchecked")
								E newInstance = (E) constructor.newInstance((AnyResult) this);
								return newInstance;
							}
						}
						clazz = clazz.getSuperclass();
					} while (AnyExpression.class.isAssignableFrom(clazz));
			} catch (Exception ex) {
				System.out.println("" + ex.getLocalizedMessage());
				System.out.println("" + ex.getStackTrace()[0]);
				System.out.println("" + ex.getStackTrace()[1]);
				System.out.println("" + ex.getStackTrace()[2]);
				System.out.println("" + ex.getStackTrace()[3]);
				System.out.println("" + ex.getStackTrace()[4]);
				Logger.getLogger(WhenExpression.class.getName()).log(Level.SEVERE, null, ex);
			}
			return null;
		}

		@Override
		public String toSQLString(DBDefinition defn) {
			return SeparatedStringBuilder
					.startsWith(" CASE ")
					.endsWith(" ELSE " + defaultValue.toSQLString(defn) + " END")
					.addAll(
							this.clauses.stream().map((t) -> t.toSQLString(defn)).collect(Collectors.toList())
					)
					.toString();

		}

		@Override
		public QueryableDatatype<?> getQueryableDatatypeForExpressionValue() {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		@SuppressWarnings("unchecked")
		public WhenExpression<B, R, D, E> copy() {
			WhenExpression<B, R, D, E> newOne = new WhenExpression<B, R, D, E>(clauses.get(0).test, (E) clauses.get(0).returnValue);
			this.clauses.stream().skip(1).forEach((t) -> newOne.when(t.test, t.returnValue));
			newOne.defaultValue(defaultValue);
			return newOne;
		}

		@Override
		public boolean isAggregator() {
			return clauses.stream().anyMatch((t) -> t.returnValue.isAggregator() || t.test.isAggregator())
					|| defaultValue.isAggregator();
		}

		@Override
		public Set<DBRow> getTablesInvolved() {
			Set<DBRow> tables = new HashSet<>();
			clauses.stream().forEach((t) -> {
				tables.addAll(t.test.getTablesInvolved());
				tables.addAll(t.returnValue.getTablesInvolved());
			});
			tables.addAll(defaultValue.getTablesInvolved());
			return tables;
		}

		@Override
		public boolean isPurelyFunctional() {
			return clauses.stream().anyMatch((t) -> t.returnValue.isPurelyFunctional() && t.test.isPurelyFunctional())
					&& defaultValue.isPurelyFunctional();
		}

		@Override
		public boolean isComplexExpression() {
			return false;
		}

		@Override
		public String createSQLForFromClause(DBDatabase database) {
			return toSQLString(database.getDefinition());
		}

		@Override
		public String createSQLForGroupByClause(DBDatabase database) {
			return toSQLString(database.getDefinition());
		}

		@Override
		public boolean isWindowingFunction() {
			return false;
		}

		@Override
		public boolean getIncludesNull() {
			return false;
		}
	}

	protected static class WhenClause<B extends Object, R extends AnyResult<B>> implements HasSQLString {

		public BooleanExpression test;
		public R returnValue;

		WhenClause(BooleanExpression test, R value) {
			this.test = test;
			this.returnValue = value;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return " WHEN " + db.transformToWhenableType(test).toSQLString(db) + " THEN " + returnValue.toSQLString(db);
		}
	}

		protected static class BooleanExpressionNonStatement extends BooleanExpression {

		private static final long serialVersionUID = 1L;

			public BooleanExpressionNonStatement(AnyResult<?> booleanResult) {
				super(booleanResult);
			}

			@Override
			public boolean isBooleanStatement() {
				return false;
			}
		}

}
