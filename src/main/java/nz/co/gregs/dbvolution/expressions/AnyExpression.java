/*
 * Copyright 2018 gregorygraham.
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

import nz.co.gregs.dbvolution.expressions.windows.WindowFunctionFramable;
import nz.co.gregs.dbvolution.expressions.windows.CanBeWindowingFunctionWithFrame;
import nz.co.gregs.dbvolution.expressions.spatial2D.*;
import com.vividsolutions.jts.geom.LineSegment;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.results.AnyResult;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.expressions.windows.WindowFunctionRequiresOrderBy;
import nz.co.gregs.dbvolution.results.*;
import org.joda.time.Period;
import nz.co.gregs.dbvolution.expressions.windows.CanBeWindowingFunctionRequiresOrderBy;

/**
 *
 * @author gregorygraham
 * @param <B> the base Java type of this expression, e.g. Integer
 * @param <R> the Results type of this expression, e.g. IntegerExpression
 * @param <D> the QDT of this expression, e.g. DBInteger
 */
public abstract class AnyExpression<B extends Object, R extends AnyResult<B>, D extends QueryableDatatype<B>> implements ExpressionColumn<D>, AnyResult<B>, Serializable {

	private final static long serialVersionUID = 1l;

	private final AnyResult<?> innerResult;
	private final boolean nullProtectionRequired;

	/**
	 * Returns an expression that will evaluate to NULL in SQL.
	 *
	 * @return an untyped expression that returns NULL
	 */
	public AnyExpression<?, ?, ?> nullExpression() {
		return new StringExpression() {
			private final static long serialVersionUID = 1l;

			@Override
			public String toSQLString(DBDefinition db) {
				return db.getNull();
			}

		};
	}

	abstract public R expression(B value);

	abstract public R expression(R value);

	abstract public R expression(D value);
	
	public R asResult(){return (R)this;}

	@Override
	public String createSQLForFromClause(DBDatabase database) {
		return getInnerResult().createSQLForFromClause(database);
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		final AnyResult<?> inner = getInnerResult();
		Set<DBRow> result = new HashSet<DBRow>(0);
		if (inner != null) {
			result = inner.getTablesInvolved();
		}
		return result;
	}

	@Override
	public boolean isAggregator() {
		final AnyResult<?> inner = getInnerResult();
		return inner == null ? false : inner.isAggregator();
	}

	@Override
	public boolean isWindowingFunction() {
		final AnyResult<?> inner = getInnerResult();
		return inner == null ? false : inner.isWindowingFunction();
	}

	@Override
	public String toSQLString(DBDefinition db) {
		return (getInnerResult() == null) ? db.getNull() : getInnerResult().toSQLString(db);
	}

	/**
	 * A complex expression requires more than just a function call in the select
	 * clause.
	 *
	 * @return FALSE, unless you need a subtable to work out your expressions's
	 * value
	 */
	@Override
	public boolean isComplexExpression() {
		AnyResult<?> inner = getInnerResult();
		if (inner == null) {
			return false;
		} else {
			return inner.isComplexExpression();
		}
	}

	@Override
	public boolean isPurelyFunctional() {
		if (getInnerResult() == null) {
			return true;
		} else {
			return getInnerResult().isPurelyFunctional();
		}
	}

	/**
	 * Does nothing
	 *
	 */
	public AnyExpression() {
		innerResult = null;
		/* This creates a terminator expression for null-safety chains */
		nullProtectionRequired = false;
	}

	/**
	 *
	 * @param only an expression of any type
	 */
	public AnyExpression(AnyResult<?> only) {
		innerResult = only;
		nullProtectionRequired = only == null ? true : innerResult.getIncludesNull();
	}

	protected boolean isNullSafetyTerminator() {
		return getInnerResult() == null && (getIncludesNull() == false);
	}

	public AnyResult<?> getInnerResult() {
		return innerResult;
	}

	@Override
	public boolean getIncludesNull() {
		AnyResult<?> inner = getInnerResult();
		return nullProtectionRequired || (inner == null ? false : inner.getIncludesNull());
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
	 * @param value a literal value to use in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public final static BooleanExpression value(Boolean value) {
		return new BooleanExpression(value);
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
	 * @param integer a literal value to use in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public final static IntegerExpression value(Integer integer) {
		return new IntegerExpression(integer);
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
	 * @param integer a literal value to use in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public final static IntegerExpression value(Long integer) {
		return new IntegerExpression(integer);
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
	 * @param integer a literal value to use in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public final static IntegerExpression value(int integer) {
		return new IntegerExpression(integer);
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
	 * @param integer a literal value to use in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public final static IntegerExpression value(long integer) {
		return new IntegerExpression(integer);
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
	 * @param number a literal value to use in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public final static NumberExpression value(Number number) {
		return new NumberExpression(number);
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
	 * @param string a literal value to use in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public final static StringExpression value(String string) {
		return new StringExpression(string);
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
	 * @param date a literal value to use in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public final static DateExpression value(Date date) {
		return new DateExpression(date);
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
	 * @param date a literal value to use in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public final static LocalDateExpression value(LocalDate date) {
		return new LocalDateExpression(date);
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
	 * @param date a literal value to use in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public final static LocalDateTimeExpression value(LocalDateTime date) {
		return new LocalDateTimeExpression(date);
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
	 * @param date a literal value to use in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public final static InstantExpression value(Instant date) {
		return new InstantExpression(date);
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
	 * @param period a literal value to use in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public final static DateRepeatExpression value(Period period) {
		return new DateRepeatExpression(period);
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
	 * @param value a literal value to use in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public final static BooleanExpression value(BooleanResult value) {
		return new BooleanExpression(value);
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
	 * @param integer a literal value to use in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public final static IntegerExpression value(IntegerResult integer) {
		return new IntegerExpression(integer);
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
	 * @param number a literal value to use in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public final static NumberExpression value(NumberResult number) {
		return new NumberExpression(number);
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
	 * @param string a literal value to use in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public final static StringExpression value(StringResult string) {
		return new StringExpression(string);
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
	 * @param date a literal value to use in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public final static DateExpression value(DateResult date) {
		return new DateExpression(date);
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
	 * @param period a literal value to use in the expression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBExpression instance that is appropriate to the subclass and the
	 * value supplied.
	 */
	public final static DateRepeatExpression value(DateRepeatResult period) {
		return new DateRepeatExpression(period);
	}

	public final static Point2DExpression value(Point point) {
		return new Point2DExpression(point);
	}

	public final static MultiPoint2DExpression value(MultiPoint mpoint) {
		return new MultiPoint2DExpression(mpoint);
	}

	public final static Line2DExpression value(LineString line) {
		return new Line2DExpression(line);
	}

	public final static LineSegment2DExpression value(LineSegment linesegment) {
		return new LineSegment2DExpression(linesegment);
	}

	public final static Polygon2DExpression value(Polygon polygon) {
		return new Polygon2DExpression(polygon);
	}

	public final static BooleanExpression nullBoolean() {
		return new BooleanExpression().nullExpression();
	}

	public final static IntegerExpression nullInteger() {
		return new IntegerExpression().nullExpression();
	}

	public final static NumberExpression nullNumber() {
		return new NumberExpression().nullExpression();
	}

	public final static StringExpression nullString() {
		return new StringExpression().nullExpression();
	}

	public final static DateExpression nullDate() {
		return new DateExpression().nullExpression();
	}

	public final static LocalDateExpression nullLocalDate() {
		return new LocalDateExpression().nullExpression();
	}

	public final static LocalDateTimeExpression nullLocalDateTime() {
		return new LocalDateTimeExpression().nullExpression();
	}

	public final static InstantExpression nullInstant() {
		return new InstantExpression().nullExpression();
	}

	public final static DateRepeatExpression nullDateRepeat() {
		return new DateRepeatExpression().nullExpression();
	}

	public final static Point2DExpression nullPoint2D() {
		return new Point2DExpression((Point2DResult) null).nullExpression();
	}

	public final static MultiPoint2DExpression nullMultiPoint2D() {
		return new MultiPoint2DExpression().nullExpression();
	}

	public final static Line2DExpression nullLine2D() {
		return new Line2DExpression((Point) null).nullExpression();
	}

	public final static LineSegment2DExpression nullLineSegment2D() {
		return new LineSegment2DExpression((LineSegment2DResult) null).nullExpression();
	}

	public final static Polygon2DExpression nullPolygon2D() {
		return new Polygon2DExpression((Polygon2DResult) null).nullExpression();
	}

	@Override
	public String createSQLForGroupByClause(DBDatabase database) {
		return "";
	}

	public SortProvider ascending() {
		return new SortProvider.Ascending(this);
	}

	public SortProvider descending() {
		return new SortProvider.Descending(this);
	}

	public SortProvider lowestFirst() {
		return ascending();
	}

	public SortProvider highestFirst() {
		return descending();
	}

	public SortProvider lowestLast() {
		return descending();
	}

	public SortProvider highestLast() {
		return ascending();
	}

	/**
	 * Synonym for {@link #countNotNull() }.
	 *
	 * <p>
	 * Creates an expression that will count all the values of the column
	 * supplied.</p>
	 *
	 * <p>
	 * Count is an aggregator function for use in DBReport or in a column
	 * expression.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a number expression.
	 */
	public final CountExpression count() {
		return countNotNull();
	}

	/**
	 * Creates an expression that will count all the rows with non-null values in
	 * the column supplied.
	 *
	 * <p>
	 * Count is an aggregator function for use in DBReport or in a column
	 * expression.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a number expression.
	 */
	public CountExpression countNotNull() {
		return new CountExpression(this);
	}

	/**
	 * Creates an expression that will count all the distinct values in the column
	 * supplied.
	 *
	 * <p>
	 * Count is an aggregator function for use in DBReport or in a column
	 * expression.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a number expression.
	 */
	public CountDistinctExpression countDistinctValues() {
		return new CountDistinctExpression(this);
	}

	/**
	 * Creates a running count expression based on the values specified.
	 *
	 * <p>
	 * This expression uses the windowing functions to create a partitioned count
	 * window which returns the value of the expression plus all the preceding
	 * counts.</p>
	 *
	 * <p>
	 * Please note that, like all windowing functions, the ordering on the
	 * expression is unrelated to the ordering on the query.</p>
	 *
	 * <p>
	 * If you would like more control over the running total use something like
	 * tableName.column(tableName.priceColumn).sum().over() to get started.</p>
	 *
	 * @param expressionsToPartitionBy
	 * @param expressionsToOrderBy
	 * @return
	 */
	public IntegerExpression runningCount(RangeExpression[] expressionsToPartitionBy, SortProvider... expressionsToOrderBy) {
		return this.count().over().partition(expressionsToPartitionBy).orderBy(expressionsToOrderBy).withoutFrame();
	}

	public static WindowFunctionRequiresOrderBy<IntegerExpression> rank() {
		return new RankExpression().over();
	}

	public static WindowFunctionRequiresOrderBy<NumberExpression> percentageRank() {
		return new PercentageExpression().over();
	}

	public static WindowFunctionRequiresOrderBy<IntegerExpression> denseRank() {
		return new DenseRankExpression().over();
	}

	public static WindowFunctionRequiresOrderBy<IntegerExpression> rowNumber() {
		return new RowNumberExpression().over();
	}

	public static WindowFunctionRequiresOrderBy<IntegerExpression> nTile(Integer tiles) {
		return new NTileExpression(tiles).over();
	}

	public static WindowFunctionRequiresOrderBy<IntegerExpression> nTile(IntegerExpression tiles) {
		return new NTileExpression(tiles).over();
	}

	public static WindowFunctionRequiresOrderBy<IntegerExpression> nTile(Long tiles) {
		return new NTileExpression(tiles).over();
	}

	public static class CountExpression extends IntegerExpression implements CanBeWindowingFunctionWithFrame<IntegerExpression> {

		public CountExpression(AnyResult<?> only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.getCountFunctionName() + "(" + getInnerResult().toSQLString(db) + ")";
		}

		@Override
		public boolean isAggregator() {
			return true;
		}

		@Override
		public CountExpression copy() {
			return new CountExpression(
					(AnyResult<?>) (getInnerResult() == null ? null : getInnerResult().copy())
			);
		}

		@Override
		public WindowFunctionFramable<IntegerExpression> over() {
			return new WindowFunctionFramable<IntegerExpression>(new IntegerExpression(this));
		}

	}

	public static class CountDistinctExpression extends IntegerExpression implements CanBeWindowingFunctionWithFrame<IntegerExpression> {

		public CountDistinctExpression(AnyResult<?> only) {
			super(only);
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.getCountFunctionName() + "(DISTINCT " + getInnerResult().toSQLString(db) + ")";
		}

		@Override
		public boolean isAggregator() {
			return true;
		}

		@Override
		public CountDistinctExpression copy() {
			return new CountDistinctExpression(
					(AnyResult<?>) (getInnerResult() == null ? null : getInnerResult().copy())
			);
		}

		@Override
		public WindowFunctionFramable<IntegerExpression> over() {
			return new WindowFunctionFramable<IntegerExpression>(new IntegerExpression(this));
		}

	}

	/**
	 * Aggregrator that counts all the rows of the query.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the count of all the values from the column.
	 */
	public static CountAllExpression countAll() {
		return new CountAllExpression();
	}

	public static class CountAllExpression extends IntegerExpression implements CanBeWindowingFunctionWithFrame<IntegerExpression> {

		public CountAllExpression() {
			super();
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.getCountFunctionName() + "(*)";
		}

		@Override
		public boolean isAggregator() {
			return true;
		}

		@Override
		public CountAllExpression copy() {
			return new CountAllExpression();
		}

		@Override
		public WindowFunctionFramable<IntegerExpression> over() {
			return new WindowFunctionFramable<IntegerExpression>(new IntegerExpression(this));
		}
	}

	public static class RankExpression extends IntegerExpression implements CanBeWindowingFunctionRequiresOrderBy<IntegerExpression> {

		public RankExpression() {
			super();
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.getRankFunctionName() + "()";
		}

		@Override
		public boolean isAggregator() {
			return false;
		}

		@Override
		public RankExpression copy() {
			return new RankExpression();
		}

		@Override
		public WindowFunctionRequiresOrderBy<IntegerExpression> over() {
			return new WindowFunctionRequiresOrderBy<IntegerExpression>(new IntegerExpression(this));
		}
	}

	public static class PercentageExpression extends NumberExpression implements CanBeWindowingFunctionRequiresOrderBy<NumberExpression> {

		public PercentageExpression() {
			super();
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.getPercentRankFunctionName() + "()";
		}

		@Override
		public boolean isAggregator() {
			return false;
		}

		@Override
		public PercentageExpression copy() {
			return new PercentageExpression();
		}

		@Override
		public WindowFunctionRequiresOrderBy<NumberExpression> over() {
			return new WindowFunctionRequiresOrderBy<NumberExpression>(new NumberExpression(this));
		}
	}

	private static class DenseRankExpression extends IntegerExpression implements CanBeWindowingFunctionRequiresOrderBy<IntegerExpression> {

		public DenseRankExpression() {
			super();
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.getDenseRankFunctionName() + "()";
		}

		@Override
		public boolean isAggregator() {
			return false;
		}

		@Override
		public DenseRankExpression copy() {
			return new DenseRankExpression();
		}

		@Override
		public WindowFunctionRequiresOrderBy<IntegerExpression> over() {
			return new WindowFunctionRequiresOrderBy<IntegerExpression>(new IntegerExpression(this));
		}
	}

	private static class RowNumberExpression extends IntegerExpression implements CanBeWindowingFunctionRequiresOrderBy<IntegerExpression> {

		public RowNumberExpression() {
			super();
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.getRowNumberFunctionName() + "()";
		}

		@Override
		public boolean isAggregator() {
			return false;
		}

		@Override
		public RowNumberExpression copy() {
			return new RowNumberExpression();
		}

		@Override
		public WindowFunctionRequiresOrderBy<IntegerExpression> over() {
			return new WindowFunctionRequiresOrderBy<IntegerExpression>(new IntegerExpression(this));
		}
	}

	public static class NTileExpression extends IntegerExpression implements CanBeWindowingFunctionRequiresOrderBy<IntegerExpression> {

		public NTileExpression(IntegerExpression only) {
			super(only);
		}

		public NTileExpression(Long only) {
			super(new IntegerExpression(only));
		}

		public NTileExpression(Integer only) {
			super(new IntegerExpression(only));
		}
		private final static long serialVersionUID = 1l;

		@Override
		public String toSQLString(DBDefinition db) {
			return db.getNTilesFunctionName() + "(" + getInnerResult().toSQLString(db) + ")";
		}

		@Override
		public boolean isAggregator() {
			return false;
		}

		@Override
		public NTileExpression copy() {
			return new NTileExpression(
					(IntegerExpression) (getInnerResult() == null ? null : getInnerResult().copy())
			);
		}

		@Override
		public WindowFunctionRequiresOrderBy<IntegerExpression> over() {
			return new WindowFunctionRequiresOrderBy<IntegerExpression>(new IntegerExpression(this));
		}
	}
}
