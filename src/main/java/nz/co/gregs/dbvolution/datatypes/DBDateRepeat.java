/*
 * Copyright 2015 gregory.graham.
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
package nz.co.gregs.dbvolution.datatypes;

import nz.co.gregs.dbvolution.results.DateRepeatResult;
import java.sql.ResultSet;
import java.sql.SQLException;
import nz.co.gregs.dbvolution.columns.DateRepeatColumn;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
import nz.co.gregs.dbvolution.expressions.*;
import nz.co.gregs.dbvolution.query.RowDefinition;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormat;

/**
 * Encapsulates database values that are differences between dates.
 *
 * <p>
 * Use {@link DBDateRepeat} when the column is used to represent an specific
 * period of time between events expressed in days, weeks, months or years.
 *
 * <p>
 * Please note that exact differences between dates should probably be derived
 * using {@link DateExpression#secondsFrom(java.util.Date)} and stored with a
 * DBNumber.
 *
 * <p>
 * Generally DBDateRepeat is declared inside your DBRow sub-class as:
 * {@code @DBColumn public DBDateRepeat myColumn = new DBDateRepeat();}
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBDateRepeat extends QueryableDatatype<Period> implements DateRepeatResult {

	private static final long serialVersionUID = 1L;

	/**
	 * Default constructor.
	 *
	 */
	public DBDateRepeat() {
		super();
	}

	/**
	 * Creates a new DBDateRepeat with the value specified.
	 *
	 * @param interval the interval that this QDT will represent
	 */
	public DBDateRepeat(Period interval) {
		super(interval);
	}

	/**
	 * Creates a DBDateRepeat with the DateRepeatExpression specified.
	 *
	 * <p>
	 * Very useful for adding column expressions to a DBRow subclass.
	 *
	 * @param interval the interval that this QDT will represent
	 */
	public DBDateRepeat(DateRepeatExpression interval) {
		super(interval);
	}

	/**
	 * Set the value of this DBDateRepeat to the period specified.
	 *
	 * <p>
	 * Use this method before inserting the value into the database.
	 *
	 * @param newLiteralValue the new value
	 */
	@Override
	public void setValue(Period newLiteralValue) {
		super.setLiteralValue(newLiteralValue);
	}

	/**
	 * Returns the value of this DBDateRepeat as a Period, if the value is defined
	 * and is not null.
	 *
	 * <p>
	 * Returns NULL otherwise.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a Period
	 */
	public Period periodValue() {
		if (!isDefined() || isNull()) {
			return null;
		} else {
			return getLiteralValue();
		}
	}

	@Override
	public Period getValue() {
		return periodValue();
	}

	@Override
	public String getSQLDatatype() {
		return " VARCHAR(100) ";
	}

	@Override
	protected String formatValueForSQLStatement(DBDefinition db) {
		Period interval = getLiteralValue();
		if (interval == null) {
			return "NULL";
		} else {
			String str = db.transformPeriodIntoDateRepeat(interval);
			return str;
		}
	}

	@Override
	protected Period getFromResultSet(DBDefinition database, ResultSet resultSet, String fullColumnName) throws SQLException {
		String intervalStr = resultSet.getString(fullColumnName);
		if (intervalStr == null || intervalStr.isEmpty()) {
			return null;
		} else {
			return database.parseDateRepeatFromGetString(intervalStr);
		}
	}

	@Override
	public boolean isAggregator() {
		return false;
	}

	@Override
	public DBDateRepeat copy() {
		return (DBDateRepeat) super.copy();
	}

	@Override
	public boolean getIncludesNull() {
		return false;
	}

	@Override
	public String toString() {
		if (getLiteralValue() == null) {
			return super.toString(); //To change body of generated methods, choose Tools | Templates.
		} else {
			Period period = getLiteralValue();
			return PeriodFormat.getDefault().print(period);
		}
	}

	@Override
	public StringExpression stringResult() {
		return DateRepeatExpression.value(this).stringResult();
	}

	@Override
	protected void setValueFromStandardStringEncoding(String encodedValue) {
		throw new UnsupportedOperationException("DBDateRepeat does not support setValueFromStandardStringEncoding(String) yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public DateRepeatColumn getColumn(RowDefinition row) throws IncorrectRowProviderInstanceSuppliedException {
		return new DateRepeatColumn(row, this);
	}

	/**
	 * Set the value to be inserted when no value has been set, using
	 * {@link #setValue(java.lang.Object) setValue(...)}, for the QDT.
	 *
	 * <p>
	 * The value is only used during the initial insert and does not effect the
	 * definition of the column within the database.</p>
	 *
	 * <p>
	 * Care should be taken when using this as some "obvious" uses are better
	 * handled using the
	 * {@link #setDefaultInsertValue(nz.co.gregs.dbvolution.results.AnyResult) expression version}.
	 * In particular, setDefaultInsertValue(new Date()) is probably NOT what you
	 * want, setDefaultInsertValue(DateExpression.currentDate()) will produce a
	 * correct creation date value.</p>
	 *
	 * @param value the value to use during insertion when no particular value has
	 * been specified.
	 * @return This QDT
	 */
	@Override
	public synchronized DBDateRepeat setDefaultInsertValue(Period value) {
		super.setDefaultInsertValue(value);
		return this;
	}

	/**
	 * Set the value to be inserted when no value has been set, using
	 * {@link #setValue(java.lang.Object) setValue(...)}, for the QDT.
	 *
	 * <p>
	 * The value is only used during the initial insert and does not effect the
	 * definition of the column within the database.</p>
	 *
	 * @param value the value to use during insertion when no particular value has
	 * been specified.
	 * @return This QDT
	 */
	public synchronized DBDateRepeat setDefaultInsertValue(DateRepeatResult value) {
		super.setDefaultInsertValue(value);
		return this;
	}

	/**
	 * Set the value to be used during an update when no value has been set, using
	 * {@link #setValue(java.lang.Object) setValue(...)}, for the QDT.
	 *
	 * <p>
	 * The value is only used during updates and does not effect the definition of
	 * the column within the database nor the initial value of the column.</p>
	 *
	 * <p>
	 * Care should be taken when using this as some "obvious" uses are better
	 * handled using the
	 * {@link #setDefaultUpdateValue(nz.co.gregs.dbvolution.results.AnyResult) expression version}.
	 * In particular, setDefaultUpdateValue(new Date()) is probably NOT what you
	 * want, setDefaultUpdateValue(DateExpression.currentDate()) will produce a
	 * correct update time value.</p>
	 *
	 * @param value the value to use during update when no particular value has
	 * been specified.
	 * @return This QDT
	 */
	@Override
	public synchronized DBDateRepeat setDefaultUpdateValue(Period value) {
		super.setDefaultUpdateValue(value);
		return this;
	}

	/**
	 * Set the value to be used during an update when no value has been set, using
	 * {@link #setValue(java.lang.Object) setValue(...)}, for the QDT.
	 *
	 * <p>
	 * The value is only used during updates and does not effect the definition of
	 * the column within the database nor the initial value of the column.</p>
	 *
	 * @param value the value to use during update when no particular value has
	 * been specified.
	 * @return This QDT
	 */
	public synchronized DBDateRepeat setDefaultUpdateValue(DateRepeatResult value) {
		super.setDefaultUpdateValue(value);
		return this;
	}
}
