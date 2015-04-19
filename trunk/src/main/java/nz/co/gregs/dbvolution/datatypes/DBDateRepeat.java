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

import java.sql.ResultSet;
import java.sql.SQLException;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.expressions.*;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormat;

/**
 * Encapsulates database values that are differences between dates.
 *
 * <p>
 * Use {@link DBDateRepeat} when the column is used to represent an specific period of time between events expressed in days, weeks, months or years.
 * 
 * <p>Please note that exact differences between dates should probably be derived using {@link DateExpression#secondsFrom(java.util.Date)} and stored with a DBNumber.
 *
 * <p>
 * Generally DBDateRepeat is declared inside your DBRow sub-class as:
 * {@code @DBColumn public DBDateRepeat myColumn = new DBDateRepeat();}
 * 
 * @author Gregory Graham
 */
public class DBDateRepeat extends QueryableDatatype implements DateRepeatResult {

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
	 * @param interval
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
	 * @param interval
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
	 * @param newLiteralValue
	 */
	public void setValue(Period newLiteralValue) {
		super.setLiteralValue(newLiteralValue);
	}

	/**
	 * Returns the value of this DBDateRepeat as a Period, if the value is defined and is not null.
	 * 
	 * <p>
	 * Returns NULL otherwise.
	 *
	 * @return
	 */
	public Period periodValue() {
		if (!isDefined() || isNull()) {
			return null;
		} else {
			return (Period) getLiteralValue();
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
	protected String formatValueForSQLStatement(DBDatabase db) {
		Period interval = (Period) getLiteralValue();
		if (interval == null) {
			return "NULL";
		} else {
			String str = db.getDefinition().transformPeriodIntoDateRepeat(interval);
			return str;
		}
	}

	@Override
	protected Period getFromResultSet(DBDatabase database, ResultSet resultSet, String fullColumnName) throws SQLException {
		String intervalStr = resultSet.getString(fullColumnName);
		if (intervalStr == null || intervalStr.equals("")) {
			return null;
		} else {
			return database.getDefinition().parseDateRepeatFromGetString(intervalStr);
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
			Period period = (Period) getLiteralValue();
			return PeriodFormat.getDefault().print(period);
		}
	}
}
