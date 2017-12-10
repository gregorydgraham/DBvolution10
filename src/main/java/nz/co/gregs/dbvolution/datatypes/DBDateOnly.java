/*
 * Copyright 2013 Gregory Graham.
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

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.expressions.DateExpression;

/**
 * Encapsulates date values that only include year, month, and day values.
 *
 * <p>
 * Use this when the actual date value only stores a partial date without any
 * time value. The instance will behave as a {@link DBDate} but time information
 * will be ignore/discarded.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBDateOnly extends DBDate {

	private static final long serialVersionUID = 1L;

	/**
	 * The default constructor for DBDateOnly.
	 *
	 * <p>
	 * Creates an unset undefined DBDateOnly object.
	 *
	 */
	public DBDateOnly() {
	}

	/**
	 * Creates a DBDateOnly with the value provided.
	 *
	 * <p>
	 * The resulting DBDate will be set as having the value provided but will not
	 * be defined in the database.
	 *
	 * <p>
	 * Any time information in the date will be ignored.
	 *
	 * @param date	date
	 */
	public DBDateOnly(Date date) {
		super(date);
	}

	/**
	 * Creates a DBDateOnly with the value provided.
	 *
	 * <p>
	 * The resulting DBDate will be set as having the value provided but will not
	 * be defined in the database.
	 *
	 * <p>
	 * Only date information in the timestamp will be retained.
	 *
	 * @param timestamp	timestamp
	 */
	public DBDateOnly(Timestamp timestamp) {
		super(timestamp);
	}

	/**
	 * Creates a column expression with a date result from the expression
	 * provided.
	 *
	 * <p>
	 * Used in {@link DBReport}, and some {@link DBRow}, sub-classes to derive
	 * data from the database prior to retrieval.
	 *
	 * @param dateExpression	dateExpression
	 */
	public DBDateOnly(DateExpression dateExpression) {
		super(dateExpression);
	}

	@Override
	public String getSQLDatatype() {
		return "DATE";
	}

	@Override
	public Date dateValue() {
		Date dateValue = super.dateValue();
		Date finalDateValue = dateValue;
		if (dateValue != null) {
			Calendar cal = Calendar.getInstance();
			cal.setTime(dateValue);
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
			finalDateValue = cal.getTime();
		}
		return finalDateValue;
	}

	@Override
	public String formatValueForSQLStatement(DBDefinition db) {
		final Date dateValue = dateValue();
		return db.getDateFormattedForQuery(dateValue);
	}
}
