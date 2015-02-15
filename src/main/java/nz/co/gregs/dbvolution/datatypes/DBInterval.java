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
 *
 * @author gregory.graham
 */
public class DBInterval extends QueryableDatatype implements IntervalResult {

	private static final long serialVersionUID = 1L;

	public DBInterval() {
		super();
	}

	public DBInterval(Period interval) {
		super(interval);
	}

	public DBInterval(IntervalExpression interval) {
		super(interval);
	}

	public void setValue(Period newLiteralValue) {
		super.setLiteralValue(newLiteralValue);
	}

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
		return " INTERVAL ";
	}

	@Override
	protected String formatValueForSQLStatement(DBDatabase db) {
		Period interval = (Period) getLiteralValue();
		if (interval == null) {
			return "NULL";
		} else {
			String str = db.getDefinition().transformPeriodIntoInterval(interval);
			return str;
		}
	}

	@Override
	protected Period getFromResultSet(DBDatabase database, ResultSet resultSet, String fullColumnName) throws SQLException {
		String intervalStr = resultSet.getString(fullColumnName);
		if (intervalStr == null || intervalStr.equals("")) {
			return null;
		} else {
			return database.getDefinition().parseIntervalFromGetString(intervalStr);
		}
	}

	@Override
	public boolean isAggregator() {
		return false;
	}

	@Override
	public DBInterval copy() {
		return (DBInterval) super.copy();
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
			Period interval = (Period) getLiteralValue();
			return PeriodFormat.getDefault().print(interval);
		}
	}
}
