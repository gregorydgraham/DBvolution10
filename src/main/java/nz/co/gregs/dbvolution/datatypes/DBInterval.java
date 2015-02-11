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
			StringBuilder str = db.getDefinition().transformPeriodIntoInterval(interval);
			return str.toString();
		}
	}

	@Override
	protected Period getFromResultSet(DBDatabase database, ResultSet resultSet, String fullColumnName) throws SQLException {
		String intervalStr = resultSet.getString(fullColumnName);
		//8 years 7 mons 47 days 04:03:02.001
		int years = 0;
		int months = 0;
		int days = 0;
		int hours = 0;
		int minutes = 0;
		int seconds = 0;
		int millis = 0;
		if (intervalStr.contains("years")) {
			final String replaced = intervalStr.replaceAll(".*([0-9]+) years.*", "$1");
			years = Integer.parseInt(replaced);
		}
		if (intervalStr.contains("mons")) {
			final String replaced = intervalStr.replaceAll(".* ([0-9]+) mons.*", "$1");
			months = Integer.parseInt(replaced);
		}
		if (intervalStr.contains("days")) {
			final String replaced = intervalStr.replaceAll(".* ([0-9]+) days.*", "$1");
			days = Integer.parseInt(replaced);
		}
		if (intervalStr.matches(".* [0-9]+:.*")) {
			String replaced = intervalStr.replaceAll(".* ([0-9]+):[0-9]+:[0-9]+[.0-9]*.*", "$1");
			hours = Integer.parseInt(replaced);
			replaced = intervalStr.replaceAll(".* [0-9]+:([0-9]+):[0-9]+[.0-9]*.*", "$1");
			minutes = Integer.parseInt(replaced);
			replaced = intervalStr.replaceAll(".* [0-9]+:[0-9]+:([0-9]+)[.0-9]*.*", "$1");
			seconds = Integer.parseInt(replaced);
		}
		if (intervalStr.matches(".*\\.[0-9]+.*")) {
			final String replaced = "0" + intervalStr.replaceAll(".*\\.([0-9]+).*", "$1");
			millis = (new Double(Double.parseDouble(replaced))).intValue();
		}
		Period parsePeriod = new Period().withYears(years).withMonths(months).withDays(days).withHours(hours).withMinutes(minutes).withSeconds(seconds).withMillis(millis);
		return parsePeriod;
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
