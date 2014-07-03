/*
 * Copyright 2014 gregory.graham.
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
package nz.co.gregs.dbvolution.databases.definitions;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.query.QueryOptions;

/**
 *
 * @author gregory.graham
 */
public class SQLiteDefinition extends DBDefinition {

	private static final DateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd, HH:mm:ss.SSS");

	@Override
	public String getDateFormattedForQuery(Date date) {
		return " STRFTIME('%s', '" + DATETIME_FORMAT.format(date) + "') ";
	}

	@Override
	public Object getLimitRowsSubClauseDuringSelectClause(QueryOptions options) {
		return "";
	}

	@Override
	public boolean supportsGeneratedKeys(QueryOptions options) {
		return false;
	}

	@Override
	public String formatTableName(DBRow table) {
		return super.formatTableName(table).toUpperCase();
	}

	@Override
	public String getDropTableStart() {
		return super.getDropTableStart() + " IF EXISTS "; //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public boolean prefersTrailingPrimaryKeyDefinition() {
		return false;
	}

	@Override
	public String getColumnAutoIncrementSuffix() {
		return " PRIMARY KEY AUTOINCREMENT ";
	}

	@Override
	protected String getSQLTypeOfDBDatatype(QueryableDatatype qdt) {
		if (qdt instanceof DBLargeObject) {
			return " TEXT ";
		} else {
			return super.getSQLTypeOfDBDatatype(qdt);
		}
	}

	@Override
	public boolean prefersLargeObjectsAsText() {
		return true;
	}

	@Override
	public String doSubstringTransform(String originalString, String start, String length) {
		return " SUBSTR("
				+ originalString
				+ ", "
				+ start
				+ ","
				+ length
				+ ") ";
	}

	@Override
	public String getCurrentDateFunctionName() {
		return " date('now') " ;
	}
}
