/*
 * Copyright 2014 gregorygraham.
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
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBJavaObject;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.query.RowDefinition;

public class JavaDBDefinition extends DBDefinition {

	private static final DateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static final String[] reservedWordsArray = new String[]{};
	private static final List<String> reservedWords = Arrays.asList(reservedWordsArray);

	@Override
	public String getDateFormattedForQuery(Date date) {
//		yyyy-mm-dd hh[:mm[:ss
		return "TIMESTAMP('" + DATETIME_FORMAT.format(date) + "')";
	}

	@Override
	protected String getSQLTypeOfDBDatatype(QueryableDatatype qdt) {
		if (qdt instanceof DBBoolean) {
			return "SMALLINT";
		} else if (qdt instanceof DBJavaObject) {
			return "BLOB";
		} else if (qdt instanceof DBDate) {
			return "TIMESTAMP";
		} else {
			return super.getSQLTypeOfDBDatatype(qdt); //To change body of generated methods, choose Tools | Templates.
		}
	}

	@Override
	public String formatTableName(DBRow table
	) {
		final String sqlObjectName = table.getTableName();
		return formatNameForJavaDB(sqlObjectName);
	}

	@Override
	public String getPrimaryKeySequenceName(String table, String column
	) {
		return formatNameForJavaDB(super.getPrimaryKeySequenceName(table, column));
	}

	@Override
	public String getPrimaryKeyTriggerName(String table, String column
	) {
		return formatNameForJavaDB(super.getPrimaryKeyTriggerName(table, column));
	}

	@Override
	public String formatColumnName(String column
	) {
		return formatNameForJavaDB(super.formatColumnName(column));
	}

	private static String formatNameForJavaDB(final String sqlObjectName) {
		if (sqlObjectName.length() < 30 && !(reservedWords.contains(sqlObjectName.toUpperCase()))) {
			return sqlObjectName.replaceAll("^[_-]", "O").replaceAll("-", "_");
		} else {
			return ("O" + sqlObjectName.hashCode()).replaceAll("^[_-]", "O").replaceAll("-", "_");
		}
	}

	@Override
	public String getTableAlias(RowDefinition tabRow) {
		return "\"" + super.getTableAlias(tabRow) + "\"";
	}

	@Override
	public String formatForColumnAlias(final String actualName) {
		String formattedName = actualName.replaceAll("\\.", "__");
		return ("DB" + formattedName.hashCode()).replaceAll("-", "_") + "";
	}

	@Override
	public String beginTableAlias() {
		return " ";
	}

	@Override
	public String endInsertLine() {
		return "";
	}

	@Override
	public String endDeleteLine() {
		return "";
	}

	@Override
	public String endSQLStatement() {
		return "";
	}

	@Override
	public String getStringLengthFunctionName() {
		return "LENGTH";
	}

	@Override
	public String doSubstringTransform(String originalString, String start, String length) {
		return " SUBSTR("
				+ originalString
				+ ", "
				+ start
				+ (length.trim().isEmpty() ? "" : ", " + length)
				+ ") ";
	}
	
	

}
