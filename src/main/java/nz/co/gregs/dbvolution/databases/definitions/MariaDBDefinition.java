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
package nz.co.gregs.dbvolution.databases.definitions;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import nz.co.gregs.dbvolution.databases.MariaClusterDB;
import nz.co.gregs.dbvolution.databases.MariaDB;
import nz.co.gregs.dbvolution.datatypes.DBLargeBinary;
import nz.co.gregs.dbvolution.datatypes.DBLargeText;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 * Defines the features of the MariaDB database that differ from the standard
 * database.
 *
 * <p>
 * This DBDefinition is automatically included in {@link MariaDB} and
 * {@link MariaClusterDB} instances, and you should not need to use it directly.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class MariaDBDefinition extends DBDefinition {

	private final DateFormat DATETIME_FORMAT = new SimpleDateFormat("dd,MM,yyyy HH:mm:ss");

	@Override
	@SuppressWarnings("deprecation")
	public String getDateFormattedForQuery(Date date) {

		return " STR_TO_DATE('" + DATETIME_FORMAT.format(date) + "', '%d,%m,%Y %H:%i:%s') ";

	}

	@Override
	public String getEqualsComparator() {
		return " = ";
	}

	@Override
	public String getNotEqualsComparator() {
		return " <> ";
	}

	@Override
	public String getDatabaseDataTypeOfQueryableDatatype(QueryableDatatype<?> qdt) {
		if (qdt instanceof DBString) {
			return " NVARCHAR(1000) CHARACTER SET utf8 COLLATE utf8_bin ";
		} else if (qdt instanceof DBDate) {
			return " DATETIME ";
		} else if (qdt instanceof DBInteger) {
			return " BIGINT ";
		} else if (qdt instanceof DBLargeBinary) {
			return " LONGBLOB ";
		} else if (qdt instanceof DBLargeText) {
			return " LONGTEXT ";
		} else if (qdt instanceof DBLargeObject) {
			return " LONGBLOB ";
		} else {
			return super.getDatabaseDataTypeOfQueryableDatatype(qdt);
		}
	}

//	@Override
//	public String getDropDatabase(String databaseName) {
//		return "DROP DATABASE IF EXISTS " + databaseName + ";";
//	}
	@Override
	public String doConcatTransform(String firstString, String secondString) {
		return " CONCAT(" + firstString + ", " + secondString + ") ";
	}

	@Override
	public String getTruncFunctionName() {
		return "truncate";
	}

	@Override
	public String doStringEqualsTransform(String firstString, String secondString) {
		return "(" + firstString + " = binary " + secondString + ")";
	}

	@Override
	public String getColumnAutoIncrementSuffix() {
		return " AUTO_INCREMENT ";
	}

	@Override
	public String doDayOfWeekTransform(String dateSQL) {
		return " DAYOFWEEK(" + dateSQL + ")";
	}

	@Override
	public boolean supportsRecursiveQueriesNatively() {
		return false;
	}

	@Override
	public boolean supportsFullOuterJoinNatively() {
		return false;
	}
}
