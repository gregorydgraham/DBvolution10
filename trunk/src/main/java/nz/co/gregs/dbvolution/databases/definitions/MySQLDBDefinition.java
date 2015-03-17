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
package nz.co.gregs.dbvolution.databases.definitions;

import nz.co.gregs.dbvolution.datatypes.spatial2D.DBGeometry2D;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import nz.co.gregs.dbvolution.databases.MySQLDB;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBPoint2D;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;

/**
 * Defines the features of the MySQL database that differ from the standard
 * database.
 *
 * <p>
 * This DBDefinition is automatically included in {@link MySQLDB} instances, and
 * you should not need to use it directly.
 *
 * @author Gregory Graham
 */
public class MySQLDBDefinition extends DBDefinition {

	private static final DateFormat DATETIME_FORMAT = new SimpleDateFormat("dd,MM,yyyy HH:mm:ss");

	@Override
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
	public String getSQLTypeOfDBDatatype(QueryableDatatype qdt) {
		if (qdt instanceof DBString) {
			return " VARCHAR(1000) CHARACTER SET utf8 COLLATE utf8_bin ";
		} else if (qdt instanceof DBDate) {
			return " DATETIME(6) ";
		} else if (qdt instanceof DBByteArray) {
			return " LONGBLOB ";
		} else if (qdt instanceof DBLargeObject) {
			return " LONGBLOB ";
		} else if (qdt instanceof DBBooleanArray) {
			return " VARCHAR(64) ";
		} else {
			return qdt.getSQLDatatype();
		}
	}

	@Override
	public Object doColumnTransformForSelect(QueryableDatatype qdt, String selectableName) {
		if (qdt instanceof DBGeometry2D) {
			return "AsText(" + selectableName + ")";
		} else if (qdt instanceof DBPoint2D) {
			return "AsText(" + selectableName + ")";
		} else {
			return selectableName;
		}
	}

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
	public String doModulusTransform(String firstNumber, String secondNumber) {
		return getTruncFunctionName() + "(" + super.doModulusTransform(firstNumber, secondNumber) + ",0)";
	}

//	@Override
//	public String doMillisecondTransform(String dateExpression) {
//		return "(EXTRACT(MICROSECOND FROM " + dateExpression + ")/1000.0)";
//	}
	@Override
	public String doDayDifferenceTransform(String dateValue, String otherDateValue) {
		return "TIMESTAMPDIFF(DAY, " + dateValue + "," + otherDateValue + ")";
	}

	@Override
	public String doWeekDifferenceTransform(String dateValue, String otherDateValue) {
		return "TIMESTAMPDIFF(WEEK, " + dateValue + "," + otherDateValue + ")";
	}

	@Override
	public String doMonthDifferenceTransform(String dateValue, String otherDateValue) {
		return "TIMESTAMPDIFF(MONTH, " + dateValue + "," + otherDateValue + ")";
	}

	@Override
	public String doYearDifferenceTransform(String dateValue, String otherDateValue) {
		return "TIMESTAMPDIFF(YEAR, " + dateValue + "," + otherDateValue + ")";
	}

	@Override
	public String doHourDifferenceTransform(String dateValue, String otherDateValue) {
		return "TIMESTAMPDIFF(HOUR, " + dateValue + "," + otherDateValue + ")";
	}

	@Override
	public String doMinuteDifferenceTransform(String dateValue, String otherDateValue) {
		return "TIMESTAMPDIFF(MINUTE, " + dateValue + "," + otherDateValue + ")";
	}

	@Override
	public String doSecondDifferenceTransform(String dateValue, String otherDateValue) {
		return "TIMESTAMPDIFF(SECOND, " + dateValue + "," + otherDateValue + ")";
	}

	@Override
	protected boolean hasSpecialPrimaryKeyTypeForDBDatatype(PropertyWrapper field) {
		if (field.getQueryableDatatype() instanceof DBString) {
			return true;
		} else {
			return super.hasSpecialPrimaryKeyTypeForDBDatatype(field);
		}
	}

	@Override
	protected String getSpecialPrimaryKeyTypeOfDBDatatype(PropertyWrapper field) {
		if (field.getQueryableDatatype() instanceof DBString) {
			return " VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_bin ";
		} else {
			return super.getSpecialPrimaryKeyTypeOfDBDatatype(field);
		}
	}

	@Override
	public String doDayOfWeekTransform(String dateSQL) {
		return " DAYOFWEEK(" + dateSQL + ")";
	}

	@Override
	public String getIndexClauseForCreateTable(PropertyWrapper field) {
		if (field.getQueryableDatatype() instanceof DBString) {
			return "CREATE INDEX " + formatNameForDatabase("DBI_" + field.tableName() + "_" + field.columnName()) + " ON " + formatNameForDatabase(field.tableName()) + "(" + formatNameForDatabase(field.columnName()) + "(255))";
		} else {
			return super.getIndexClauseForCreateTable(field);
		}
	}

	@Override
	public boolean supportsArraysNatively() {
		return false;
	}

	@Override
	public String doGeometry2DEqualsTransform(String firstGeometry, String secondGeometry) {
		return "Equals(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doGeometry2DIntersectionTransform(String firstGeometry, String secondGeometry) {
		return "Intersects(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doGeometry2DContainsTransform(String firstGeometry, String secondGeometry) {
		return "Contains(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doGeometry2DDoesNotIntersectTransform(String firstGeometry, String secondGeometry) {
		return "Disjoint(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doGeometry2DOverlapsTransform(String firstGeometry, String secondGeometry) {
		return "Overlaps(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doGeometry2DTouchesTransform(String firstGeometry, String secondGeometry) {
		return "Touches(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doGeometry2DWithinTransform(String firstGeometry, String secondGeometry) {
		//Returns 1 or 0 to indicate whether g1 is spatially within g2. This tests the opposite relationship as Contains(). 
		return "Within(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doGeometry2DGetDimensionTransform(String thisGeometry) {
		return "Dimension(" + thisGeometry + ")";
	}

	@Override
	public String doGeometry2DGetBoundingBoxTransform(String thisGeometry) {
		return "Envelope(" + thisGeometry + ")";
	}

	@Override
	public String doGeometry2DGetAreaTransform(String thisGeometry) {
		return "Area(" + thisGeometry + ")";
	}

	@Override
	public String doGeometry2DGetExteriorRingTransform(String thisGeometry) {
		return "ExteriorRing(" + thisGeometry + ")";
	}

	@Override
	public boolean supportsHyperbolicFunctionsNatively() {
		return false;
	}

	@Override
	public String doPoint2DEqualsTransform(String firstPoint, String secondPoint) {
		return "Equals(" + firstPoint + ", " + secondPoint + ")";
	}


}
