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

import nz.co.gregs.dbvolution.internal.query.LargeObjectHandlerType;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.WKTReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.SQLiteDB;
import nz.co.gregs.dbvolution.databases.supports.SupportsDateRepeatDatatypeFunctions;
import nz.co.gregs.dbvolution.databases.supports.SupportsPolygonDatatype;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.datatypes.spatial2D.*;
import nz.co.gregs.dbvolution.exceptions.IncorrectGeometryReturnedForDatatype;
import nz.co.gregs.dbvolution.generation.DBTableField;
import nz.co.gregs.dbvolution.internal.datatypes.DateRepeatImpl;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import nz.co.gregs.dbvolution.internal.sqlite.*;
import org.joda.time.Period;

/**
 * Defines the features of the SQLite database that differ from the standard
 * database.
 *
 * <p>
 * This DBDefinition is automatically included in {@link SQLiteDB} instances,
 * and you should not need to use it directly.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class SQLiteDefinition extends DBDefinition implements SupportsDateRepeatDatatypeFunctions, SupportsPolygonDatatype {

	/**
	 * The date format used internally within DBvolution's SQLite implementation.
	 *
	 */
	private final DateFormat DATETIME_PRECISE_FORMAT = getDateTimeFormat();//
	private static final SimpleDateFormat DATETIME_SIMPLE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	@Override
	public String getDateFormattedForQuery(Date date) {
		return " strftime('%Y-%m-%d %H:%M:%f', '" + DATETIME_PRECISE_FORMAT.format(date) + "') ";
	}

	@Override
	public boolean supportsGeneratedKeys() {
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
	protected boolean hasSpecialAutoIncrementType() {
		return true;
	}

	@Override
	protected String getSpecialAutoIncrementType() {
		return " INTEGER PRIMARY KEY AUTOINCREMENT ";
	}

	@Override
	protected String getDatabaseDataTypeOfQueryableDatatype(QueryableDatatype<?> qdt) {
		if (qdt instanceof DBLargeText) {
			return " NTEXT ";
		} else if (qdt instanceof DBInteger) {
			return " BIGINT ";
		} else if (qdt instanceof DBJavaObject) {
			return " BLOB ";
		} else if (qdt instanceof DBLargeBinary) {
			return " BLOB ";
		} else if (qdt instanceof DBBooleanArray) {
			return " VARCHAR(64) ";
		} else if (qdt instanceof DBDate) {
			return " DATETIME ";
		} else if (qdt instanceof DBPoint2D) {
			return " VARCHAR(2000) ";
		} else if (qdt instanceof DBLine2D) {
			return " VARCHAR(2001) ";
		} else if (qdt instanceof DBMultiPoint2D) {
			return " VARCHAR(2002) ";
		} else {
			return super.getDatabaseDataTypeOfQueryableDatatype(qdt);
		}
	}

	@Override
	public void sanityCheckDBTableField(DBTableField dbTableField) {
		if (dbTableField.isPrimaryKey && 
				(dbTableField.columnType.equals(DBInteger.class)||dbTableField.columnType.equals(DBNumber.class))) {
			dbTableField.isAutoIncrement = true;
		}
	}

	@Override
	public boolean prefersLargeObjectsReadAsBase64CharacterStream(DBLargeObject<?> lob) {
		return !(lob instanceof DBLargeText);
	}

	@Override
	public boolean prefersLargeObjectsSetAsBase64String(DBLargeObject<?> lob) {
		return !(lob instanceof DBLargeText);
	}

	/**
	 * Indicates whether the database prefers reading BLOBs using the getBytes()
	 * method.
	 *
	 * @param lob the type of large object that is being processed
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the default implementation returns FALSE
	 */
	@Override
	public boolean prefersLargeObjectsReadAsBytes(DBLargeObject<?> lob) {
		return false;
	}

	/**
	 * Indicates whether the database prefers reading BLOBs using the getClob()
	 * method.
	 *
	 * @param lob the type of large object that is being processed
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the default implementation returns FALSE
	 */
	@Override
	public boolean prefersLargeObjectsReadAsCLOB(DBLargeObject<?> lob) {
		return false;
	}

	/**
	 * Indicates whether the database prefers reading BLOBs using the getBlob()
	 * method.
	 *
	 * @param lob the type of large object that is being processed
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the default implementation returns FALSE
	 */
	@Override
	public boolean prefersLargeObjectsReadAsBLOB(DBLargeObject<?> lob) {
		return false;
	}

	/**
	 * Indicates that the database prefers Large Object values to be set using the
	 * setCharacterStream method.
	 *
	 * <p>
	 * If both {@link #prefersLargeObjectsSetAsCharacterStream(nz.co.gregs.dbvolution.datatypes.DBLargeObject)
	 * } and
	 * {@link #prefersLargeObjectsSetAsBase64String(nz.co.gregs.dbvolution.datatypes.DBLargeObject) }
	 * return FALSE, DBvolution will use the setBinaryStream method to set the
	 * value.
	 *
	 * @param lob the DBLargeObject which we are querying about.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the default implementation returns FALSE.
	 */
	@Override
	public boolean prefersLargeObjectsSetAsCharacterStream(DBLargeObject<?> lob) {
		return false;
	}

	/**
	 * Indicates that the database prefers Large Object values to be set using the
	 * setBLOB method.
	 *
	 * <p>
	 * If both {@link #prefersLargeObjectsSetAsCharacterStream(nz.co.gregs.dbvolution.datatypes.DBLargeObject)
	 * } and
	 * {@link #prefersLargeObjectsSetAsBase64String(nz.co.gregs.dbvolution.datatypes.DBLargeObject) }
	 * return FALSE, DBvolution will use the setBinaryStream method to set the
	 * value.
	 *
	 * @param lob the DBLargeObject which we are querying about.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the default implementation returns FALSE.
	 */
	@Override
	public boolean prefersLargeObjectsSetAsBLOB(DBLargeObject<?> lob) {
		return false;
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
	protected String getCurrentDateOnlyFunctionName() {
		return "DATETIME";
	}

	@Override
	public String doCurrentDateOnlyTransform() {
		return " strftime('%Y-%m-%d %H:%M:%f', 'now','localtime') ";
	}

	@Override
	protected String getCurrentDateTimeFunction() {
		return " strftime('%Y-%m-%d %H:%M:%f', 'now','localtime') ";
	}

	@Override
	public String getStringLengthFunctionName() {
		return "LENGTH";
	}

	@Override
	public String getTruncFunctionName() {
		// TRUNC is defined in SQLiteDB as a user defined function.
		return "TRUNC";
	}

	@Override
	public String doPositionInStringTransform(String originalString, String stringToFind) {
		return "LOCATION_OF(" + originalString + ", " + stringToFind + ")";
	}

	@Override
	public String getCurrentUserFunctionName() {
		return "CURRENT_USER()";
	}

	@Override
	public String getStandardDeviationFunctionName() {
		return "STDEV";
	}

	@Override
	public String doMonthTransform(String dateExpression) {
		return " (CAST(strftime('%m', " + dateExpression + ") as INTEGER))";
	}

	@Override
	public String doYearTransform(String dateExpression) {
		return " (CAST(strftime('%Y', " + dateExpression + ") as INTEGER))";
	}

	@Override
	public String doDayTransform(String dateExpression) {
		return " (CAST(strftime('%d', " + dateExpression + ") as INTEGER))";
	}

	@Override
	public String doHourTransform(String dateExpression) {
		return " (CAST(strftime('%H', " + dateExpression + ") as INTEGER))";
	}

	@Override
	public String doMinuteTransform(String dateExpression) {
		return " (CAST(strftime('%M', " + dateExpression + ") as INTEGER))";
	}

	@Override
	public String doSecondTransform(String dateExpression) {
		return " (CAST(strftime('%S', " + dateExpression + ") as INTEGER))";
	}

	@Override
	public String doSubsecondTransform(String dateExpression) {
		return " ((CAST(strftime('%f', " + dateExpression + ") as REAL))-(CAST(strftime('%S', " + dateExpression + ") as INTEGER)))";
	}

	@Override
	public String getGreatestOfFunctionName() {
		return " MAX "; //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String getLeastOfFunctionName() {
		return " MIN "; //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public boolean prefersDatesReadAsStrings() {
		return true;
	}

	@Override
	public synchronized Date parseDateFromGetString(String getStringDate) throws ParseException {
		try {
			return DATETIME_PRECISE_FORMAT.parse(getStringDate);
		} catch (ParseException ex) {
			return (DATETIME_SIMPLE_FORMAT).parse(getStringDate);
		}
	}

	@Override
	public boolean supportsRetrievingLastInsertedRowViaSQL() {
		return true;
	}

	@Override
	public String getRetrieveLastInsertedRowSQL() {
		return "select last_insert_rowid();";
	}

	/**
	 * Indicates whether the database supports the modulus function.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the default implementation returns TRUE.
	 */
	@Override
	public boolean supportsModulusFunction() {
		return false;
	}

	@Override
	public String doAddSecondsTransform(String dateValue, String numberOfSeconds) {
		return "strftime('%Y-%m-%d %H:%M:%f', (" + dateValue + "), (" + numberOfSeconds + ")||' SECOND')";
	}

	@Override
	public String doAddMinutesTransform(String dateValue, String numberOfMinutes) {
		return "strftime('%Y-%m-%d %H:%M:%f', (" + dateValue + "), (" + numberOfMinutes + ")||' minute')";
	}

	@Override
	public String doAddHoursTransform(String dateValue, String numberOfHours) {
		return "strftime('%Y-%m-%d %H:%M:%f', (" + dateValue + "), (" + numberOfHours + ")||' hour')";
	}

	@Override
	public String doAddDaysTransform(String dateValue, String numberOfDays) {
		return "strftime('%Y-%m-%d %H:%M:%f', (" + dateValue + "), (" + numberOfDays + ")||' days')";
	}

	@Override
	public String doAddWeeksTransform(String dateValue, String numberOfWeeks) {
		return "strftime('%Y-%m-%d %H:%M:%f', (" + dateValue + "), (7*(" + numberOfWeeks + "))||' days')";
	}

	@Override
	public String doAddMonthsTransform(String dateValue, String numberOfMonths) {
		return "strftime('%Y-%m-%d %H:%M:%f', (" + dateValue + "), (" + numberOfMonths + ")||' month')";
	}

	@Override
	public String doAddYearsTransform(String dateValue, String numberOfYears) {
		return "strftime('%Y-%m-%d %H:%M:%f', (" + dateValue + "), (" + numberOfYears + ")||' year')";
	}

	@Override
	public String doDayDifferenceTransform(String dateValue, String otherDateValue) {
		return "(julianday(" + otherDateValue + ") - julianday(" + dateValue + "))";
	}

	@Override
	public String doWeekDifferenceTransform(String dateValue, String otherDateValue) {
		return "(" + doDayDifferenceTransform(dateValue, otherDateValue) + "/7)";
	}

	@Override
	public String doMonthDifferenceTransform(String dateValue, String otherDateValue) {
		return "(strftime('%m'," + otherDateValue + ")+12*strftime('%Y'," + otherDateValue + ")) - (strftime('%m'," + dateValue + ")+12*strftime('%Y'," + dateValue + "))";
	}

	@Override
	public String doYearDifferenceTransform(String dateValue, String otherDateValue) {
		return "(strftime('%Y'," + otherDateValue + ")) - (strftime('%Y'," + dateValue + "))";
	}

	@Override
	public String doHourDifferenceTransform(String dateValue, String otherDateValue) {
		return "(cast((strftime('%s'," + otherDateValue + ")-strftime('%s'," + dateValue + ")) AS real)/60/60)";
	}

	@Override
	public String doMinuteDifferenceTransform(String dateValue, String otherDateValue) {
		return "(cast((strftime('%s'," + otherDateValue + ")-strftime('%s'," + dateValue + ")) AS real)/60)";
	}

	@Override
	public String doSecondDifferenceTransform(String dateValue, String otherDateValue) {
		return "cast((strftime('%s'," + otherDateValue + ")-strftime('%s'," + dateValue + ")) AS real)";
	}

	@Override
	public String doDayOfWeekTransform(String dateSQL) {
		return " (cast(STRFTIME('%w', (" + dateSQL + ")) AS real)+1)";
	}

	@Override
	public boolean supportsArraysNatively() {
		return false;
	}

	@Override
	public String getAlterTableAddForeignKeyStatement(DBRow newTableRow, PropertyWrapper field) {
		if (field.isForeignKey()) {
			return "ALTER TABLE " + this.formatTableName(newTableRow) + " ADD " + field.columnName() + " REFERENCES " + field.referencedTableName() + "(" + field.referencedColumnName() + ") ";

		}
		return "";
	}

	@Override
	public String transformPeriodIntoDateRepeat(Period interval) {
		return "'" + DateRepeatImpl.getDateRepeatString(interval) + "'";
	}

	@Override
	public String doDateMinusToDateRepeatTransformation(String leftHandSide, String rightHandSide) {
		return " " + DateRepeatFunctions.DATEREPEAT_CREATION_FUNCTION + "(" + leftHandSide + ", " + rightHandSide + ")";
	}

	@Override
	public String doDatePlusDateRepeatTransform(String leftHandSide, String rightHandSide) {
		return " " + DateRepeatFunctions.DATEREPEAT_DATEADDITION_FUNCTION + "(" + leftHandSide + ", " + rightHandSide + ")";
	}

	@Override
	public String doDateMinusDateRepeatTransform(String leftHandSide, String rightHandSide) {
		return " " + DateRepeatFunctions.DATEREPEAT_DATESUBTRACTION_FUNCTION + "(" + leftHandSide + ", " + rightHandSide + ")";
	}

	@Override
	public String doDateRepeatEqualsTransform(String leftHandSide, String rightHandSide) {
		return "(" + leftHandSide + " = " + rightHandSide + ")";
	}

	@Override
	public String doDateRepeatLessThanTransform(String leftHandSide, String rightHandSide) {
		return DateRepeatFunctions.DATEREPEAT_LESSTHAN_FUNCTION + "(" + leftHandSide + ", " + rightHandSide + ")";
	}

	@Override
	public String doDateRepeatLessThanEqualsTransform(String leftHandSide, String rightHandSide) {
		return DateRepeatFunctions.DATEREPEAT_LESSTHANEQUALS_FUNCTION + "(" + leftHandSide + ", " + rightHandSide + ")";
	}

	@Override
	public String doDateRepeatGreaterThanTransform(String leftHandSide, String rightHandSide) {
		return DateRepeatFunctions.DATEREPEAT_GREATERTHAN_FUNCTION + "(" + leftHandSide + ", " + rightHandSide + ")";
	}

	@Override
	public String doDateRepeatGreaterThanEqualsTransform(String leftHandSide, String rightHandSide) {
		return DateRepeatFunctions.DATEREPEAT_GREATERTHANEQUALS_FUNCTION + "(" + leftHandSide + ", " + rightHandSide + ")";
	}

	@Override
	public String doDateRepeatGetYearsTransform(String intervalStr) {
		return DateRepeatFunctions.DATEREPEAT_YEAR_PART_FUNCTION + "(" + intervalStr + ")";
	}

	@Override
	public String doDateRepeatGetMonthsTransform(String intervalStr) {
		return DateRepeatFunctions.DATEREPEAT_MONTH_PART_FUNCTION + "(" + intervalStr + ")";
	}

	@Override
	public String doDateRepeatGetDaysTransform(String intervalStr) {
		return DateRepeatFunctions.DATEREPEAT_DAY_PART_FUNCTION + "(" + intervalStr + ")";
	}

	@Override
	public String doDateRepeatGetHoursTransform(String intervalStr) {
		return DateRepeatFunctions.DATEREPEAT_HOUR_PART_FUNCTION + "(" + intervalStr + ")";
	}

	@Override
	public String doDateRepeatGetMinutesTransform(String intervalStr) {
		return DateRepeatFunctions.DATEREPEAT_MINUTE_PART_FUNCTION + "(" + intervalStr + ")";
	}

	@Override
	public String doDateRepeatGetSecondsTransform(String intervalStr) {
		return DateRepeatFunctions.DATEREPEAT_SECOND_PART_FUNCTION + "(" + intervalStr + ")";
	}

	@Override
	public boolean supportsArcSineFunction() {
		return false;
	}

	@Override
	public String transformCoordinatesIntoDatabasePoint2DFormat(String xValue, String yValue) {
		return "'POINT (" + xValue + " " + yValue + ")'";
	}

	@Override
	public String doPoint2DEqualsTransform(String firstPoint, String secondPoint) {
		return Point2DFunctions.EQUALS_FUNCTION + "(" + firstPoint + ", " + secondPoint + ")";
	}

	@Override
	public String doPoint2DGetXTransform(String toSQLString) {
		return Point2DFunctions.GETX_FUNCTION + "(" + toSQLString + ")";
	}

	@Override
	public String doPoint2DGetYTransform(String toSQLString) {
		return Point2DFunctions.GETY_FUNCTION + "(" + toSQLString + ")";
	}

	@Override
	public String doPoint2DMeasurableDimensionsTransform(String toSQLString) {
		return Point2DFunctions.GETDIMENSION_FUNCTION + "(" + toSQLString + ")";
	}

	@Override
	public String doPoint2DGetBoundingBoxTransform(String toSQLString) {
		return Point2DFunctions.GETBOUNDINGBOX_FUNCTION + "(" + toSQLString + ")";
	}

	@Override
	public String doPoint2DAsTextTransform(String toSQLString) {
		return Point2DFunctions.ASTEXT_FUNCTION + "(" + toSQLString + ")";
	}

	@Override
	public String transformPolygonIntoDatabasePolygon2DFormat(Polygon geom) {
		String wktValue = geom.toText();
		return Polygon2DFunctions.CREATE_FROM_WKTPOLYGON2D + "('" + wktValue + "')";
	}

	@Override
	public String doPolygon2DGetMaxXTransform(String toSQLString) {
		return Polygon2DFunctions.MAX_X + "(" + toSQLString + ")";
	}

	@Override
	public String doPolygon2DGetMinXTransform(String toSQLString) {
		return Polygon2DFunctions.MIN_X + "(" + toSQLString + ")";
	}

	@Override
	public String doPolygon2DGetMaxYTransform(String toSQLString) {
		return Polygon2DFunctions.MAX_Y + "(" + toSQLString + ")";
	}

	@Override
	public String doPolygon2DGetMinYTransform(String toSQLString) {
		return Polygon2DFunctions.MIN_Y + "(" + toSQLString + ")";
	}

	@Override
	public String doPolygon2DMeasurableDimensionsTransform(String toSQLString) {
		return Polygon2DFunctions.DIMENSION + "(" + toSQLString + ")";
	}

	@Override
	public String doPolygon2DGetBoundingBoxTransform(String toSQLString) {
		return Polygon2DFunctions.BOUNDINGBOX + "(" + toSQLString + ")";
	}

	@Override
	public String doPolygon2DGetAreaTransform(String polygonSQL) {
		return Polygon2DFunctions.AREA + "(" + polygonSQL + ")";
	}

	@Override
	public String doPolygon2DGetExteriorRingTransform(String firstGeometry) {
		return Polygon2DFunctions.EXTERIORRING + "(" + firstGeometry + ")";
	}

	@Override
	public String doPolygon2DEqualsTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.EQUALS + "(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DUnionTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.UNION + "(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DIntersectionTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.INTERSECTION + "(" + firstGeometry + ", " + secondGeometry + ")";
	}
	
	@Override
	public String doPolygon2DIntersectsTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.INTERSECTS + "(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DContainsPolygon2DTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.CONTAINS_POLYGON2D + "(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DDoesNotIntersectTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.DISJOINT + "(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DOverlapsTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.OVERLAPS + "(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DTouchesTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.TOUCHES + "(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DWithinTransform(String firstGeometry, String secondGeometry) {
		//indicate whether g1 is spatially within g2. This is the inverse of Contains(). 
		// i.e. G1.within(G2) === G2.contains(G1)
		return Polygon2DFunctions.WITHIN + "(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DContainsPoint2DTransform(String firstGeometry, String secondGeometry) {
		//indicate whether g1 is spatially within g2. This is the inverse of Contains(). 
		// i.e. G1.within(G2) === G2.contains(G1)
		return Polygon2DFunctions.CONTAINS_POINT2D + "(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DAsTextTransform(String polygonSQL) {
		return Polygon2DFunctions.ASTEXT_FUNCTION + "(" + polygonSQL + ")";
	}

	@Override
	public String doLine2DAsTextTransform(String lineSQL) {
		return Line2DFunctions.ASTEXT_FUNCTION + "(" + lineSQL + ")";
	}

	@Override
	public String doLine2DEqualsTransform(String firstLine, String secondLine) {
		return Line2DFunctions.EQUALS_FUNCTION + "(" + firstLine + ", " + secondLine + ")";
	}

	@Override
	public String doLine2DMeasurableDimensionsTransform(String lineSQL) {
		return Line2DFunctions.GETDIMENSION_FUNCTION + "(" + lineSQL + ")";
	}

	@Override
	public String doLine2DGetBoundingBoxTransform(String lineSQL) {
		return Line2DFunctions.GETBOUNDINGBOX_FUNCTION + "(" + lineSQL + ")";
	}

	@Override
	public String doLine2DGetMaxXTransform(String lineSQL) {
		return Line2DFunctions.GETMAXX_FUNCTION + "(" + lineSQL + ")";
	}

	@Override
	public String doLine2DGetMaxYTransform(String lineSQL) {
		return Line2DFunctions.GETMAXY_FUNCTION + "(" + lineSQL + ")";
	}

	@Override
	public String doLine2DGetMinXTransform(String lineSQL) {
		return Line2DFunctions.GETMINX_FUNCTION + "(" + lineSQL + ")";
	}

	@Override
	public String doLine2DGetMinYTransform(String lineSQL) {
		return Line2DFunctions.GETMINY_FUNCTION + "(" + lineSQL + ")";
	}

	@Override
	public String doLine2DIntersectsLine2DTransform(String toSQLString, String toSQLString0) {
		return Line2DFunctions.INTERSECTS + "(" + toSQLString + ", " + toSQLString0 + ")";
	}

	@Override
	public String doLine2DIntersectionPointWithLine2DTransform(String toSQLString, String toSQLString0) {
		return Line2DFunctions.INTERSECTIONWITH_LINE2D + "((" + toSQLString + "), (" + toSQLString0 + "))";
	}

	@Override
	public String doLine2DAllIntersectionPointsWithLine2DTransform(String toSQLString, String toSQLString0) {
		return Line2DFunctions.ALLINTERSECTIONSWITH_LINE2D + "((" + toSQLString + "), (" + toSQLString0 + "))";
	}

	@Override
	public LineSegment transformDatabaseLineSegment2DValueToJTSLineSegment(String lineSegmentAsSQL) throws com.vividsolutions.jts.io.ParseException {
		LineString lineString = (new GeometryFactory()).createLineString(new Coordinate[]{});
		WKTReader wktReader = new WKTReader();
		Geometry geometry = wktReader.read(lineSegmentAsSQL);
		if (geometry instanceof LineString) {
			lineString = (LineString) geometry;
			if (lineSegmentAsSQL == null) {
				return null;
			} else {
				return new LineSegment(lineString.getCoordinateN(0), lineString.getCoordinateN(1));
			}
		} else {
			throw new IncorrectGeometryReturnedForDatatype(geometry, lineString);
		}
	}

	@Override
	public String transformLineSegmentIntoDatabaseLineSegment2DFormat(LineSegment lineSegment) {
		LineString line = (new GeometryFactory()).createLineString(new Coordinate[]{lineSegment.getCoordinate(0), lineSegment.getCoordinate(1)});
		String wktValue = line.toText();
		return "'" + wktValue + "'";
	}

	@Override
	public String doLineSegment2DIntersectsLineSegment2DTransform(String toSQLString, String toSQLString0) {
		return doLine2DIntersectsLine2DTransform(toSQLString, toSQLString0);
	}

	@Override
	public String doLineSegment2DGetMaxXTransform(String toSQLString) {
		return LineSegment2DFunctions.GETMAXX_FUNCTION + "(" + toSQLString + ")";
	}

	@Override
	public String doLineSegment2DGetMinXTransform(String toSQLString) {
		return LineSegment2DFunctions.GETMINX_FUNCTION + "(" + toSQLString + ")";
	}

	@Override
	public String doLineSegment2DGetMaxYTransform(String toSQLString) {
		return LineSegment2DFunctions.GETMAXY_FUNCTION + "(" + toSQLString + ")";
	}

	@Override
	public String doLineSegment2DGetMinYTransform(String toSQLString) {
		return LineSegment2DFunctions.GETMINY_FUNCTION + "(" + toSQLString + ")";
	}

	@Override
	public String doLineSegment2DGetBoundingBoxTransform(String toSQLString) {
		return LineSegment2DFunctions.GETBOUNDINGBOX_FUNCTION + "(" + toSQLString + ")";
	}

	@Override
	public String doLineSegment2DDimensionTransform(String toSQLString) {
		return LineSegment2DFunctions.GETDIMENSION_FUNCTION + "(" + toSQLString + ")";
	}

	@Override
	public String doLineSegment2DNotEqualsTransform(String toSQLString, String toSQLString0) {
		return "!(" + LineSegment2DFunctions.EQUALS_FUNCTION + "((" + toSQLString + "),(" + toSQLString0 + ")))";
	}

	@Override
	public String doLineSegment2DEqualsTransform(String toSQLString, String toSQLString0) {
		return LineSegment2DFunctions.EQUALS_FUNCTION + "((" + toSQLString + "),(" + toSQLString0 + "))";
	}

	@Override
	public String doLineSegment2DAsTextTransform(String toSQLString) {
		return LineSegment2DFunctions.ASTEXT_FUNCTION + "(" + toSQLString + ")";
	}

	@Override
	public String doLineSegment2DIntersectionPointWithLineSegment2DTransform(String toSQLString, String toSQLString0) {
		return LineSegment2DFunctions.INTERSECTIONWITH_LINESEGMENT2D + "((" + toSQLString + "), (" + toSQLString0 + "))";
	}

	@Override
	public String doMultiPoint2DEqualsTransform(String first, String second) {
		return MultiPoint2DFunctions.EQUALS_FUNCTION + "((" + first + "), (" + second + "))";
	}

	@Override
	public String doMultiPoint2DGetPointAtIndexTransform(String first, String index) {
		return MultiPoint2DFunctions.GETPOINTATINDEX_FUNCTION + "((" + first + "), (" + index + "))";
	}

	@Override
	public String doMultiPoint2DGetNumberOfPointsTransform(String first) {
		return MultiPoint2DFunctions.GETNUMBEROFPOINTS_FUNCTION + "(" + first + ")";
	}

	@Override
	public String doMultiPoint2DMeasurableDimensionsTransform(String first) {
		return MultiPoint2DFunctions.GETDIMENSION_FUNCTION + "(" + first + ")";
	}

	@Override
	public String doMultiPoint2DGetBoundingBoxTransform(String first) {
		return MultiPoint2DFunctions.GETBOUNDINGBOX_FUNCTION + "(" + first + ")";
	}

	@Override
	public String doMultiPoint2DAsTextTransform(String first) {
		return MultiPoint2DFunctions.ASTEXT_FUNCTION + "(" + first + ")";
	}

	@Override
	public String doMultiPoint2DToLine2DTransform(String first) {
		return MultiPoint2DFunctions.ASLINE2D + "(" + first + ")";
	}

	@Override
	public String doMultiPoint2DGetMinYTransform(String first) {
		return MultiPoint2DFunctions.GETMINY_FUNCTION + "(" + first + ")";
	}

	@Override
	public String doMultiPoint2DGetMinXTransform(String first) {
		return MultiPoint2DFunctions.GETMINX_FUNCTION + "(" + first + ")";
	}

	@Override
	public String doMultiPoint2DGetMaxYTransform(String first) {
		return MultiPoint2DFunctions.GETMAXY_FUNCTION + "(" + first + ")";
	}

	@Override
	public String doMultiPoint2DGetMaxXTransform(String first) {
		return MultiPoint2DFunctions.GETMAXX_FUNCTION + "(" + first + ")";
	}

	@Override
	public String getTrueValue() {
		return " 1 ";
	}

	@Override
	public String getFalseValue() {
		return " 0 ";
	}

	public DateFormat getDateTimeFormat() {
		return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	}

	@Override
	public LargeObjectHandlerType preferredLargeObjectWriter(DBLargeObject<?> lob) {
		if (lob instanceof DBLargeBinary) {
			return LargeObjectHandlerType.BYTE;
		} else if (lob instanceof DBLargeText) {
			return LargeObjectHandlerType.BASE64;
		} else if (lob instanceof DBJavaObject) {
			return LargeObjectHandlerType.BYTE;
		} else {
			return super.preferredLargeObjectWriter(lob);
		}
	}

	@Override
	public LargeObjectHandlerType preferredLargeObjectReader(DBLargeObject<?> lob) {
		if (lob instanceof DBLargeBinary) {
			return LargeObjectHandlerType.BINARYSTREAM;
		} else if (lob instanceof DBLargeText) {
			return LargeObjectHandlerType.BASE64;
		} else if (lob instanceof DBJavaObject) {
			return LargeObjectHandlerType.BYTE;
		} else {
			return super.preferredLargeObjectReader(lob);
		}
	}

	/**
	 * Return the function name for the Natural Logarithm function.
	 *
	 * <p>
	 * For SQLite this method returns <b>log</b></p>
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the name of the function to use when rounding numbers up
	 */
	@Override
	public String getNaturalLogFunctionName() {
		return "log";
	}

	@Override
	public String doRandomNumberTransform() {
		return " ABS(RANDOM()/9223372036854775808)";
	}

	@Override
	public Boolean supportsUnionDistinct() {
		return false;
	}

	@Override
	public boolean supportsFullOuterJoinNatively() {
		return false;
	}

	@Override
	public boolean supportsRightOuterJoinNatively() {
		return false;
	}

	@Override
	public boolean supportsAlterTableAddConstraint() {
		return false;
	}

	@Override
	public boolean supportsTableCheckingViaMetaData() {
		return false;
	}
}
