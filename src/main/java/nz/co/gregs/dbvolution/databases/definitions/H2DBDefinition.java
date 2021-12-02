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

import nz.co.gregs.dbvolution.internal.query.LargeObjectHandlerType;
import com.vividsolutions.jts.geom.Polygon;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.H2DB;
import nz.co.gregs.dbvolution.databases.supports.SupportsPolygonDatatype;
import nz.co.gregs.dbvolution.datatypes.DBDateRepeat;
import nz.co.gregs.dbvolution.datatypes.DBInstant;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBJavaObject;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.datatypes.DBLargeText;
import nz.co.gregs.dbvolution.datatypes.DBLocalDateTime;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.datatypes.spatial2D.*;
import nz.co.gregs.dbvolution.internal.h2.*;

/**
 * Defines the features of the H2 database that differ from the standard
 * database.
 *
 * <p>
 * This DBDefinition is automatically included in {@link H2DB} instances, and
 * you should not need to use it directly.
 *
 * @author Gregory Graham
 */
public class H2DBDefinition extends DBDefinition implements SupportsPolygonDatatype {

	public static final long serialVersionUID = 1L;

	private static final String DATE_FORMAT_STR = "yyyy-M-d HH:mm:ss.SSSSSSSSS Z";
	private static final String H2_DATE_FORMAT_INCLUDING_TIMEZONE = "yyyy-M-d HH:mm:ss.SSSSSSSSS Z";//2017-02-18 18:59:59.000 +10:00

	private static SimpleDateFormat getStringToDateFormat() {
		return new SimpleDateFormat(DATE_FORMAT_STR);
	}
	
	@Override
	public String getDateFormattedForQuery(Date date) {
		if (date == null) {
			return getNull();
		}
		return "PARSEDATETIME('" + getStringToDateFormat().format(date) + "','" + H2_DATE_FORMAT_INCLUDING_TIMEZONE + "')";
	}

	@Override
	public String getDatePartsFormattedForQuery(String years, String months, String days, String hours, String minutes, String seconds, String subsecond, String timeZoneSign, String timeZoneHourOffset, String timeZoneMinuteOffSet) {
		String result = "PARSEDATETIME("
				+ "''||" + years
				+ "||'-'||" + doLeftPadTransform(months, "'0'", "2")
				+ "||'-'||" + doLeftPadTransform(days, "'0'", "2")
				+ "||' '||" + doLeftPadTransform(hours, "'0'", "2")
				+ "||':'||" + doLeftPadTransform(minutes, "'0'", "2")
				+ "||':'||" + doIfThenElseTransform(doIntegerEqualsTransform(doStringLengthTransform("''||" + seconds), "1"), "'0'", "''")
				+ "||" + seconds + "||' '||'" + timeZoneSign + "'"
				+ "||" + doLeftPadTransform(timeZoneHourOffset, "'0'", "2")
				+ "||" + doLeftPadTransform(timeZoneMinuteOffSet, "'0'", "2")
				+ ", '" + "yyyy-M-d HH:mm:ss Z" + "'"
				+ ")";
		result = "TIMESTAMPADD('NANOSECOND', (" + subsecond + "*1000000000), " + result + ")";
		return result;
	}

	@Override
	public String getLocalDatePartsFormattedForQuery(String years, String months, String days, String hours, String minutes, String seconds, String subsecond, String timeZoneSign, String timeZoneHourOffset, String timeZoneMinuteOffSet) {
		String result = "PARSEDATETIME("
				+ "''||" + years
				+ "||'-'||" + doLeftPadTransform(months, "'0'", "2")
				+ "||'-'||" + doLeftPadTransform(days, "'0'", "2")
				+ "||' '||" + doLeftPadTransform(hours, "'0'", "2")
				+ "||':'||" + doLeftPadTransform(minutes, "'0'", "2")
				+ "||':'||" + doIfThenElseTransform(doIntegerEqualsTransform(doStringLengthTransform("''||" + seconds), "1"), "'0'", "''")
				+ "||" + seconds
				+ ", '" + "yyyy-M-d HH:mm:ss" + "'"
				+ ")";
		result = "TIMESTAMPADD('NANOSECOND', (" + subsecond + "*1000000000), " + result + ")";
		return result;
	}

	@Override
	public String formatTableName(DBRow table) {
		return table.getTableName().toUpperCase();
	}

	@Override
	public String formatColumnName(String columnName) {
		return columnName.toUpperCase();
	}

	@Override
	protected String getDatabaseDataTypeOfQueryableDatatype(QueryableDatatype<?> qdt) {
		if (qdt instanceof DBInteger) {
			return " BIGINT ";
		} else if (qdt instanceof DBInstant) {
			return "TIMESTAMP(9) WITH TIME ZONE";
		} else if (qdt instanceof DBLocalDateTime) {
			return "TIMESTAMP(9)";
		} else if (qdt instanceof DBDateRepeat) {
			return DataTypes.DATEREPEAT.datatype();
		} else if (qdt instanceof DBDateRepeat) {
			return DataTypes.DATEREPEAT.datatype();
		} else if (qdt instanceof DBPoint2D) {
			return DataTypes.POINT2D.datatype();
		} else if (qdt instanceof DBLineSegment2D) {
			return DataTypes.LINESEGMENT2D.datatype();
		} else if (qdt instanceof DBLine2D) {
			return DataTypes.LINE2D.datatype();
		} else if (qdt instanceof DBPolygon2D) {
			return DataTypes.POLYGON2D.datatype();
		} else if (qdt instanceof DBMultiPoint2D) {
			return DataTypes.MULTIPOINT2D.datatype();
		} else {
			return super.getDatabaseDataTypeOfQueryableDatatype(qdt);
		}
	}

	@Override
	public String doStringLengthTransform(String enclosedValue) {
		return " CAST(" + getStringLengthFunctionName() + "( " + enclosedValue + " ) as NUMERIC(" + getNumericPrecision() + "," + getNumericScale() + "))";
	}

	@Override
	public String doDateAtTimeZoneTransform(String dateSQL, TimeZone timeZone) throws UnsupportedOperationException {
		throw new UnsupportedOperationException("H2DBDefinition does not support doDateAtTimeZoneTransform(String, TimeZone) yet.");
	}

	@Override
	public String doDateAddDaysTransform(String dayValue, String numberOfDays) {
		return "DATEADD('day'," + numberOfDays + ", " + dayValue + ")";
	}

	@Override
	public String doDateAddSecondsTransform(String secondValue, String numberOfSeconds) {
		return "DATEADD('second'," + numberOfSeconds + "," + secondValue + ")";
	}

	@Override
	public String doDateAddMinutesTransform(String secondValue, String numberOfMinutes) {
		return "DATEADD('minute'," + numberOfMinutes + "," + secondValue + ")";
	}

	@Override
	public String doDateAddHoursTransform(String dateValue, String numberOfHours) {
		return "DATEADD('hour'," + numberOfHours + "," + dateValue + ")";
	}

	@Override
	public String doDateAddWeeksTransform(String dateValue, String numberOfWeeks) {
		return "DATEADD('WEEK'," + numberOfWeeks + "," + dateValue + ")";
	}

	@Override
	public String doDateAddMonthsTransform(String dateValue, String numberOfMonths) {
		return "DATEADD('month'," + numberOfMonths + "," + dateValue + ")";
	}

	@Override
	public String doDateAddYearsTransform(String dateValue, String numberOfYears) {
		return "DATEADD('year'," + numberOfYears + "," + dateValue + ")";
	}

	@Override
	public String doInstantAddDaysTransform(String instantValue, String numberOfDays) {
		return doInsertInstantTimeZoneTransform(instantValue, "DATEADD('day'," + numberOfDays + ", " + doRemoveInstantTimeZoneTransform(instantValue) + ")");
	}

	@Override
	public String doInstantAddSecondsTransform(String instantValue, String numberOfSeconds) {
		return doInsertInstantTimeZoneTransform(instantValue, "DATEADD('second'," + numberOfSeconds + "," + doRemoveInstantTimeZoneTransform(instantValue) + ")");
	}

	@Override
	public String doInstantAddMinutesTransform(String instantValue, String numberOfMinutes) {
		return doInsertInstantTimeZoneTransform(instantValue, "DATEADD('minute'," + numberOfMinutes + "," + doRemoveInstantTimeZoneTransform(instantValue) + ")");
	}

	@Override
	public String doInstantAddHoursTransform(String instantValue, String numberOfHours) {
		return doInsertInstantTimeZoneTransform(instantValue, "DATEADD('hour'," + numberOfHours + "," + doRemoveInstantTimeZoneTransform(instantValue) + ")");
	}

	@Override
	public String doInstantAddWeeksTransform(String instantValue, String numberOfWeeks) {
		return doInsertInstantTimeZoneTransform(instantValue, "DATEADD('WEEK'," + numberOfWeeks + "," + doRemoveInstantTimeZoneTransform(instantValue) + ")");
	}

	@Override
	public String doInstantAddMonthsTransform(String instantValue, String numberOfMonths) {
		return doInsertInstantTimeZoneTransform(instantValue, "DATEADD('month'," + numberOfMonths + "," + doRemoveInstantTimeZoneTransform(instantValue) + ")");
	}

	@Override
	public String doInstantAddYearsTransform(String instantValue, String numberOfYears) {
		return doInsertInstantTimeZoneTransform(instantValue, "DATEADD('year'," + numberOfYears + "," + doRemoveInstantTimeZoneTransform(instantValue) + ")");
	}

	/**
	 * Defines the function used to get the current timestamp from the database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the H@ implementation subtracts the time zone from the current
	 * timestamp
	 */
	@Override
	protected String getCurrentDateTimeFunction() {
		return " CURRENT_TIMESTAMP(9) ";
	}

//	@Override
//	public String getDefaultTimeZoneSign() {
//		return "case when extract(timezone_hour from current_timestamp(9))>=0 then '+' else '-' end";
//	}
//
//	@Override
//	public String getDefaultTimeZoneHour() {
//		return "extract(timezone_hour from current_timestamp(9))";
//	}
//
//	@Override
//	public String getDefaultTimeZoneMinute() {
//		return "extract(timezone_minute from current_timestamp(9))";
//	}

	@Override
	public String doDayOfWeekTransform(String dateSQL) {
		return " DAY_OF_WEEK(" + dateSQL + ")";
	}

	@Override
	public String doInstantDayOfWeekTransform(String dateSQL) {
		return " DAY_OF_WEEK(" + doComparableInstantTransform(dateSQL) + ")";
	}

	/**
	 * Returns the instant expression in the standard format that can be used to
	 * have consistent comparisons.
	 *
	 * <p>
	 * This generally adds the timezone back into the instant to convert it into a
	 * local datetime for databases, like H2, which have only partial support for
	 * Timestamp With Time Zone.</p>
	 *
	 * @param instantExpression the instant datatype expression to make comparable
	 * @return string the instantexpression converted into a comparable expression
	 */
	@Override
	public String doComparableInstantTransform(String instantExpression) {
		return doRemoveInstantTimeZoneTransform(instantExpression);
	}

	@Override
	public String doBooleanArrayTransform(Boolean[] bools) {
		StringBuilder str = new StringBuilder();
		str.append("(");
		String separator = "";
		switch (bools.length) {
			case 0:
				return "()";
			case 1:
				return "(" + bools[0] + ",)";
			default:
				for (Boolean bool : bools) {
					str.append(separator).append(bool.toString().toUpperCase());
					separator = ",";
				}
				str.append(")");
				return str.toString();
		}
	}

	@Override
	public String doDateMinusToDateRepeatTransformation(String leftHandSide, String rightHandSide) {
		return " " + DateRepeatFunctions.CREATE + "(" + leftHandSide + ", " + rightHandSide + ")";
	}

	@Override
	public String doDatePlusDateRepeatTransform(String leftHandSide, String rightHandSide) {
		return " " + DateRepeatFunctions.DATEADDITION + "(" + leftHandSide + ", " + rightHandSide + ")";
	}

	@Override
	public String doDateMinusDateRepeatTransform(String leftHandSide, String rightHandSide) {
		return " " + DateRepeatFunctions.DATESUBTRACTION + "(" + leftHandSide + ", " + rightHandSide + ")";
	}

	@Override
	public String doDateRepeatEqualsTransform(String leftHandSide, String rightHandSide) {
		return " " + DateRepeatFunctions.EQUALS + "(" + leftHandSide + ", " + rightHandSide + ")";
	}

	@Override
	public String doDateRepeatNotEqualsTransform(String leftHandSide, String rightHandSide) {
		return " " + DateRepeatFunctions.NOTEQUALS + "(" + leftHandSide + ", " + rightHandSide + ")";
	}

	@Override
	public String doDateRepeatLessThanTransform(String leftHandSide, String rightHandSide) {
		return DateRepeatFunctions.LESSTHAN + "(" + leftHandSide + ", " + rightHandSide + ")";
	}

	@Override
	public String doDateRepeatLessThanEqualsTransform(String leftHandSide, String rightHandSide) {
		return DateRepeatFunctions.LESSTHANEQUALS + "(" + leftHandSide + ", " + rightHandSide + ")";
	}

	@Override
	public String doDateRepeatGreaterThanTransform(String leftHandSide, String rightHandSide) {
		return DateRepeatFunctions.GREATERTHAN + "(" + leftHandSide + ", " + rightHandSide + ")";
	}

	@Override
	public String doDateRepeatGreaterThanEqualsTransform(String leftHandSide, String rightHandSide) {
		return DateRepeatFunctions.GREATERTHANEQUALS + "(" + leftHandSide + ", " + rightHandSide + ")";
	}

	@Override
	public String doDateRepeatGetYearsTransform(String intervalStr) {
		return DateRepeatFunctions.YEAR_PART + "(" + intervalStr + ")";
	}

	@Override
	public String doDateRepeatGetMonthsTransform(String intervalStr) {
		return DateRepeatFunctions.MONTH_PART + "(" + intervalStr + ")";
	}

	@Override
	public String doDateRepeatGetDaysTransform(String intervalStr) {
		return DateRepeatFunctions.DAY_PART + "(" + intervalStr + ")";
	}

	@Override
	public String doDateRepeatGetHoursTransform(String intervalStr) {
		return DateRepeatFunctions.HOUR_PART + "(" + intervalStr + ")";
	}

	@Override
	public String doDateRepeatGetMinutesTransform(String intervalStr) {
		return DateRepeatFunctions.MINUTE_PART + "(" + intervalStr + ")";
	}

	@Override
	public String doDateRepeatGetSecondsTransform(String intervalStr) {
		return DateRepeatFunctions.SECOND_PART + "(" + intervalStr + ")";
	}

	@Override
	public String doLine2DAsTextTransform(String toSQLString) {
		return Line2DFunctions.ASTEXT + "(" + toSQLString + ")";
	}

	@Override
	public String doLine2DEqualsTransform(String toSQLString, String toSQLString0) {
		return Line2DFunctions.EQUALS + "(" + toSQLString + ", " + toSQLString0 + ")";
	}

	@Override
	public String doLine2DMeasurableDimensionsTransform(String toSQLString) {
		return Line2DFunctions.DIMENSION + "(" + toSQLString + ")";
	}

	@Override
	public String doLine2DGetBoundingBoxTransform(String toSQLString) {
		return Line2DFunctions.BOUNDINGBOX + "(" + toSQLString + ")";
	}

	@Override
	public String doLine2DGetMaxXTransform(String toSQLString) {
		return Line2DFunctions.MAXX + "(" + toSQLString + ")";
	}

	@Override
	public String doLine2DGetMinXTransform(String toSQLString) {
		return Line2DFunctions.MINX + "(" + toSQLString + ")";
	}

	@Override
	public String doLine2DGetMaxYTransform(String toSQLString) {
		return Line2DFunctions.MAXY + "(" + toSQLString + ")";
	}

	@Override
	public String doLine2DGetMinYTransform(String toSQLString) {
		return Line2DFunctions.MINY + "(" + toSQLString + ")";
	}

	@Override
	public String doLine2DIntersectsLine2DTransform(String toSQLString, String toSQLString0) {
		return Line2DFunctions.INTERSECTS_LINE2D + "((" + toSQLString + "), (" + toSQLString0 + "))";
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
	public String doPoint2DEqualsTransform(String firstPoint, String secondPoint) {
		return Point2DFunctions.EQUALS + "(" + firstPoint + ", " + secondPoint + ")";
	}

	@Override
	public String doPoint2DGetXTransform(String toSQLString) {
		return Point2DFunctions.GETX + "(" + toSQLString + ")";
	}

	@Override
	public String doPoint2DGetYTransform(String toSQLString) {
		return Point2DFunctions.GETY + "(" + toSQLString + ")";
	}

	@Override
	public String doPoint2DMeasurableDimensionsTransform(String toSQLString) {
		return Point2DFunctions.DIMENSION + "(" + toSQLString + ")";
	}

	@Override
	public String doPoint2DGetBoundingBoxTransform(String toSQLString) {
		return Point2DFunctions.BOUNDINGBOX + "(" + toSQLString + ")";
	}

	@Override
	public String transformCoordinatesIntoDatabasePoint2DFormat(String xValue, String yValue) {
		return "'POINT (" + xValue + " " + yValue + ")'";
	}

	@Override
	public String doPoint2DAsTextTransform(String toSQLString) {
		return Point2DFunctions.ASTEXT + "(" + toSQLString + ")";
	}

	@Override
	public String transformPolygonIntoDatabasePolygon2DFormat(Polygon geom) {
		String wktValue = geom.toText();
		return Polygon2DFunctions.CREATE_FROM_WKTPOLYGON2D.alias() + "('" + wktValue + "')";
	}

	@Override
	public String doPolygon2DAsTextTransform(String polygonSQL) {
		return polygonSQL;
	}

	@Override
	public String doPolygon2DEqualsTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.EQUALS.alias() + "(" + firstGeometry + ", " + secondGeometry + ") ";
	}

	@Override
	public String doPolygon2DUnionTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.INTERSECTION.alias() + "(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DIntersectionTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.INTERSECTION.alias() + "(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DIntersectsTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.INTERSECTS.alias() + "(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DContainsPolygon2DTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.CONTAINS_POLYGON2D.alias() + "(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DContainsPoint2DTransform(String polygon2DSQL, String point2DSQL) {
		return Polygon2DFunctions.CONTAINS_POINT2D.alias() + "(" + polygon2DSQL + ", " + point2DSQL + ")";
	}

	@Override
	public String doPolygon2DDoesNotIntersectTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.DISJOINT.alias() + "(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DOverlapsTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.OVERLAPS.alias() + "(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DTouchesTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.TOUCHES.alias() + "(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DWithinTransform(String firstGeometry, String secondGeometry) {
		//indicate whether g1 is spatially within g2. This is the inverse of Contains(). 
		// i.e. G1.within(G2) === G2.contains(G1)
		return Polygon2DFunctions.WITHIN.alias() + "(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DMeasurableDimensionsTransform(String toSQLString) {
		return Polygon2DFunctions.DIMENSION.alias() + "(" + toSQLString + ")";
	}

	@Override
	public String doPolygon2DGetBoundingBoxTransform(String toSQLString) {
		return Polygon2DFunctions.BOUNDINGBOX.alias() + "(" + toSQLString + ")";
	}

	@Override
	public String doPolygon2DGetAreaTransform(String toSQLString) {
		return Polygon2DFunctions.AREA.alias() + "(" + toSQLString + ")";
	}

	@Override
	public String doPolygon2DGetExteriorRingTransform(String toSQLString) {
		return Polygon2DFunctions.EXTERIORRING.alias() + "(" + toSQLString + ")";
	}

	@Override
	public String doPolygon2DGetMaxXTransform(String toSQLString) {
		return Polygon2DFunctions.MAX_X.alias() + "(" + toSQLString + ")";
	}

	@Override
	public String doPolygon2DGetMinXTransform(String toSQLString) {
		return Polygon2DFunctions.MIN_Y.alias() + "(" + toSQLString + ")";
	}

	@Override
	public String doPolygon2DGetMaxYTransform(String toSQLString) {
		return Polygon2DFunctions.MAX_Y.alias() + "(" + toSQLString + ")";
	}

	@Override
	public String doPolygon2DGetMinYTransform(String toSQLString) {
		return Polygon2DFunctions.MIN_Y.alias() + "(" + toSQLString + ")";
	}

	@Override
	public String doLineSegment2DIntersectsLineSegment2DTransform(String toSQLString, String toSQLString0) {
		return LineSegment2DFunctions.INTERSECTS_LINESEGMENT2D + "((" + toSQLString + "), (" + toSQLString0 + "))";
	}

	@Override
	public String doLineSegment2DGetMaxXTransform(String toSQLString) {
		return LineSegment2DFunctions.MAXX + "(" + toSQLString + ")";
	}

	@Override
	public String doLineSegment2DGetMinXTransform(String toSQLString) {
		return LineSegment2DFunctions.MINX + "(" + toSQLString + ")";
	}

	@Override
	public String doLineSegment2DGetMaxYTransform(String toSQLString) {
		return LineSegment2DFunctions.MAXY + "(" + toSQLString + ")";
	}

	@Override
	public String doLineSegment2DGetMinYTransform(String toSQLString) {
		return LineSegment2DFunctions.MINY + "(" + toSQLString + ")";
	}

	@Override
	public String doLineSegment2DGetBoundingBoxTransform(String toSQLString) {
		return LineSegment2DFunctions.BOUNDINGBOX + "(" + toSQLString + ")";
	}

	@Override
	public String doLineSegment2DDimensionTransform(String toSQLString) {
		return LineSegment2DFunctions.DIMENSION + "(" + toSQLString + ")";
	}

	@Override
	public String doLineSegment2DNotEqualsTransform(String toSQLString, String toSQLString0) {
		return "!" + LineSegment2DFunctions.EQUALS + "((" + toSQLString + "), (" + toSQLString0 + "))";
	}

	@Override
	public String doLineSegment2DEqualsTransform(String toSQLString, String toSQLString0) {
		return LineSegment2DFunctions.EQUALS + "((" + toSQLString + "), (" + toSQLString0 + "))";
	}

	@Override
	public String doLineSegment2DAsTextTransform(String toSQLString) {
		return LineSegment2DFunctions.ASTEXT + "(" + toSQLString + ")";
	}

	@Override
	public String doLineSegment2DIntersectionPointWithLineSegment2DTransform(String toSQLString, String toSQLString0) {
		return LineSegment2DFunctions.INTERSECTIONPOINT_LINESEGMENT2D + "((" + toSQLString + "), (" + toSQLString0 + "))";
	}

	@Override
	public String doMultiPoint2DEqualsTransform(String first, String second) {
		return MultiPoint2DFunctions.EQUALS + "((" + first + "), (" + second + "), " + MultiPoint2DFunctions.getCurrentVersion() + ")";
	}

	@Override
	public String doMultiPoint2DGetPointAtIndexTransform(String first, String index) {
		return MultiPoint2DFunctions.GETPOINTATINDEX_FUNCTION + "((" + first + "), (" + index + "), " + MultiPoint2DFunctions.getCurrentVersion() + ")";
	}

	@Override
	public String doMultiPoint2DGetNumberOfPointsTransform(String first) {
		return MultiPoint2DFunctions.GETNUMBEROFPOINTS_FUNCTION + "(" + first + ", " + MultiPoint2DFunctions.getCurrentVersion() + ")";
	}

	@Override
	public String doMultiPoint2DMeasurableDimensionsTransform(String first) {
		return MultiPoint2DFunctions.DIMENSION + "(" + first + ", " + MultiPoint2DFunctions.getCurrentVersion() + ")";
	}

	@Override
	public String doMultiPoint2DGetBoundingBoxTransform(String first) {
		return MultiPoint2DFunctions.BOUNDINGBOX + "(" + first + ", " + MultiPoint2DFunctions.getCurrentVersion() + ")";
	}

	@Override
	public String doMultiPoint2DAsTextTransform(String first) {
		return MultiPoint2DFunctions.ASTEXT + "(" + first + ", " + MultiPoint2DFunctions.getCurrentVersion() + ")";
	}

	@Override
	public String doMultiPoint2DToLine2DTransform(String first) {
		return MultiPoint2DFunctions.ASLINE2D + "(" + first + ", " + MultiPoint2DFunctions.getCurrentVersion() + ")";
	}

	@Override
	public String doMultiPoint2DGetMinYTransform(String first) {
		return MultiPoint2DFunctions.MINY + "(" + first + ", " + MultiPoint2DFunctions.getCurrentVersion() + ")";
	}

	@Override
	public String doMultiPoint2DGetMinXTransform(String first) {
		return MultiPoint2DFunctions.MINX + "(" + first + ", " + MultiPoint2DFunctions.getCurrentVersion() + ")";
	}

	@Override
	public String doMultiPoint2DGetMaxYTransform(String first) {
		return MultiPoint2DFunctions.MAXY + "(" + first + ", " + MultiPoint2DFunctions.getCurrentVersion() + ")";
	}

	@Override
	public String doMultiPoint2DGetMaxXTransform(String first) {
		return MultiPoint2DFunctions.MAXX + "(" + first + ", " + MultiPoint2DFunctions.getCurrentVersion() + ")";
	}

	@Override
	public LargeObjectHandlerType preferredLargeObjectWriter(DBLargeObject<?> lob) {
		if (lob instanceof DBLargeText) {
			return LargeObjectHandlerType.CLOB;
		} else if (lob instanceof DBJavaObject) {
			return LargeObjectHandlerType.BINARYSTREAM;
		} else {
			return super.preferredLargeObjectWriter(lob);
		}
	}

	@Override
	public LargeObjectHandlerType preferredLargeObjectReader(DBLargeObject<?> lob) {
		if (lob instanceof DBLargeText) {
			return LargeObjectHandlerType.BLOB;
		} else if (lob instanceof DBJavaObject) {
			return LargeObjectHandlerType.BINARYSTREAM;
		} else {
			return super.preferredLargeObjectReader(lob);
		}
	}

	@Override
	public boolean supportsFullOuterJoinNatively() {
		return false;
	}

	@Override
	public boolean supportsTableCheckingViaMetaData() {
		return false;
	}

	@Override
	public String doStringAccumulateTransform(String accumulateColumn, String separator, String referencedTable) {
		return "GROUP_CONCAT(" + accumulateColumn + " SEPARATOR " + doStringLiteralWrapping(separator) + ")";
	}

	@Override
	public String doStringAccumulateTransform(String accumulateColumn, String separator, String orderByColumnName, String referencedTable) {
		return "GROUP_CONCAT(" + accumulateColumn + " ORDER BY " + orderByColumnName + " SEPARATOR " + doStringLiteralWrapping(separator) + ")";
	}

	/**
	 * Creates the CURRENTTIME function for this database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the default implementation returns " CURRENT_TIMESTAMP "
	 */
	@Override
	public String doCurrentUTCTimeTransform() {
		return "/*doCurrentUTCTimeTransform*/dateadd(timezone_minute, -1*extract(timezone_minute from current_timestamp(9)), dateadd(timezone_hour, -1*extract(timezone_hour from current_timestamp(9)), dateadd(minute, -1*extract(timezone_minute from current_timestamp(9)), dateadd(hour, -1*extract(timezone_hour from current_timestamp(9)), current_timestamp(9)))))";
	}

	@Override
	public String doCurrentUTCDateTimeTransform() {
		return "/*doCurrentUTCDateTimeTransform*/dateadd(timezone_minute, -1*extract(timezone_minute from current_timestamp(9)), dateadd(timezone_hour, -1*extract(timezone_hour from current_timestamp(9)), dateadd(minute, -1*extract(timezone_minute from current_timestamp(9)), dateadd(hour, -1*extract(timezone_hour from current_timestamp(9)), current_timestamp(9)))))";
	}

	public String doRemoveInstantTimeZoneTransform(String instantValue) {
		return "/*remove timezone*/dateadd(minute, -1*extract(timezone_minute from " + instantValue + "), dateadd(hour, -1*extract(timezone_hour from " + instantValue + "), " + instantValue + "))/*!remove timezone*/";
	}

	public String doInsertInstantTimeZoneTransform(String instantValueWithCorrectTZ, String dateValue) {
		return "/*insert timezone*/dateadd(minute, extract(timezone_minute from " + instantValueWithCorrectTZ + "), dateadd(hour, extract(timezone_hour from " + instantValueWithCorrectTZ + "), " + dateValue + "))/*!insert timezone*/";
	}

	@Override
	public boolean supportsDateRepeatDatatypeFunctions() {
		return true;
	}

	@Override
	public GroupByClauseMethod[] preferredGroupByClauseMethod() {
		return new GroupByClauseMethod[]{GroupByClauseMethod.GROUPBYEXPRESSION,GroupByClauseMethod.SELECTEXPRESSION, GroupByClauseMethod.ALIAS, GroupByClauseMethod.INDEX};
	}

}
