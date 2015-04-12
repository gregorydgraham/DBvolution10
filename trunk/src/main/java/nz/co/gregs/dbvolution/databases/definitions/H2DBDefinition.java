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

import com.vividsolutions.jts.geom.Polygon;
import java.text.SimpleDateFormat;
import java.util.Date;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.H2DB;
import nz.co.gregs.dbvolution.datatypes.DBDateRepeat;
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
public class H2DBDefinition extends DBDefinition {

	private final String dateFormatStr = "yyyy-M-d HH:mm:ss Z";
	private final String h2DateFormatStr = "yyyy-M-d HH:mm:ss Z";
	private final SimpleDateFormat strToDateFormat = new SimpleDateFormat(dateFormatStr);

	@Override
	public String getDateFormattedForQuery(Date date) {
		if (date == null) {
			return getNull();
		}
		return "PARSEDATETIME('" + strToDateFormat.format(date) + "','" + h2DateFormatStr + "')";
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
	protected String getSQLTypeOfDBDatatype(QueryableDatatype qdt) {
		if (qdt instanceof DBDateRepeat) {
			return DateRepeatFunctions.DATATYPE;
		} else if (qdt instanceof DBPoint2D) {
			return Point2D.POINT2D.datatype();
		} else if (qdt instanceof DBLine2D) {
			return Line2D.LINE2D.datatype();
		} else if (qdt instanceof DBPolygon2D) {
			return Polygon2D.POLYGON2D.datatype();
		} else {
			return super.getSQLTypeOfDBDatatype(qdt);
		}
	}

	@Override
	public String doStringLengthTransform(String enclosedValue) {
		return " CAST(" + getStringLengthFunctionName() + "( " + enclosedValue + " ) as NUMERIC(15,10))";
	}

	@Override
	public String doAddDaysTransform(String dayValue, String numberOfDays) {
		return "DATEADD('day'," + numberOfDays + ", " + dayValue + ")";
	}

//	@Override
//	public String doAddMillisecondsTransform(String secondValue, String numberOfSeconds) {
//		return "DATEADD('millisecond'," + numberOfSeconds + "," + secondValue + ")";
//	}
	@Override
	public String doAddSecondsTransform(String secondValue, String numberOfSeconds) {
		return "DATEADD('second'," + numberOfSeconds + "," + secondValue + ")";
	}

	@Override
	public String doAddMinutesTransform(String secondValue, String numberOfMinutes) {
		return "DATEADD('minute'," + numberOfMinutes + "," + secondValue + ")";
	}

	@Override
	public String doAddHoursTransform(String hourValue, String numberOfSeconds) {
		return "DATEADD('hour'," + numberOfSeconds + "," + hourValue + ")";
	}

	@Override
	public String doAddWeeksTransform(String dateValue, String numberOfWeeks) {
		return "DATEADD('WEEK'," + numberOfWeeks + "," + dateValue + ")";
	}

	@Override
	public String doAddMonthsTransform(String dateValue, String numberOfMonths) {
		return "DATEADD('month'," + numberOfMonths + "," + dateValue + ")";
	}

	@Override
	public String doAddYearsTransform(String dateValue, String numberOfYears) {
		return "DATEADD('year'," + numberOfYears + "," + dateValue + ")";
	}

	/**
	 * Defines the function used to get the current timestamp from the database.
	 *
	 * @return the H@ implementation subtracts the time zone from the current
	 * timestamp
	 */
	@Override
	protected String getCurrentDateTimeFunction() {
//		SimpleDateFormat format = new SimpleDateFormat("Z");
//		long rawTimezone = Long.parseLong(format.format(new Date()).replaceAll("\\+", ""));
//		long timezone = rawTimezone/100+((rawTimezone%100)*(100/60));
//		return " DATEADD('hour',-1* "+timezone+",CURRENT_TIMESTAMP )";
		return " CURRENT_TIMESTAMP ";
	}

	@Override
	public String doDayOfWeekTransform(String dateSQL) {
		return " DAY_OF_WEEK(" + dateSQL + ")";
	}

	@Override
	public String doBooleanArrayTransform(Boolean[] bools) {
		StringBuilder str = new StringBuilder();
		str.append("(");
		String separator = "";
		if (bools.length == 0) {
			return "()";
		} else if (bools.length == 1) {
			return "(" + bools[0] + ",)";
		} else {
			for (Boolean bool : bools) {
				str.append(separator).append(bool.toString().toUpperCase());
				separator = ",";
			}
			str.append(")");
			return str.toString();
		}
	}

//	@Override
//	public String transformPeriodIntoDateRepeat(Period interval) {
//		return "'"+DateRepeatImpl.getIntervalString(interval)+"'";
//	}
//	@Override
//	public Period parseDateRepeatFromGetString(String intervalStr) {
//		return DateRepeatImpl.parseDateRepeatFromGetString(intervalStr);
//	}
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
		return " " + DateRepeatFunctions.DATEREPEAT_EQUALS_FUNCTION + "(" + leftHandSide + ", " + rightHandSide + ")";
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
	public String doLine2DAsTextTransform(String toSQLString) {
		return Line2DFunctions.ASTEXT + "(" + toSQLString + ")";
	}

	@Override
	public String doLine2DEqualsTransform(String toSQLString, String toSQLString0) {
		return Line2DFunctions.EQUALS + "(" + toSQLString + ", " + toSQLString0 + ")";
	}

	@Override
	public String doLine2DDimensionTransform(String toSQLString) {
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
	public String doPoint2DDimensionTransform(String toSQLString) {
		return Point2DFunctions.DIMENSION + "(" + toSQLString + ")";
	}

	@Override
	public String doPoint2DGetBoundingBoxTransform(String toSQLString) {
		return Point2DFunctions.BOUNDINGBOX + "(" + toSQLString + ")";
	}

	@Override
	public String transformCoordinatesIntoDatabasePointFormat(String xValue, String yValue) {
		return "'POINT (" + xValue + " " + yValue + ")'";
	}

	@Override
	public String doPoint2DAsTextTransform(String toSQLString) {
		return Point2DFunctions.ASTEXT + "(" + toSQLString + ")";
	}

	@Override
	public String doDBPolygon2DFormatTransform(Polygon geom) {
		String wktValue = geom.toText();
		return Polygon2DFunctions.CREATE_FROM_WKTPOLYGON2D.alias() + "('" + wktValue + "')";
	}

	@Override
	public String doPolygon2DEqualsTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.EQUALS.alias() + "(" + firstGeometry + ", " + secondGeometry + ") ";
	}

	@Override
	public String doPolygon2DIntersectsTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.INTERSECTS.alias() + "(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DContainsTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.CONTAINS.alias() + "(" + firstGeometry + ", " + secondGeometry + ")";
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
	public String doPolygon2DGetDimensionTransform(String toSQLString) {
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
}
