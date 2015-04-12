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

import com.vividsolutions.jts.geom.*;
import java.text.*;
import java.util.*;
import nz.co.gregs.dbvolution.databases.PostgresDB;
import nz.co.gregs.dbvolution.databases.PostgresDBOverSSL;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.datatypes.spatial2D.*;
import nz.co.gregs.dbvolution.internal.postgres.Line2DFunctions;
import nz.co.gregs.dbvolution.internal.postgres.StringFunctions;

/**
 * Defines the features of the PostgreSQL database that differ from the standard
 * database.
 *
 * <p>
 * This DBDefinition is automatically included in {@link PostgresDB} and
 * {@link PostgresDBOverSSL} instances, and you should not need to use it
 * directly.
 *
 * @author Gregory Graham
 */
public class PostgresDBDefinition extends DBDefinition {

	private static final DateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyyMMdd HH:mm:ss.SSS");

	private static final String[] reservedWordsArray = new String[]{"LIMIT", "END"};
	private static final List<String> reservedWords = Arrays.asList(reservedWordsArray);


	@Override
	public String getDropDatabase(String databaseName) throws UnsupportedOperationException {
		return "DROP DATABASE IF EXISTS '"+databaseName+"';";
	}

	@Override
	public String getDropTableStart() {
		return super.getDropTableStart() + " IF EXISTS "; //To change body of generated methods, choose Tools | Templates.
	}
	
	@Override
	public String formatPrimaryKeyForRetrievingGeneratedKeys(String primaryKeyColumnName) {
		return primaryKeyColumnName.toLowerCase();
	}
	
	@Override
	protected String formatNameForDatabase(final String sqlObjectName) {
		if (!(reservedWords.contains(sqlObjectName.toUpperCase()))) {
			return super.formatNameForDatabase(sqlObjectName);
		} else {
			return formatNameForDatabase("p" + super.formatNameForDatabase(sqlObjectName));
		}
	}
	
	@Override
	public String getDateFormattedForQuery(Date date) {
		return "('" + DATETIME_FORMAT.format(date) + "'::timestamp)";
	}

	@Override
	public String getSQLTypeOfDBDatatype(QueryableDatatype qdt) {
		if (qdt instanceof DBByteArray) {
			return " BYTEA ";
		} else if (qdt instanceof DBLargeObject) {
			return " BYTEA ";
		} else if (qdt instanceof DBBoolean) {
			return " BOOLEAN ";
		} else  if (qdt instanceof DBBooleanArray) {
			return " BOOL[] ";
		} else  if (qdt instanceof DBLine2D) {
			return " PATH ";
		} else {
			return super.getSQLTypeOfDBDatatype(qdt);
		}
	}

	@Override
	public Object getOrderByDirectionClause(Boolean sortOrder) {
		if (sortOrder == null) {
			return "";
		} else if (sortOrder) {
			return " ASC NULLS FIRST ";
		} else {
			return " DESC NULLS LAST ";
		}
	}

	@Override
	public String doTruncTransform(String firstString, String secondString) {
		return getTruncFunctionName() + "((" + firstString + ")::numeric, " + secondString + ")";
	}

	@Override
	public String doBitsToIntegerTransform(String columnName) {
		return columnName + "::integer";
	}

	@Override
	public String doIntegerToBitTransform(String columnName) {
		return columnName + "::bit";
	}

	@Override
	public String doBitsValueTransform(boolean[] boolArray) {
		String boolStr = "";
		for (boolean c : boolArray) {
			if (c) {
				boolStr += "1";
			} else {
				boolStr += "0";
			}
		}
		return "B'" + boolStr + "'";
	}

	@Override
	public String getColumnAutoIncrementSuffix() {
		return "";
	}

	@Override
	protected boolean hasSpecialAutoIncrementType() {
		return true;
	}

	@Override
	protected String getSpecialAutoIncrementType() {
		return " SERIAL ";
	}

	@Override
	public boolean supportsModulusFunction() {
		return false;
	}

//	@Override
//	public String doAddMillisecondsTransform(String dateValue, String numberOfSeconds) {
//		return "(" + dateValue + "+ (" + numberOfSeconds + ")*INTERVAL '1 MILLISECOND' )";
//	}
	
	@Override
	public String doAddSecondsTransform(String dateValue, String numberOfSeconds) {
		return "(" + dateValue + "+ (" + numberOfSeconds + ")*INTERVAL '1 SECOND' )";
	}

	@Override
	public String doAddMinutesTransform(String dateValue, String numberOfMinutes) {
		return "(" + dateValue + "+ (" + numberOfMinutes + ")*INTERVAL '1 MINUTE' )";
	}

	@Override
	public String doAddDaysTransform(String dateValue, String numberOfDays) {
		return "(" + dateValue + "+ (" + numberOfDays + ")*INTERVAL '1 DAY' )";
	}

	@Override
	public String doAddHoursTransform(String dateValue, String numberOfHours) {
		return "(" + dateValue + "+ (" + numberOfHours + ")*INTERVAL '1 HOUR')";
	}

	@Override
	public String doAddWeeksTransform(String dateValue, String numberOfWeeks) {
		return "(" + dateValue + "+ (" + numberOfWeeks + ")*INTERVAL '1 WEEK')";
	}

	@Override
	public String doAddMonthsTransform(String dateValue, String numberOfWeeks) {
		return "(" + dateValue + "+ (" + numberOfWeeks + ")*INTERVAL '1 MONTH')";
	}

	@Override
	public String doAddYearsTransform(String dateValue, String numberOfWeeks) {
		return "(" + dateValue + "+ (" + numberOfWeeks + ")*(INTERVAL '1 YEAR'))";
	}

	@Override
	public String doBooleanValueTransform(Boolean boolValue) {
		return "" + (boolValue ? 1 : 0) + "::bool";
	}

	@Override
	public String doCurrentDateOnlyTransform() {
		return super.getCurrentDateOnlyFunctionName(); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doDayDifferenceTransform(String dateValue, String otherDateValue) {
		return "(EXTRACT(DAY from (" + otherDateValue + ")-(" + dateValue + ")))";
	}

	@Override
	public String doWeekDifferenceTransform(String dateValue, String otherDateValue) {
		return "ROUND(" + doDayDifferenceTransform(dateValue, otherDateValue) + "/7)";
	}

	private String doAgeTransformation(String dateValue, String otherDateValue) {
		return "age((" + dateValue + "), (" + otherDateValue + "))";
	}

	@Override
	public String doMonthDifferenceTransform(String dateValue, String otherDateValue) {
		return "ROUND(EXTRACT(YEAR FROM " + doAgeTransformation(dateValue, otherDateValue) + ") * 12 + EXTRACT(MONTH FROM " + doAgeTransformation(dateValue, otherDateValue) + ")*-1)";
	}

	@Override
	public String doYearDifferenceTransform(String dateValue, String otherDateValue) {
		return "round(EXTRACT(YEAR FROM " + doAgeTransformation(dateValue, otherDateValue) + ")*-1)";
	}

	@Override
	public String doHourDifferenceTransform(String dateValue, String otherDateValue) {
		return "round(EXTRACT(EPOCH FROM (" + dateValue + ") - (" + otherDateValue + ")) / -3600)";
	}

	@Override
	public String doMinuteDifferenceTransform(String dateValue, String otherDateValue) {
		return "round(EXTRACT(EPOCH FROM (" + dateValue + ") - (" + otherDateValue + ")) / -60)";
	}

	@Override
	public String doSecondDifferenceTransform(String dateValue, String otherDateValue) {
		return "round(EXTRACT(EPOCH FROM (" + dateValue + ") - (" + otherDateValue + "))*-1)";
	}

//	@Override
//	public String doMillisecondDifferenceTransform(String dateValue, String otherDateValue) {
//		return "round(EXTRACT(EPOCH FROM (" + dateValue + ") - (" + otherDateValue + "))*-1000)";
//	}

	@Override
	public String doDayOfWeekTransform(String dateSQL) {
		return " (EXTRACT(DOW FROM ("+dateSQL+"))+1)";
	}

	@Override
	public String doBooleanArrayTransform(Boolean[] bools) {
		StringBuilder str = new StringBuilder();
		str.append("'{");
		String separator = "";
		if (bools.length == 0) {
			return "'{}'";
		} else if (bools.length == 1) {
			return "'{" + bools[0] + "}'";
		} else {
			for (Boolean bool : bools) {
				str.append(separator).append(bool?1:0);
				separator =",";
			}
			str.append("}'");
			return str.toString();
		}
	}
	
	@Override
	public String doPolygon2DIntersectsTransform(String firstGeometry, String secondGeometry) {
		return "ST_Intersects(" + firstGeometry + ", " + secondGeometry + ")";
	}
	
//	@Override
//	public String doDateRepeatGetYearsTransform(String interval) {
//		return doTruncTransform("(EXTRACT(DAY FROM "+interval+")/365.25)","0");
//	}
//	
//	@Override
//	public String doDateRepeatGetMonthsTransform(String interval) {
//		return doModulusTransform(doTruncTransform("((EXTRACT(DAY FROM "+interval+")/30.4375))","0"), "12");
//	}
//	
//	@Override
//	public String doDateRepeatGetDaysTransform(String interval) {
//		return "(EXTRACT(DAY FROM "+interval+")";
//	}
//	
//	@Override
//	public String doDateRepeatGetHoursTransform(String interval) {
//		return "EXTRACT(HOUR FROM "+interval+")";
//	}
//	
//	@Override
//	public String doDateRepeatGetMinutesTransform(String interval) {
//		return "EXTRACT(MINUTE FROM "+interval+")";
//	}
//	
//	@Override
//	public String doDateRepeatGetSecondsTransform(String interval) {
//		return "EXTRACT(SECOND FROM "+interval+")";
//	}
//	
//	@Override
//	public String doDateRepeatGetMillisecondsTransform(String interval) {
//		return "EXTRACT(MILLISECONDS FROM "+interval+")";
//	}

	@Override
	public boolean supportsHyperbolicFunctionsNatively() {
		return false;//To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doStringToNumberTransform(String stringResultContainingANumber) {
		return "(CASE WHEN ("+stringResultContainingANumber+") IS NULL OR ("+stringResultContainingANumber+") = '' THEN 0 ELSE TO_NUMBER("+stringResultContainingANumber+", 'S999999999999999D9999999') END)";
	}

	@Override
	public boolean supportsArcSineFunction() {
		return false;
	}

	@Override
	public String transformCoordinatesIntoDatabasePointFormat(String xValue, String yValue) {
		return "POINT (" +xValue+", "+yValue+")";
	}

	@Override
	public String doPoint2DEqualsTransform(String firstPoint, String secondPoint) {
		return "((" + firstPoint + ")~=( " + secondPoint + "))";
	}

	@Override
	public String doPoint2DGetXTransform(String point2D) {
		return "(" + point2D + ")[0]";
	}

	@Override
	public String doPoint2DGetYTransform(String point2D) {
		return "("+point2D + ")[1]";
	}

	@Override
	public String doPoint2DDimensionTransform(String point2D) {
		return " 0 ";
	}

	@Override
	public String doPoint2DGetBoundingBoxTransform(String point2D) {
		return "POLYGON(BOX("+point2D+","+ point2D+"))";
	}

	@Override
	public String doPoint2DAsTextTransform(String point2DString) {
		return "("+point2DString+")::VARCHAR";
	}

	@Override
	public String transformPointIntoDatabaseFormat(Point point) {
		return "POINT (" +point.getX()+", "+point.getY()+")";
	}

	//path '[(0,0),(1,1),(2,0)]'
	@Override
	public String transformLineStringIntoDatabaseFormat(LineString line) {
		StringBuilder str = new  StringBuilder();
		String separator = "";
		Coordinate[] coordinates = line.getCoordinates();
		for (Coordinate coordinate : coordinates) {
			str.append(separator).append("(").append(coordinate.x).append(",").append(coordinate.y).append(")");
			separator=",";
		}
		return "PATH '[" +str+"]'";
	}
	
	@Override
	public Object doColumnTransformForSelect(QueryableDatatype qdt, String selectableName) {
		if (qdt instanceof DBPolygon2D) {
			return "(" + selectableName + ")::VARCHAR";
		} else if (qdt instanceof DBPoint2D) {
			return "(" + selectableName + ")::VARCHAR";
		} else if (qdt instanceof DBLine2D) {
			return "(" + selectableName + ")::VARCHAR";
		} else {
			return selectableName;
		}
	}

	@Override
	public Point transformDatabaseValueToJTSPoint(String pointAsString) throws com.vividsolutions.jts.io.ParseException {
		Point point = null;
		if (pointAsString.matches(" *\\( *[-0-9.]+, *[-0-9.]+ *\\) *")){
			String[] split = pointAsString.split("[^-0-9.]+");
			for (String split1 : split) {
				System.out.println("DATABASE VALUE: "+split1);
			}
		GeometryFactory geometryFactory = new GeometryFactory();
			final double x = Double.parseDouble(split[1]);
			final double y = Double.parseDouble(split[2]);
			point = geometryFactory.createPoint(new Coordinate(x, y));
		} else {
//			throw new IncorrectGeometryReturnedForDatatype(geometry, point);
		}
		return point;
	}
	
	// ((2,3),(2,3),(2,3),(2,3)) => POLYGON ((2 3, 2 3, 2 3, 2 3, 2 3))
	@Override
	public Polygon transformDatabaseValueToJTSPolygon(String geometryAsString) throws com.vividsolutions.jts.io.ParseException {
		String string = "POLYGON "+geometryAsString.replaceAll("\\),\\(", ", ").replaceAll("([-0-9.]+),([-0-9.]+)", "$1 $2");
		String[] splits = geometryAsString.split("[(),]+");
		System.out.println(geometryAsString+" => "+string);
		List<Coordinate> coords = new ArrayList<Coordinate>();
		Coordinate firstCoord = null;
		for (int i = 1; i < splits.length; i++) {
			String splitX = splits[i];
			String splitY = splits[i+1];
			System.out.println("COORD: "+splitX+", "+splitY);
			final Coordinate coordinate = new Coordinate(Double.parseDouble(splitX), Double.parseDouble(splitY));
			coords.add(coordinate);
			if (firstCoord==null){
				firstCoord=coordinate;
			}
			i++;
		}
		coords.add(firstCoord);
		final GeometryFactory geometryFactory = new GeometryFactory();
		Polygon polygon = geometryFactory.createPolygon(coords.toArray(new Coordinate[]{}));
		return polygon;
	}

	@Override
	public LineString transformDatabaseValueToJTSLineString(String lineStringAsString)  throws com.vividsolutions.jts.io.ParseException {
		String string = "LINESTRING "+lineStringAsString.replaceAll("\\),\\(", ", ").replaceAll("([-0-9.]+),([-0-9.]+)", "$1 $2");
		String[] splits = lineStringAsString.split("[(),]+");
		System.out.println(lineStringAsString+" => "+string);
		List<Coordinate> coords = new ArrayList<Coordinate>();
		Coordinate firstCoord = null;
		for (int i = 1; i < splits.length-1; i++) {
			String splitX = splits[i];
			String splitY = splits[i+1];
			System.out.println("COORD: "+splitX+", "+splitY);
			final Coordinate coordinate = new Coordinate(Double.parseDouble(splitX), Double.parseDouble(splitY));
			coords.add(coordinate);
			if (firstCoord==null){
				firstCoord=coordinate;
			}
			i++;
		}
		coords.add(firstCoord);
		final GeometryFactory geometryFactory = new GeometryFactory();
		LineString lineString = geometryFactory.createLineString(coords.toArray(new Coordinate[]{}));
		return lineString;
	}

	@Override
	public String doLine2DEqualsTransform(String firstLineSQL, String secondLineSQL) {
		return "("+firstLineSQL+")::TEXT = ("+secondLineSQL+")::TEXT";
	}
	
	@Override
	public String doLine2DAsTextTransform(String line2DSQL) {
		return "("+line2DSQL+")::TEXT";
	}
	
	@Override
	public String doLine2DGetBoundingBoxTransform(String toSQLString) {
		return Line2DFunctions.BOUNDINGBOX+"(" + toSQLString + ")";
	}

	@Override
	public String doLine2DGetMaxXTransform(String toSQLString) {
		return Line2DFunctions.MAXX+"(" + toSQLString + ")";
	}

	@Override
	public String doLine2DGetMinXTransform(String toSQLString) {
		return Line2DFunctions.MINX+"(" + toSQLString + ")";
	}

	@Override
	public String doLine2DGetMaxYTransform(String toSQLString) {
		return Line2DFunctions.MAXY+"(" + toSQLString + ")";
	}

	@Override
	public String doLine2DGetMinYTransform(String toSQLString) {
		return Line2DFunctions.MINY+"(" + toSQLString + ")";
	}

	@Override
	public String doSubstringBeforeTransform(String fromThis, String beforeThis) {
		return StringFunctions.SUBSTRINGBEFORE+"("+fromThis+", "+beforeThis+")";
	}

	@Override
	public String doSubstringAfterTransform(String fromThis, String afterThis) {
		return StringFunctions.SUBSTRINGAFTER+"("+fromThis+", "+afterThis+")";
	}
}
