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

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import java.text.*;
import java.util.*;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.MSSQLServerDB;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBLine2D;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBPolygon2D;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBPoint2D;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.internal.sqlserver.Line2DFunctions;
import nz.co.gregs.dbvolution.query.QueryOptions;

/**
 * Defines the features of the Microsoft SQL Server database that differ from
 * the standard database.
 *
 * <p>
 * This DBDefinition is automatically included in {@link MSSQLServerDB}
 * instances, and you should not need to use it directly.
 *
 * @author Gregory Graham
 */
public class MSSQLServerDBDefinition extends DBDefinition {

	private static final String[] reservedWordsArray = new String[]{"ADD", "EXTERNAL", "PROCEDURE", "ALL", "FETCH", "PUBLIC", "ALTER", "FILE", "RAISERROR", "AND", "FILLFACTOR", "READ", "ANY", "FOR", "READTEXT", "AS", "FOREIGN", "RECONFIGURE", "ASC", "FREETEXT", "REFERENCES", "AUTHORIZATION", "FREETEXTTABLE", "REPLICATION", "BACKUP", "FROM", "RESTORE", "BEGIN", "FULL", "RESTRICT", "BETWEEN", "FUNCTION", "RETURN", "BREAK", "GOTO", "REVERT", "BROWSE", "GRANT", "REVOKE", "BULK", "GROUP", "RIGHT", "BY", "HAVING", "ROLLBACK", "CASCADE", "HOLDLOCK", "ROWCOUNT", "CASE", "IDENTITY", "ROWGUIDCOL", "CHECK", "IDENTITY_INSERT", "RULE", "CHECKPOINT", "IDENTITYCOL", "SAVE", "CLOSE", "IF", "SCHEMA", "CLUSTERED", "IN", "SECURITYAUDIT", "COALESCE", "INDEX", "SELECT", "COLLATE", "INNER", "SEMANTICKEYPHRASETABLE", "COLUMN", "INSERT", "SEMANTICSIMILARITYDETAILSTABLE", "COMMIT", "INTERSECT", "SEMANTICSIMILARITYTABLE", "COMPUTE", "INTO", "SESSION_USER", "CONSTRAINT", "IS", "SET", "CONTAINS", "JOIN", "SETUSER", "CONTAINSTABLE", "KEY", "SHUTDOWN", "CONTINUE", "KILL", "SOME", "CONVERT", "LEFT", "STATISTICS", "CREATE", "LIKE", "SYSTEM_USER", "CROSS", "LINENO", "TABLE", "CURRENT", "LOAD", "TABLESAMPLE", "CURRENT_DATE", "MERGE", "TEXTSIZE", "CURRENT_TIME", "NATIONAL", "THEN", "CURRENT_TIMESTAMP", "NOCHECK", "TO", "CURRENT_USER", "NONCLUSTERED", "TOP", "CURSOR", "NOT", "TRAN", "DATABASE", "NULL", "TRANSACTION", "DBCC", "NULLIF", "TRIGGER", "DEALLOCATE", "OF", "TRUNCATE", "DECLARE", "OFF", "TRY_CONVERT", "DEFAULT", "OFFSETS", "TSEQUAL", "DELETE", "ON", "UNION", "DENY", "OPEN", "UNIQUE", "DESC", "OPENDATASOURCE", "UNPIVOT", "DISK", "OPENQUERY", "UPDATE", "DISTINCT", "OPENROWSET", "UPDATETEXT", "DISTRIBUTED", "OPENXML", "USE", "DOUBLE", "OPTION", "USER", "DROP", "OR", "VALUES", "DUMP", "ORDER", "VARYING", "ELSE", "OUTER", "VIEW", "END", "OVER", "WAITFOR", "ERRLVL", "PERCENT", "WHEN", "ESCAPE", "PIVOT", "WHERE", "EXCEPT", "PLAN", "WHILE", "EXEC", "PRECISION", "WITH", "EXECUTE", "PRIMARY", "WITHIN GROUP", "EXISTS", "PRINT", "WRITETEXT", "EXIT", "PROC"};
	private static final List<String> reservedWords = Arrays.asList(reservedWordsArray);

	@Override
	public String getDateFormattedForQuery(Date date) {
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		DateFormat tzFormat = new SimpleDateFormat("Z");
		String tz = tzFormat.format(date);
		if (tz.length() == 4) {
			tz = "+" + tz.substring(0, 2) + ":" + tz.substring(2, 4);
		} else if (tz.length() == 5) {
			tz = tz.substring(0, 3) + ":" + tz.substring(3, 5);
		} else {
			throw new DBRuntimeException("TIMEZONE was :\"" + tz + "\"");
		}
		final String result = " CAST('" + format.format(date) + " " + tz + "' as DATETIMEOFFSET) ";
//		System.out.println(""+result);
		return result;
	}

	@Override
	protected String getSQLTypeOfDBDatatype(QueryableDatatype qdt) {
		if (qdt instanceof DBBoolean) {
			return " BIT ";
		} else if (qdt instanceof DBBooleanArray) {
			return " VARCHAR(64) ";
		} else if (qdt instanceof DBDate) {
			return " DATETIMEOFFSET ";
		} else if (qdt instanceof DBLargeObject) {
			return " NTEXT ";
		} else if (qdt instanceof DBString) {
			return " NVARCHAR(1000) COLLATE Latin1_General_CS_AS_KS_WS ";
		} else if (qdt instanceof DBPoint2D) {
			return " GEOMETRY ";
		} else if (qdt instanceof DBLine2D) {
			return " GEOMETRY ";
		} else if (qdt instanceof DBPolygon2D) {
			return " GEOMETRY ";
		} else {
			return super.getSQLTypeOfDBDatatype(qdt);
		}
	}

	@Override
	public boolean prefersDatesReadAsStrings() {
		return true;
	}

	@Override
	public Date parseDateFromGetString(String getStringDate) throws ParseException {
		String tempString = getStringDate.replaceAll(":([0-9]*)$", "$1");
		Date parsed;
		try {
			parsed = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z").parse(tempString);
		} catch (ParseException ex) {
			parsed = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(tempString);
		}
		return parsed;
	}

	@Override
	public String formatTableName(DBRow table) {
		return "[" + table.getTableName() + "]";
	}

	@Override
	protected String formatNameForDatabase(final String sqlObjectName) {
		if (reservedWords.contains(sqlObjectName.toUpperCase())) {
			return ("O" + sqlObjectName.hashCode()).replaceAll("^[_-]", "O").replaceAll("-", "_");
		}
		return sqlObjectName;
	}

	@Override
	public Object endSQLStatement() {
		return "";
	}

	@Override
	public Object getLimitRowsSubClauseDuringSelectClause(QueryOptions options) {
		return " TOP(" + options.getRowLimit() + ") "; //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public Object getLimitRowsSubClauseAfterWhereClause(QueryOptions options) {
		return "";
	}

	@Override
	public String doSubstringTransform(String originalString, String start, String length) {
		return " SUBSTRING("
				+ originalString
				+ ", "
				+ start
				+ (length.trim().isEmpty() ? "" : ", " + length)
				+ ") ";
	}

	@Override
	public String getStringLengthFunctionName() {
		return "LEN";
	}

	/**
	 * SQLServer follows the standard, unlike anyone else, and pads the short
	 * string with spaces before comparing.
	 *
	 * <p>
	 * This effectively means strings are trimmed during comparisons whether you
	 * like it or not.
	 *
	 * <p>
	 * While this seems useful, in fact it prevents checking for incorrect strings
	 * and breaks the industrial standard.
	 *
	 * @param firstSQLExpression
	 * @param secondSQLExpression
	 * @return
	 */
	@Override
	public String doStringEqualsTransform(String firstSQLExpression, String secondSQLExpression) {
		return "(" + firstSQLExpression + "+'@') = (" + secondSQLExpression + "+'@')";
	}

	@Override
	public String doTrimFunction(String enclosedValue) {
		return " LTRIM(RTRIM(" + enclosedValue + ")) "; //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doPositionInStringTransform(String originalString, String stringToFind) {
		return "CHARINDEX(" + stringToFind + ", " + originalString + ")";
	}

	@Override
	public String doConcatTransform(String firstString, String secondString) {
		return firstString + "+" + secondString;
	}

	@Override
	public String getIfNullFunctionName() {
		return "ISNULL"; //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * MSSQLserver only supports integer degrees, and that's not good enough.
	 *
	 * @return false
	 */
	@Override
	public boolean supportsDegreesFunction() {
		return false;
	}

	@Override
	public String getStandardDeviationFunctionName() {
		return "STDEV";
	}

	/**
	 * Wraps the provided SQL snippet in a statement that the length of the value
	 * of the snippet.
	 *
	 * @param enclosedValue	enclosedValue
	 * @return SQL snippet
	 */
	@Override
	public String doStringLengthTransform(String enclosedValue) {
		return " CAST(" + getStringLengthFunctionName() + "( " + enclosedValue + " ) as NUMERIC(15,10))";
	}
	
	@Override
	public boolean supportsPagingNatively(QueryOptions options) {
		return false;
	}

	@Override
	public String getColumnAutoIncrementSuffix() {
		return " IDENTITY ";
	}

	@Override
	public boolean prefersLargeObjectsSetAsBase64String() {
		return true;
	}

	@Override
	public boolean prefersLargeObjectsReadAsBase64CharacterStream() {
		return true;
	}

	@Override
	protected String getCurrentDateOnlyFunctionName() {
		return " GETDATE";
	}

//	@Override
//	public String doAddMillisecondsTransform(String dateValue, String numberOfSeconds) {
//		return "DATEADD( MILLISECOND, " + numberOfSeconds + "," + dateValue + ")";
//	}
	@Override
	public String doAddSecondsTransform(String dateValue, String numberOfSeconds) {
		return "DATEADD( SECOND, " + numberOfSeconds + "," + dateValue + ")";
	}

	@Override
	public String doAddMinutesTransform(String dateValue, String numberOfMinutes) {
		return "DATEADD( MINUTE, " + numberOfMinutes + "," + dateValue + ")";
	}

	@Override
	public String doAddDaysTransform(String dateValue, String numberOfDays) {
		return "DATEADD( DAY, " + numberOfDays + "," + dateValue + ")";
	}

	@Override
	public String doAddHoursTransform(String dateValue, String numberOfHours) {
		return "DATEADD( HOUR, " + numberOfHours + "," + dateValue + ")";
	}

	@Override
	public String doAddWeeksTransform(String dateValue, String numberOfWeeks) {
		return "DATEADD( WEEK, " + numberOfWeeks + "," + dateValue + ")";
	}

	@Override
	public String doAddMonthsTransform(String dateValue, String numberOfMonths) {
		return "DATEADD( MONTH, " + numberOfMonths + "," + dateValue + ")";
	}

	@Override
	public String doAddYearsTransform(String dateValue, String numberOfYears) {
		return "DATEADD( YEAR, " + numberOfYears + "," + dateValue + ")";
	}

	@Override
	public String doDayDifferenceTransform(String dateValue, String otherDateValue) {
		return "(DATEDIFF(DAY, " + dateValue + "," + otherDateValue + "))";
	}

	@Override
	public String doWeekDifferenceTransform(String dateValue, String otherDateValue) {
		return "(DATEDIFF(WEEK, " + dateValue + "," + otherDateValue + "))";
	}

	@Override
	public String doMonthDifferenceTransform(String dateValue, String otherDateValue) {
		return "(DATEDIFF(MONTH, " + dateValue + "," + otherDateValue + "))";
	}

	@Override
	public String doYearDifferenceTransform(String dateValue, String otherDateValue) {
		return "(DATEDIFF(YEAR, " + dateValue + "," + otherDateValue + "))";
	}

	@Override
	public String doHourDifferenceTransform(String dateValue, String otherDateValue) {
		return "(DATEDIFF(HOUR, " + dateValue + "," + otherDateValue + "))";
	}

	@Override
	public String doMinuteDifferenceTransform(String dateValue, String otherDateValue) {
		return "(DATEDIFF(MINUTE, " + dateValue + "," + otherDateValue + "))";
	}

	@Override
	public String doSecondDifferenceTransform(String dateValue, String otherDateValue) {
		return "(DATEDIFF(SECOND, " + dateValue + "," + otherDateValue + "))";
	}

//	@Override
//	public String doMillisecondDifferenceTransform(String dateValue, String otherDateValue) {
//		return "(DATEDIFF(MILLISECOND, " + dateValue + "," + otherDateValue + "))";
//	}
	@Override
	public String doTruncTransform(String realNumberExpression, String numberOfDecimalPlacesExpression) {
		//When the third parameter != 0 it truncates rather than rounds
		return " ROUND(" + realNumberExpression + ", " + numberOfDecimalPlacesExpression + ", 1)";
	}

	@Override
	public String doYearTransform(String dateExpression) {
		return "DATEPART(YEAR, " + dateExpression + ")";
	}

	@Override
	public String doMonthTransform(String dateExpression) {
		return "DATEPART(MONTH, " + dateExpression + ")";
	}

	@Override
	public String doDayTransform(String dateExpression) {
		return "DATEPART(DAY, " + dateExpression + ")";
	}

	@Override
	public String doHourTransform(String dateExpression) {
		return "DATEPART(HOUR, " + dateExpression + ")";
	}

	@Override
	public String doMinuteTransform(String dateExpression) {
		return "DATEPART(MINUTE, " + dateExpression + ")";
	}

	@Override
	public String doSecondTransform(String dateExpression) {
		return "DATEPART(SECOND , " + dateExpression + ")";
	}

//	@Override
//	public String doMillisecondTransform(String dateExpression) {
//		return "DATEPART(MILLISECOND , " + dateExpression + ")";
//	}
	/**
	 * MS SQLServer does not support the LEASTOF operation natively.
	 *
	 * @return FALSE
	 */
	@Override
	protected boolean supportsLeastOfNatively() {
		return false;
	}

	/**
	 * MS SQLServer does not support the GREATESTOF operation natively.
	 *
	 * @return FALSE
	 */
	@Override
	protected boolean supportsGreatestOfNatively() {
		return false;
	}

	/**
	 * MS SQLServer does not support the grouping by columns that do not access
	 * table data.
	 *
	 * @return FALSE
	 */
	@Override
	public boolean supportsPurelyFunctionalGroupByColumns() {
		return false;
	}

	/**
	 * Transforms a SQL snippet of a number expression into a character expression
	 * for this database.
	 *
	 * @param numberExpression	numberExpression
	 * @return a String of the SQL required to transform the number supplied into
	 * a character or String type.
	 */
	@Override
	public String doNumberToStringTransform(String numberExpression) {
		DBString dbs = new DBString();
		return "CONVERT(NVARCHAR(1000), " + numberExpression + ")";
	}

	@Override
	public String beginWithClause() {
		return " WITH ";
	}

	@Override
	public String doDayOfWeekTransform(String dateSQL) {
		return " datepart(dw,(" + dateSQL + "))";
	}

	@Override
	public String doRoundTransform(String toSQLString) {
		return "ROUND("+toSQLString+", 0)";
	}

	@Override
	public String doRoundWithDecimalPlacesTransform(String number, String decimalPlaces) {
		return "ROUND("+number+", "+decimalPlaces+")";
	}

	@Override
	public boolean supportsArraysNatively() {
		return false;
	}

	@Override
	public String getArctan2FunctionName() {
		return "ATN2";
	}

	@Override
	public boolean supportsHyperbolicFunctionsNatively() {
		return false;
	}

	@Override
	public String doStringToNumberTransform(String stringResultContainingANumber) {
		return "(CAST(0.0 as numeric(15,10))+(" + stringResultContainingANumber + "))";
	}

	@Override
	public DBExpression transformToStorableType(DBExpression columnExpression) {
		if (columnExpression instanceof BooleanExpression) {
			return ((BooleanExpression) columnExpression).ifThenElse(1, 0);
		} else {
			return super.transformToStorableType(columnExpression);
		}
	}

	@Override
	public String doPoint2DEqualsTransform(String firstPoint, String secondPoint) {
		return "((" + firstPoint + ").STEquals( " + secondPoint + ")=1)";
	}

	@Override
	public String doPoint2DGetXTransform(String point2D) {
		return "(" + point2D + ").STX";
	}

	@Override
	public String doPoint2DGetYTransform(String point2D) {
		return "("+point2D + ").STY";
	}

	@Override
	public String doPoint2DDimensionTransform(String point2D) {
		return "("+point2D+").STDimension()";
	}

	@Override
	public String doPoint2DGetBoundingBoxTransform(String point2D) {
		return "("+point2D+").STEnvelope()";
	}

	@Override
	public String doPoint2DAsTextTransform(String point2DString) {
		return "("+point2DString+").STAsText()";
	}

	@Override
	public String doLine2DAsTextTransform(String line2DSQL) {
		return "("+line2DSQL+").STAsText()";
	}

	@Override
	public String doLine2DGetMinYTransform(String toSQLString) {
		return Line2DFunctions.MINY+"("+toSQLString+")";
	}

	@Override
	public String doLine2DGetMaxYTransform(String toSQLString) {
		return Line2DFunctions.MAXY+"("+toSQLString+")";
	}

	@Override
	public String doLine2DGetMinXTransform(String toSQLString) {
		return Line2DFunctions.MINX+"("+toSQLString+")";
	}

	@Override
	public String doLine2DGetMaxXTransform(String toSQLString) {
		return Line2DFunctions.MAXX+"("+toSQLString+")";
	}

	@Override
	public String doLine2DGetBoundingBoxTransform(String toSQLString) {
		return "("+toSQLString+").STEnvelope()";
	}

	@Override
	public String doLine2DDimensionTransform(String toSQLString) {
		return super.doLine2DDimensionTransform(toSQLString); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doLine2DEqualsTransform(String toSQLString, String toSQLString0) {
		return super.doLine2DEqualsTransform(toSQLString, toSQLString0); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String transformPolygonIntoDatabaseFormat(Polygon polygon) {
		return "geometry::STGeomFromText ('" +polygon.toText()+"',0)";
	}
	
	@Override
	public String transformLineStringIntoDatabaseFormat(LineString line) {
		return "geometry::STGeomFromText ('" +line.toText()+"',0)";
	}
	
	@Override
	public String transformCoordinatesIntoDatabasePointFormat(String xValue, String yValue) {
		return "geometry::STGeomFromText ('POINT (" + xValue+" "+yValue + ")',0)";
	}
	
	@Override
	public String transformPointIntoDatabaseFormat(Point point) {
		return "geometry::STGeomFromText ('" +point.toText()+"',0)";
	}
	
	@Override
	public Object doColumnTransformForSelect(QueryableDatatype qdt, String selectableName) {
		if (qdt instanceof DBPolygon2D) {
			return "(" + selectableName + ").STAsText()";
		} else if (qdt instanceof DBPoint2D) {
			return "CAST((" + selectableName + ").STAsText() AS VARCHAR(2000))";
		} else if (qdt instanceof DBLine2D) {
			return "CAST((" + selectableName + ").STAsText() AS VARCHAR(2000))";
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
//	@Override
//	public Geometry transformDatabaseValueToJTSPolygon(String geometryAsString) throws com.vividsolutions.jts.io.ParseException {
//		String string = "POLYGON "+geometryAsString.replaceAll("\\),\\(", ", ").replaceAll("([-0-9.]+),([-0-9.]+)", "$1 $2");
//		String[] splits = geometryAsString.split("[(),]+");
//		System.out.println(geometryAsString+" => "+string);
//		List<Coordinate> coords = new ArrayList<Coordinate>();
//		Coordinate firstCoord = null;
//		for (int i = 1; i < splits.length; i++) {
//			String splitX = splits[i];
//			String splitY = splits[i+1];
//			System.out.println("COORD: "+splitX+", "+splitY);
//			final Coordinate coordinate = new Coordinate(Double.parseDouble(splitX), Double.parseDouble(splitY));
//			coords.add(coordinate);
//			if (firstCoord==null){
//				firstCoord=coordinate;
//			}
//			i++;
//		}
//		coords.add(firstCoord);
//		final GeometryFactory geometryFactory = new GeometryFactory();
//		Polygon polygon = geometryFactory.createPolygon(coords.toArray(new Coordinate[]{}));
//		return polygon;
//	}
}
