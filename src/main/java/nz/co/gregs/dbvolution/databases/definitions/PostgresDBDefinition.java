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
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.Line2DExpression;
import nz.co.gregs.dbvolution.expressions.MultiPoint2DExpression;
import nz.co.gregs.dbvolution.expressions.Polygon2DExpression;
import nz.co.gregs.dbvolution.internal.postgres.*;

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
		return "DROP DATABASE IF EXISTS '" + databaseName + "';";
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
		} else if (qdt instanceof DBBooleanArray) {
			return " BOOL[] ";
		} else if (qdt instanceof DBLine2D) {
			return " PATH ";
		} else if (qdt instanceof DBLineSegment2D) {
			return " PATH ";
		} else if (qdt instanceof DBMultiPoint2D) {
			return " GEOMETRY ";
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
		return " (EXTRACT(DOW FROM (" + dateSQL + "))+1)";
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
				str.append(separator).append(bool ? 1 : 0);
				separator = ",";
			}
			str.append("}'");
			return str.toString();
		}
	}

	@Override
	public String doPolygon2DIntersectionTransform(String firstGeometry, String secondGeometry) {
		return "ST_INTERSECTION((" + firstGeometry + ")::GEOMETRY,  (" + secondGeometry + ")::GEOMETRY)";
	}

	@Override
	public String doPolygon2DOverlapsTransform(String firstGeometry, String secondGeometry) {
//		return "(" + firstGeometry + ") ?#  (" + secondGeometry + ")";
		return "ST_OVERLAPS((" + firstGeometry + ")::GEOMETRY , (" + secondGeometry + ")::GEOMETRY)";
	}

	@Override
	public String doPolygon2DIntersectsTransform(String firstGeometry, String secondGeometry) {
//		return "(" + firstGeometry + ") ?#  (" + secondGeometry + ")";
		return "((" + firstGeometry + ") && (" + secondGeometry + "))";
	}

	@Override
	public String doPolygon2DTouchesTransform(String firstGeometry, String secondGeometry) {
		return "ST_TOUCHES((" + firstGeometry + ")::GEOMETRY , (" + secondGeometry + ")::GEOMETRY)";
	}

	@Override
	public String doPolygon2DGetAreaTransform(String toSQLString) {
		return "ST_AREA((" + toSQLString + ")::GEOMETRY)";
	}

	@Override
	public String doPolygon2DGetBoundingBoxTransform(String toSQLString) {
		return "BOX2D(" + toSQLString + "::GEOMETRY)";
	}

	@Override
	public String doPolygon2DEqualsTransform(String firstGeometry, String secondGeometry) {
		return "ST_EQUALS((" + firstGeometry + ")::GEOMETRY , (" + secondGeometry + ")::GEOMETRY)";
	}

	/**
	 * Test whether the first polygon completely contains the second polygon.
	 *
	 * @param firstGeometry
	 * @param secondGeometry
	 * @return SQL that is TRUE if the first polygon contains the second.
	 */
	@Override
	public String doPolygon2DContainsPolygon2DTransform(String firstGeometry, String secondGeometry) {
		return "ST_CONTAINS((" + firstGeometry + ")::GEOMETRY , (" + secondGeometry + ")::GEOMETRY)";
	}

	@Override
	public String doPolygon2DContainsPoint2DTransform(String firstGeometry, String secondGeometry) {
		return "ST_CONTAINS((" + firstGeometry + ")::GEOMETRY , (" + secondGeometry + ")::GEOMETRY)";
	}

	/**
	 * Inverse of {@link #doPolygon2DIntersectsTransform(java.lang.String, java.lang.String)
	 * }, tests whether the 2 polygons are non-coincident.
	 *
	 * @param firstGeometry
	 * @param secondGeometry
	 * @return SQL that is FALSE if the polygons intersect.
	 */
	@Override
	public String doPolygon2DDoesNotIntersectTransform(String firstGeometry, String secondGeometry) {
		return "ST_DISJOINT((" + firstGeometry + ")::GEOMETRY , (" + secondGeometry + ")::GEOMETRY)";
	}

	/**
	 * Test whether the first polygon is completely within the second polygon.
	 *
	 * <p>
	 * Compare this to {@link #doPolygon2DContainsPolygon2DTransform(java.lang.String, java.lang.String)
	 * }
	 *
	 * @param firstGeometry
	 * @param secondGeometry
	 * @return SQL that is TRUE if the first polygon is within the second.
	 */
	@Override
	public String doPolygon2DWithinTransform(String firstGeometry, String secondGeometry) {
		//indicate whether g1 is spatially within g2. This is the inverse of Contains(). 
		// i.e. G1.within(G2) === G2.contains(G1)
		return "ST_WITHIN((" + firstGeometry + ")::GEOMETRY , (" + secondGeometry + ")::GEOMETRY)";
	}

	/**
	 * Returns the dimension of the polygon.
	 *
	 * <p>
	 * This will be "2"
	 *
	 * @param toSQLString
	 * @return "2" unless something has gone horribly wrong.
	 */
	@Override
	public String doPolygon2DGetDimensionTransform(String toSQLString) {
		return "ST_DIMENSION((" + toSQLString + ")::GEOMETRY)";
	}

	/**
	 * Defines the transformation require to transform an SQL Polygon2D into a
	 * linestring representing the exterior ring of the polygon.
	 *
	 * @param polygon2DSQL
	 * @return SQL
	 */
	@Override
	public String doPolygon2DGetExteriorRingTransform(String polygon2DSQL) {
		return "PATH(ST_EXTERIORRING((" + polygon2DSQL + ")::GEOMETRY))";
	}

	/**
	 * Generate the SQL that will return the largest X value within the Polygon2D
	 * expression.
	 *
	 * @param polygon2DSQL
	 * @return SQL
	 */
	@Override
	public String doPolygon2DGetMaxXTransform(String polygon2DSQL) {
		return "ST_XMAX((" + polygon2DSQL + ")::GEOMETRY)";
	}

	/**
	 * Generate the SQL that will return the smallest X value within the Polygon2D
	 * expression.
	 *
	 * @param polygon2DSQL
	 * @return SQL
	 */
	@Override
	public String doPolygon2DGetMinXTransform(String polygon2DSQL) {
		return "ST_XMIN((" + polygon2DSQL + ")::GEOMETRY)";
	}

	/**
	 * Generate the SQL that will return the largest X value within the Polygon2D
	 * expression.
	 *
	 * @param polygon2DSQL
	 * @return SQL
	 */
	@Override
	public String doPolygon2DGetMaxYTransform(String polygon2DSQL) {
		return "ST_YMAX((" + polygon2DSQL + ")::GEOMETRY)";
	}

	/**
	 * Generate the SQL that will return the smallest Y value within the Polygon2D
	 * expression.
	 *
	 * @param polygon2DSQL
	 * @return SQL
	 */
	@Override
	public String doPolygon2DGetMinYTransform(String polygon2DSQL) {
		return "ST_YMIN((" + polygon2DSQL + ")::GEOMETRY)";
	}

	@Override
	public String doPolygon2DAsTextTransform(String toSQLString) {
		return "ST_ASTEXT(("+toSQLString+")::GEOMETRY)";
	}

	@Override
	public boolean supportsHyperbolicFunctionsNatively() {
		return false;//To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doStringToNumberTransform(String stringResultContainingANumber) {
		return "(CASE WHEN (" + stringResultContainingANumber + ") IS NULL OR (" + stringResultContainingANumber + ") = '' THEN 0 ELSE TO_NUMBER(" + stringResultContainingANumber + ", 'S999999999999999D9999999') END)";
	}

	@Override
	public boolean supportsArcSineFunction() {
		return false;
	}

	@Override
	public String transformCoordinatesIntoDatabasePoint2DFormat(String xValue, String yValue) {
		return "POINT (" + xValue + ", " + yValue + ")";
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
		return "(" + point2D + ")[1]";
	}

	@Override
	public String doPoint2DDimensionTransform(String point2D) {
		return " 0 ";
	}

	@Override
	public String doPoint2DGetBoundingBoxTransform(String point2D) {
		return "POLYGON(BOX(" + point2D + "," + point2D + "))";
	}

	@Override
	public String doPoint2DAsTextTransform(String point2DString) {
		return "(" + point2DString + ")::VARCHAR";
	}

	@Override
	public String transformPoint2DIntoDatabaseFormat(Point point) {
		return "POINT (" + point.getX() + ", " + point.getY() + ")";
	}

	//path '[(0,0),(1,1),(2,0)]'
	@Override
	public String transformLineStringIntoDatabaseLine2DFormat(LineString line) {
		StringBuilder str = new StringBuilder();
		String separator = "";
		Coordinate[] coordinates = line.getCoordinates();
		for (Coordinate coordinate : coordinates) {
			str.append(separator).append("(").append(coordinate.x).append(",").append(coordinate.y).append(")");
			separator = ",";
		}
		return "PATH '[" + str + "]'";
	}

	@Override
	public Object doColumnTransformForSelect(QueryableDatatype qdt, String selectableName) {
		if (qdt instanceof DBPolygon2D) {
			return "(" + selectableName + ")::VARCHAR";
		} else if (qdt instanceof DBPoint2D) {
			return "(" + selectableName + ")::VARCHAR";
		} else if (qdt instanceof DBLine2D) {
			return "(" + selectableName + ")::VARCHAR";
		} else if (qdt instanceof DBMultiPoint2D) {
			return "ST_ASTEXT(" + selectableName + ")::VARCHAR";
		} else {
			return selectableName;
		}
	}

	@Override
	public Point transformDatabasePoint2DValueToJTSPoint(String pointAsString) throws com.vividsolutions.jts.io.ParseException {
		Point point = null;
		if (pointAsString.matches(" *\\( *[-0-9.]+, *[-0-9.]+ *\\) *")) {
			String[] split = pointAsString.split("[^-0-9.]+");
			for (String split1 : split) {
				System.out.println("DATABASE VALUE: " + split1);
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

	// ((2,3),(2,3),(2,3),(2,3))
	// POLYGON((2 3,2 4,3 4,3 3,2 3))
	@Override
	public Polygon transformDatabasePolygon2DToJTSPolygon(String geometryAsString) throws com.vividsolutions.jts.io.ParseException {
		String string = "POLYGON " + geometryAsString.replaceAll("\\),\\(", ", ").replaceAll("([-0-9.]+),([-0-9.]+)", "$1 $2");
		System.out.println(geometryAsString + " => " + string);
		String[] splits = geometryAsString.split("[^0-9.]+");
		List<Coordinate> coords = new ArrayList<Coordinate>();
		Coordinate firstCoord = null;
		for (int i = 1; i < splits.length; i++) {
			String splitX = splits[i];
			String splitY = splits[i + 1];
			System.out.println("COORD: " + splitX + ", " + splitY);
			final Coordinate coordinate = new Coordinate(Double.parseDouble(splitX), Double.parseDouble(splitY));
			coords.add(coordinate);
			if (firstCoord == null) {
				firstCoord = coordinate;
			}
			i++;
		}
//		coords.add(firstCoord);
		if (coords.size()==1){
			coords.add(firstCoord);
			coords.add(firstCoord);
			coords.add(firstCoord);
			coords.add(firstCoord);
		}
		final GeometryFactory geometryFactory = new GeometryFactory();
		Polygon polygon = geometryFactory.createPolygon(coords.toArray(new Coordinate[]{}));
		return polygon;
	}

	@Override
	public LineString transformDatabaseLine2DValueToJTSLineString(String lineStringAsString) throws com.vividsolutions.jts.io.ParseException {
		LineString lineString = null;
		if (!lineStringAsString.matches("^ *LINESTRING.*")) {
			String string = "LINESTRING " + lineStringAsString.replaceAll("\\),\\(", ", ").replaceAll("([-0-9.]+),([-0-9.]+)", "$1 $2");
			String[] splits = lineStringAsString.split("[(),]+");
			System.out.println(lineStringAsString + " => " + string);
			Coordinate firstCoord = null;
			List<Coordinate> coords = new ArrayList<Coordinate>();
			for (int i = 1; i < splits.length - 1; i++) {
				String splitX = splits[i];
				String splitY = splits[i + 1];
				System.out.println("COORD: " + splitX + ", " + splitY);
				final Coordinate coordinate = new Coordinate(Double.parseDouble(splitX), Double.parseDouble(splitY));
				coords.add(coordinate);
				if (firstCoord == null) {
					firstCoord = coordinate;
				}
				i++;
			}
			coords.add(firstCoord);
			final GeometryFactory geometryFactory = new GeometryFactory();
			lineString = geometryFactory.createLineString(coords.toArray(new Coordinate[]{}));
		} else {
			return super.transformDatabaseLine2DValueToJTSLineString(lineStringAsString);
		}
		return lineString;
	}

	@Override
	public String doLine2DEqualsTransform(String firstLineSQL, String secondLineSQL) {
		return "(" + firstLineSQL + ")::TEXT = (" + secondLineSQL + ")::TEXT";
	}

	@Override
	public String doLine2DAsTextTransform(String line2DSQL) {
		return "(" + line2DSQL + ")::TEXT";
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
	public String doLine2DIntersectsLine2DTransform(String firstGeometry, String secondGeometry) {
		return "ST_INTERSECTS((" + firstGeometry + ")::GEOMETRY , (" + secondGeometry + ")::GEOMETRY)";
	}

	@Override
	public String doLine2DIntersectionPointWithLine2DTransform(String firstGeometry, String secondGeometry) {
		return Line2DFunctions.INTERSECTIONWITHLINE2D+"((" + firstGeometry + ") , (" + secondGeometry + "))";
	}

	@Override
	public String doLine2DAllIntersectionPointsWithLine2DTransform(String firstGeometry, String secondGeometry) {
		return Line2DFunctions.INTERSECTIONPOINTSWITHLINE2D+"((" + firstGeometry + ") , (" + secondGeometry + "))";
	}
	
	@Override
	public String doSubstringBeforeTransform(String fromThis, String beforeThis) {
		return StringFunctions.SUBSTRINGBEFORE + "(" + fromThis + ", " + beforeThis + ")";
	}

	@Override
	public String doSubstringAfterTransform(String fromThis, String afterThis) {
		return StringFunctions.SUBSTRINGAFTER + "(" + fromThis + ", " + afterThis + ")";
	}

	//INSERT INTO BasicSpatialTable( myfirstgeom)  VALUES ( polygon 'POLYGON ((5 10, 6 10, 6 11, 5 11, 5 10))')
	//INSERT INTO BasicSpatialTable( myfirstgeom)  VALUES ( polygon '((5,10), (6,10), (6,11), (5,11), (5,10))');
	@Override
	public String doDBPolygon2DFormatTransform(Polygon polygon) {

		StringBuilder str = new StringBuilder();
		String separator = "";
		Coordinate[] coordinates = polygon.getCoordinates();
		for (Coordinate coordinate : coordinates) {
			str.append(separator).append("(").append(coordinate.x).append(",").append(coordinate.y).append(")");
			separator = ",";
		}

		return "POLYGON '(" + str + ")'";
	}

	@Override
	public DBExpression transformToStorableType(DBExpression columnExpression) {
		if (columnExpression instanceof Line2DExpression) {
			return ((Line2DExpression) columnExpression).stringResult();
		} else if (columnExpression instanceof MultiPoint2DExpression) {
			return ((MultiPoint2DExpression) columnExpression).stringResult();
		} else if (columnExpression instanceof Polygon2DExpression) {
			return ((Polygon2DExpression) columnExpression).stringResult();
		} else {
			return super.transformToStorableType(columnExpression);
		}
	}

	@Override
	public String transformLineSegmentIntoDatabaseLineSegment2DFormat(LineSegment lineSegment) {
		LineString toGeometry = lineSegment.toGeometry(new GeometryFactory());
		return transformLineStringIntoDatabaseLine2DFormat(toGeometry);
//		String str;
//		Coordinate coord1 = lineSegment.getCoordinate(0);
//		Coordinate coord2 = lineSegment.getCoordinate(1);
//		str = "LSEG (POINT '("+coord1.x+","+coord1.y+")',POINT '("+coord2.x+","+coord2.y+")')";
//		return str;
	}

	@Override
	public LineSegment transformDatabaseLineSegment2DValueToJTSLineSegment(String lineStringAsString) throws com.vividsolutions.jts.io.ParseException {
		LineSegment lineString = null;
		if (!lineStringAsString.matches("^ *LINESTRING.*")) {
			String string = "LINESTRING " + lineStringAsString.replaceAll("\\),\\(", ", ").replaceAll("([-0-9.]+),([-0-9.]+)", "$1 $2");
			String[] splits = lineStringAsString.split("[(),]+");
			System.out.println(lineStringAsString + " => " + string);
			Coordinate firstCoord = null;
			List<Coordinate> coords = new ArrayList<Coordinate>();
			for (int i = 1; i < splits.length - 1; i+=2) {
				String splitX = splits[i];
				String splitY = splits[i + 1];
				System.out.println("COORD: " + splitX + ", " + splitY);
				final Coordinate coordinate = new Coordinate(Double.parseDouble(splitX), Double.parseDouble(splitY));
				coords.add(coordinate);
				if (firstCoord == null) {
					firstCoord = coordinate;
				}
//				i++;
			}
			coords.add(firstCoord);
			lineString = new LineSegment(coords.get(0), coords.get(1));
		} else {
			return super.transformDatabaseLineSegment2DValueToJTSLineSegment(lineStringAsString);
		}
		return lineString;
	}

	@Override
	public String doLineSegment2DIntersectsLineSegment2DTransform(String toSQLString, String toSQLString0) {
		return "ST_INTERSECTS((" + toSQLString + ")::GEOMETRY , (" + toSQLString0 + ")::GEOMETRY)";
	}

	@Override
	public String doLineSegment2DGetMaxXTransform(String toSQLString) {
		return Line2DFunctions.MAXX+"("+toSQLString+")";
	}

	@Override
	public String doLineSegment2DGetMinXTransform(String toSQLString) {
		return Line2DFunctions.MINX+"("+toSQLString+")";
	}

	@Override
	public String doLineSegment2DGetMaxYTransform(String toSQLString) {
		return Line2DFunctions.MAXY+"("+toSQLString+")";
	}

	@Override
	public String doLineSegment2DGetMinYTransform(String toSQLString) {
		return Line2DFunctions.MINY+"("+toSQLString+")";
	}

	@Override
	public String doLineSegment2DGetBoundingBoxTransform(String toSQLString) {
		return Line2DFunctions.BOUNDINGBOX+"(("+toSQLString+")::PATH)";
	}

	@Override
	public String doLineSegment2DNotEqualsTransform(String toSQLString, String toSQLString0) {
		return "(("+toSQLString+"):TEXT <> ("+toSQLString0+")::TEXT)";
	}

	@Override
	public String doLineSegment2DEqualsTransform(String toSQLString, String toSQLString0) {
		return "(("+toSQLString+")::TEXT = ("+toSQLString0+")::TEXT)";
	}

	@Override
	public String doLineSegment2DAsTextTransform(String toSQLString) {
		return "("+toSQLString+")::TEXT";
	}
	
	@Override
	public String doLineSegment2DIntersectionPointWithLineSegment2DTransform(String firstLineSegment, String secondLineSegment) {
		return "ST_PointFROMTEXT(ST_ASTEXT(ST_INTERSECTION((" + firstLineSegment + ")::GEOMETRY , (" + secondLineSegment + ")::GEOMETRY)))::POINT";
	}

	@Override
	public String transformMultiPoint2DToDatabaseMultiPoint2DValue(MultiPoint points) {
		return super.transformMultiPoint2DToDatabaseMultiPoint2DValue(points);
	}

	@Override
	public MultiPoint transformDatabaseMultiPoint2DValueToJTSMultiPoint(String pointsAsString) throws com.vividsolutions.jts.io.ParseException {
		return super.transformDatabaseMultiPoint2DValueToJTSMultiPoint(pointsAsString);
	}

	@Override
	public String doMultiPoint2DEqualsTransform(String first, String second) {
		return "ST_EQUALS(" + first + ", " + second + ")";
	}

	@Override
	public String doMultiPoint2DGetPointAtIndexTransform(String first, String index) {
		return "ST_GEOMETRYN(" + first + ", " + index + ")::POINT";
	}

	@Override
	public String doMultiPoint2DGetNumberOfPointsTransform(String first) {
		return "ST_NPOINTS(" + first + ")";
	}

	@Override
	public String doMultiPoint2DDimensionTransform(String first) {
		return "ST_DIMENSION(" + first + ")";
	}

	@Override
	public String doMultiPoint2DGetBoundingBoxTransform(String first) {
		return "ST_REVERSE(ST_ENVELOPE(" + first + "))";
	}

	@Override
	public String doMultiPoint2DAsTextTransform(String first) {
		return "ST_ASTEXT((" + first + ")::GEOMETRY)::TEXT";
	}

	@Override
	public String doMultiPoint2DToLine2DTransform(String first) {
		return MultiPoint2DFunctions.ASLINE2D+"(("+first+"))::PATH";
	}

//	@Override
//	public String doMultiPoint2DToPolygon2DTransform(String first) {
//		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//	}

	@Override
	public String doMultiPoint2DGetMinYTransform(String toSQLString) {
		return "ST_YMIN(" + toSQLString + ")";
	}

	@Override
	public String doMultiPoint2DGetMinXTransform(String toSQLString) {
		return "ST_XMIN(" + toSQLString + ")";
	}

	@Override
	public String doMultiPoint2DGetMaxYTransform(String toSQLString) {
		return "ST_YMAX(" + toSQLString + ")";
	}

	@Override
	public String doMultiPoint2DGetMaxXTransform(String toSQLString) {
		return "ST_XMAX(" + toSQLString + ")";
	}

}
