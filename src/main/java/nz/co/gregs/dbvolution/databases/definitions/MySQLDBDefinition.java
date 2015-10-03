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
import com.vividsolutions.jts.io.WKTReader;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBPolygon2D;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import nz.co.gregs.dbvolution.databases.MySQLDB;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBLine2D;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBLineSegment2D;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBMultiPoint2D;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBPoint2D;
import nz.co.gregs.dbvolution.exceptions.IncorrectGeometryReturnedForDatatype;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import nz.co.gregs.dbvolution.query.QueryOptions;

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

	private static final DateFormat DATETIME_FORMAT = new SimpleDateFormat("dd,MM,yyyy HH:mm:ss.SSS");

	@Override
	@SuppressWarnings("deprecation")
	public String getDateFormattedForQuery(Date date) {
		return " STR_TO_DATE('" + DATETIME_FORMAT.format(date) + "', '%d,%m,%Y %H:%i:%s.%f') ";
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
	public String getDatabaseDataTypeOfQueryableDatatype(QueryableDatatype qdt) {
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
			return super.getDatabaseDataTypeOfQueryableDatatype(qdt);
		}
	}

	@Override
	public Class<? extends QueryableDatatype> getQueryableDatatypeClassForSQLDatatype(String typeName) {
		if (typeName.equals("POLYGON")) {
			return DBPolygon2D.class;
		} else if (typeName.equals("LINESTRING")) {
			return DBLine2D.class;
		} else if (typeName.equals("POINT")) {
			return DBPoint2D.class;
		} else if (typeName.equals("MULTIPOINT")) {
			return DBMultiPoint2D.class; // obviously this is not going to work in all cases 
		} else {
			return null;
		}
	}

	@Override
	public String doColumnTransformForSelect(QueryableDatatype qdt, String selectableName) {
		if (qdt instanceof DBPolygon2D) {
			return "AsText(" + selectableName + ")";
		} else if (qdt instanceof DBPoint2D) {
			return "AsText(" + selectableName + ")";
		} else if (qdt instanceof DBLine2D) {
			return "AsText(" + selectableName + ")";
		} else if (qdt instanceof DBLineSegment2D) {
			return "AsText(" + selectableName + ")";
		} else if (qdt instanceof DBMultiPoint2D) {
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

	/**
	 * Provides the function of the function that provides the standard deviation
	 * of a selection.
	 *
	 * @return "stddev"
	 */
	@Override
	public String getStandardDeviationFunctionName() {
		return "STDDEV_SAMP";
	}

	@Override
	public String doSubsecondTransform(String dateExpression) {
		return "(EXTRACT(MICROSECOND FROM " + dateExpression + ")/1000000.0000000)";
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
	public String transformPolygonIntoDatabasePolygon2DFormat(Polygon geom) {
		String wktValue = geom.toText();
		return "PolyFromText('" + wktValue + "')";
	}

	@Override
	public String doPolygon2DEqualsTransform(String firstGeometry, String secondGeometry) {
		return "ST_Equals(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DIntersectsTransform(String firstGeometry, String secondGeometry) {
		return "ST_Intersects(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DContainsPolygon2DTransform(String firstGeometry, String secondGeometry) {
		return "Contains(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DContainsPoint2DTransform(String polygon2DSQL, String point2DSQL) {
		return "Contains(" + polygon2DSQL + ", " + point2DSQL + ")";
	}

	@Override
	public String doPolygon2DDoesNotIntersectTransform(String firstGeometry, String secondGeometry) {
		return "ST_Disjoint(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DOverlapsTransform(String firstGeometry, String secondGeometry) {
		return "Overlaps(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DTouchesTransform(String firstGeometry, String secondGeometry) {
		return "ST_Touches(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DWithinTransform(String firstGeometry, String secondGeometry) {
		//Returns 1 or 0 to indicate whether g1 is spatially within g2. This tests the opposite relationship as Contains(). 
		return "Within(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DMeasurableDimensionsTransform(String thisGeometry) {
		return "ST_Dimension(" + thisGeometry + ")";
	}

	@Override
	public String doPolygon2DGetBoundingBoxTransform(String thisGeometry) {
		return "Envelope(" + thisGeometry + ")";
	}

	@Override
	public String doPolygon2DGetAreaTransform(String thisGeometry) {
		return "Area(" + thisGeometry + ")";
	}

	@Override
	public String doPolygon2DGetExteriorRingTransform(String thisGeometry) {
		return "ExteriorRing(" + thisGeometry + ")";
	}

	@Override
	public String doPolygon2DGetMaxXTransform(String toSQLString) {
		return "X(PointN(ExteriorRing(Envelope(" + toSQLString + ")),3))";
	}

	@Override
	public String doPolygon2DGetMinXTransform(String toSQLString) {
		return "X(PointN(ExteriorRing(Envelope(" + toSQLString + ")),1))";
	}

	@Override
	public String doPolygon2DGetMaxYTransform(String toSQLString) {
		return "Y(PointN(ExteriorRing(Envelope(" + toSQLString + ")),3))";
	}

	@Override
	public String doPolygon2DGetMinYTransform(String toSQLString) {
		return "Y(PointN(ExteriorRing(Envelope(" + toSQLString + ")),1))";
	}

	@Override
	public boolean supportsHyperbolicFunctionsNatively() {
		return false;
	}

	@Override
	public String transformCoordinatesIntoDatabasePoint2DFormat(String xValue, String yValue) {
		return "PointFromText('POINT (" + xValue + " " + yValue + ")')";
	}

	@Override
	public String doPoint2DEqualsTransform(String firstPoint, String secondPoint) {
		return "Equals(" + firstPoint + ", " + secondPoint + ")";
	}

	@Override
	public String doPoint2DGetXTransform(String point2D) {
		return " X(" + point2D + ")";
	}

	@Override
	public String doPoint2DGetYTransform(String point2D) {
		return " Y(" + point2D + ")";
	}

	@Override
	public String doPoint2DMeasurableDimensionsTransform(String point2D) {
		return doPolygon2DMeasurableDimensionsTransform(point2D);
	}

	@Override
	public String doPoint2DGetBoundingBoxTransform(String point2D) {
		return doPolygon2DGetBoundingBoxTransform(point2D);
	}

	@Override
	public String doPoint2DAsTextTransform(String point2DString) {
		return " AsText(" + point2DString + ")";
	}

	@Override
	public String transformPoint2DIntoDatabaseFormat(Point point) {
		String wktValue = point.toText();
		return "PointFromText('" + wktValue + "')";
	}

	@Override
	public String transformLineStringIntoDatabaseLine2DFormat(LineString line) {
		String wktValue = line.toText();
		return "LineFromText('" + wktValue + "')";
	}

	@Override
	public String doLine2DGetBoundingBoxTransform(String toSQLString) {
		return "Envelope(" + toSQLString + ")";
	}

	@Override
	public String doLine2DGetMaxXTransform(String toSQLString) {
		return "X(PointN(ExteriorRing(Envelope(" + toSQLString + ")),3))";
	}

	@Override
	public String doLine2DGetMinXTransform(String toSQLString) {
		return "X(PointN(ExteriorRing(Envelope(" + toSQLString + ")),1))";
	}

	@Override
	public String doLine2DGetMaxYTransform(String toSQLString) {
		return "Y(PointN(ExteriorRing(Envelope(" + toSQLString + ")),3))";
	}

	@Override
	public String doLine2DGetMinYTransform(String toSQLString) {
		return "Y(PointN(ExteriorRing(Envelope(" + toSQLString + ")),1))";
	}

	@Override
	public String doLine2DIntersectsLine2DTransform(String firstLine, String secondLine) {
		return "Touches((" + firstLine + "), (" + secondLine + "))";
	}

	@Override
	public String doLine2DIntersectionPointWithLine2DTransform(String firstLine, String secondLine) {
		return "ST_Intersection((" + firstLine + "), (" + secondLine + "))";
	}

	@Override
	public String doLine2DAllIntersectionPointsWithLine2DTransform(String firstGeometry, String secondGeometry) {
		return "ST_Intersection((" + firstGeometry + "), (" + secondGeometry + "))";
	}

	@Override
	public LineSegment transformDatabaseLineSegment2DValueToJTSLineSegment(String lineSegmentAsSQL) throws com.vividsolutions.jts.io.ParseException {
		LineString line = transformDatabaseLine2DValueToJTSLineString(lineSegmentAsSQL);
		LineSegment lineSegment = new LineSegment(line.getCoordinateN(0), line.getCoordinateN(1));
		return lineSegment;
	}

	@Override
	public String transformLineSegmentIntoDatabaseLineSegment2DFormat(LineSegment lineSegment) {
		LineString line = (new GeometryFactory()).createLineString(new Coordinate[]{lineSegment.getCoordinate(0), lineSegment.getCoordinate(1)});
		return transformLineStringIntoDatabaseLine2DFormat(line);
//		String wktValue = line.toText();
//		return "'" + wktValue + "'";
	}

	@Override
	public String doLineSegment2DIntersectsLineSegment2DTransform(String toSQLString, String toSQLString0) {
		return doLine2DIntersectsLine2DTransform(toSQLString, toSQLString0);
	}

	@Override
	public String doLineSegment2DGetMaxXTransform(String toSQLString) {
		return doLine2DGetMaxXTransform(toSQLString);
	}

	@Override
	public String doLineSegment2DGetMinXTransform(String toSQLString) {
		return doLine2DGetMinXTransform(toSQLString);
	}

	@Override
	public String doLineSegment2DGetMaxYTransform(String toSQLString) {
		return doLine2DGetMaxYTransform(toSQLString);
	}

	@Override
	public String doLineSegment2DGetMinYTransform(String toSQLString) {
		return doLine2DGetMinYTransform(toSQLString);
	}

	@Override
	public String doLineSegment2DGetBoundingBoxTransform(String toSQLString) {
		return doLine2DGetBoundingBoxTransform(toSQLString);
	}

	@Override
	public String doLineSegment2DDimensionTransform(String toSQLString) {
		return doLine2DMeasurableDimensionsTransform(toSQLString);
	}

	@Override
	public String doLineSegment2DNotEqualsTransform(String toSQLString, String toSQLString0) {
		return doLine2DNotEqualsTransform(toSQLString, toSQLString0);
	}

	@Override
	public String doLineSegment2DEqualsTransform(String toSQLString, String toSQLString0) {
		return doLine2DEqualsTransform(toSQLString, toSQLString0);
	}

	@Override
	public String doLineSegment2DAsTextTransform(String toSQLString) {
		return doLine2DAsTextTransform(toSQLString);
	}

	@Override
	public String doLineSegment2DIntersectionPointWithLineSegment2DTransform(String firstLineSegment, String secondLineSegment) {
		return doLine2DIntersectionPointWithLine2DTransform(firstLineSegment, secondLineSegment);
	}

	@Override
	public String transformMultiPoint2DToDatabaseMultiPoint2DValue(MultiPoint points) {
		String wktValue = points.toText().replace("((", "(").replace("))", ")").replaceAll("\\), \\(", ", ");
		return "MPointFromText('" + wktValue + "')";
	}

	@Override
	public MultiPoint transformDatabaseMultiPoint2DValueToJTSMultiPoint(String pointsAsString) throws com.vividsolutions.jts.io.ParseException {
		System.out.println("" + pointsAsString);
		MultiPoint mpoint = null;
		WKTReader wktReader = new WKTReader();
		Geometry geometry = wktReader.read(pointsAsString);
		if (geometry instanceof MultiPoint) {
			mpoint = (MultiPoint) geometry;
		} else if (geometry instanceof Point) {
			Point point = (Point) geometry;
			mpoint = (new GeometryFactory()).createMultiPoint(new Point[]{point});
		} else {
			throw new IncorrectGeometryReturnedForDatatype(geometry, (new GeometryFactory()).createMultiPoint(new Point[]{}));
		}
		return mpoint;
	}

	@Override
	public String doMultiPoint2DEqualsTransform(String first, String second) {
		return "Equals(" + first + ", " + second + ")";
	}

	@Override
	public String doMultiPoint2DGetPointAtIndexTransform(String first, String index) {
		return "PointN(" + doMultiPoint2DToLine2DTransform(first) + ", " + index + ")";
	}

	@Override
	public String doMultiPoint2DGetNumberOfPointsTransform(String first) {
		return "NumPoints(" + doMultiPoint2DToLine2DTransform(first) + ")";
	}

	@Override
	public String doMultiPoint2DMeasurableDimensionsTransform(String first) {
		return "Dimension(" + first + ")";
	}

	@Override
	public String doMultiPoint2DGetBoundingBoxTransform(String first) {
		return "Envelope(" + first + ")";
	}

	@Override
	public String doMultiPoint2DAsTextTransform(String first) {
		return "AsText(" + first + ")";
	}

	@Override
	public String doMultiPoint2DToLine2DTransform(String first) {
		return "LineFromText(REPLACE(ASTEXT(" + first + "),'MULTIPOINT', 'LINESTRING'))";
	}

//	@Override
//	public String doMultiPoint2DToPolygon2DTransform(String first) {
//		return "LineFromText(REPLACE(ASTEXT(" + first + "),'MULTIPOINT', 'POLYGON'))";	
//	}
	@Override
	public String doMultiPoint2DGetMinYTransform(String first) {
		return "Y(PointN(ExteriorRing(Envelope(" + first + ")),1))";
	}

	@Override
	public String doMultiPoint2DGetMinXTransform(String first) {
		return "X(PointN(ExteriorRing(Envelope(" + first + ")),1))";
	}

	@Override
	public String doMultiPoint2DGetMaxYTransform(String first) {
		return "Y(PointN(ExteriorRing(Envelope(" + first + ")),3))";
	}

	@Override
	public String doMultiPoint2DGetMaxXTransform(String first) {
		return "X(PointN(ExteriorRing(Envelope(" + first + ")),3))";
	}

// Relies on Java8 :(
//	@Override
//	public String doDateAtTimeZoneTransform(String dateSQL, TimeZone timeZone) {
//		return "CONVERT_TZ(" + dateSQL + ", 'SYSTEM', '" + timeZone.toZoneId().getId() + "') ";
//	}
}
