/*
 * Copyright 2015 gregorygraham.
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
import com.vividsolutions.jts.geom.LineSegment;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.results.Spatial2DResult;

/**
 * A subclass of OracleDB that contains definitions of standard Spatial
 * functions shared by Oracle databases with Spatial functions.
 *
 * @author gregorygraham
 */
public class OracleSpatialDBDefinition extends OracleDBDefinition {

	@Override
	public String getSQLTypeOfDBDatatype(QueryableDatatype qdt) {
		if (qdt instanceof Spatial2DResult) {
			return " SDO_GEOMETRY ";
//		} else if (qdt instanceof DBLine2D) {
//			return " SDO_GEOMETRY ";
//		} else if (qdt instanceof DBLineSegment2D) {
//			return " SDO_GEOMETRY ";
//		} else if (qdt instanceof DBPolygon2D) {
//			return " SDO_GEOMETRY ";
		} else {
			return super.getSQLTypeOfDBDatatype(qdt);
		}
	}

	@Override
	public boolean requiresSpatial2DIndexes() {
		return true;
	}

	@Override
	public List<String> getSpatial2DIndexSQL(DBDatabase aThis, final String formatTableName, final String formatColumnName) {
		return new ArrayList<String>() {
			public static final long serialVersionUID = 1;

			{
				add(
						"INSERT INTO USER_SDO_GEOM_METADATA \n"
						+ "  VALUES (\n"
						+ "  '" + formatTableName + "',\n"
						+ "  '" + formatColumnName + "',\n"
						+ "  MDSYS.SDO_DIM_ARRAY(\n"
						+ "    MDSYS.SDO_DIM_ELEMENT('X', -9999999999, 9999999999, 0.0000000001),\n"
						+ "    MDSYS.SDO_DIM_ELEMENT('Y', -9999999999, 9999999999, 0.0000000001)\n"
						+ "     ),\n"
						+ "  NULL   -- SRID\n"
						+ ")");
				add("CREATE INDEX " + formatNameForDatabase("DBV_" + formatTableName + "_" + formatColumnName + "_sp2didx") + " ON " + formatTableName + " (" + formatColumnName + ") INDEXTYPE IS MDSYS.SPATIAL_INDEX");
			}
		};
	}

	@Override
	public String transformPoint2DIntoDatabaseFormat(Point point) {
		final Coordinate coordinate = point.getCoordinate();
		return transformCoordinatesIntoDatabasePoint2DFormat("" + coordinate.x, "" + coordinate.y);
	}

	@Override
	public String transformCoordinatesIntoDatabasePoint2DFormat(String xValue, String yValue) {
		return "SDO_GEOMETRY(2001, NULL, SDO_POINT_TYPE(" + xValue + ", " + yValue + ",NULL), NULL, NULL)";
//		return "SDO_UTIL.FROM_WKTGEOMETRY('"+point.toText()+"')";
	}

	@Override
	public String transformLineStringIntoDatabaseLine2DFormat(LineString point) {
//		final Coordinate coordinate = point.getCoordinate();
//		return "SDO_GEOMETRY(2002, NULL, SDO_POINT_TYPE(" + coordinate.x + ", " + coordinate.y + ",NULL), NULL, NULL)";
		return "SDO_UTIL.FROM_WKTGEOMETRY('" + point.toText() + "')";
	}

	@Override
	public String transformPolygonIntoDatabasePolygon2DFormat(Polygon point) {
//		final Coordinate coordinate = point.getCoordinate();
//		return "SDO_GEOMETRY(2003, NULL, SDO_POINT_TYPE(" + coordinate.x + ", " + coordinate.y + ",NULL), NULL, NULL)";
		return "SDO_UTIL.FROM_WKTGEOMETRY('" + point.toText() + "')";
	}

	@Override
	public String transformMultiPoint2DToDatabaseMultiPoint2DValue(MultiPoint point) {
//		final Coordinate coordinate = point.getCoordinate();
//		return "SDO_GEOMETRY(2005, NULL, SDO_POINT_TYPE(" + coordinate.x + ", " + coordinate.y + ",NULL), NULL, NULL)";
		return "SDO_UTIL.FROM_WKTGEOMETRY('" + point.toText() + "')";
	}

	@Override
	public String transformLineSegmentIntoDatabaseLineSegment2DFormat(LineSegment lineSegment) {
//		final Coordinate coordinate = point.p0;
//		final Coordinate otherCoord = point.p1;
		
		//MDSYS.SDO_GEOMETRY(2002, NULL, NULL,
		//MDSYS.SDO_ELEM_INFO_ARRAY(1,4,2, 1,2,1, 9,2,2),
		//MDSYS.SDO_ORDINATE_ARRAY(15,10, 25,10, 30,5, 38,5, 38,10, 35,15, 25,20))
		
		return "MDSYS.SDO_GEOMETRY(2002, NULL, NULL,"
				+ "MDSYS.SDO_ELEM_INFO_ARRAY(1,2,1),"
				+ "MDSYS.SDO_ORDINATE_ARRAY("+lineSegment.p0.x+", "+lineSegment.p0.y+", "+lineSegment.p1.x+", "+lineSegment.p1.y+")"
				+ ")";
	}

	@Override
	public String doPoint2DDistanceBetweenTransform(String polygon2DSQL, String otherPolygon2DSQL) {
		return "SDO_GEOM.SDO_DISTANCE(" + polygon2DSQL + ", " + otherPolygon2DSQL + ", 0.000001)"; //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doPoint2DArrayToPolygon2DTransform(List<String> pointSQL) {
		return super.doPoint2DArrayToPolygon2DTransform(pointSQL); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doPoint2DAsTextTransform(String point2DSQL) {
		return "TO_CHAR(SDO_UTIL.TO_WKTGEOMETRY(" + point2DSQL + "))";
	}

	@Override
	public String doPoint2DGetBoundingBoxTransform(String point2DSQL) {
		return "SDO_GEOM.SDO_MBR(" + point2DSQL + ")";
	}

	@Override
	public String doPoint2DMeasurableDimensionsTransform(String point2DSQL) {
		return "0";
		//return "(" + point2DSQL + ").GET_DIMS()"; get_dims() will return 2 as in 2D, whereas we require 0
	}

	@Override
	public String doPoint2DGetYTransform(String point2DSQL) {
		return "(" + point2DSQL + ").SDO_POINT.Y";
	}

	@Override
	public String doPoint2DGetXTransform(String point2DSQL) {
		return "(" + point2DSQL + ").SDO_POINT.X";
	}

	@Override
	public String doPoint2DEqualsTransform(String firstPoint, String secondPoint) {
		return "SDO_GEOM.RELATE(" + firstPoint + ", 'equal', " + secondPoint + ", 0.0000005)='EQUAL'";
	}

	/**
	 * Convert the String object returned by the database into a JTS LineSegment object.
	 *
	 * @param lineSegmentAsSQL
	 * @return a JTS LineSegment derived from the database's response, may be null.
	 * @throws com.vividsolutions.jts.io.ParseException
	 */
//	public LineSegment transformDatabaseLineSegment2DValueToJTSLineSegment(String lineSegmentAsSQL) throws com.vividsolutions.jts.io.ParseException {
//		LineString lineString = null;
//		WKTReader wktReader = new WKTReader();
//		Geometry geometry = wktReader.read(lineSegmentAsSQL);
//		if (geometry instanceof LineString) {
//			lineString = (LineString) geometry;
//			if (lineSegmentAsSQL == null) {
//				return null;
//			} else {
//				return new LineSegment(lineString.getCoordinateN(0), lineString.getCoordinateN(1));
//			}
//		} else {
//			throw new IncorrectGeometryReturnedForDatatype(geometry, lineString);
//		}
//	}

	/**
	 * Generates the database specific SQL for testing whether the 2 line segment expressions ever cross.
	 *
	 * @param firstSQL
	 * @param secondSQL
	 * @return an SQL expression that will report whether the 2 line segments intersect.
	 * @see #doLineSegment2DIntersectionPointWithLineSegment2DTransform(java.lang.String, java.lang.String) 
	 */
	@Override
	public String doLineSegment2DIntersectsLineSegment2DTransform(String firstSQL, String secondSQL) {
		return "SDO_GEOM.RELATE(" + firstSQL + ", 'ANYINTERACT', " + secondSQL + ", 0.0000005)='TRUE'";
	}

	/**
	 * Generate the SQL required to find the largest X value in the line segment SQL expression.
	 *
	 * @param lineSegment
	 * @return SQL
	 */
	@Override
	public String doLineSegment2DGetMaxXTransform(String lineSegment) {
		return "SDO_GEOM.SDO_MAX_MBR_ORDINATE("+lineSegment+", MDSYS.SDO_DIM_ARRAY(\n" +
"						MDSYS.SDO_DIM_ELEMENT('X', -9999999999, 9999999999, 0.0000000001),\n" +
"					           MDSYS.SDO_DIM_ELEMENT('Y', -9999999999, 9999999999, 0.0000000001)\n" +
"						), 1)";
	}

	/**
	 * Generate the SQL required to find the smallest X value in the line segment SQL expression.
	 * 
	 * @param lineSegment
	 * @return SQL
	 */
	@Override
	public String doLineSegment2DGetMinXTransform(String lineSegment) {
		return "SDO_GEOM.SDO_MIN_MBR_ORDINATE("+lineSegment+", MDSYS.SDO_DIM_ARRAY(\n" +
"						MDSYS.SDO_DIM_ELEMENT('X', -9999999999, 9999999999, 0.0000000001),\n" +
"					           MDSYS.SDO_DIM_ELEMENT('Y', -9999999999, 9999999999, 0.0000000001)\n" +
"						), 1)";
	}

	/**
	 * Generate the SQL required to find the largest Y value in the line segment SQL expression.
	 *
	 * @param lineSegment
	 * @return SQL
	 */
	@Override
	public String doLineSegment2DGetMaxYTransform(String lineSegment) {
		return "SDO_GEOM.SDO_MAX_MBR_ORDINATE("+lineSegment+", MDSYS.SDO_DIM_ARRAY(\n" +
"						MDSYS.SDO_DIM_ELEMENT('X', -9999999999, 9999999999, 0.0000000001),\n" +
"					           MDSYS.SDO_DIM_ELEMENT('Y', -9999999999, 9999999999, 0.0000000001)\n" +
"						), 2)";
	}

	/**
	 * Generate the SQL required to find the smallest Y value in the line segment SQL expression.
	 *
	 * @param lineSegment
	 * @return SQL
	 */
	@Override
	public String doLineSegment2DGetMinYTransform(String lineSegment) {
		return "SDO_GEOM.SDO_MIN_MBR_ORDINATE("+lineSegment+", MDSYS.SDO_DIM_ARRAY(\n" +
"						MDSYS.SDO_DIM_ELEMENT('X', -9999999999, 9999999999, 0.0000000001),\n" +
"					           MDSYS.SDO_DIM_ELEMENT('Y', -9999999999, 9999999999, 0.0000000001)\n" +
"						), 2)";
	}

	/**
	 * Generate the SQL required to the rectangular boundary that fully encloses the line segment SQL expression.
	 *
	 * @param lineSegment
	 * @return SQL
	 */
	@Override
	public String doLineSegment2DGetBoundingBoxTransform(String lineSegment) {
		return "SDO_GEOM.SDO_MBR("+lineSegment+")";
//		throw new UnsupportedOperationException("Not supported yet."); 
	}

	/**
	 * Generate the SQL required to find the dimension of the line segment SQL expression.
	 *
	 * @param lineSegment
	 * @return SQL
	 */
	@Override
	public String doLineSegment2DDimensionTransform(String lineSegment) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generate the SQL required to find whether the 2 line segment SQL expressions are NOT equal.
	 *
	 * @param firstLineSegment
	 * @param secondLineSegment
	 * @return SQL
	 */
	@Override
	public String doLineSegment2DNotEqualsTransform(String firstLineSegment, String secondLineSegment) {
				return "SDO_GEOM.RELATE(" + firstLineSegment + ", 'equal', " + secondLineSegment + ", 0.0000005)='FALSE'";
	}

	/**
	 * Generate the SQL required to find whether the 2 line segment SQL expressions are equal.
	 *
	 * @param firstLineSegment
	 * @param secondLineSegment
	 * @return
	 */
	@Override
	public String doLineSegment2DEqualsTransform(String firstLineSegment, String secondLineSegment) {
				return "SDO_GEOM.RELATE(" + firstLineSegment + ", 'equal', " + secondLineSegment + ", 0.0000005)='EQUAL'";
	}

	/**
	 * Generate the SQL required to convert the line segment SQL expression into the WKT string format.
	 *
	 * @param lineSegment
	 * @return
	 */
	@Override
	public String doLineSegment2DAsTextTransform(String lineSegment) {
				return "TO_CHAR(SDO_UTIL.TO_WKTGEOMETRY(" + lineSegment + "))";
	}

	/**
	 * Generate the SQL required to find the intersection point of the 2 line segment SQL expressions.
	 *
	 * @param firstLineSegment
	 * @param secondLineSegment
	 * @return an SQL expression that will evaluate to the intersection point of the 2 line segments or NULL.
	 */
	@Override
	public String doLineSegment2DIntersectionPointWithLineSegment2DTransform(String firstLineSegment, String secondLineSegment) {
		return "SDO_GEOM.SDO_INTERSECTION("+firstLineSegment+", "+secondLineSegment+", 0.0000005)";
	}
}
