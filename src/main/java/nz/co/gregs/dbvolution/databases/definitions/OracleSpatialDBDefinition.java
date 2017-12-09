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
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBLine2D;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBLineSegment2D;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBMultiPoint2D;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBPoint2D;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBPolygon2D;
import nz.co.gregs.dbvolution.results.Spatial2DResult;

/**
 * A subclass of OracleDB that contains definitions of standard Spatial
 * functions shared by Oracle databases with Spatial functions.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class OracleSpatialDBDefinition extends OracleDBDefinition {

	@Override
	public String getDatabaseDataTypeOfQueryableDatatype(QueryableDatatype<?> qdt) {
		if (qdt instanceof Spatial2DResult) {
			return " SDO_GEOMETRY ";
//		} else if (qdt instanceof DBLine2D) {
//			return " SDO_GEOMETRY ";
//		} else if (qdt instanceof DBLineSegment2D) {
//			return " SDO_GEOMETRY ";
//		} else if (qdt instanceof DBPolygon2D) {
//			return " SDO_GEOMETRY ";
		} else {
			return super.getDatabaseDataTypeOfQueryableDatatype(qdt);
		}
	}

	@Override
	public String doColumnTransformForSelect(QueryableDatatype<?> qdt, String selectableName) {
		if (qdt instanceof DBPolygon2D) {
			return doPolygon2DAsTextTransform(selectableName);
		} else if (qdt instanceof DBLine2D) {
			return doLine2DAsTextTransform(selectableName);
		} else if (qdt instanceof DBPoint2D) {
			return doPoint2DAsTextTransform(selectableName);
		} else if (qdt instanceof DBLineSegment2D) {
			return doLineSegment2DAsTextTransform(selectableName);
		} else if (qdt instanceof DBMultiPoint2D) {
			return doMultiPoint2DAsTextTransform(selectableName);
		} else {
			return selectableName;
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
				//add("DROP INDEX " + formatNameForDatabase("DBV_" + formatTableName + "_" + formatColumnName + "_sp2didx")+"");
				add("delete from USER_SDO_GEOM_METADATA "
						+ "where table_name = '" + formatTableName.toUpperCase() + "' "
						+ "and column_name = '" + formatColumnName.toUpperCase() + "'");
				add(
						"INSERT INTO USER_SDO_GEOM_METADATA \n"
						+ "  select \n"
						+ "  '" + formatTableName + "',\n"
						+ "  '" + formatColumnName + "',\n"
						+ "  MDSYS.SDO_DIM_ARRAY(\n"
						+ "    MDSYS.SDO_DIM_ELEMENT('X', -9999999999, 9999999999, 0.0000000001),\n"
						+ "    MDSYS.SDO_DIM_ELEMENT('Y', -9999999999, 9999999999, 0.0000000001)\n"
						+ "     ),\n"
						+ "  NULL   -- SRID\n"
						+ " from dual where not exists ("
						+ "select * from USER_SDO_GEOM_METADATA  "
						+ "where table_name = '" + formatTableName.toUpperCase() + "' "
						+ "and column_name = '" + formatColumnName.toUpperCase() + "')");
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

//	@Override
//	public String OldtransformPolygonIntoDatabasePolygon2DFormat(Polygon point) {
////		final Coordinate coordinate = point.getCoordinate();
////		return "SDO_GEOMETRY(2003, NULL, SDO_POINT_TYPE(" + coordinate.x + ", " + coordinate.y + ",NULL), NULL, NULL)";
//		return "SDO_UTIL.FROM_WKTGEOMETRY('" + point.toText() + "')";
//	}
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
				+ "MDSYS.SDO_ORDINATE_ARRAY(" + lineSegment.p0.x + ", " + lineSegment.p0.y + ", " + lineSegment.p1.x + ", " + lineSegment.p1.y + ")"
				+ ")";
	}

	@Override
	public String doPoint2DDistanceBetweenTransform(String polygon2DSQL, String otherPolygon2DSQL) {
		return "SDO_GEOM.SDO_DISTANCE(" + polygon2DSQL + ", " + otherPolygon2DSQL + ", 0.000001)"; //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String transformPoint2DArrayToDatabasePolygon2DFormat(List<String> pointSQL) {
		StringBuilder ordinateArray = new StringBuilder("MDSYS.SDO_ORDINATE_ARRAY(");
		final String ordinateSep = ", ";
		String pairSep = "";
		for (String pointish : pointSQL) {
			ordinateArray
					.append(pairSep)
					.append(doPoint2DGetXTransform(pointish))
					.append(ordinateSep)
					.append(doPoint2DGetYTransform(pointish));
			pairSep = ", ";
		}
		//+ lineSegment.p0.x + ", " + lineSegment.p0.y + ", " + lineSegment.p1.x + ", " + lineSegment.p1.y 
		ordinateArray.append(")");
		return "MDSYS.SDO_GEOMETRY(2003, NULL, NULL,"
				+ "MDSYS.SDO_ELEM_INFO_ARRAY(1,2003,1),"
				+ ordinateArray
				+ ")";
	}

	@Override
	public String transformCoordinateArrayToDatabasePolygon2DFormat(List<String> pointSQL) {
		StringBuilder ordinateArray = new StringBuilder("MDSYS.SDO_ORDINATE_ARRAY(");
		final String ordinateSep = ", ";
		String pairSep = "";
		String sep = pairSep;
		for (String pointish : pointSQL) {
			ordinateArray
					.append(sep)
					.append(pointish);
			pairSep = ", ";
			if (sep.equals(ordinateSep)) {
				sep = pairSep;
			} else {
				sep = ordinateSep;
			}
		}
		//+ lineSegment.p0.x + ", " + lineSegment.p0.y + ", " + lineSegment.p1.x + ", " + lineSegment.p1.y 
		ordinateArray.append(")");
		return "MDSYS.SDO_GEOMETRY(2003, NULL, NULL,"
				+ "MDSYS.SDO_ELEM_INFO_ARRAY(1,2003,1),"
				+ ordinateArray
				+ ")";
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

	@Override
	public String doLineSegment2DIntersectsLineSegment2DTransform(String firstSQL, String secondSQL) {
		return "SDO_GEOM.RELATE(" + firstSQL + ", 'ANYINTERACT', " + secondSQL + ", 0.0000005)='TRUE'";
	}

	@Override
	public String doLineSegment2DGetMaxXTransform(String lineSegment) {
		return "SDO_GEOM.SDO_MAX_MBR_ORDINATE(" + lineSegment + ", MDSYS.SDO_DIM_ARRAY(\n"
				+ "						MDSYS.SDO_DIM_ELEMENT('X', -9999999999, 9999999999, 0.0000000001),\n"
				+ "					           MDSYS.SDO_DIM_ELEMENT('Y', -9999999999, 9999999999, 0.0000000001)\n"
				+ "						), 1)";
	}

	@Override
	public String doLineSegment2DGetMinXTransform(String lineSegment) {
		return "SDO_GEOM.SDO_MIN_MBR_ORDINATE(" + lineSegment + ", MDSYS.SDO_DIM_ARRAY(\n"
				+ "						MDSYS.SDO_DIM_ELEMENT('X', -9999999999, 9999999999, 0.0000000001),\n"
				+ "					           MDSYS.SDO_DIM_ELEMENT('Y', -9999999999, 9999999999, 0.0000000001)\n"
				+ "						), 1)";
	}

	@Override
	public String doLineSegment2DGetMaxYTransform(String lineSegment) {
		return "SDO_GEOM.SDO_MAX_MBR_ORDINATE(" + lineSegment + ", MDSYS.SDO_DIM_ARRAY(\n"
				+ "						MDSYS.SDO_DIM_ELEMENT('X', -9999999999, 9999999999, 0.0000000001),\n"
				+ "					           MDSYS.SDO_DIM_ELEMENT('Y', -9999999999, 9999999999, 0.0000000001)\n"
				+ "						), 2)";
	}

	@Override
	public String doLineSegment2DGetMinYTransform(String lineSegment) {
		return "SDO_GEOM.SDO_MIN_MBR_ORDINATE(" + lineSegment + ", MDSYS.SDO_DIM_ARRAY(\n"
				+ "						MDSYS.SDO_DIM_ELEMENT('X', -9999999999, 9999999999, 0.0000000001),\n"
				+ "					           MDSYS.SDO_DIM_ELEMENT('Y', -9999999999, 9999999999, 0.0000000001)\n"
				+ "						), 2)";
	}

	@Override
	public String doLineSegment2DGetBoundingBoxTransform(String lineSegment) {
		return "SDO_GEOM.SDO_MBR(" + lineSegment + ")";
	}

	@Override
	public String doLineSegment2DDimensionTransform(String lineSegment) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doLineSegment2DNotEqualsTransform(String firstLineSegment, String secondLineSegment) {
		return "SDO_GEOM.RELATE(" + firstLineSegment + ", 'equal', " + secondLineSegment + ", 0.0000005)='FALSE'";
	}

	@Override
	public String doLineSegment2DEqualsTransform(String firstLineSegment, String secondLineSegment) {
		return "SDO_GEOM.RELATE(" + firstLineSegment + ", 'equal', " + secondLineSegment + ", 0.0000005)='EQUAL'";
	}

	@Override
	public String doLineSegment2DAsTextTransform(String lineSegment) {
		return "TO_CHAR(SDO_UTIL.TO_WKTGEOMETRY(" + lineSegment + "))";
	}

	@Override
	public String doLineSegment2DIntersectionPointWithLineSegment2DTransform(String firstLineSegment, String secondLineSegment) {
		return "SDO_GEOM.SDO_INTERSECTION(" + firstLineSegment + ", " + secondLineSegment + ", 0.0000005)";
	}

	@Override
	public String doMultiPoint2DEqualsTransform(String first, String second) {
		return "SDO_GEOM.RELATE(" + first + ", 'equal', " + second + ", 0.0000005)='EQUAL'";
	}

	@Override
	public String doMultiPoint2DNotEqualsTransform(String first, String second) {
		return "SDO_GEOM.RELATE(" + first + ", 'equal', " + second + ", 0.0000005)='FALSE'";
	}

	@Override
	public String doMultiPoint2DGetPointAtIndexTransform(String first, String index) {
		return "(" + first + ").SDO_ORDINATES(" + index + ")";
	}

	@Override
	public String doMultiPoint2DGetNumberOfPointsTransform(String multiPoint2D) {
		return "(" + multiPoint2D + ").SDO_ORDINATES.count";
	}

	@Override
	public String doMultiPoint2DMeasurableDimensionsTransform(String multipoint2D) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doMultiPoint2DGetBoundingBoxTransform(String multiPoint2D) {
		return "SDO_GEOM.SDO_MBR(" + multiPoint2D + ")";
	}

	@Override
	public String doMultiPoint2DAsTextTransform(String multiPoint2D) {
		return "TO_CHAR(SDO_UTIL.TO_WKTGEOMETRY(" + multiPoint2D + "))";
	}

	@Override
	public String doMultiPoint2DToLine2DTransform(String multiPoint2D) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doMultiPoint2DGetMinYTransform(String multiPoint2D) {
		return "SDO_GEOM.SDO_MIN_MBR_ORDINATE(" + multiPoint2D + ", MDSYS.SDO_DIM_ARRAY(\n"
				+ "						MDSYS.SDO_DIM_ELEMENT('X', -9999999999, 9999999999, 0.0000000001),\n"
				+ "					           MDSYS.SDO_DIM_ELEMENT('Y', -9999999999, 9999999999, 0.0000000001)\n"
				+ "						), 2)";
	}

	@Override
	public String doMultiPoint2DGetMinXTransform(String multiPoint2D) {
		return "SDO_GEOM.SDO_MIN_MBR_ORDINATE(" + multiPoint2D + ", MDSYS.SDO_DIM_ARRAY(\n"
				+ "						MDSYS.SDO_DIM_ELEMENT('X', -9999999999, 9999999999, 0.0000000001),\n"
				+ "					           MDSYS.SDO_DIM_ELEMENT('Y', -9999999999, 9999999999, 0.0000000001)\n"
				+ "						), 1)";
	}

	@Override
	public String doMultiPoint2DGetMaxYTransform(String multiPoint2D) {
		return "SDO_GEOM.SDO_MAX_MBR_ORDINATE(" + multiPoint2D + ", MDSYS.SDO_DIM_ARRAY(\n"
				+ "						MDSYS.SDO_DIM_ELEMENT('X', -9999999999, 9999999999, 0.0000000001),\n"
				+ "					           MDSYS.SDO_DIM_ELEMENT('Y', -9999999999, 9999999999, 0.0000000001)\n"
				+ "						), 2)";
	}

	@Override
	public String doMultiPoint2DGetMaxXTransform(String multiPoint2D) {
		return "SDO_GEOM.SDO_MAX_MBR_ORDINATE(" + multiPoint2D + ", MDSYS.SDO_DIM_ARRAY(\n"
				+ "						MDSYS.SDO_DIM_ELEMENT('X', -9999999999, 9999999999, 0.0000000001),\n"
				+ "					           MDSYS.SDO_DIM_ELEMENT('Y', -9999999999, 9999999999, 0.0000000001)\n"
				+ "						), 1)";
	}

	@Override
	public String doLine2DGetMagnitudeTransform(String line2DSQL) {
		return super.doLine2DGetMagnitudeTransform(line2DSQL); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doLine2DHasMagnitudeTransform(String line2DSQL) {
		return super.doLine2DHasMagnitudeTransform(line2DSQL); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doLine2DSpatialDimensionsTransform(String line2DSQL) {
		return "(" + line2DSQL + ").GET_DIMS()";
	}

	@Override
	public String doLine2DAllIntersectionPointsWithLine2DTransform(String firstGeometry, String secondGeometry) {
		return "SDO_GEOM.SDO_INTERSECTION(" + firstGeometry + ", " + secondGeometry + ", 0.0000005)";
	}

	@Override
	public String doLine2DIntersectionPointWithLine2DTransform(String firstLine, String secondLine) {
		return "SDO_GEOM.SDO_INTERSECTION(" + firstLine + ", " + secondLine + ", 0.0000005)";
	}

	@Override
	public String doLine2DIntersectsLine2DTransform(String firstLine, String secondLine) {
		return "SDO_GEOM.RELATE(" + firstLine + ", 'ANYINTERACT', " + secondLine + ", 0.0000005)='TRUE'";
	}

	@Override
	public String doLine2DGetMinYTransform(String line2DSQL) {
		return "SDO_GEOM.SDO_MIN_MBR_ORDINATE(" + line2DSQL + ", MDSYS.SDO_DIM_ARRAY(\n"
				+ "						MDSYS.SDO_DIM_ELEMENT('X', -9999999999, 9999999999, 0.0000000001),\n"
				+ "					           MDSYS.SDO_DIM_ELEMENT('Y', -9999999999, 9999999999, 0.0000000001)\n"
				+ "						), 2)";
	}

	@Override
	public String doLine2DGetMaxYTransform(String line2DSQL) {
		return "SDO_GEOM.SDO_MAX_MBR_ORDINATE(" + line2DSQL + ", MDSYS.SDO_DIM_ARRAY(\n"
				+ "						MDSYS.SDO_DIM_ELEMENT('X', -9999999999, 9999999999, 0.0000000001),\n"
				+ "					           MDSYS.SDO_DIM_ELEMENT('Y', -9999999999, 9999999999, 0.0000000001)\n"
				+ "						), 2)";
	}

	@Override
	public String doLine2DGetMinXTransform(String line2DSQL) {
		return "SDO_GEOM.SDO_MIN_MBR_ORDINATE(" + line2DSQL + ", MDSYS.SDO_DIM_ARRAY(\n"
				+ "						MDSYS.SDO_DIM_ELEMENT('X', -9999999999, 9999999999, 0.0000000001),\n"
				+ "					           MDSYS.SDO_DIM_ELEMENT('Y', -9999999999, 9999999999, 0.0000000001)\n"
				+ "						), 1)";
	}

	@Override
	public String doLine2DGetMaxXTransform(String line2DSQL) {
		return "SDO_GEOM.SDO_MAX_MBR_ORDINATE(" + line2DSQL + ", MDSYS.SDO_DIM_ARRAY(\n"
				+ "						MDSYS.SDO_DIM_ELEMENT('X', -9999999999, 9999999999, 0.0000000001),\n"
				+ "					           MDSYS.SDO_DIM_ELEMENT('Y', -9999999999, 9999999999, 0.0000000001)\n"
				+ "						), 1)";
	}

	@Override
	public String doLine2DGetBoundingBoxTransform(String line2DSQL) {
		return "SDO_GEOM.SDO_MBR(" + line2DSQL + ")";
	}

	@Override
	public String doLine2DMeasurableDimensionsTransform(String line2DSQL) {
		return super.doLine2DMeasurableDimensionsTransform(line2DSQL); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doLine2DNotEqualsTransform(String line2DSQL, String otherLine2DSQL) {
		return "SDO_GEOM.RELATE(" + line2DSQL + ", 'equal', " + otherLine2DSQL + ", 0.0000005)='FALSE'";
	}

	@Override
	public String doLine2DEqualsTransform(String line2DSQL, String otherLine2DSQL) {
		return "SDO_GEOM.RELATE(" + line2DSQL + ", 'equal', " + otherLine2DSQL + ", 0.0000005)='EQUAL'";
	}

	@Override
	public String doLine2DAsTextTransform(String line2DSQL) {
		return "TO_CHAR(SDO_UTIL.TO_WKTGEOMETRY(" + line2DSQL + "))";
	}

	@Override
	public String doPolygon2DGetMagnitudeTransform(String polygon2DSQL) {
		return super.doPolygon2DGetMagnitudeTransform(polygon2DSQL); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doPolygon2DSpatialDimensionsTransform(String polygon2DSQL) {
		return "(" + polygon2DSQL + ").GET_DIMS()";
	}

	@Override
	public String doPolygon2DAsTextTransform(String polygonSQL) {
		return "TO_CHAR(SDO_UTIL.TO_WKTGEOMETRY(" + polygonSQL + "))";
	}

	@Override
	public String doPolygon2DContainsPoint2DTransform(String polygon2DSQL, String point2DSQL) {
		return "SDO_GEOM.RELATE(" + polygon2DSQL + ", 'CONTAINS', " + point2DSQL + ", 0.0000005)='CONTAINS'";
	}

	@Override
	public String transformPolygonIntoDatabasePolygon2DFormat(Polygon polygon2D) {
		return "SDO_UTIL.FROM_WKTGEOMETRY(" + polygon2D.toText() + ")";
	}

	@Override
	public String doPolygon2DGetMinYTransform(String polygon2DSQL) {
		return "SDO_GEOM.SDO_MIN_MBR_ORDINATE(" + polygon2DSQL + ", MDSYS.SDO_DIM_ARRAY(\n"
				+ "						MDSYS.SDO_DIM_ELEMENT('X', -9999999999, 9999999999, 0.0000000001),\n"
				+ "					           MDSYS.SDO_DIM_ELEMENT('Y', -9999999999, 9999999999, 0.0000000001)\n"
				+ "						), 2)";
	}

	@Override
	public String doPolygon2DGetMaxYTransform(String polygon2DSQL) {
		return "SDO_GEOM.SDO_MAX_MBR_ORDINATE(" + polygon2DSQL + ", MDSYS.SDO_DIM_ARRAY(\n"
				+ "						MDSYS.SDO_DIM_ELEMENT('X', -9999999999, 9999999999, 0.0000000001),\n"
				+ "					           MDSYS.SDO_DIM_ELEMENT('Y', -9999999999, 9999999999, 0.0000000001)\n"
				+ "						), 2)";
	}

	@Override
	public String doPolygon2DGetMinXTransform(String polygon2DSQL) {
		return "SDO_GEOM.SDO_MIN_MBR_ORDINATE(" + polygon2DSQL + ", MDSYS.SDO_DIM_ARRAY(\n"
				+ "						MDSYS.SDO_DIM_ELEMENT('X', -9999999999, 9999999999, 0.0000000001),\n"
				+ "					           MDSYS.SDO_DIM_ELEMENT('Y', -9999999999, 9999999999, 0.0000000001)\n"
				+ "						), 1)";
	}

	@Override
	public String doPolygon2DGetMaxXTransform(String polygon2DSQL) {
		return "SDO_GEOM.SDO_MAX_MBR_ORDINATE(" + polygon2DSQL + ", MDSYS.SDO_DIM_ARRAY(\n"
				+ "						MDSYS.SDO_DIM_ELEMENT('X', -9999999999, 9999999999, 0.0000000001),\n"
				+ "					           MDSYS.SDO_DIM_ELEMENT('Y', -9999999999, 9999999999, 0.0000000001)\n"
				+ "						), 1)";
	}

	@Override
	public String doPolygon2DGetExteriorRingTransform(String polygon2DSQL) {
		return "SDO_UTIL.POLYGONTOLINE(" + polygon2DSQL + ")";
	}

	@Override
	public String doPolygon2DGetAreaTransform(String polygon2DSQL) {
		return "ABS(SDO_GEOM.SDO_AREA(" + polygon2DSQL + ", 0.0000005))";
	}

	@Override
	public String doPolygon2DGetBoundingBoxTransform(String polygon2DSQL) {
		return "SDO_GEOM.SDO_MBR(" + polygon2DSQL + ")";
	}

	@Override
	public String doPolygon2DMeasurableDimensionsTransform(String toSQLString) {
		return "2";
	}

	@Override
	public String doPolygon2DWithinTransform(String firstGeometry, String secondGeometry) {
		return "SDO_GEOM.RELATE(" + firstGeometry + ", 'COVEREDBY+INSIDE', " + secondGeometry + ", 0.0000005)<>'FALSE'";
	}

	@Override
	public String doPolygon2DTouchesTransform(String firstGeometry, String secondGeometry) {
		return "SDO_GEOM.RELATE(" + firstGeometry + ", 'TOUCH', " + secondGeometry + ", 0.0000005)='TOUCH'";
	}

	@Override
	public String doPolygon2DOverlapsTransform(String firstGeometry, String secondGeometry) {
		return "SDO_GEOM.RELATE(" + firstGeometry + ", 'OVERLAPBDYINTERSECT', " + secondGeometry + ", 0.0000005)='OVERLAPBDYINTERSECT'";
	}

	@Override
	public String doPolygon2DDoesNotIntersectTransform(String firstGeometry, String secondGeometry) {
		return "SDO_GEOM.RELATE(" + firstGeometry + ", 'DISJOINT', " + secondGeometry + ", 0.0000005)='DISJOINT'";
	}

	@Override
	public String doPolygon2DContainsPolygon2DTransform(String firstGeometry, String secondGeometry) {
		return "SDO_GEOM.RELATE(" + firstGeometry + ", 'COVERS+CONTAINS', " + secondGeometry + ", 0.0000005)<>'FALSE'";
	}

	@Override
	public String doPolygon2DIntersectsTransform(String firstGeometry, String secondGeometry) {
		return "SDO_GEOM.RELATE(" + firstGeometry + ", 'ANYINTERACT', " + secondGeometry + ", 0.0000005)='TRUE'";
	}

	@Override
	public String doPolygon2DIntersectionTransform(String firstGeometry, String secondGeometry) {
		return "SDO_GEOM.SDO_INTERSECTION(" + firstGeometry + ", " + secondGeometry + ", 0.0000005)";
	}

	@Override
	public String doPolygon2DEqualsTransform(String firstGeometry, String secondGeometry) {
		return "SDO_GEOM.RELATE(" + firstGeometry + ", 'EQUAL', " + secondGeometry + ", 0.0000005)='EQUAL'";
	}

}
