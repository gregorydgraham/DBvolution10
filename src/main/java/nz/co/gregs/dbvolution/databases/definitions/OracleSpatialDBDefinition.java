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
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineSegment;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.datatypes.spatial2D.*;

/**
 * A subclass of OracleDB that contains definitions of standard Spatial
 * functions shared by Oracle databases with Spatial functions.
 *
 * @author gregorygraham
 */
public class OracleSpatialDBDefinition extends OracleDBDefinition {

	@Override
	public String getSQLTypeOfDBDatatype(QueryableDatatype qdt) {
		if (qdt instanceof DBPoint2D) {
			return " SDO_GEOMETRY ";
		} else if (qdt instanceof DBLine2D) {
			return " SDO_GEOMETRY ";
		} else if (qdt instanceof DBPolygon2D) {
			return " SDO_GEOMETRY ";
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
	public String transformLineSegmentIntoDatabaseLineSegment2DFormat(LineSegment point) {
//		final Coordinate coordinate = point.p0;
//		final Coordinate otherCoord = point.p1;
//		return "SDO_GEOMETRY(2002, NULL, SDO_POINT_TYPE(" + coordinate.x + ", " + coordinate.y + ",NULL), NULL, NULL)";
		return "SDO_UTIL.FROM_WKTGEOMETRY('" + point.toGeometry(new GeometryFactory()).toText() + "')";
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
	public String doPoint2DDimensionTransform(String point2DSQL) {
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

}
