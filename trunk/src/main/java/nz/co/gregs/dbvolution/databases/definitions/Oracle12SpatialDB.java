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
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBLine2D;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBPoint2D;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBPolygon2D;


public class Oracle12SpatialDB extends Oracle12DBDefinition {
		

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
/*SDO_GEOMETRY(
    2003,  -- 2= two-dimensional, 0 = m value is not included, 03 = polygon
    NULL,
    NULL,
    SDO_ELEM_INFO_ARRAY(1,1003,1), -- one polygon (exterior polygon ring)
    SDO_ORDINATE_ARRAY(3,3, 6,3, 6,5, 4,5, 3,3)
  )*/
	@Override
	public String transformPolygonIntoDatabaseFormat(Polygon polygon) {
		return super.transformPolygonIntoDatabaseFormat(polygon); //To change body of generated methods, choose Tools | Templates.
	}

/*SDO_GEOMETRY(
    2002,  -- 2= two-dimensional, 0 = m value is not included, 02 = l1ne or curve
    NULL,
    NULL,
    SDO_ELEM_INFO_ARRAY(1,1003,1), -- one polygon (exterior polygon ring)
    SDO_ORDINATE_ARRAY(3,3, 6,3, 6,5, 4,5, 3,3)
  )*/
	@Override
	public String transformLineStringIntoDatabaseFormat(LineString lineString) {
		return super.transformLineStringIntoDatabaseFormat(lineString); //To change body of generated methods, choose Tools | Templates.
	}

/*SDO_GEOMETRY(
	2001, -- 2= two-dimensional, 0 = m value is not included, 01 = point
	NULL,
	SDO_POINT_TYPE(15917.343,28141.968,NULL),
	NULL,
	NULL)
    */
	@Override
	public String transformPointIntoDatabaseFormat(Point point) {
		final Coordinate coordinate = point.getCoordinate();
		return "SDO_GEOMETRY(2001, NULL, SDO_POINT_TYPE("+coordinate.x+", " +coordinate.y+",NULL), NULL, NULL)";
	}
	
	@Override
	public LineString transformDatabaseValueToJTSLineString(String lineStringAsString) throws ParseException {
		return super.transformDatabaseValueToJTSLineString(lineStringAsString); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public Polygon transformDatabaseValueToJTSPolygon(String geometryAsString) throws ParseException {
		return super.transformDatabaseValueToJTSPolygon(geometryAsString); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public Point transformDatabaseValueToJTSPoint(String pointAsString) throws ParseException {
		return super.transformDatabaseValueToJTSPoint(pointAsString); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String transformCoordinatesIntoDatabasePointFormat(String xValue, String yValue) {
		return super.transformCoordinatesIntoDatabasePointFormat(xValue, yValue); //To change body of generated methods, choose Tools | Templates.
	}

}
