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

import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import nz.co.gregs.dbvolution.databases.Oracle12DB;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.results.Spatial2DResult;

/**
 * Defines the features of the Oracle 12 database when spatial options are
 * available.
 *
 * <p>
 * {@link Oracle12DB} instances automatically use the
 * {@link Oracle12DBDefinition}, switch to this definition if Oracle's builtin
 * spatial options are available to you.
 *
 * @author Gregory Graham
 */
public class Oracle12SpatialDB extends Oracle12DBDefinition {

	@Override
	public String getSQLTypeOfDBDatatype(QueryableDatatype qdt) {
		if (qdt instanceof Spatial2DResult) {
			return " SDO_GEOMETRY ";
//		} else if (qdt instanceof DBLine2D) {
//			return " SDO_GEOMETRY ";
//		} else if (qdt instanceof DBPolygon2D) {
//			return " SDO_GEOMETRY ";
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
	public String OldtransformPolygonIntoDatabasePolygon2DFormat(Polygon polygon) {
		return super.OldtransformPolygonIntoDatabasePolygon2DFormat(polygon); //To change body of generated methods, choose Tools | Templates.
	}

	/*SDO_GEOMETRY(
	 2002,  -- 2= two-dimensional, 0 = m value is not included, 02 = l1ne or curve
	 NULL,
	 NULL,
	 SDO_ELEM_INFO_ARRAY(1,1003,1), -- one polygon (exterior polygon ring)
	 SDO_ORDINATE_ARRAY(3,3, 6,3, 6,5, 4,5, 3,3)
	 )*/
	@Override
	public String transformLineStringIntoDatabaseLine2DFormat(LineString lineString) {
		return super.transformLineStringIntoDatabaseLine2DFormat(lineString); //To change body of generated methods, choose Tools | Templates.
	}

	/*SDO_GEOMETRY(
	 2001, -- 2= two-dimensional, 0 = m value is not included, 01 = point
	 NULL,
	 SDO_POINT_TYPE(15917.343,28141.968,NULL),
	 NULL,
	 NULL)
	 */
//	@Override
//	public String transformPoint2DIntoDatabaseFormat(Point point) {
//		final Coordinate coordinate = point.getCoordinate();
//		return "SDO_GEOMETRY(2001, NULL, SDO_POINT_TYPE(" + coordinate.x + ", " + coordinate.y + ",NULL), NULL, NULL)";
//	}

	@Override
	public LineString transformDatabaseLine2DValueToJTSLineString(String lineStringAsString) throws ParseException {
		return super.transformDatabaseLine2DValueToJTSLineString(lineStringAsString); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public Polygon transformDatabasePolygon2DToJTSPolygon(String geometryAsString) throws ParseException {
		return super.transformDatabasePolygon2DToJTSPolygon(geometryAsString); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public Point transformDatabasePoint2DValueToJTSPoint(String pointAsString) throws ParseException {
		return super.transformDatabasePoint2DValueToJTSPoint(pointAsString); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String transformCoordinatesIntoDatabasePoint2DFormat(String xValue, String yValue) {
		return super.transformCoordinatesIntoDatabasePoint2DFormat(xValue, yValue); //To change body of generated methods, choose Tools | Templates.
	}

}
