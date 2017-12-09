/*
 * Copyright 2014 Gregory Graham.
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

import nz.co.gregs.dbvolution.internal.oracle.aws.MultiPoint2DFunctions;
import nz.co.gregs.dbvolution.internal.oracle.aws.Polygon2DFunctions;
import nz.co.gregs.dbvolution.internal.oracle.aws.LineSegment2DFunctions;
import nz.co.gregs.dbvolution.internal.oracle.aws.Line2DFunctions;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.WKTReader;
import java.util.List;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.datatypes.spatial2D.*;
import nz.co.gregs.dbvolution.exceptions.IncorrectGeometryReturnedForDatatype;

/**
 * Defines the features of Amazon's Oracle RDS databases that differ from the
 * standard database.
 *
 * <p>
 * This DBDefinition is sub-classed by {@link Oracle11XEDBDefinition} and
 * {@link  Oracle12DBDefinition} to provide the full set of features required to
 * use an Oracle database.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class OracleAWSDBDefinition extends OracleDBDefinition {

	@Override
	public String getDatabaseDataTypeOfQueryableDatatype(QueryableDatatype<?> qdt) {
		if (qdt instanceof DBPoint2D) {
			return " VARCHAR(2001) ";
		} else if (qdt instanceof DBLine2D) {
			return " VARCHAR(2002) ";
		} else if (qdt instanceof DBPolygon2D) {
			return " VARCHAR(2003) ";
		} else if (qdt instanceof DBLineSegment2D) {
			return " VARCHAR(2004) ";
		} else if (qdt instanceof DBMultiPoint2D) {
			return " VARCHAR(2005) ";
		} else {
			return super.getDatabaseDataTypeOfQueryableDatatype(qdt);
		}
	}

	@Override
	public boolean requiresSpatial2DIndexes() {
		return false;
	}

	@Override
	public List<String> getSpatial2DIndexSQL(DBDatabase aThis, final String formatTableName, final String formatColumnName) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public String transformCoordinatesIntoDatabasePoint2DFormat(String xValue, String yValue) {
		return "'POINT (" + xValue + " " + yValue + ")'";
	}

	@Override
	public String doPoint2DDistanceBetweenTransform(String polygon2DSQL, String otherPolygon2DSQL) {
		throw new UnsupportedOperationException("Spatial functions have been removed from AWS Oracle");
	}

	@Override
	public String transformPoint2DArrayToDatabasePolygon2DFormat(List<String> pointSQL) {
		throw new UnsupportedOperationException("Spatial functions have been removed from AWS Oracle");
	}

	@Override
	public String doPoint2DAsTextTransform(String point2DSQL) {
		throw new UnsupportedOperationException("Spatial functions have been removed from AWS Oracle");
	}

	@Override
	public String doPoint2DGetBoundingBoxTransform(String point2DSQL) {
		throw new UnsupportedOperationException("Spatial functions have been removed from AWS Oracle");
	}

	@Override
	public String doPoint2DMeasurableDimensionsTransform(String point2DSQL) {
		throw new UnsupportedOperationException("Spatial functions have been removed from AWS Oracle");
	}

	@Override
	public String doPoint2DGetYTransform(String point2DSQL) {
		throw new UnsupportedOperationException("Spatial functions have been removed from AWS Oracle");
	}

	@Override
	public String doPoint2DGetXTransform(String point2DSQL) {
		throw new UnsupportedOperationException("Spatial functions have been removed from AWS Oracle");
	}

	@Override
	public String doPoint2DEqualsTransform(String firstPoint, String secondPoint) {
		throw new UnsupportedOperationException("Spatial functions have been removed from AWS Oracle");
	}

	@Override
	public String transformLineStringIntoDatabaseLine2DFormat(LineString lineString) {
		String wktValue = lineString.toText();
		return "'" + wktValue + "'";
	}

	@Override
	public String transformLineSegmentIntoDatabaseLineSegment2DFormat(LineSegment lineSegment) {
		LineString line = (new GeometryFactory()).createLineString(new Coordinate[]{lineSegment.getCoordinate(0), lineSegment.getCoordinate(1)});
		String wktValue = line.toText();
		return "'" + wktValue + "'";
	}

//	@Override
//	public String OldtransformPolygonIntoDatabasePolygon2DFormat(Polygon polygon) {
//		String wktValue = polygon.toText();
//		return "'" + wktValue + "'";
//	}
	@Override
	public String doLine2DAsTextTransform(String line2DSQL) {
		return "(" + line2DSQL + ")";
	}

	@Override
	public String doLine2DGetMinYTransform(String toSQLString) {
		return Line2DFunctions.MINY + "(" + toSQLString + ")";
	}

	@Override
	public String doLine2DGetMaxYTransform(String toSQLString) {
		return Line2DFunctions.MAXY + "(" + toSQLString + ")";
	}

	@Override
	public String doLine2DGetMinXTransform(String toSQLString) {
		return Line2DFunctions.MINX + "(" + toSQLString + ")";
	}

	@Override
	public String doLine2DGetMaxXTransform(String toSQLString) {
		return Line2DFunctions.MAXX + "(" + toSQLString + ")";
	}

	@Override
	public String doLine2DGetBoundingBoxTransform(String toSQLString) {
		return Line2DFunctions.BOUNDINGBOX + "(" + toSQLString + ")";
	}

	@Override
	public String doLine2DIntersectsLine2DTransform(String firstLine, String secondLine) {
		return "(" + Line2DFunctions.INTERSECTSLINE2D + "((" + firstLine + "), (" + secondLine + "))=1)";
	}

	@Override
	public String doLine2DIntersectionPointWithLine2DTransform(String firstLine, String secondLine) {
		return Line2DFunctions.INTERSECTNWLINE2D + "((" + firstLine + "), (" + secondLine + "))";
	}

	@Override
	public String doLine2DAllIntersectionPointsWithLine2DTransform(String firstGeometry, String secondGeometry) {
		return Line2DFunctions.ALLINTERSECTSL2D + "((" + firstGeometry + "), (" + secondGeometry + "))";
	}

	@Override
	public String transformPolygonIntoDatabasePolygon2DFormat(Polygon geom) {
		String wktValue = geom.toText();
		return Polygon2DFunctions.CREATE_WKTPOLY2D + "('" + wktValue + "')";
	}

	@Override
	public String doPolygon2DGetMinYTransform(String polygon2DSQL) {
		return Polygon2DFunctions.MINY + "(" + polygon2DSQL + ")";
	}

	@Override
	public String doPolygon2DGetMaxYTransform(String polygon2DSQL) {
		return Polygon2DFunctions.MAXY + "(" + polygon2DSQL + ")";
	}

	@Override
	public String doPolygon2DGetMinXTransform(String polygon2DSQL) {
		return Polygon2DFunctions.MINX + "(" + polygon2DSQL + ")";
	}

	@Override
	public String doPolygon2DGetMaxXTransform(String polygon2DSQL) {
		return Polygon2DFunctions.MAXX + "(" + polygon2DSQL + ")";
	}

	@Override
	public String doPolygon2DGetExteriorRingTransform(String polygon2DSQL) {
		return Polygon2DFunctions.EXTERIORRING + "(" + polygon2DSQL + ")";
	}

	@Override
	public String doPolygon2DGetAreaTransform(String polygon2DSQL) {
		return Polygon2DFunctions.AREA + "(" + polygon2DSQL + ")";
	}

	@Override
	public String doPolygon2DGetBoundingBoxTransform(String polygon2DSQL) {
		return Polygon2DFunctions.BOUNDINGBOX + "(" + polygon2DSQL + ")";
	}

	@Override
	public String doPolygon2DMeasurableDimensionsTransform(String toSQLString) {
		return Polygon2DFunctions.DIMENSION + "(" + toSQLString + ")";
	}

	@Override
	public String doPolygon2DWithinTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.WITHIN + "(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DTouchesTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.TOUCHES + "(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DOverlapsTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.OVERLAPS + "(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DDoesNotIntersectTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.DISJOINT + "(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DContainsPolygon2DTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.CONTAINS + "(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DIntersectsTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.INTERSECTS + "(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DIntersectionTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.INTERSECTION + "(" + firstGeometry + ", " + secondGeometry + ")";
	}

	@Override
	public String doPolygon2DEqualsTransform(String firstGeometry, String secondGeometry) {
		return "(" + Polygon2DFunctions.EQUALS + "(" + firstGeometry + ", " + secondGeometry + ")=1)";
	}

	@Override
	public String doLineSegment2DIntersectsLineSegment2DTransform(String firstSQL, String secondSQL) {
		return "(" + LineSegment2DFunctions.INTERSECTS_LSEG2D + "((" + firstSQL + "), (" + secondSQL + "))=1)";
	}

	@Override
	public String doLineSegment2DGetMaxXTransform(String lineSegment) {
		return LineSegment2DFunctions.MAXX + "(" + lineSegment + ")";
	}

	@Override
	public String doLineSegment2DGetMinXTransform(String lineSegment) {
		return LineSegment2DFunctions.MINX + "(" + lineSegment + ")";
	}

	@Override
	public String doLineSegment2DGetMaxYTransform(String lineSegment) {
		return LineSegment2DFunctions.MAXY + "(" + lineSegment + ")";
	}

	@Override
	public String doLineSegment2DGetMinYTransform(String lineSegment) {
		return LineSegment2DFunctions.MINY + "(" + lineSegment + ")";
	}

	@Override
	public String doLineSegment2DGetBoundingBoxTransform(String lineSegment) {
		return LineSegment2DFunctions.BOUNDINGBOX + "(" + lineSegment + ")";
	}

	@Override
	public String doLineSegment2DDimensionTransform(String lineSegment) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doLineSegment2DNotEqualsTransform(String firstLineSegment, String secondLineSegment) {
		return " NOT " + LineSegment2DFunctions.EQUALS + "((" + firstLineSegment + "), (" + secondLineSegment + "))";
	}

	@Override
	public String doLineSegment2DEqualsTransform(String firstLineSegment, String secondLineSegment) {
		return "(" + LineSegment2DFunctions.EQUALS + "((" + firstLineSegment + "), (" + secondLineSegment + "))=1)";
	}

	@Override
	public String doLineSegment2DAsTextTransform(String lineSegment) {
		return lineSegment;
	}

	@Override
	public String doLineSegment2DIntersectionPointWithLineSegment2DTransform(String firstLineSegment, String secondLineSegment) {
		return LineSegment2DFunctions.INTERSECTPT_LSEG2D + "((" + firstLineSegment + "), (" + secondLineSegment + "))";
	}

	@Override
	public String transformMultiPoint2DToDatabaseMultiPoint2DValue(MultiPoint points) {
		String wktValue = points.toText();
		return "'" + wktValue + "'";
	}

	@Override
	public MultiPoint transformDatabaseMultiPoint2DValueToJTSMultiPoint(String pointsAsString) throws com.vividsolutions.jts.io.ParseException {
		MultiPoint mpoint = null;
		WKTReader wktReader = new WKTReader();
		Geometry geometry = wktReader.read(pointsAsString);
		if (geometry instanceof MultiPoint) {
			mpoint = (MultiPoint) geometry;
		} else if (geometry instanceof Point) {
			mpoint = (new GeometryFactory().createMultiPoint(new Point[]{((Point) geometry)}));
		} else {
			throw new IncorrectGeometryReturnedForDatatype(geometry, geometry);
		}
		return mpoint;
	}

	@Override
	public String doMultiPoint2DEqualsTransform(String first, String second) {
		return "(" + MultiPoint2DFunctions.EQUALS + "(" + first + ", " + second + ")=1)";
	}

	@Override
	public String doMultiPoint2DGetPointAtIndexTransform(String first, String index) {
		return "" + MultiPoint2DFunctions.GETFROMINDEX + "(" + first + ", " + index + ")";
	}

	@Override
	public String doMultiPoint2DGetNumberOfPointsTransform(String first) {
		return "" + MultiPoint2DFunctions.POINTCOUNT + "(" + first + ")";
	}

	@Override
	public String doMultiPoint2DMeasurableDimensionsTransform(String first) {
		return "" + MultiPoint2DFunctions.DIMENSION + "(" + first + ")";
	}

	@Override
	public String doMultiPoint2DGetBoundingBoxTransform(String first) {
		return "" + MultiPoint2DFunctions.BOUNDINGBOX + "(" + first + ")";
	}

	@Override
	public String doMultiPoint2DAsTextTransform(String first) {
		return "" + MultiPoint2DFunctions.ASTEXT + "(" + first + ")";
	}

	@Override
	public String doMultiPoint2DToLine2DTransform(String first) {
		return "" + MultiPoint2DFunctions.ASLINE2D + "(" + first + ")";
	}

	@Override
	public String doMultiPoint2DGetMinYTransform(String first) {
		return "" + MultiPoint2DFunctions.MINY + "(" + first + ")";
	}

	@Override
	public String doMultiPoint2DGetMinXTransform(String first) {
		return "" + MultiPoint2DFunctions.MINX + "(" + first + ")";
	}

	@Override
	public String doMultiPoint2DGetMaxYTransform(String first) {
		return "" + MultiPoint2DFunctions.MAXY + "(" + first + ")";
	}

	@Override
	public String doMultiPoint2DGetMaxXTransform(String first) {
		return "" + MultiPoint2DFunctions.MAXX + "(" + first + ")";
	}

}
