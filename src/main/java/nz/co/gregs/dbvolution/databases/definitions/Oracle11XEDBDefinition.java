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
import com.vividsolutions.jts.geom.LineSegment;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.Oracle11XEDB;
import nz.co.gregs.dbvolution.internal.oracle.xe.GeometryFunctions;
import nz.co.gregs.dbvolution.internal.oracle.xe.Line2DFunctions;
import nz.co.gregs.dbvolution.internal.oracle.xe.MultiPoint2DFunctions;
import nz.co.gregs.dbvolution.query.QueryOptions;

/**
 * Defines the features of the Oracle 11 database that differ from the standard
 * database.
 *
 * <p>
 * This DBDefinition is automatically included in {@link Oracle11XEDB}
 * instances, and you should not need to use it directly.
 *
 * @author Gregory Graham
 */
public class Oracle11XEDBDefinition extends OracleSpatialDBDefinition {

	@Override
	public Object getLimitRowsSubClauseDuringSelectClause(QueryOptions options) {
		return " /*+ FIRST_ROWS(" + options.getRowLimit() + ") */ ";
	}

	@Override
	public Object getLimitRowsSubClauseAfterWhereClause(QueryOptions options) {
		return "";
	}

	@Override
	public String getColumnAutoIncrementSuffix() {
		return "";
	}

	@Override
	public boolean prefersTriggerBasedIdentities() {
		return true;
	}

	@Override
	public List<String> getTriggerBasedIdentitySQL(DBDatabase DB, String table, String column) {

		List<String> result = new ArrayList<String>();
		String sequenceName = getPrimaryKeySequenceName(table, column);
		result.add("CREATE SEQUENCE " + sequenceName);

		String triggerName = getPrimaryKeyTriggerName(table, column);
		result.add("CREATE OR REPLACE TRIGGER " + DB.getUsername() + "." + triggerName + " \n"
				+ "    BEFORE INSERT ON " + DB.getUsername() + "." + table + " \n"
				+ "    FOR EACH ROW\n"
				+ "    WHEN (new." + column + " IS NULL)\n"
				+ "    BEGIN\n"
				+ "      SELECT " + sequenceName + ".NEXTVAL\n"
				+ "      INTO   :new." + column + "\n"
				+ "      FROM   dual;\n"
				//				+ ":new."+column+" := "+sequenceName+".nextval; \n"
				+ "    END;\n");

		return result;
	}
	
	@Override
	public String doPoint2DGetBoundingBoxTransform(String point2DSQL) {
		throw new UnsupportedOperationException("Bounding Box is an unsupported operation in Oracle11 XE.");
	}

	@Override
	public String doPoint2DAsTextTransform(String point2DSQL) {
		return "'POINT ('||" + doPoint2DGetXTransform(point2DSQL) + "||' '||" + doPoint2DGetYTransform(point2DSQL) + "||')'";
	}

	@Override
	public String doLineSegment2DGetBoundingBoxTransform(String lineSegmentSQL) {
		throw new UnsupportedOperationException("Bounding Box is an unsupported operation in Oracle11 XE.");
	}

	@Override
	public String doLineSegment2DAsTextTransform(String lineSegmentSQL) {
		return "'LINESTRING ('||" + doPoint2DGetXTransform(doLineSegment2DStartPointTransform(lineSegmentSQL))
				+ "||' '||" + doPoint2DGetYTransform(doLineSegment2DStartPointTransform(lineSegmentSQL))
				+ "||', '||" + doPoint2DGetXTransform(doLineSegment2DEndPointTransform(lineSegmentSQL))
				+ "||' '||" + doPoint2DGetYTransform(doLineSegment2DEndPointTransform(lineSegmentSQL))
				+ "||')'";
	}

	@Override
	public String doLineSegment2DStartPointTransform(String lineSegmentSQL) {
		return "" + GeometryFunctions.GETPOINTATINDEX + "(" + lineSegmentSQL + ", 1)";
	}

	@Override
	public String doLineSegment2DEndPointTransform(String lineSegmentSQL) {
		return "" + GeometryFunctions.GETPOINTATINDEX + "(" + lineSegmentSQL + ", -1)";
	}

	@Override
	public String doLineSegment2DGetMaxXTransform(final String lineSegment) {
		return doGreatestOfTransformation(
				new ArrayList<String>() {
					{
						add(doPoint2DGetXTransform(doLineSegment2DStartPointTransform(lineSegment)));
						add(doPoint2DGetXTransform(doLineSegment2DEndPointTransform(lineSegment)));
					}
				}
		);
	}

	@Override
	public String doLineSegment2DGetMinXTransform(final String lineSegment) {
		return doLeastOfTransformation(
				new ArrayList<String>() {
					{
						add(doPoint2DGetXTransform(doLineSegment2DStartPointTransform(lineSegment)));
						add(doPoint2DGetXTransform(doLineSegment2DEndPointTransform(lineSegment)));
					}
				}
		);
	}

	@Override
	public String doLineSegment2DGetMaxYTransform(final String lineSegment) {
		return doGreatestOfTransformation(
				new ArrayList<String>() {
					{
						add(doPoint2DGetYTransform(doLineSegment2DStartPointTransform(lineSegment)));
						add(doPoint2DGetYTransform(doLineSegment2DEndPointTransform(lineSegment)));
					}
				}
		);
	}

	@Override
	public String doLineSegment2DGetMinYTransform(final String lineSegment) {
		return doLeastOfTransformation(
				new ArrayList<String>() {
					{
						add(doPoint2DGetYTransform(doLineSegment2DStartPointTransform(lineSegment)));
						add(doPoint2DGetYTransform(doLineSegment2DEndPointTransform(lineSegment)));
					}
				}
		);
	}

	@Override
	public String doMultiPoint2DGetMaxXTransform(String multiPoint2D) {
		return super.doMultiPoint2DGetMaxXTransform(multiPoint2D); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doMultiPoint2DGetMaxYTransform(String multiPoint2D) {
		return super.doMultiPoint2DGetMaxYTransform(multiPoint2D); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doMultiPoint2DGetMinXTransform(String multiPoint2D) {
		return super.doMultiPoint2DGetMinXTransform(multiPoint2D); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doMultiPoint2DGetMinYTransform(String multiPoint2D) {
		return super.doMultiPoint2DGetMinYTransform(multiPoint2D); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doMultiPoint2DToLine2DTransform(String multiPoint2D) {
		return MultiPoint2DFunctions.ASLINE2D+"("+multiPoint2D+")";
	}

	@Override
	public String doMultiPoint2DAsTextTransform(String multiPoint2D) {
		return nz.co.gregs.dbvolution.internal.oracle.xe.GeometryFunctions.ASTEXT+"("+multiPoint2D+")";
	}

	@Override
	public String doMultiPoint2DGetBoundingBoxTransform(String multiPoint2D) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public String doMultiPoint2DMeasurableDimensionsTransform(String multipoint2D) {
		return super.doMultiPoint2DMeasurableDimensionsTransform(multipoint2D); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doMultiPoint2DGetNumberOfPointsTransform(String multiPoint2D) {
		return MultiPoint2DFunctions.NUMPOINTS+"("+multiPoint2D+")";
	}

	@Override
	public String doMultiPoint2DGetPointAtIndexTransform(String first, String index) {
		return MultiPoint2DFunctions.GETPOINTATINDEX+"("+first+", "+index+")";
	}

	@Override
	public String doMultiPoint2DNotEqualsTransform(String first, String second) {
		return super.doMultiPoint2DNotEqualsTransform(first, second); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doMultiPoint2DEqualsTransform(String first, String second) {
		return super.doMultiPoint2DEqualsTransform(first, second); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String transformMultiPoint2DToDatabaseMultiPoint2DValue(MultiPoint mpoint) {
		StringBuilder ordinateArray = new StringBuilder("MDSYS.SDO_ORDINATE_ARRAY(");
		final String ordinateSep = ", ";
		String pairSep = "";
		for (Coordinate coordinate : mpoint.getCoordinates()) {
			ordinateArray.append(pairSep).append(coordinate.x).append(ordinateSep).append(coordinate.y);
			pairSep = ", ";
		}
		//+ lineSegment.p0.x + ", " + lineSegment.p0.y + ", " + lineSegment.p1.x + ", " + lineSegment.p1.y 
		ordinateArray.append(")");
		return "MDSYS.SDO_GEOMETRY(2005, NULL, NULL,"
				+ "MDSYS.SDO_ELEM_INFO_ARRAY(1,1," + mpoint.getNumPoints() + "),"
				+ ordinateArray
				+ ")";

//		return super.transformMultiPoint2DToDatabaseMultiPoint2DValue(point); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public MultiPoint transformDatabaseMultiPoint2DValueToJTSMultiPoint(String pointsAsString) throws ParseException {
		return super.transformDatabaseMultiPoint2DValueToJTSMultiPoint(pointsAsString); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doPolygon2DAsTextTransform(String polygonSQL) {
		return nz.co.gregs.dbvolution.internal.oracle.xe.GeometryFunctions.ASTEXT+"("+polygonSQL+")";
	}

	@Override
	public String doLine2DAsTextTransform(String line2DSQL) {
		return nz.co.gregs.dbvolution.internal.oracle.xe.GeometryFunctions.ASTEXT+"("+line2DSQL+")";
	}

	@Override
	public String transformLineSegmentIntoDatabaseLineSegment2DFormat(LineSegment lineSegment) {
		return super.transformLineSegmentIntoDatabaseLineSegment2DFormat(lineSegment);
	}

	@Override
	public String transformPolygonIntoDatabasePolygon2DFormat(Polygon polygon) {
		StringBuilder ordinateArray = new StringBuilder("MDSYS.SDO_ORDINATE_ARRAY(");
		final String ordinateSep = ", ";
		String pairSep = "";
		for (Coordinate coordinate : polygon.getCoordinates()) {
			ordinateArray.append(pairSep).append(coordinate.x).append(ordinateSep).append(coordinate.y);
			pairSep = ", ";
		}
		//+ lineSegment.p0.x + ", " + lineSegment.p0.y + ", " + lineSegment.p1.x + ", " + lineSegment.p1.y 
		ordinateArray.append(")");
		return "MDSYS.SDO_GEOMETRY(2003, NULL, NULL,"
				+ "MDSYS.SDO_ELEM_INFO_ARRAY(1,1," + polygon.getNumPoints() + "),"
				+ ordinateArray
				+ ")";
//		return super.transformPolygonIntoDatabasePolygon2DFormat(point);
	}

	@Override
	public String transformLineStringIntoDatabaseLine2DFormat(LineString lineString) {
		StringBuilder ordinateArray = new StringBuilder("MDSYS.SDO_ORDINATE_ARRAY(");
		final String ordinateSep = ", ";
		String pairSep = "";
		for (Coordinate coordinate : lineString.getCoordinates()) {
			ordinateArray.append(pairSep).append(coordinate.x).append(ordinateSep).append(coordinate.y);
			pairSep = ", ";
		}
		//+ lineSegment.p0.x + ", " + lineSegment.p0.y + ", " + lineSegment.p1.x + ", " + lineSegment.p1.y 
		ordinateArray.append(")");
		return "MDSYS.SDO_GEOMETRY(2002, NULL, NULL,"
				+ "MDSYS.SDO_ELEM_INFO_ARRAY(1,1," + lineString.getNumPoints() + "),"
				+ ordinateArray
				+ ")";
		//return super.transformLineStringIntoDatabaseLine2DFormat(point);
	}

	@Override
	public String transformCoordinatesIntoDatabasePoint2DFormat(String xValue, String yValue) {
		return super.transformCoordinatesIntoDatabasePoint2DFormat(xValue, yValue);
	}

	@Override
	public String transformPoint2DIntoDatabaseFormat(Point point) {
		return super.transformPoint2DIntoDatabaseFormat(point);
	}

//	@Override
//	public String doPoint2DArrayToPolygon2DTransform(List<String> pointSQL) {
//		StringBuilder ordinateArray = new StringBuilder("MDSYS.SDO_ORDINATE_ARRAY(");
//		final String ordinateSep = ", ";
//		String pairSep = "";
//		for (String pointish : pointSQL) {
//			ordinateArray
//					.append(pairSep)
//					.append(doPoint2DGetXTransform(pointish))
//					.append(ordinateSep)
//					.append(doPoint2DGetYTransform(pointish));
//			pairSep = ", ";
//		}
//		//+ lineSegment.p0.x + ", " + lineSegment.p0.y + ", " + lineSegment.p1.x + ", " + lineSegment.p1.y 
//		ordinateArray.append(")");
//		return "MDSYS.SDO_GEOMETRY(2003, NULL, NULL,"
//				+ "MDSYS.SDO_ELEM_INFO_ARRAY(1,1," + pointSQL.size()+ "),"
//				+ ordinateArray
//				+ ")";
//	}

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
		return super.doLine2DSpatialDimensionsTransform(line2DSQL); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doLine2DAllIntersectionPointsWithLine2DTransform(String firstGeometry, String secondGeometry) {
		return super.doLine2DAllIntersectionPointsWithLine2DTransform(firstGeometry, secondGeometry); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doLine2DIntersectionPointWithLine2DTransform(String firstLine, String secondLine) {
		return super.doLine2DIntersectionPointWithLine2DTransform(firstLine, secondLine); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doLine2DIntersectsLine2DTransform(String firstLine, String secondLine) {
		return Line2DFunctions.INTERSECTSLINE2D+"("+firstLine+", "+secondLine+")";
	}

	@Override
	public String doLine2DGetMinYTransform(String line2DSQL) {
		return super.doLine2DGetMinYTransform(line2DSQL); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doLine2DGetMaxYTransform(String line2DSQL) {
		return super.doLine2DGetMaxYTransform(line2DSQL); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doLine2DGetMinXTransform(String line2DSQL) {
		return super.doLine2DGetMinXTransform(line2DSQL); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doLine2DGetMaxXTransform(String line2DSQL) {
		return super.doLine2DGetMaxXTransform(line2DSQL); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doLine2DGetBoundingBoxTransform(String line2DSQL) {
		throw new UnsupportedOperationException("Oracle SDO_MBR returns strange results for points and straight lines.");
	}

	@Override
	public String doLine2DMeasurableDimensionsTransform(String line2DSQL) {
		return super.doLine2DMeasurableDimensionsTransform(line2DSQL); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doLine2DNotEqualsTransform(String line2DSQL, String otherLine2DSQL) {
		return super.doLine2DNotEqualsTransform(line2DSQL, otherLine2DSQL); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doLine2DEqualsTransform(String line2DSQL, String otherLine2DSQL) {
		return super.doLine2DEqualsTransform(line2DSQL, otherLine2DSQL); //To change body of generated methods, choose Tools | Templates.
	}


}
