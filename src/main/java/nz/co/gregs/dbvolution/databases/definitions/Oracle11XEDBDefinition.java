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
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Polygon;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.Oracle11XEDB;
import nz.co.gregs.dbvolution.internal.oracle.xe.GeometryFunctions;
import nz.co.gregs.dbvolution.internal.oracle.xe.MultiPoint2DFunctions;
import nz.co.gregs.dbvolution.internal.query.QueryOptions;
import nz.co.gregs.dbvolution.internal.query.QueryState;

/**
 * Defines the features of the Oracle 11 database that differ from the standard
 * database.
 *
 * <p>
 * This DBDefinition is automatically included in {@link Oracle11XEDB}
 * instances, and you should not need to use it directly.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class Oracle11XEDBDefinition extends OracleSpatialDBDefinition {

	@Override
	public Object getLimitRowsSubClauseDuringSelectClause(QueryOptions options) {
		return " /*+ FIRST_ROWS(" + options.getRowLimit() + ") */ ";
	}

	@Override
	public String getLimitRowsSubClauseAfterWhereClause(QueryState state, QueryOptions options) {
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

		List<String> result = new ArrayList<>();
		String sequenceName = getPrimaryKeySequenceName(table, column);
		final String primaryKeyTriggerName = getPrimaryKeyTriggerName(table, column);
		result.add("DROP TRIGGER " + primaryKeyTriggerName + "");
		result.add("DROP SEQUENCE " + sequenceName + "");
		result.add("CREATE SEQUENCE " + sequenceName);
		result.add("CREATE OR REPLACE TRIGGER " + DB.getUsername() + "." + primaryKeyTriggerName + " \n"
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
	public List<String> dropTriggerBasedIdentitySQL(DBDatabase DB, String table, String column) {

		List<String> result = new ArrayList<>();
		result.add("DROP TRIGGER " + getPrimaryKeyTriggerName(table, column) + "");
		result.add("DROP SEQUENCE " + getPrimaryKeySequenceName(table, column) + "");
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
			static final long serialVersionUID = 1L;

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
			static final long serialVersionUID = 1L;

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
			static final long serialVersionUID = 1L;

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
			static final long serialVersionUID = 1L;

			{
				add(doPoint2DGetYTransform(doLineSegment2DStartPointTransform(lineSegment)));
				add(doPoint2DGetYTransform(doLineSegment2DEndPointTransform(lineSegment)));
			}
		}
		);
	}

	@Override
	public String doMultiPoint2DToLine2DTransform(String multiPoint2D) {
		return MultiPoint2DFunctions.ASLINE2D + "(" + multiPoint2D + ")";
	}

	@Override
	public String doMultiPoint2DAsTextTransform(String multiPoint2D) {
		return nz.co.gregs.dbvolution.internal.oracle.xe.GeometryFunctions.ASTEXT + "(" + multiPoint2D + ")";
	}

	@Override
	public String doMultiPoint2DGetBoundingBoxTransform(String multiPoint2D) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public String doMultiPoint2DGetNumberOfPointsTransform(String multiPoint2D) {
		return MultiPoint2DFunctions.NUMPOINTS + "(" + multiPoint2D + ")";
	}

	@Override
	public String doMultiPoint2DGetPointAtIndexTransform(String first, String index) {
		return MultiPoint2DFunctions.GETPOINTATINDEX + "(" + first + ", " + index + ")";
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
		ordinateArray.append(")");
		return "MDSYS.SDO_GEOMETRY(2005, NULL, NULL,"
				+ "MDSYS.SDO_ELEM_INFO_ARRAY(1,1," + mpoint.getNumPoints() + "),"
				+ ordinateArray
				+ ")";
	}

	@Override
	public String doPolygon2DAsTextTransform(String polygonSQL) {
		return nz.co.gregs.dbvolution.internal.oracle.xe.GeometryFunctions.ASTEXT + "(" + polygonSQL + ")";
	}

	@Override
	public String doLine2DAsTextTransform(String line2DSQL) {
		return nz.co.gregs.dbvolution.internal.oracle.xe.GeometryFunctions.ASTEXT + "(" + line2DSQL + ")";
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
		ordinateArray.append(")");
		return "MDSYS.SDO_GEOMETRY(2002, NULL, NULL,"
				+ "MDSYS.SDO_ELEM_INFO_ARRAY(1,2,1),"
				+ ordinateArray
				+ ")";
	}

	@Override
	public String doLine2DGetBoundingBoxTransform(String line2DSQL) {
		throw new UnsupportedOperationException("Oracle SDO_MBR returns strange results for points and straight lines.");
	}

	@Override
	public String doPolygon2DGetBoundingBoxTransform(String toSQLString) {
		throw new UnsupportedOperationException("Oracle SDO_MBR returns a diagonal line as the bounding box.");
	}

	@Override
	public String transformPolygonIntoDatabasePolygon2DFormat(Polygon polygon2D) {
		StringBuilder ordinateArray = new StringBuilder("MDSYS.SDO_ORDINATE_ARRAY(");
		final String ordinateSep = ", ";
		String pairSep = "";
		for (Coordinate coordinate : polygon2D.getCoordinates()) {
			ordinateArray.append(pairSep).append(coordinate.x).append(ordinateSep).append(coordinate.y);
			pairSep = ", ";
		}
		ordinateArray.append(")");
		return "MDSYS.SDO_GEOMETRY(2003, NULL, NULL,"
				+ "MDSYS.SDO_ELEM_INFO_ARRAY(1,2003,1),"
				+ ordinateArray
				+ ")";
	}
}
