/*
 * Copyright 2015 gregory.graham.
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
package nz.co.gregs.dbvolution.internal.sqlite;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKTReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.datatypes.spatial3D.GeometryFactory3D;
import nz.co.gregs.dbvolution.datatypes.spatial3D.LineSegmentZ;
import nz.co.gregs.dbvolution.datatypes.spatial3D.LineStringZ;
import nz.co.gregs.dbvolution.datatypes.spatial3D.PointZ;
import nz.co.gregs.dbvolution.datatypes.spatial3D.PolygonZ;
import org.sqlite.Function;

/**
 *
 * @author gregory.graham
 */
public class LineSegment3DFunctions {

	/**
	 *
	 */
	public final static String CREATE_FROM_COORDS_FUNCTION = "DBV_CREATE_LINESEGMENT3D_FROM_COORDS";

	/**
	 *
	 */
	public final static String EQUALS_FUNCTION = "DBV_LINESEGMENT3D_EQUALS";

	/**
	 *
	 */
	public final static String GETMAXX_FUNCTION = "DBV_LINESEGMENT3D_GETMAXX";

	/**
	 *
	 */
	public final static String GETMAXY_FUNCTION = "DBV_LINESEGMENT3D_GETMAXY";

	/**
	 *
	 */
	public final static String GETMINX_FUNCTION = "DBV_LINESEGMENT3D_GETMINX";

	/**
	 *
	 */
	public final static String GETMINY_FUNCTION = "DBV_LINE3D_GETMINY";

	/**
	 *
	 */
	public final static String GETMAXZ_FUNCTION = "DBV_LINESEGMENT3D_GETMAXZ";

	/**
	 *
	 */
	public final static String GETMINZ_FUNCTION = "DBV_LINESEGMENT3D_GETMINZ";

	/**
	 *
	 */
	public final static String GETDIMENSION_FUNCTION = "DBV_LINESEGMENT3D_GETDIMENSION";

	/**
	 *
	 */
	public final static String GETBOUNDINGBOX_FUNCTION = "DBV_LINESEGMENT3D_GETBOUNDINGBOX";

	/**
	 *
	 */
	public final static String ASTEXT_FUNCTION = "DBV_LINESEGMENT3D_ASTEXT";

	/**
	 *
	 */
//	public final static String SPATIAL_LINE_MIN_X_COORD_FUNCTION = "DBV_LINESEGMENT3D_MIN_X3D_COORD";
	/**
	 *
	 */
//	public final static String SPATIAL_LINE_MAX_Y_COORD_FUNCTION = "DBV_LINESEGMENT3D_MAX_Y3D_COORD";
	/**
	 *
	 */
//	public final static String SPATIAL_LINE_MIN_Y_COORD_FUNCTION = "DBV_LINESEGMENT3D_MIN_Y3D_COORD";
	/**
	 *
	 */
//	public final static String SPATIAL_LINE_MAX_X_COORD_FUNCTION = "DBV_LINESEGMENT3D_MAX_X3D_COORD";
	/**
	 *
	 */
	public final static String INTERSECTS = "DBV_LINESEGMENT3D_INTERSECTS_LINESEGMENT3D";

	/**
	 *
	 */
	public final static String INTERSECTIONWITH_LINESEGMENT3D = "DBV_LINESEGMENT3D_INTERSECTIONWITH_LINESEGMENT3D";

	private LineSegment3DFunctions() {
	}

	/**
	 *
	 * @param connection
	 * @throws SQLException
	 */
	public static void addFunctions(Connection connection) throws SQLException {
		Function.destroy(connection, CREATE_FROM_COORDS_FUNCTION);
		Function.destroy(connection, EQUALS_FUNCTION);
		Function.destroy(connection, GETMAXX_FUNCTION);
		Function.destroy(connection, GETMAXY_FUNCTION);
		Function.destroy(connection, GETMINX_FUNCTION);
		Function.destroy(connection, GETMINY_FUNCTION);
		Function.destroy(connection, GETMAXZ_FUNCTION);
		Function.destroy(connection, GETMINZ_FUNCTION);
		Function.destroy(connection, GETDIMENSION_FUNCTION);
		Function.destroy(connection, GETBOUNDINGBOX_FUNCTION);
		Function.destroy(connection, ASTEXT_FUNCTION);
		Function.destroy(connection, INTERSECTS);
		Function.destroy(connection, INTERSECTIONWITH_LINESEGMENT3D);
		Function.create(connection, CREATE_FROM_COORDS_FUNCTION, new CreateFromCoords());
		Function.create(connection, EQUALS_FUNCTION, new Equals());
		Function.create(connection, GETMAXX_FUNCTION, new GetMaxX());
		Function.create(connection, GETMAXY_FUNCTION, new GetMaxY());
		Function.create(connection, GETMINX_FUNCTION, new GetMinX());
		Function.create(connection, GETMINY_FUNCTION, new GetMinY());
		Function.create(connection, GETMAXZ_FUNCTION, new GetMaxZ());
		Function.create(connection, GETMINZ_FUNCTION, new GetMinZ());
		Function.create(connection, GETDIMENSION_FUNCTION, new GetDimension());
		Function.create(connection, GETBOUNDINGBOX_FUNCTION, new GetBoundingBox());
		Function.create(connection, ASTEXT_FUNCTION, new AsText());
		Function.create(connection, INTERSECTS, new Intersects());
		Function.create(connection, INTERSECTIONWITH_LINESEGMENT3D, new IntersectionWith());
	}

	private static class CreateFromCoords extends PolygonFunction {
//LINESTRING (2 3, 3 4)

		@Override
		protected void xFunc() throws SQLException {
			Integer numberOfArguments = args();
			if (numberOfArguments != 2) {
				result();
			} else {
				String resultStr = "LINESTRING (";
				String sep = "";
				for (int i = 0; i < numberOfArguments; i += 3) {
					Double x = value_double(i);
					Double y = value_double(i + 1);
					Double z = value_double(i + 2);
					if (x == null || y == null || z == null) {
						result();
						return;
					} else {
						resultStr += sep + x + " " + y + " " + z;
						sep = ", ";
					}
				}
				resultStr += ")";
				result(resultStr);
			}
		}
	}

	private static class Equals extends PolygonFunction {

		@Override
		protected void xFunc() throws SQLException {
			String firstPoint = value_text(0);
			String secondPoint = value_text(1);
			if (firstPoint == null || secondPoint == null) {
				result();
			} else {
				result(firstPoint.equals(secondPoint) ? 1 : 0);
			}
		}
	}

	private static class GetMaxX extends PolygonFunction {

		@Override
		protected void xFunc() throws SQLException {
			String firstLine = value_text(0);
			if (firstLine == null) {
				result();
			} else {
				Double maxX = null;
				String[] split = firstLine.split("[ (),]+");
				for (int i = 1; i < split.length; i += 3) {
					double x = Double.parseDouble(split[i]);
					if (maxX == null || maxX < x) {
						maxX = x;
					}
				}
				result(maxX);
			}
		}

	}

	private static class GetMaxY extends PolygonFunction {

		@Override
		protected void xFunc() throws SQLException {
			String firstPoint = value_text(0);
			if (firstPoint == null) {
				result();
			} else {
				Double maxY = null;
				String[] split = firstPoint.split("[ (),]+");
				for (int i = 1; i < split.length; i += 3) {
					double y = Double.parseDouble(split[i + 1]);
					if (maxY == null || maxY < y) {
						maxY = y;
					}
				}
				result(maxY);
			}
		}
	}

	private static class GetMaxZ extends PolygonFunction {

		@Override
		protected void xFunc() throws SQLException {
			String firstPoint = value_text(0);
			if (firstPoint == null) {
				result();
			} else {
				Double max = null;
				String[] split = firstPoint.split("[ (),]+");
				for (int i = 1; i < split.length; i += 3) {
					double z = Double.parseDouble(split[i + 2]);
					if (max == null || max < z) {
						max = z;
					}
				}
				result(max);
			}
		}
	}

	private static class GetMinX extends PolygonFunction {

		@Override
		protected void xFunc() throws SQLException {
			String firstPoint = value_text(0);
			if (firstPoint == null) {
				result();
			} else {
				Double minX = null;
				String[] split = firstPoint.split("[ (),]+");
				for (int i = 1; i < split.length; i += 3) {
					double x = Double.parseDouble(split[i]);
					if (minX == null || minX > x) {
						minX = x;
					}
				}
				result(minX);
			}
		}

	}

	private static class GetMinY extends PolygonFunction {

		@Override
		protected void xFunc() throws SQLException {
			String firstPoint = value_text(0);
			if (firstPoint == null) {
				result();
			} else {
				Double minY = null;
				String[] split = firstPoint.split("[ (),]+");
				for (int i = 1; i < split.length; i += 3) {
					double y = Double.parseDouble(split[i + 1]);
					if (minY == null || minY > y) {
						minY = y;
					}
				}
				result(minY);
			}
		}
	}

	private static class GetMinZ extends PolygonFunction {

		@Override
		protected void xFunc() throws SQLException {
			String firstPoint = value_text(0);
			if (firstPoint == null) {
				result();
			} else {
				Double min = null;
				String[] split = firstPoint.split("[ (),]+");
				for (int i = 1; i < split.length; i += 3) {
					double z = Double.parseDouble(split[i + 2]);
					if (min == null || min > z) {
						min = z;
					}
				}
				result(min);
			}
		}
	}

	private static class Intersects extends PolygonFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				String firstLineStr = value_text(0);
				String secondLineStr = value_text(1);
				if (firstLineStr == null) {
					result();
				} else if (secondLineStr == null) {
					result();
				} else {
					LineString firstLine = getLineStringZ(firstLineStr);
					LineString secondLine = getLineStringZ(secondLineStr);
					if (firstLine == null || secondLine == null) {
						result();
					} else {
						result(firstLine.intersects(secondLine) ? 1 : 0);
					}
				}
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(Line3DFunctions.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite Polygon", ex);
			}
		}
	}

	private static class IntersectionWith extends PolygonFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				String firstLineStr = value_text(0);
				String secondLineStr = value_text(1);
				if (firstLineStr == null) {
					result();
				} else if (secondLineStr == null) {
					result();
				} else {
					LineSegmentZ firstLine = getLineSegmentZ(firstLineStr);
					LineSegmentZ secondLine = getLineSegmentZ(secondLineStr);
					if (firstLine == null || secondLine == null) {
						result();
					} else {
						final Geometry intersectionPoint = firstLine.intersection(secondLine);
						if (intersectionPoint instanceof PointZ) {
							result(intersectionPoint.toText());
						} else {
							result();
						}
					}
				}
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(Line3DFunctions.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite geometry", ex);
			}
		}
	}

	private static class GetDimension extends PolygonFunction {

		@Override
		protected void xFunc() throws SQLException {
			result(1);
		}
	}

	private static class GetBoundingBox extends PolygonFunction {

		@Override
		protected void xFunc() throws SQLException {
			String firstLine = value_text(0);
			if (firstLine == null) {
				result();
			} else {
				Double maxX = null;
				Double maxY = null;
				Double maxZ = null;
				Double minX = null;
				Double minY = null;
				Double minZ = null;
				String[] split = firstLine.split("[ (),]+");
				for (int i = 1; i < split.length; i += 3) {
					double x = Double.parseDouble(split[i]);
					double y = Double.parseDouble(split[i + 1]);
					double z = Double.parseDouble(split[i + 2]);
					if (maxX == null || maxX < x) {
						maxX = x;
					}
					if (maxY == null || maxY < y) {
						maxY = y;
					}
					if (maxZ == null || maxZ < z) {
						maxZ = z;
					}
					if (minX == null || minX > x) {
						minX = x;
					}
					if (minY == null || minY > y) {
						minY = y;
					}
					if (minZ == null || minZ > z) {
						minZ = z;
					}
				}
				String resultString = "POLYGON ((" + minX + " " + minY + " " + minZ + ", " + maxX + " " + minY + " " + minZ + ", " + maxX + " " + maxY + " " + minZ + ", " + maxX + " " + maxY + " " + maxZ + ", " + minX + " " + maxY + " " + maxZ + ", " + minX + " " + minY + " " + maxZ + ", " + minX + " " + minY + " " + minZ + "))";
				result(resultString);
			}
		}
	}

	private static class AsText extends Function {

		@Override
		protected void xFunc() throws SQLException {
			String point = value_text(0);
			result(point);
		}
	}

	private static abstract class PolygonFunction extends Function {

		PolygonZ getPolygonZ(String possiblePoly) throws com.vividsolutions.jts.io.ParseException {
			WKTReader wktReader = new WKTReader();
			Geometry firstGeom = wktReader.read(possiblePoly);
			if (firstGeom instanceof Polygon) {
				return new GeometryFactory3D().createPolygonZ((Polygon) firstGeom);
			}
			return null;
		}

		LineStringZ getLineStringZ(String possiblePoly) throws com.vividsolutions.jts.io.ParseException {
			WKTReader wktReader = new WKTReader();
			Geometry firstGeom = wktReader.read(possiblePoly);
			if (firstGeom instanceof LineString) {
				return new GeometryFactory3D().createLineStringZ((LineString) firstGeom);
			}
			return null;
		}

		LineSegmentZ getLineSegmentZ(String possiblePoly) throws com.vividsolutions.jts.io.ParseException {
			WKTReader wktReader = new WKTReader();
			Geometry firstGeom = wktReader.read(possiblePoly);
			if (firstGeom instanceof LineString && ((LineString) firstGeom).getNumPoints() == 2) {
				return new GeometryFactory3D().createLineSegmentZ(firstGeom.getCoordinates());
			}
			return null;
		}

		PointZ getPointZ(String possiblePoly) throws com.vividsolutions.jts.io.ParseException {
			WKTReader wktReader = new WKTReader();
			Geometry firstGeom = wktReader.read(possiblePoly);
			if (firstGeom instanceof Point) {
				return new GeometryFactory3D().createPointZ((Point) firstGeom);
			}
			return null;
		}
	}

}
