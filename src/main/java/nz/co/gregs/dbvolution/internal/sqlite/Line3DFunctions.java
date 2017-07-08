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
package nz.co.gregs.dbvolution.internal.sqlite;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKTReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.datatypes.spatial3D.GeometryFactory3D;
import nz.co.gregs.dbvolution.datatypes.spatial3D.LineStringZ;
import nz.co.gregs.dbvolution.datatypes.spatial3D.PointZ;
import nz.co.gregs.dbvolution.datatypes.spatial3D.PolygonZ;
import org.sqlite.Function;

/**
 *
 * @author gregorygraham
 */
public class Line3DFunctions {

	/**
	 *
	 */
	public final static String CREATE_FROM_COORDS_FUNCTION = "DBV_CREATE_LINE3D_FROM_COORDS";

	/**
	 *
	 */
	public final static String EQUALS_FUNCTION = "DBV_LINE3D_EQUALS";

	/**
	 *
	 */
	public final static String GETMAXX_FUNCTION = "DBV_LINE3D_GETMAXX";

	/**
	 *
	 */
	public final static String GETMAXY_FUNCTION = "DBV_LINE3D_GETMAXY";

	/**
	 *
	 */
	public final static String GETMINX_FUNCTION = "DBV_LINE3D_GETMINX";

	/**
	 *
	 */
	public final static String GETMINY_FUNCTION = "DBV_LINE3D_GETMINY";

	/**
	 *
	 */
	public final static String GETMAXZ_FUNCTION = "DBV_LINE3D_GETMAXZ";

	/**
	 *
	 */
	public final static String GETMINZ_FUNCTION = "DBV_LINE3D_GETMINZ";

	/**
	 *
	 */
	public final static String GETDIMENSION_FUNCTION = "DBV_LINE3D_GETDIMENSION";

	/**
	 *
	 */
	public final static String GETBOUNDINGBOX_FUNCTION = "DBV_LINE3D_GETBOUNDINGBOX";

	/**
	 *
	 */
	public final static String ASTEXT_FUNCTION = "DBV_LINE3D_ASTEXT";

	/**
	 *
	 */
//	public final static String SPATIAL_LINE_MIN_X_COORD_FUNCTION = "DBV_LINE_MIN_X3D_COORD";

	/**
	 *
	 */
//	public final static String SPATIAL_LINE_MAX_Y_COORD_FUNCTION = "DBV_LINE_MAX_Y3D_COORD";

	/**
	 *
	 */
//	public final static String SPATIAL_LINE_MIN_Y_COORD_FUNCTION = "DBV_LINE_MIN_Y3D_COORD";

	/**
	 *
	 */
//	public final static String SPATIAL_LINE_MAX_Z_COORD_FUNCTION = "DBV_LINE_MAX_Z3D_COORD";

	/**
	 *
	 */
	public final static String INTERSECTS = "DBV_LINE3D_INTERSECTS_LINE3D";

	/**
	 *
	 */
	public final static String INTERSECTIONWITH_LINE3D = "DBV_LINE3D_INTERSECTIONWITH_LINE3D";

	/**
	 *
	 */
	public final static String ALLINTERSECTIONSWITH_LINE3D = "DBV_LINE3D_ALLINTERSECTIONSWITH_LINE3D";

	private Line3DFunctions() {
	}

	/**
	 *
	 * @param connection
	 * @throws SQLException
	 */
	public static void addFunctions(Connection connection) throws SQLException {
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
		Function.create(connection, INTERSECTIONWITH_LINE3D, new IntersectionWith());
		Function.create(connection, ALLINTERSECTIONSWITH_LINE3D, new AllIntersectionsWith());
	}

	private static class CreateFromCoords extends PolygonFunction {
//LINESTRING (2 3, 3 4)

		@Override
		protected void xFunc() throws SQLException {
			Integer numberOfArguments = args();
			if (numberOfArguments % 3 != 0) {
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
					double z = Double.parseDouble(split[i]);
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
					LineStringZ firstLine = getLineStringZ(firstLineStr);
					LineStringZ secondLine = getLineStringZ(secondLineStr);
					if (firstLine == null || secondLine == null) {
						result();
					} else {
						result(firstLine.intersects(secondLine) ? 1 : 0);
					}
				}
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(Line2DFunctions.class.getName()).log(Level.SEVERE, null, ex);
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
					LineStringZ firstLine = getLineStringZ(firstLineStr);
					LineStringZ secondLine = getLineStringZ(secondLineStr);
					if (firstLine == null || secondLine == null) {
						result();
					} else {
						final Geometry intersection = firstLine.intersection(secondLine);
						if (intersection == null || intersection.isEmpty()) {
							result();
						} else {
							result(intersection.getGeometryN(0).toText());
						}
					}
				}
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(Line2DFunctions.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite Polygon", ex);
			}
		}
	}

	private static class AllIntersectionsWith extends PolygonFunction {

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
					LineStringZ firstLine = getLineStringZ(firstLineStr);
					LineStringZ secondLine = getLineStringZ(secondLineStr);
					if (firstLine == null || secondLine == null) {
						result();
					} else {
						List<PointZ> pointList = new ArrayList<>();
						final Geometry intersection = firstLine.intersection(secondLine);
						if (intersection == null || intersection.isEmpty()) {
							result();
						} else {
							int numPoints = intersection.getNumPoints();
							for (int i = 0; i < numPoints; i++) {
								Geometry geometryN = intersection.getGeometryN(i);
								if ((geometryN!=null)&&(geometryN instanceof Point)) {
									pointList.add(new GeometryFactory3D().createPointZ((Point) geometryN));
								}
							}
							if (pointList.isEmpty()) {
								result();
							} else {
								PointZ[] pointArray = new PointZ[numPoints];
								pointArray = pointList.toArray(pointArray);
								result((new GeometryFactory3D()).createMultiPointZ(pointArray).toText());
							}
						}
					}
				}
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(Line2DFunctions.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite Polygon", ex);
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
