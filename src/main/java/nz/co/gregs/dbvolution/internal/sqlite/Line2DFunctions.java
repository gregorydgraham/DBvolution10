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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.sqlite.Function;

/**
 *
 *
 * @author gregorygraham
 */
public class Line2DFunctions {

	/**
	 *
	 */
	public final static String CREATE_FROM_COORDS_FUNCTION = "DBV_CREATE_LINE2D_FROM_COORDS";

	/**
	 *
	 */
	public final static String EQUALS_FUNCTION = "DBV_LINE2D_EQUALS";

	/**
	 *
	 */
	public final static String GETMAXX_FUNCTION = "DBV_LINE2D_GETMAXX";

	/**
	 *
	 */
	public final static String GETMAXY_FUNCTION = "DBV_LINE2D_GETMAXY";

	/**
	 *
	 */
	public final static String GETMINX_FUNCTION = "DBV_LINE2D_GETMINX";

	/**
	 *
	 */
	public final static String GETMINY_FUNCTION = "DBV_LINE2D_GETMINY";

	/**
	 *
	 */
	public final static String GETDIMENSION_FUNCTION = "DBV_LINE2D_GETDIMENSION";

	/**
	 *
	 */
	public final static String GETBOUNDINGBOX_FUNCTION = "DBV_LINE2D_GETBOUNDINGBOX";

	/**
	 *
	 */
	public final static String ASTEXT_FUNCTION = "DBV_LINE2D_ASTEXT";

	/**
	 *
	 */
	public final static String SPATIAL_LINE_MIN_X_COORD_FUNCTION = "DBV_LINE_MIN_X2D_COORD";

	/**
	 *
	 */
	public final static String SPATIAL_LINE_MAX_Y_COORD_FUNCTION = "DBV_LINE_MAX_Y2D_COORD";

	/**
	 *
	 */
	public final static String SPATIAL_LINE_MIN_Y_COORD_FUNCTION = "DBV_LINE_MIN_Y2D_COORD";

	/**
	 *
	 */
	public final static String SPATIAL_LINE_MAX_X_COORD_FUNCTION = "DBV_LINE_MAX_X2D_COORD";

	/**
	 *
	 */
	public final static String INTERSECTS = "DBV_LINE2D_INTERSECTS_LINE2D";

	/**
	 *
	 */
	public final static String INTERSECTIONWITH_LINE2D = "DBV_LINE2D_INTERSECTIONWITH_LINE2D";

	/**
	 *
	 */
	public final static String ALLINTERSECTIONSWITH_LINE2D = "DBV_LINE2D_ALLINTERSECTIONSWITH_LINE2D";

	private Line2DFunctions() {
	}

	/**
	 *
	 * @param connection the database to add functions to
	 * @throws SQLException database errors
	 */
	public static void addFunctions(Connection connection) throws SQLException {
		Function.create(connection, CREATE_FROM_COORDS_FUNCTION, new CreateFromCoords());
		Function.create(connection, EQUALS_FUNCTION, new Equals());
		Function.create(connection, GETMAXX_FUNCTION, new GetMaxX());
		Function.create(connection, GETMAXY_FUNCTION, new GetMaxY());
		Function.create(connection, GETMINX_FUNCTION, new GetMinX());
		Function.create(connection, GETMINY_FUNCTION, new GetMinY());
		Function.create(connection, GETDIMENSION_FUNCTION, new GetDimension());
		Function.create(connection, GETBOUNDINGBOX_FUNCTION, new GetBoundingBox());
		Function.create(connection, ASTEXT_FUNCTION, new AsText());
		Function.create(connection, INTERSECTS, new Intersects());
		Function.create(connection, INTERSECTIONWITH_LINE2D, new IntersectionWith());
		Function.create(connection, ALLINTERSECTIONSWITH_LINE2D, new AllIntersectionsWith());
	}

	private static class CreateFromCoords extends PolygonFunction {
//LINESTRING (2 3, 3 4)

		@Override
		protected void xFunc() throws SQLException {
			Integer numberOfArguments = args();
			if (numberOfArguments % 2 != 0) {
				result();
			} else {
				StringBuilder resultStr = new StringBuilder("LINESTRING (");
				String sep = "";
				for (int i = 0; i < numberOfArguments; i += 2) {
					Double x = value_double(i);
					Double y = value_double(i + 1);
					if (x == null || y == null) {
						result();
						return;
					} else {
						resultStr.append(sep).append(x).append(" ").append(y);
						sep = ", ";
					}
				}
				resultStr.append(")");
				result(resultStr.toString());
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
				for (int i = 1; i < split.length; i += 2) {
					double x = Double.parseDouble(split[i]);
//					double y = Double.parseDouble(split[i + 1]);
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
				for (int i = 1; i < split.length; i += 2) {
//					double x = Double.parseDouble(split[i]);
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
				for (int i = 1; i < split.length; i += 2) {
					double x = Double.parseDouble(split[i]);
//					double y = Double.parseDouble(split[i + 1]);
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
				for (int i = 1; i < split.length; i += 2) {
//					double x = Double.parseDouble(split[i]);
					double y = Double.parseDouble(split[i + 1]);
					if (minY == null || minY > y) {
						minY = y;
					}
				}
				result(minY);
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
					LineString firstLine = getLineString(firstLineStr);
					LineString secondLine = getLineString(secondLineStr);
					if (firstLine == null || secondLine == null) {
						result();
					} else {
						result(firstLine.intersects(secondLine) ? 1 : 0);
					}
				}
			} catch (SQLException|ParseException ex) {
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
					LineString firstLine = getLineString(firstLineStr);
					LineString secondLine = getLineString(secondLineStr);
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
			} catch (SQLException|ParseException ex) {
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
					LineString firstLine = getLineString(firstLineStr);
					LineString secondLine = getLineString(secondLineStr);
					if (firstLine == null || secondLine == null) {
						result();
					} else {
						List<Point> pointList = new ArrayList<Point>();
						final Geometry intersection = firstLine.intersection(secondLine);
						if (intersection == null || intersection.isEmpty()) {
							result();
						} else {
							int numPoints = intersection.getNumPoints();
							for (int i = 0; i < numPoints; i++) {
								Geometry geometryN = intersection.getGeometryN(i);
								if ((geometryN != null) && (geometryN instanceof Point)) {
									pointList.add((Point) geometryN);
								}
							}
							if (pointList.isEmpty()) {
								result();
							} else {
								Point[] pointArray = new Point[numPoints];
								pointArray = pointList.toArray(pointArray);
								result((new GeometryFactory()).createMultiPoint(pointArray).toText());
							}
						}
					}
				}
			} catch (SQLException|ParseException ex) {
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
				Double minX = null;
				Double minY = null;
				String[] split = firstLine.split("[ (),]+");
				for (int i = 1; i < split.length; i += 2) {
					double x = Double.parseDouble(split[i]);
					double y = Double.parseDouble(split[i + 1]);
					if (maxX == null || maxX < x) {
						maxX = x;
					}
					if (maxY == null || maxY < y) {
						maxY = y;
					}
					if (minX == null || minX > x) {
						minX = x;
					}
					if (minY == null || minY > y) {
						minY = y;
					}
				}
				String resultString = "POLYGON ((" + minX + " " + minY + ", " + maxX + " " + minY + ", " + maxX + " " + maxY + ", " + minX + " " + maxY + ", " + minX + " " + minY + "))";
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

		Polygon getPolygon(String possiblePoly) throws ParseException {
			WKTReader wktReader = new WKTReader();
			Geometry firstGeom = wktReader.read(possiblePoly);
			if (firstGeom instanceof Polygon) {
				return (Polygon) firstGeom;
			}
			return null;
		}

		LineString getLineString(String possiblePoly) throws ParseException {
			WKTReader wktReader = new WKTReader();
			Geometry firstGeom = wktReader.read(possiblePoly);
			if (firstGeom instanceof LineString) {
				return (LineString) firstGeom;
			}
			return null;
		}

		Point getPoint(String possiblePoly) throws ParseException {
			WKTReader wktReader = new WKTReader();
			Geometry firstGeom = wktReader.read(possiblePoly);
			if (firstGeom instanceof Point) {
				return (Point) firstGeom;
			}
			return null;
		}
	}

}
