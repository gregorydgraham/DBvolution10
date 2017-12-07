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
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKTReader;
import java.sql.Connection;
import java.sql.SQLException;
import org.sqlite.Function;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregory.graham
 */
public class MultiPoint2DFunctions {

	/**
	 *
	 */
	public final static String CREATE_FROM_COORDS_FUNCTION = "DBV_CREATE_MPOINT2D_FROM_COORDS";

	/**
	 *
	 */
	public final static String EQUALS_FUNCTION = "DBV_MPOINT2D_EQUALS";

	/**
	 *
	 */
	public final static String GETMAXX_FUNCTION = "DBV_MPOINT2D_GETMAXX";

	/**
	 *
	 */
	public final static String GETMAXY_FUNCTION = "DBV_MPOINT2D_GETMAXY";

	/**
	 *
	 */
	public final static String GETMINX_FUNCTION = "DBV_MPOINT2D_GETMINX";

	/**
	 *
	 */
	public final static String GETMINY_FUNCTION = "DBV_MPOINT2D_GETMINY";

	/**
	 *
	 */
	public final static String GETDIMENSION_FUNCTION = "DBV_MPOINT2D_GETDIMENSION";

	/**
	 *
	 */
	public final static String GETBOUNDINGBOX_FUNCTION = "DBV_MPOINT2D_GETBOUNDINGBOX";

	/**
	 *
	 */
	public final static String GETNUMBEROFPOINTS_FUNCTION = "DBV_MPOINT2D_GETNUMBEROFPOINTS";

	/**
	 *
	 */
	public final static String GETPOINTATINDEX_FUNCTION = "DBV_MPOINT2D_GETPOINTATINDEX";

	/**
	 *
	 */
	public final static String ASTEXT_FUNCTION = "DBV_MPOINT2D_ASTEXT";

	/**
	 *
	 */
	public final static String ASLINE2D = "DBV_MPOINT2D_ASLINE2D";
//	public static String ASPOLYGON2D = "DBV_MPOINT2D_ASPOLYGON2D";

	private MultiPoint2DFunctions() {
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
		Function.create(connection, GETDIMENSION_FUNCTION, new GetDimension());
		Function.create(connection, GETBOUNDINGBOX_FUNCTION, new GetBoundingBox());
		Function.create(connection, GETNUMBEROFPOINTS_FUNCTION, new GetNumberOfPoints());
		Function.create(connection, GETPOINTATINDEX_FUNCTION, new GetPointAtIndex());
		Function.create(connection, ASTEXT_FUNCTION, new AsText());
		Function.create(connection, ASLINE2D, new AsLine2D());
//		Function.create(connection, ASPOLYGON2D, new AsPolygon2D());
	}

	private static class CreateFromCoords extends PolygonFunction {
//MULTIPOINT (2 3, 3 4)

		@Override
		protected void xFunc() throws SQLException {
			Integer numberOfArguments = args();
			if (numberOfArguments == 0) {
				result();
			} else {
				StringBuilder resultStr = new StringBuilder("MULTIPOINT (");
				String sep = "";
				for (int i = 0; i < numberOfArguments; i += 2) {
					Double x = value_double(i);
					Double y = value_double(i + 1);
					resultStr.append(sep).append(x).append(" ").append(y);
					sep = ", ";
				}
				resultStr.append(")");
				result(resultStr.toString());
			}
		}
	}

	private static class GetNumberOfPoints extends PolygonFunction {
//'MULTIPOINT ((2 3), (3 4), (4 5))'

		@Override
		protected void xFunc() throws SQLException {
			String multipoint = value_text(0);
			if (multipoint == null || multipoint.equals("")) {
				result();
			} else {
				Double maxX = null;
				String[] split = multipoint.trim().split("[ (),]+");
				result((split.length - 1) / 2);
			}
		}
	}

	private static class GetPointAtIndex extends PolygonFunction {
//MULTIPOINT (2 3, 3 4)

		@Override
		protected void xFunc() throws SQLException {
			String multipoint = value_text(0);
			int index = value_int(1);
			final int indexInMPoint = index * 2;
			if (multipoint == null || indexInMPoint <= 0) {
				result();
			} else {
				String[] split = multipoint.split("[ (),]+");
				if (indexInMPoint > split.length) {
					result();
				} else {
					String x = split[indexInMPoint - 1];
					String y = split[indexInMPoint];
					result("POINT (" + x + " " + y + ")");
				}
			}
		}
	}

	private static class Equals extends PolygonFunction {

		@Override
		protected void xFunc() throws SQLException {
			String firstMPoint = value_text(0);
			String secondMPoint = value_text(1);
			if (firstMPoint == null || secondMPoint == null) {
				result();
			} else {
				result(firstMPoint.equals(secondMPoint) ? 1 : 0);
			}
		}
	}

	private static class GetMaxX extends PolygonFunction {

		@Override
		protected void xFunc() throws SQLException {
			String multipoint = value_text(0);
			if (multipoint == null) {
				result();
			} else {
				Double maxX = null;
				String[] split = multipoint.split("[ (),]+");
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
			String multipoint = value_text(0);
			if (multipoint == null) {
				result();
			} else {
				Double maxY = null;
				String[] split = multipoint.split("[ (),]+");
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
			String multipoint = value_text(0);
			if (multipoint == null) {
				result();
			} else {
				Double minX = null;
				String[] split = multipoint.split("[ (),]+");
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
			String multipoint = value_text(0);
			if (multipoint == null) {
				result();
			} else {
				Double minY = null;
				String[] split = multipoint.split("[ (),]+");
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

	private static class GetDimension extends PolygonFunction {

		@Override
		protected void xFunc() throws SQLException {
			result(0);
		}
	}

	private static class GetBoundingBox extends PolygonFunction {

		@Override
		protected void xFunc() throws SQLException {
			String multipoint = value_text(0);
			if (multipoint == null) {
				result();
			} else {
				Double maxX = null;
				Double maxY = null;
				Double minX = null;
				Double minY = null;
				String[] split = multipoint.split("[ (),]+");
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
			String multipoint = value_text(0);
			result(multipoint);
		}
	}

	private static class AsLine2D extends Function {

		@Override
		protected void xFunc() throws SQLException {
			String multipoint = value_text(0);
			String line = multipoint.replace("), (", ", ").replace("MULTIPOINT", "LINESTRING").replace("((", "(").replace("))", ")");
			result(line);
		}
	}

//	private static class AsPolygon2D extends PolygonFunction {
//
//		@Override
//		protected void xFunc() throws SQLException {
//			try {
//				String mpointStr = value_text(0);
//				if (mpointStr == null) {
//					result();
//				} else {
//					MultiPoint mPoint = getMultiPoint(mpointStr);
//					if (mPoint == null) {
//						result();
//					} else {
//						Polygon poly = null;
//						int numPoints = mPoint.getNumPoints();
//						if (numPoints < 3) {
//							result();
//						} else {
//							Coordinate[] coordinates = mPoint.getCoordinates();
//							if (coordinates[0].equals2D(coordinates[coordinates.length - 1])) {
//								poly = (Polygon) mPoint.convexHull();
//							} else {
//								List<Coordinate> asList = Arrays.asList(coordinates);
//								asList.add(coordinates[0]);
//								final GeometryFactory geometryFactory = new GeometryFactory();
//								poly = geometryFactory.createPolygon(asList.toArray(coordinates));
//							}
//							result(poly.toText());
//						}
//					}
//				}
//			} catch (ParseException ex) {
//				Logger.getLogger(MultiPoint2DFunctions.class.getName()).log(Level.SEVERE, null, ex);
//			}
//		}
//	}
	private static abstract class PolygonFunction extends Function {

		Polygon getPolygon(String possiblePoly) throws com.vividsolutions.jts.io.ParseException {
			WKTReader wktReader = new WKTReader();
			Geometry firstGeom = wktReader.read(possiblePoly);
			if (firstGeom instanceof Polygon) {
				return (Polygon) firstGeom;
			}
			return null;
		}

		LineString getLineString(String possiblePoly) throws com.vividsolutions.jts.io.ParseException {
			WKTReader wktReader = new WKTReader();
			Geometry firstGeom = wktReader.read(possiblePoly);
			if (firstGeom instanceof LineString) {
				return (LineString) firstGeom;
			}
			return null;
		}

		Point getPoint(String possiblePoly) throws com.vividsolutions.jts.io.ParseException {
			WKTReader wktReader = new WKTReader();
			Geometry firstGeom = wktReader.read(possiblePoly);
			if (firstGeom instanceof Point) {
				return (Point) firstGeom;
			}
			return null;
		}

		MultiPoint getMultiPoint(String possiblePoly) throws com.vividsolutions.jts.io.ParseException {
			WKTReader wktReader = new WKTReader();
			Geometry firstGeom = wktReader.read(possiblePoly);
			if (firstGeom instanceof MultiPoint) {
				return (MultiPoint) firstGeom;
			}
			return null;
		}
	}

}
