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

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKTReader;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.databases.SQLiteDB;
import org.sqlite.Function;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class Polygon2DFunctions {

	/**
	 *
	 */
	public final static String CREATE_FROM_WKTPOLYGON2D = "DBV_CREATE_POLYGON2D_FROM_WKTPOLYGON";

	/**
	 *
	 */
	public final static String CREATE_FROM_POINT2DS = "DBV_CREATE_POLYGON2D_FROM_POINTS2D";

	/**
	 *
	 */
	public final static String EQUALS = "DBV_POLYGON2D_EQUALS";

	/**
	 *
	 */
	public final static String AREA = "DBV_POLYGON2D_AREA";

	/**
	 *
	 */
	public final static String DIMENSION = "DBV_POLYGON2D_DIMENSION";

	/**
	 *
	 */
	public final static String MIN_Y = "DBV_POLYGON2D_MIN_Y2D_COORD";

	/**
	 *
	 */
	public final static String MAX_Y = "DBV_POLYGON2D_MAX_Y2D_COORD";

	/**
	 *
	 */
	public final static String MAX_X = "DBV_POLYGON2D_MAX_X2D_COORD";

	/**
	 *
	 */
	public final static String MIN_X = "DBV_POLYGON2D_MIN_X2D_COORD";

	/**
	 *
	 */
	public final static String BOUNDINGBOX = "DBV_POLYGON2D_BOUNDINGBOX2D";

	/**
	 *
	 */
	public final static String TOUCHES = "DBV_POLYGON2D_TOUCHES";

	/**
	 *
	 */
	public final static String EXTERIORRING = "DBV_POLYGON2D_EXTERIORRING";

	/**
	 *
	 */
	public final static String CONTAINS_POLYGON2D = "DBV_POLYGON2D_CONTAINS";

	/**
	 *
	 */
	public final static String WITHIN = "DBV_POLYGON2D_WITHIN";

	/**
	 *
	 */
	public final static String OVERLAPS = "DBV_POLYGON2D_OVERLAPS";

	/**
	 *
	 */
	public final static String INTERSECTS = "DBV_POLYGON2D_INTERSECTS";
	public final static String INTERSECTION = "DBV_POLYGON2D_INTERSECTION";
	public final static String UNION = "DBV_POLYGON2D_UNION";

	/**
	 *
	 */
	public final static String DISJOINT = "DBV_POLYGON2D_DISJOINT";

	/**
	 *
	 */
	public final static String CONTAINS_POINT2D = "DBV_POLYGON2D_CONTAINS_POINT2D";
	public final static String ASTEXT_FUNCTION = "DBV_POLYGON2D_ASTEXT";

	private Polygon2DFunctions() {
	}

	/**
	 *
	 * @param connection
	 * @throws SQLException
	 */
	public static void addFunctions(java.sql.Connection connection) throws SQLException {
		add(connection, DIMENSION, new SpatialDimension());
		add(connection, EQUALS, new Equals());
		add(connection, AREA, new Area());
		add(connection, TOUCHES, new Touches());
		add(connection, EXTERIORRING, new ExteriorRing());
		add(connection, CONTAINS_POLYGON2D, new Contains());
		add(connection, WITHIN, new Within());
		add(connection, OVERLAPS, new Overlaps());
		add(connection, INTERSECTS, new Intersects());
		add(connection, INTERSECTION, new Intersection());
		add(connection, UNION, new Union());
		add(connection, DISJOINT, new Disjoint());
		add(connection, CREATE_FROM_WKTPOLYGON2D, new CreatePolygonFromWKTPolygon2D());
		add(connection, CREATE_FROM_POINT2DS, new CreatePolygonFromPoint2Ds());
		add(connection, MAX_X, new MaxX());
		add(connection, MIN_X, new MinX());
		add(connection, MAX_Y, new MaxY());
		add(connection, MIN_Y, new MinY());
		add(connection, BOUNDINGBOX, new BoundingBox());
		add(connection, CONTAINS_POINT2D, new ContainsPoint2D());
		add(connection, ASTEXT_FUNCTION, new AsText());
	}

	private static void add(java.sql.Connection connection, String functionName, Function function) throws SQLException {
		Function.destroy(connection, functionName);
		Function.create(connection, functionName, function);
	}

	/**
	 * Implements Polygon2D DIMENSION for SQLite
	 *
	 */
	private static class SpatialDimension extends Function {

		@Override
		protected void xFunc() throws SQLException {
			result(2);
		}
	}

	/**
	 * Implements Polygon2D AsText for SQLite
	 *
	 */
	private static class AsText extends Function {

		@Override
		protected void xFunc() throws SQLException {
			String point = value_text(0);
			result(point);
		}
	}

	/**
	 * Implements Polygon2D CREATE for SQLite
	 *
	 */
	private static class CreatePolygonFromWKTPolygon2D extends PolygonFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				Polygon polygon = getPolygon(value_text(0));
				//			polygon.normalize();
				result(polygon.toText());
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(Polygon2DFunctions.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
	}

	/**
	 * Implements Polygon2D CREATE for SQLite
	 *
	 */
	private static class CreatePolygonFromPoint2Ds extends Function {

		@Override
		protected void xFunc() throws SQLException {
			try {
				WKTReader wktReader = new WKTReader();
				GeometryFactory factory = new GeometryFactory();
				List<Coordinate> coords = new ArrayList<Coordinate>();
				String originalStr;
				int numberOfPoints = args();
				for (int index = 0; index < numberOfPoints; index++) {
					originalStr = value_text(index);
					if (originalStr == null) {
						result((String) null);
					} else {
						Point point;
						Geometry geometry;
						geometry = wktReader.read(originalStr);
						if (geometry instanceof Point) {
							point = (Point) geometry;
							coords.add(point.getCoordinate());
						} else {
							throw new ParseException(originalStr, 0);
						}
					}
				}
				Polygon createPolygon = factory.createPolygon(coords.toArray(new Coordinate[]{}));
				createPolygon.normalize();
				result(createPolygon.toText());
			} catch (ParseException | com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite Polygon", ex);
			}
		}
	}

	private static class MaxX extends Function {

		@Override
		protected void xFunc() throws SQLException {
			try {
				WKTReader wktReader = new WKTReader();
				String originalStr;
				originalStr = value_text(0);
				if (originalStr == null) {
					result((String) null);
				} else {
					Geometry geometry;
					geometry = wktReader.read(originalStr);
					if (geometry instanceof Polygon) {
						Polygon polygon = (Polygon) geometry;
						Double maxX = null;
						Coordinate[] coordinates = polygon.getCoordinates();
						for (Coordinate coordinate : coordinates) {
							if (maxX == null || coordinate.x > maxX) {
								maxX = coordinate.x;
							}
						}
						result(maxX);
					} else {
						throw new ParseException(originalStr, 0);
					}
				}
			} catch (ParseException | com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite Polygon", ex);
			}
		}
	}

	private static class MinX extends Function {

		@Override
		protected void xFunc() throws SQLException {
			try {
				WKTReader wktReader = new WKTReader();
				String originalStr = value_text(0);
				if (originalStr == null) {
					result((String) null);
				} else {
					Polygon polygon;
					Geometry geometry;
					geometry = wktReader.read(originalStr);
					if (geometry instanceof Polygon) {
						polygon = (Polygon) geometry;
						Double minX = null;
						Coordinate[] coordinates = polygon.getCoordinates();
						for (Coordinate coordinate : coordinates) {
							if (minX == null || coordinate.x < minX) {
								minX = coordinate.x;
							}
						}
						result(minX);
					} else {
						throw new ParseException(originalStr, 0);
					}
				}
			} catch (ParseException | com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite Polygon", ex);
			}
		}
	}

	private static class MaxY extends Function {

		@Override
		protected void xFunc() throws SQLException {
			try {
				WKTReader wktReader = new WKTReader();
				String originalStr = value_text(0);
				if (originalStr == null) {
					result((String) null);
				} else {
					Polygon polygon;
					Geometry geometry;
					geometry = wktReader.read(originalStr);
					if (geometry instanceof Polygon) {
						polygon = (Polygon) geometry;
						Double maxY = null;
						Coordinate[] coordinates = polygon.getCoordinates();
						for (Coordinate coordinate : coordinates) {
							if (maxY == null || coordinate.y > maxY) {
								maxY = coordinate.y;
							}
						}
						result(maxY);
					} else {
						throw new ParseException(originalStr, 0);
					}
				}
//				Polygon createPolygon = factory.createPolygon(coords.toArray(new Coordinate[]{}));
//				result(createPolygon.toText());
			} catch (ParseException | com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite Polygon", ex);
			}
		}
	}

	private static class MinY extends Function {

		@Override
		protected void xFunc() throws SQLException {
			try {
				WKTReader wktReader = new WKTReader();
				String originalStr = value_text(0);
				if (originalStr == null) {
					result((String) null);
				} else {
					Polygon polygon;
					Geometry geometry;
					geometry = wktReader.read(originalStr);
					if (geometry instanceof Polygon) {
						polygon = (Polygon) geometry;
						Double minY = null;
						Coordinate[] coordinates = polygon.getCoordinates();
						for (Coordinate coordinate : coordinates) {
							if (minY == null || coordinate.y < minY) {
								minY = coordinate.y;
							}
						}
						result(minY);
					} else {
						throw new ParseException(originalStr, 0);
					}
				}
			} catch (com.vividsolutions.jts.io.ParseException | SQLException | ParseException ex) {
				throw new RuntimeException("Failed To Parse Polygon", ex);
			}
		}
	}

	private static class BoundingBox extends Function {

		@Override
		protected void xFunc() throws SQLException {
			try {
				WKTReader wktReader = new WKTReader();
				GeometryFactory factory = new GeometryFactory();
				String originalStr = value_text(0);
				if (originalStr == null) {
					result((String) null);
				} else {
					Polygon polygon;
					Geometry geometry;
					geometry = wktReader.read(originalStr);
					if (geometry instanceof Polygon) {
						polygon = (Polygon) geometry;
						Double minX = null;
						Double minY = null;
						Double maxX = null;
						Double maxY = null;
						Coordinate[] coordinates = polygon.getCoordinates();
						for (Coordinate coordinate : coordinates) {
							if (minX == null || coordinate.x < minX) {
								minX = coordinate.x;
							}
							if (minY == null || coordinate.y < minY) {
								minY = coordinate.y;
							}
							if (maxX == null || coordinate.x > maxX) {
								maxX = coordinate.x;
							}
							if (maxY == null || coordinate.y > minY) {
								maxY = coordinate.y;
							}
						}
						Polygon createPolygon = factory.createPolygon(new Coordinate[]{
							new Coordinate(minX, minY),
							new Coordinate(maxX, minY),
							new Coordinate(maxX, maxY),
							new Coordinate(minX, maxY),
							new Coordinate(minX, minY),});
						createPolygon.normalize();
						result(createPolygon.toText());
					} else {
						throw new ParseException(originalStr, 0);
					}
				}
			} catch (ParseException | com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite Polygon", ex);
			}
		}
	}

	private static class Equals extends PolygonFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				Polygon firstPoly = getPolygon(value_text(0));
				Polygon secondPoly = getPolygon(value_text(1));
				if (firstPoly == null || secondPoly == null) {
					result();
				} else {
					firstPoly.normalize();
					secondPoly.normalize();
					result(firstPoly.toText().equals(secondPoly.toText()) ? 1 : 0);
				}
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite Polygon", ex);
			}
		}
	}

	private static class Area extends PolygonFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				Polygon poly = getPolygon(value_text(0));
				if (poly == null) {
					result();
				} else {
					result(poly.getArea());
				}
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite Polygon", ex);
			}
		}
	}

	private static class Touches extends PolygonFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				Polygon poly1 = getPolygon(value_text(0));
				Polygon poly2 = getPolygon(value_text(1));
				if (poly1 == null || poly2 == null) {
					result();
				} else {
					result(poly1.touches(poly2) ? 1 : 0);
				}
			} catch (com.vividsolutions.jts.io.ParseException | SQLException ex) {
				throw new RuntimeException("Failed To Parse Polygon", ex);
			}
		}
	}

	private static class ExteriorRing extends PolygonFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				Polygon poly1 = getPolygon(value_text(0));
				if (poly1 == null) {
					result();
				} else {
					final LineString exteriorRing = poly1.getExteriorRing();
					exteriorRing.normalize();
//					Polygon exteriorPolygon = (new GeometryFactory()).createPolygon(exteriorRing.getCoordinateSequence());
//					result(exteriorPolygon.toText());
					LineString createLineString = (new GeometryFactory()).createLineString(exteriorRing.getCoordinates());
					Geometry reverse = createLineString.reverse();
					result(reverse.toText());
				}
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite Polygon", ex);
			}
		}
	}

	private static class Contains extends PolygonFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				Polygon poly1 = getPolygon(value_text(0));
				Polygon poly2 = getPolygon(value_text(1));
				if (poly1 == null || poly2 == null) {
					result();
				} else {
					result(poly1.contains(poly2) ? 1 : 0);
				}
			} catch (com.vividsolutions.jts.io.ParseException | SQLException ex) {
				throw new RuntimeException("Failed To Parse Polygon", ex);
			}
		}
	}

	private static class ContainsPoint2D extends PolygonFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				Polygon poly1 = getPolygon(value_text(0));
				Point point = getPoint(value_text(1));
				if (poly1 == null || point == null) {
					result();
				} else {
					result(poly1.contains(point) ? 1 : 0);
				}
			} catch (com.vividsolutions.jts.io.ParseException | SQLException ex) {
				throw new RuntimeException("Failed To Parse Polygon or Point", ex);
			}
		}
	}

	private static class Within extends PolygonFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				Polygon poly1 = getPolygon(value_text(0));
				Polygon poly2 = getPolygon(value_text(1));
				if (poly1 == null || poly2 == null) {
					result();
				} else {
					result(poly1.within(poly2) ? 1 : 0);
				}
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite Polygon", ex);
			}
		}
	}

	private static class Overlaps extends PolygonFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				Polygon poly1 = getPolygon(value_text(0));
				Polygon poly2 = getPolygon(value_text(1));
				if (poly1 == null || poly2 == null) {
					result();
				} else {
					result(poly1.overlaps(poly2) ? 1 : 0);
				}
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite Polygon", ex);
			}
		}
	}

	private static class Intersects extends PolygonFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				Polygon poly1 = getPolygon(value_text(0));
				Polygon poly2 = getPolygon(value_text(1));
				if (poly1 == null || poly2 == null) {
					result();
				} else {
					result(poly1.intersects(poly2) ? 1 : 0);
				}
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite Polygon", ex);
			}
		}
	}

	private static class Intersection extends PolygonFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				Polygon poly1 = getPolygon(value_text(0));
				Polygon poly2 = getPolygon(value_text(1));
				if (poly1 == null || poly2 == null) {
					result();
				} else {
					result(poly1.intersection(poly2).toText());
				}
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite Polygon", ex);
			}
		}
	}


	private static class Union extends PolygonFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				Polygon poly1 = getPolygon(value_text(0));
				Polygon poly2 = getPolygon(value_text(1));
				if (poly1 == null || poly2 == null) {
					result();
				} else {
					result(poly1.union(poly2).toText());
				}
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite Polygon", ex);
			}
		}
	}

	private static class Disjoint extends PolygonFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				Polygon poly1 = getPolygon(value_text(0));
				Polygon poly2 = getPolygon(value_text(1));
				if (poly1 == null || poly2 == null) {
					result();
				} else {
					result(poly1.disjoint(poly2) ? 1 : 0);
				}
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(Polygon2DFunctions.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite Polygon", ex);
			}
		}
	}

	private static abstract class PolygonFunction extends Function {

		Polygon getPolygon(String possiblePoly) throws com.vividsolutions.jts.io.ParseException {
			WKTReader wktReader = new WKTReader();
			Geometry firstGeom = wktReader.read(possiblePoly);
			if (firstGeom instanceof Polygon) {
				return (Polygon) firstGeom;
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
	}
}
