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
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKTReader;
import nz.co.gregs.dbvolution.datatypes.spatial3D.PointZ;
import nz.co.gregs.dbvolution.datatypes.spatial3D.PolygonZ;
import nz.co.gregs.dbvolution.datatypes.spatial3D.GeometryFactory3D;
import nz.co.gregs.dbvolution.datatypes.spatial3D.LineStringZ;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.databases.SQLiteDB;
import nz.co.gregs.dbvolution.datatypes.spatial3D.LinearRingZ;
import org.sqlite.Function;

/**
 *
 * @author gregorygraham
 */
public class Polygon3DFunctions {

	/**
	 *
	 */
	public final static String CREATE_FROM_WKTPOLYGON3D = "DBV_CREATE_POLYGON3D_FROM_WKTPOLYGON";

	/**
	 *
	 */
	public final static String CREATE_FROM_POINT3DS = "DBV_CREATE_POLYGON3D_FROM_POINTS2D";

	/**
	 *
	 */
	public final static String EQUALS = "DBV_POLYGON3D_EQUALS";

	/**
	 *
	 */
	public final static String AREA = "DBV_POLYGON3D_AREA";

	/**
	 *
	 */
	public final static String SPATIAL_DIMENSIONS = "DBV_POLYGON3D_SPATIALDIMENSIONS";

	/**
	 *
	 */
	public final static String MEASURABLE_DIMENSIONS = "DBV_POLYGON3D_MEASURABLEDIMENSIONS";

	/**
	 *
	 */
	public final static String MIN_Z = "DBV_POLYGON3D_MIN_Z3D_COORD";

	/**
	 *
	 */
	public final static String MAX_Z = "DBV_POLYGON3D_MAX_Z3D_COORD";

	/**
	 *
	 */
	public final static String MIN_Y = "DBV_POLYGON3D_MIN_Y3D_COORD";

	/**
	 *
	 */
	public final static String MAX_Y = "DBV_POLYGON3D_MAX_Y3D_COORD";

	/**
	 *
	 */
	public final static String MAX_X = "DBV_POLYGON3D_MAX_X3D_COORD";

	/**
	 *
	 */
	public final static String MIN_X = "DBV_POLYGON3D_MIN_X3D_COORD";

	/**
	 *
	 */
	public final static String BOUNDINGBOX = "DBV_POLYGON3D_BOUNDINGBOX3D";

	/**
	 *
	 */
	public final static String TOUCHES = "DBV_POLYGON3D_TOUCHES";

	/**
	 *
	 */
	public final static String EXTERIORRING = "DBV_POLYGON3D_EXTERIORRING";

	/**
	 *
	 */
	public final static String CONTAINS_POLYGON3D = "DBV_POLYGON3D_CONTAINS";

	/**
	 *
	 */
	public final static String WITHIN = "DBV_POLYGON3D_WITHIN";

	/**
	 *
	 */
	public final static String OVERLAPS = "DBV_POLYGON3D_OVERLAPS";

	/**
	 *
	 */
	public final static String INTERSECTS = "DBV_POLYGON3D_INTERSECTS";

	/**
	 *
	 */
	public final static String DISJOINT = "DBV_POLYGON3D_DISJOINT";

	/**
	 *
	 */
	public final static String CONTAINS_POINT3D = "DBV_POLYGON3D_CONTAINS_POINT3D";
	public final static String ASTEXT_FUNCTION = "DBV_POLYGON3D_ASTEXT";

	private Polygon3DFunctions() {
	}

	/**
	 *
	 * @param connection
	 * @throws SQLException
	 */
	public static void addFunctions(java.sql.Connection connection) throws SQLException {
		add(connection, SPATIAL_DIMENSIONS, new SpatialDimensions());
		add(connection, MEASURABLE_DIMENSIONS, new MeasurableDimensions());
		add(connection, EQUALS, new Equals());
		add(connection, AREA, new Area());
		add(connection, TOUCHES, new Touches());
		add(connection, EXTERIORRING, new ExteriorRing());
		add(connection, CONTAINS_POLYGON3D, new Contains());
		add(connection, WITHIN, new Within());
		add(connection, OVERLAPS, new Overlaps());
		add(connection, INTERSECTS, new Intersects());
		add(connection, DISJOINT, new Disjoint());
		add(connection, CREATE_FROM_WKTPOLYGON3D, new CreatePolygonFromWKTPolygon3D());
		add(connection, CREATE_FROM_POINT3DS, new CreatePolygonFromPoint3Ds());
		add(connection, MAX_X, new MaxX());
		add(connection, MIN_X, new MinX());
		add(connection, MAX_Y, new MaxY());
		add(connection, MIN_Y, new MinY());
		add(connection, MAX_Z, new MaxZ());
		add(connection, MIN_Z, new MinZ());
		add(connection, BOUNDINGBOX, new BoundingBox());
		add(connection, CONTAINS_POINT3D, new ContainsPoint3D());
		add(connection, ASTEXT_FUNCTION, new AsText());
	}

	private static void add(java.sql.Connection connection, String functionName, Function function) throws SQLException {
		Function.destroy(connection, functionName);
		Function.create(connection, functionName, function);
	}

	/**
	 * Implements Polygon2D MEASURABLE_DIMENSIONS for SQLite
	 *
	 */
	private static class MeasurableDimensions extends Function {

		@Override
		protected void xFunc() throws SQLException {
			result(2);
		}
	}

	private static class SpatialDimensions extends Function {

		@Override
		protected void xFunc() throws SQLException {
			result(3);
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
	private static class CreatePolygonFromWKTPolygon3D extends Polygon3DFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				PolygonZ polygon = getPolygonZ(value_text(0));
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
	private static class CreatePolygonFromPoint3Ds extends Polygon3DFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				GeometryFactory3D factory = new GeometryFactory3D();
				List<Coordinate> coords = new ArrayList<Coordinate>();
				String originalStr;
				int numberOfPoints = args();
				for (int index = 0; index < numberOfPoints; index++) {
					originalStr = value_text(index);
					if (originalStr == null) {
						result((String) null);
					} else {
						PointZ point = null;
						point = getPointZ(originalStr);
						coords.add(point.getCoordinate());
					}
				}
				PolygonZ createPolygon = factory.createPolygonZ(coords.toArray(new Coordinate[]{}));
				createPolygon.normalize();
				result(createPolygon.toText());
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite PointZ: " + ex.getMessage(), ex);
			}
		}
	}

	private static class MaxX extends Polygon3DFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				String originalStr;
				originalStr = value_text(0);
				if (originalStr == null) {
					result((String) null);
				} else {
					PolygonZ polygon = getPolygonZ(originalStr);
					Double maxX = null;
					Coordinate[] coordinates = polygon.getCoordinates();
					for (Coordinate coordinate : coordinates) {
						if (maxX == null || coordinate.x > maxX) {
							maxX = coordinate.x;
						}
					}
					result(maxX);
				}
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(Line2DFunctions.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite PolygonZ: " + ex.getMessage(), ex);
			}
		}
	}

	private static class MinX extends Polygon3DFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				String originalStr = value_text(0);
				if (originalStr == null) {
					result((String) null);
				} else {
					PolygonZ polygon = getPolygonZ(originalStr);
					Double minX = null;
					Coordinate[] coordinates = polygon.getCoordinates();
					for (Coordinate coordinate : coordinates) {
						if (minX == null || coordinate.x < minX) {
							minX = coordinate.x;
						}
					}
					result(minX);
				}
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite PolygonZ: " + ex.getMessage(), ex);
			}
		}
	}

	private static class MaxY extends Polygon3DFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				String originalStr = value_text(0);
				if (originalStr == null) {
					result((String) null);
				} else {
					PolygonZ polygon = getPolygonZ(originalStr);
					Double maxY = null;
					Coordinate[] coordinates = polygon.getCoordinates();
					for (Coordinate coordinate : coordinates) {
						if (maxY == null || coordinate.y > maxY) {
							maxY = coordinate.y;
						}
					}
					result(maxY);
				}
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite PolygonZ: " + ex.getMessage(), ex);
			}
		}
	}

	private static class MinY extends Polygon3DFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				String originalStr = value_text(0);
				if (originalStr == null) {
					result((String) null);
				} else {
					PolygonZ polygon = getPolygonZ(originalStr);
					Double minY = null;
					Coordinate[] coordinates = polygon.getCoordinates();
					for (Coordinate coordinate : coordinates) {
						if (minY == null || coordinate.y < minY) {
							minY = coordinate.y;
						}
					}
					result(minY);
				}
			} catch (Exception ex) {
				throw new RuntimeException("Failed To Parse PolygonZ: " + ex.getMessage(), ex);
			}
		}
	}

	private static class MaxZ extends Polygon3DFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				String originalStr;
				originalStr = value_text(0);
				if (originalStr == null) {
					result((String) null);
				} else {
					PolygonZ polygon = getPolygonZ(originalStr);
					Double max = null;
					Coordinate[] coordinates = polygon.getCoordinates();
					for (Coordinate coordinate : coordinates) {
						if (max == null || coordinate.z > max) {
							max = coordinate.z;
						}
					}
					result(max);
				}
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite PolygonZ: " + ex.getMessage(), ex);
			}
		}
	}

	private static class MinZ extends Polygon3DFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				String originalStr = value_text(0);
				if (originalStr == null) {
					result((String) null);
				} else {
					PolygonZ polygon = getPolygonZ(originalStr);
					Double min = null;
					Coordinate[] coordinates = polygon.getCoordinates();
					for (Coordinate coordinate : coordinates) {
						if (min == null || coordinate.z < min) {
							min = coordinate.z;
						}
					}
					result(min);
				}
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite PolygonZ: " + ex.getMessage(), ex);
			}
		}
	}

	private static class BoundingBox extends Polygon3DFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				GeometryFactory3D factory = new GeometryFactory3D();
				String originalStr = value_text(0);
				if (originalStr == null) {
					result((String) null);
				} else {
					PolygonZ polygon = getPolygonZ(originalStr);
					Double minX = null;
					Double minY = null;
					Double minZ = null;
					Double maxX = null;
					Double maxY = null;
					Double maxZ = null;
					Coordinate[] coordinates = polygon.getCoordinates();
					for (Coordinate coordinate : coordinates) {
						if (minX == null || coordinate.x < minX) {
							minX = coordinate.x;
						}
						if (minY == null || coordinate.y < minY) {
							minY = coordinate.y;
						}
						if (minZ == null || coordinate.z < minZ) {
							minZ = coordinate.z;
						}
						if (maxX == null || coordinate.x > maxX) {
							maxX = coordinate.x;
						}
						if (maxY == null || coordinate.y > minY) {
							maxY = coordinate.y;
						}
						if (maxZ == null || coordinate.z > minZ) {
							maxZ = coordinate.z;
						}
					}
					PolygonZ createPolygon = factory.createPolygonZ(new Coordinate[]{
						new Coordinate(minX, minY, minZ),
						new Coordinate(maxX, minY, minZ),
						new Coordinate(maxX, maxY, minZ),
						new Coordinate(maxX, maxY, maxZ),
						new Coordinate(minX, maxY, maxZ),
						new Coordinate(minX, minY, maxZ),
						new Coordinate(minX, minY, minZ)
					});
					createPolygon.normalize();
					result(createPolygon.toText());
				}
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite PolygonZ: " + ex.getMessage(), ex);
			}
		}
	}

	private static class Equals extends Polygon3DFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				PolygonZ firstPoly = getPolygonZ(value_text(0));
				PolygonZ secondPoly = getPolygonZ(value_text(1));
				if (firstPoly == null || secondPoly == null) {
					result();
				} else {
					firstPoly.normalize();
					secondPoly.normalize();
					result(firstPoly.toText().equals(secondPoly.toText()) ? 1 : 0);
				}
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite PolygonZ: " + ex.getMessage(), ex);
			}
		}
	}

	private static class Area extends Polygon3DFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				PolygonZ poly = getPolygonZ(value_text(0));
				if (poly == null) {
					result();
				} else {
					result(poly.getArea());
				}
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite PolygonZ: " + ex.getMessage(), ex);
			}
		}
	}

	private static class Touches extends Polygon3DFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				final String orig = value_text(0);
				final String other = value_text(1);
				if (orig == null || other == null) {
					result();
				}
				PolygonZ poly1 = getPolygonZ(orig);
				PolygonZ poly2 = getPolygonZ(other);
				if (poly1 == null || poly2 == null) {
					result();
				} else {
					result(poly1.touches(poly2) ? 1 : 0);
				}
			} catch (Exception ex) {
				throw new RuntimeException("Failed To Parse PolygonZ: " + ex.getMessage(), ex);
			}
		}
	}

	private static class ExteriorRing extends Polygon3DFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				PolygonZ poly1 = getPolygonZ(value_text(0));
				if (poly1 == null) {
					result();
				} else {
					final LinearRingZ exteriorRing = poly1.getExteriorRingZ();
					exteriorRing.normalize();
					LineStringZ line = (new GeometryFactory3D()).createLineStringZ(exteriorRing.getCoordinates());
					Geometry reverse = line.reverse();
					result(reverse.toText());
				}
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite PolygonZ", ex);
			}
		}
	}

	private static class Contains extends Polygon3DFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				PolygonZ poly1 = getPolygonZ(value_text(0));
				PolygonZ poly2 = getPolygonZ(value_text(1));
				if (poly1 == null || poly2 == null) {
					result();
				} else {
					result(poly1.contains(poly2) ? 1 : 0);
				}
			} catch (Exception ex) {
				throw new RuntimeException("Failed To Parse PolygonZ", ex);
			}
		}
	}

	private static class ContainsPoint3D extends Polygon3DFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				PolygonZ poly1 = getPolygonZ(value_text(0));
				PointZ point = getPointZ(value_text(1));
				if (poly1 == null || point == null) {
					result();
				} else {
					result(poly1.contains(point) ? 1 : 0);
				}
			} catch (Exception ex) {
				throw new RuntimeException("Failed To Parse PolygonZ or PointZ", ex);
			}
		}
	}

	private static class Within extends Polygon3DFunction {

		@Override
		protected void xFunc() throws SQLException {
			final String orig = value_text(0);
			try {
				PolygonZ poly1 = getPolygonZ(orig);
				final String other = value_text(1);
				try {
					PolygonZ poly2 = getPolygonZ(other);
					if (poly1 == null || poly2 == null) {
						result();
					} else {
						result(poly1.within(poly2) ? 1 : 0);
					}
				} catch (com.vividsolutions.jts.io.ParseException ex) {
					System.out.println("FAILED TO PARSE: " + other);
					Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
					throw new RuntimeException("Failed To Parse SQLite PolygonZ", ex);
				}
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				System.out.println("FAILED TO PARSE: " + orig);
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite PolygonZ", ex);
			}
		}
	}

	private static class Overlaps extends Polygon3DFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				PolygonZ poly1 = getPolygonZ(value_text(0));
				PolygonZ poly2 = getPolygonZ(value_text(1));
				if (poly1 == null || poly2 == null) {
					result();
				} else {
					result(poly1.overlaps(poly2) ? 1 : 0);
				}
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite PolygonZ", ex);
			}
		}
	}

	private static class Intersects extends Polygon3DFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				PolygonZ poly1 = getPolygonZ(value_text(0));
				PolygonZ poly2 = getPolygonZ(value_text(1));
				if (poly1 == null || poly2 == null) {
					result();
				} else {
					result(poly1.intersects(poly2) ? 1 : 0);
				}
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite PolygonZ", ex);
			}
		}
	}

	private static class Disjoint extends Polygon3DFunction {

		@Override
		protected void xFunc() throws SQLException {
			try {
				PolygonZ poly1 = getPolygonZ(value_text(0));
				PolygonZ poly2 = getPolygonZ(value_text(1));
				if (poly1 == null || poly2 == null) {
					result();
				} else {
					result(poly1.disjoint(poly2) ? 1 : 0);
				}
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(Polygon2DFunctions.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite PolygonZ", ex);
			}
		}
	}

	private static abstract class Polygon3DFunction extends Function {

		PolygonZ getPolygonZ(String possiblePoly) throws com.vividsolutions.jts.io.ParseException {
			if (possiblePoly != null) {
				try {
					//System.out.print("POLYGON: " + possiblePoly);
					WKTReader wktReader = new WKTReader();
					Geometry firstGeom = wktReader.read(possiblePoly);
					//System.out.println(" SUCCESS");
					if (firstGeom instanceof Polygon) {
						return new GeometryFactory3D().createPolygonZ((Polygon) firstGeom);
					}
					return null;
				} catch (com.vividsolutions.jts.io.ParseException exp) {
					System.out.println("FAILED!!! " + possiblePoly);
					throw new RuntimeException("getPolygonZ FAILED TO PARSE: " + (possiblePoly == null ? "NULL" : possiblePoly));
				}
			} else {
				return null;
			}
		}

		PointZ getPointZ(String possiblePoint) throws com.vividsolutions.jts.io.ParseException {
			//System.out.println("POINT: " + possiblePoint);
			try {
				WKTReader wktReader = new WKTReader();
				Geometry firstGeom = wktReader.read(possiblePoint);
				if (firstGeom instanceof Point) {
					return new GeometryFactory3D().createPointZ((Point) firstGeom);
				}
				return null;

			} catch (com.vividsolutions.jts.io.ParseException exp) {
				System.out.println("FAILED!!! " + possiblePoint);
				throw new RuntimeException("getPointZ FAILED TO PARSE: " + (possiblePoint == null ? "NULL" : possiblePoint));
			}
		}
	}
}
