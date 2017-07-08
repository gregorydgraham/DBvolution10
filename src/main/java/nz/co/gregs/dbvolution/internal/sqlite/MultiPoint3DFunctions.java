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
import nz.co.gregs.dbvolution.datatypes.spatial3D.GeometryFactory3D;
import nz.co.gregs.dbvolution.datatypes.spatial3D.LineStringZ;
import nz.co.gregs.dbvolution.datatypes.spatial3D.MultiPointZ;
import nz.co.gregs.dbvolution.datatypes.spatial3D.PointZ;
import nz.co.gregs.dbvolution.datatypes.spatial3D.PolygonZ;
import org.sqlite.Function;

/**
 *
 * @author gregory.graham
 */
public class MultiPoint3DFunctions {

	/**
	 *
	 */
	public final static String CREATE_FROM_COORDS_FUNCTION = "DBV_CREATE_MPOINT3D_FROM_COORDS";

	/**
	 *
	 */
	public final static String EQUALS_FUNCTION = "DBV_MPOINT3D_EQUALS";

	/**
	 *
	 */
	public final static String GETMAXX_FUNCTION = "DBV_MPOINT3D_GETMAXX";

	/**
	 *
	 */
	public final static String GETMAXY_FUNCTION = "DBV_MPOINT3D_GETMAXY";

	/**
	 *
	 */
	public final static String GETMAXZ_FUNCTION = "DBV_MPOINT3D_GETMAXZ";

	/**
	 *
	 */
	public final static String GETMINX_FUNCTION = "DBV_MPOINT3D_GETMINX";

	/**
	 *
	 */
	public final static String GETMINY_FUNCTION = "DBV_MPOINT3D_GETMINY";

	/**
	 *
	 */
	public final static String GETMINZ_FUNCTION = "DBV_MPOINT3D_GETMINZ";

	/**
	 *
	 */
	public final static String GETDIMENSION_FUNCTION = "DBV_MPOINT3D_GETDIMENSION";

	/**
	 *
	 */
	public final static String GETBOUNDINGBOX_FUNCTION = "DBV_MPOINT3D_GETBOUNDINGBOX";

	/**
	 *
	 */
	public final static String GETNUMBEROFPOINTS_FUNCTION = "DBV_MPOINT3D_GETNUMBEROFPOINTS";

	/**
	 *
	 */
	public final static String GETPOINTATINDEX_FUNCTION = "DBV_MPOINT3D_GETPOINTATINDEX";

	/**
	 *
	 */
	public final static String ASTEXT_FUNCTION = "DBV_MPOINT3D_ASTEXT";

	/**
	 *
	 */
	public final static String ASLINE3D = "DBV_MPOINT3D_ASLINE3D";

	private MultiPoint3DFunctions() {
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
		Function.create(connection, GETMAXZ_FUNCTION, new GetMaxZ());
		Function.create(connection, GETMINX_FUNCTION, new GetMinX());
		Function.create(connection, GETMINY_FUNCTION, new GetMinY());
		Function.create(connection, GETMINZ_FUNCTION, new GetMinZ());
		Function.create(connection, GETDIMENSION_FUNCTION, new GetDimension());
		Function.create(connection, GETBOUNDINGBOX_FUNCTION, new GetBoundingBox());
		Function.create(connection, GETNUMBEROFPOINTS_FUNCTION, new GetNumberOfPoints());
		Function.create(connection, GETPOINTATINDEX_FUNCTION, new GetPointAtIndex());
		Function.create(connection, ASTEXT_FUNCTION, new AsText());
		Function.create(connection, ASLINE3D, new AsLine3D());
	}

	private static class CreateFromCoords extends PolygonFunction {
//MULTIPOINT (2 3 4, 3 4 5)

		@Override
		protected void xFunc() throws SQLException {
			Integer numberOfArguments = args();
			if (numberOfArguments == 0) {
				result();
			} else {
				String resultStr = "MULTIPOINT (";
				String sep = "";
				for (int i = 0; i < numberOfArguments; i += 3) {
					Double x = value_double(i);
					Double y = value_double(i + 1);
					Double z = value_double(i + 2);
					resultStr += sep + x + " " + y + " " + z;
					sep = ", ";
				}
				resultStr += ")";
				result(resultStr);
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
				String[] split = multipoint.trim().split("[ (),]+");
				result((split.length - 1) / 3);
			}
		}
	}

	private static class GetPointAtIndex extends PolygonFunction {
//MULTIPOINT (2 3, 3 4)

		@Override
		protected void xFunc() throws SQLException {
			String multipoint = value_text(0);
			int index = value_int(1);
			final int indexInMPoint = index * 3;
			if (multipoint == null || indexInMPoint <= 0) {
				result();
			} else {
				String[] split = multipoint.split("[ (),]+");
				if (indexInMPoint > split.length) {
					result();
				} else {
					String x = split[indexInMPoint - 1];
					String y = split[indexInMPoint];
					String z = split[indexInMPoint + 1];
					result("POINT (" + x + " " + y + " " + z + ")");
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
			String multipoint = value_text(0);
			if (multipoint == null) {
				result();
			} else {
				Double maxY = null;
				String[] split = multipoint.split("[ (),]+");
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
			String multipoint = value_text(0);
			if (multipoint == null) {
				result();
			} else {
				Double max = null;
				String[] split = multipoint.split("[ (),]+");
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

	private static class GetMinZ extends PolygonFunction {

		@Override
		protected void xFunc() throws SQLException {
			String multipoint = value_text(0);
			if (multipoint == null) {
				result();
			} else {
				Double minZ = null;
				String[] split = multipoint.split("[ (),]+");
				for (int i = 1; i < split.length; i += 3) {
					double z = Double.parseDouble(split[i + 2]);
					if (minZ == null || minZ > z) {
						minZ = z;
					}
				}
				result(minZ);
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
				Double maxZ = null;
				Double minX = null;
				Double minY = null;
				Double minZ = null;
				String[] split = multipoint.split("[ (),]+");
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
			String multipoint = value_text(0);
			result(multipoint);
		}
	}

	private static class AsLine3D extends Function {

		@Override
		protected void xFunc() throws SQLException {
			String multipoint = value_text(0);
			String line = multipoint.replace("), (", ", ").replace("MULTIPOINT", "LINESTRING").replace("((", "(").replace("))", ")");
			result(line);
		}
	}

	private static abstract class PolygonFunction extends Function {

		PolygonZ getPolygon(String possiblePoly) throws com.vividsolutions.jts.io.ParseException {
			WKTReader wktReader = new WKTReader();
			Geometry firstGeom = wktReader.read(possiblePoly);
			if (firstGeom instanceof Polygon) {
				return new GeometryFactory3D().createPolygonZ((Polygon) firstGeom);
			}
			return null;
		}

		LineStringZ getLineString(String possiblePoly) throws com.vividsolutions.jts.io.ParseException {
			WKTReader wktReader = new WKTReader();
			Geometry firstGeom = wktReader.read(possiblePoly);
			if (firstGeom instanceof LineString) {
				return new GeometryFactory3D().createLineStringZ((LineString) firstGeom);
			}
			return null;
		}

		PointZ getPoint(String possiblePoly) throws com.vividsolutions.jts.io.ParseException {
			WKTReader wktReader = new WKTReader();
			Geometry firstGeom = wktReader.read(possiblePoly);
			if (firstGeom instanceof Point) {
				return new GeometryFactory3D().createPointZ((Point) firstGeom);
			}
			return null;
		}

		MultiPointZ getMultiPoint(String possiblePoly) throws com.vividsolutions.jts.io.ParseException {
			WKTReader wktReader = new WKTReader();
			Geometry firstGeom = wktReader.read(possiblePoly);
			if (firstGeom instanceof MultiPoint) {
				return new GeometryFactory3D().createMultiPointZ((MultiPoint) firstGeom);
			}
			return null;
		}
	}

}
