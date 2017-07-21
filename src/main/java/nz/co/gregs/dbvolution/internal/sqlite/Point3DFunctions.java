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
import org.sqlite.Function;

/**
 *
 * @author gregorygraham
 */
public class Point3DFunctions {

	/**
	 *
	 */
	public final static String CREATE_FROM_POINT3DS_FUNCTION = "DBV_CREATE_POINT3D_FROM_COORDS";

	/**
	 *
	 */
	public final static String EQUALS_FUNCTION = "DBV_POINT3D_EQUALS";

	/**
	 *
	 */
	public final static String GETX_FUNCTION = "DBV_POINT3D_GETX";

	/**
	 *
	 */
	public final static String GETY_FUNCTION = "DBV_POINT3D_GETY";

	/**
	 *
	 */
	public final static String GETZ_FUNCTION = "DBV_POINT3D_GETZ";

	/**
	 *
	 */
	public final static String GETDIMENSION_FUNCTION = "DBV_POINT3D_GETDIMENSION";

	/**
	 *
	 */
	public final static String GETBOUNDINGBOX_FUNCTION = "DBV_POINT3D_GETBOUNDINGBOX";

	/**
	 *
	 */
	public final static String ASTEXT_FUNCTION = "DBV_POINT3D_ASTEXT";

	private Point3DFunctions() {
	}

	/**
	 *
	 * @param connection
	 * @throws SQLException
	 */
	public static void addFunctions(Connection connection) throws SQLException {
		Function.create(connection, CREATE_FROM_POINT3DS_FUNCTION, new CreateFromCoords());
		Function.create(connection, EQUALS_FUNCTION, new Equals());
		Function.create(connection, GETX_FUNCTION, new GetX());
		Function.create(connection, GETY_FUNCTION, new GetY());
		Function.create(connection, GETZ_FUNCTION, new GetZ());
		Function.create(connection, GETDIMENSION_FUNCTION, new GetDimension());
		Function.create(connection, GETBOUNDINGBOX_FUNCTION, new GetBoundingBox());
		Function.create(connection, ASTEXT_FUNCTION, new AsText());
	}

	private static class CreateFromCoords extends Function {

		@Override
		protected void xFunc() throws SQLException {
			Double x = value_double(0);
			Double y = value_double(1);
			Double z = value_double(2);
			if (x == null || y == null || z == null) {
				result((String) null);
			} else {
				result("POINT (" + x + " " + y + " " + z + ")");
			}
		}
	}

	private static class Equals extends Function {

		@Override
		protected void xFunc() throws SQLException {
			if (value_text(0) == null || value_text(1) == null) {
				result((String) null);
			} else {
				String[] split = value_text(0).split("[ (),]+");
				String[] split2 = value_text(1).split("[ (),]+");
				Double x1 = Double.valueOf(split[1]);
				Double x2 = Double.valueOf(split2[1]);
				Double y1 = Double.valueOf(split[2]);
				Double y2 = Double.valueOf(split2[2]);
				Double z1 = Double.valueOf(split[3]);
				Double z2 = Double.valueOf(split2[3]);
				result(x1.equals(x2) && y1.equals(y2) && z1.equals(z2) ? 1 : 0);
			}
		}
	}

	private static class GetX extends Function {

		@Override
		protected void xFunc() throws SQLException {
			String firstPoint = value_text(0);
			if (firstPoint == null) {
				result((String) null);
			} else {
				String[] split = firstPoint.split("[ ()]+");
				double x = Double.parseDouble(split[1]);
				result(x);
			}
		}
	}

	private static class GetY extends Function {

		@Override
		protected void xFunc() throws SQLException {
			String firstPoint = value_text(0);
			if (firstPoint == null) {
				result((String) null);
			} else {
				String[] split = firstPoint.split("[ ()]+");
				double y = Double.parseDouble(split[2]);
				result(y);
			}
		}
	}

	private static class GetZ extends Function {

		@Override
		protected void xFunc() throws SQLException {
			String firstPoint = value_text(0);
			if (firstPoint == null) {
				result((String) null);
			} else {
				String[] split = firstPoint.split("[ ()]+");
				double y = Double.parseDouble(split[3]);
				result(y);
			}
		}
	}

	private static class GetDimension extends Function {

		@Override
		protected void xFunc() throws SQLException {
			result(0);
		}
	}

	private static class GetBoundingBox extends Function {

		@Override
		protected void xFunc() throws SQLException {
			String firstPoint = value_text(0);
			if (firstPoint == null) {
				result((String) null);
			} else {
				String[] split = firstPoint.split("[ ()]+");
				double x = Double.parseDouble(split[1]);
				double y = Double.parseDouble(split[2]);
				double z = Double.parseDouble(split[3]);
				String point = x + " " + y + " " + z;
				String resultString = "POLYGON ((" + point + ", " + point + ", " + point + ", " + point + ", " + point + "))";
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
}
