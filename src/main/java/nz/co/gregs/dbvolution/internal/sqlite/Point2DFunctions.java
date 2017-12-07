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
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class Point2DFunctions {

	/**
	 *
	 */
	public final static String CREATE_FROM_POINT2DS_FUNCTION = "DBV_CREATE_POINT2D_FROM_COORDS";

	/**
	 *
	 */
	public final static String EQUALS_FUNCTION = "DBV_POINT2D_EQUALS";

	/**
	 *
	 */
	public final static String GETX_FUNCTION = "DBV_POINT2D_GETX";

	/**
	 *
	 */
	public final static String GETY_FUNCTION = "DBV_POINT2D_GETY";

	/**
	 *
	 */
	public final static String GETDIMENSION_FUNCTION = "DBV_POINT2D_GETDIMENSION";

	/**
	 *
	 */
	public final static String GETBOUNDINGBOX_FUNCTION = "DBV_POINT2D_GETBOUNDINGBOX";

	/**
	 *
	 */
	public final static String ASTEXT_FUNCTION = "DBV_POINT2D_ASTEXT";

	private Point2DFunctions() {
	}

	/**
	 *
	 * @param connection
	 * @throws SQLException
	 */
	public static void addFunctions(Connection connection) throws SQLException {
		Function.create(connection, CREATE_FROM_POINT2DS_FUNCTION, new CreateFromCoords());
		Function.create(connection, EQUALS_FUNCTION, new Equals());
		Function.create(connection, GETX_FUNCTION, new GetX());
		Function.create(connection, GETY_FUNCTION, new GetY());
		Function.create(connection, GETDIMENSION_FUNCTION, new GetDimension());
		Function.create(connection, GETBOUNDINGBOX_FUNCTION, new GetBoundingBox());
		Function.create(connection, ASTEXT_FUNCTION, new AsText());
	}

	private static class CreateFromCoords extends Function {

		@Override
		protected void xFunc() throws SQLException {
			Double x = value_double(0);
			Double y = value_double(1);
			if (x == null || y == null) {
				result((String) null);
			} else {
				result("POINT (" + x + " " + y + ")");
			}
		}
	}

	private static class Equals extends Function {

		@Override
		protected void xFunc() throws SQLException {
			String firstPoint = value_text(0);
			String secondPoint = value_text(1);
			if (firstPoint == null || secondPoint == null) {
				result((String) null);
			} else {
				result(firstPoint.equals(secondPoint) ? 1 : 0);
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
				String point = x + " " + y;
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
