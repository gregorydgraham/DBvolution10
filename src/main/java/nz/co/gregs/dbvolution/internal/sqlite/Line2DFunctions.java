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
public class Line2DFunctions {

	public static String CREATE_FROM_COORDS_FUNCTION = "DBV_CREATE_LINE2D_FROM_COORDS";
	public static String EQUALS_FUNCTION = "DBV_LINE2D_EQUALS";
	public static String GETMAXX_FUNCTION = "DBV_LINE2D_GETMAXX";
	public static String GETMAXY_FUNCTION = "DBV_LINE2D_GETMAXY";
	public static String GETMINX_FUNCTION = "DBV_LINE2D_GETMINX";
	public static String GETMINY_FUNCTION = "DBV_LINE2D_GETMINY";
	public static String GETDIMENSION_FUNCTION = "DBV_LINE2D_GETDIMENSION";
	public static String GETBOUNDINGBOX_FUNCTION = "DBV_LINE2D_GETBOUNDINGBOX";
	public static String ASTEXT_FUNCTION = "DBV_LINE2D_ASTEXT";

	private Line2DFunctions() {
	}

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
	}

	private static class CreateFromCoords extends Function {
//LINESTRING (2 3, 3 4)

		@Override
		protected void xFunc() throws SQLException {
			Integer numberOfArguments = args();
			if (numberOfArguments % 2 != 0) {
				result();
			} else {
				String resultStr = "LINESTRING (";
				String sep = "";
				for (int i = 0; i < numberOfArguments; i += 2) {
					Double x = value_double(i);
					Double y = value_double(i + 1);
					if (x == null || y == null) {
						result();
						return;
					} else {
						resultStr += sep + x + " " + y;
						sep = ", ";
					}
				}
				resultStr += ")";
				result(resultStr);
			}
		}
	}

	private static class Equals extends Function {

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

	private static class GetMaxX extends Function {

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
					if (maxX==null || maxX<x){
						maxX = x;
					}
				}
				result(maxX);
			}
		}

	}

	private static class GetMaxY extends Function {

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
					if (maxY==null || maxY<y){
						maxY = y;
					}
				}
				result(maxY);
			}
		}
	}

	private static class GetMinX extends Function {

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
					if (minX==null || minX>x){
						minX = x;
					}
				}
				result(minX);
			}
		}

	}

	private static class GetMinY extends Function {

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
					if (minY==null || minY>y){
						minY = y;
					}
				}
				result(minY);
			}
		}
	}

	private static class GetDimension extends Function {

		@Override
		protected void xFunc() throws SQLException {
			result(1);
		}
	}

	private static class GetBoundingBox extends Function {

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
					if (maxX==null || maxX<x){
						maxX = x;
					}
					if (maxY==null || maxY<y){
						maxY = y;
					}
					if (minX==null || minX>x){
						minX = x;
					}
					if (minY==null || minY>y){
						minY = y;
					}
				}
				String resultString = "POLYGON ((" + minX+" "+minY + ", " + maxX+" "+minY + ", " + maxX+" "+maxY + ", " + minX+" "+maxY + ", " + minX+" "+minY + "))";
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
