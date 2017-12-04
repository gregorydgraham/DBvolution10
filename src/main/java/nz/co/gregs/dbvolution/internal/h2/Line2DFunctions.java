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
package nz.co.gregs.dbvolution.internal.h2;

import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public enum Line2DFunctions implements DBVFeature {

	/**
	 *
	 */
	CREATE("DBV_CREATE_LINE2D_FROM_COORDS", "String", "Double... coords", "\n"
			+ "			Integer numberOfArguments = coords.length;\n"
			+ "			if (numberOfArguments % 2 != 0) {\n"
			+ "				return null;\n"
			+ "			} else {\n"
			+ "				String resultStr = \"LINESTRING (\";\n"
			+ "				String sep = \"\";\n"
			+ "				for (int i = 0; i < numberOfArguments; i += 2) {\n"
			+ "					Double x = coords[i];\n"
			+ "					Double y = coords[i + 1];\n"
			+ "					if (x == null || y == null) {\n"
			+ "						return null;\n"
			+ "					} else {\n"
			+ "						resultStr += sep + x + \" \" + y;\n"
			+ "						sep = \", \";\n"
			+ "					}\n"
			+ "				}\n"
			+ "				resultStr += \")\";\n"
			+ "				return resultStr;\n"
			+ "			}"),
	/**
	 *
	 */
	EQUALS("DBV_LINE2D_EQUALS", "Boolean", "String firstLine, String secondLine", "\n"
			+ "			if (firstLine == null || secondLine == null) {\n"
			+ "				return null;\n"
			+ "			} else {\n"
			+ "				return firstLine.equals(secondLine);\n"
			+ "			}"),
	/**
	 *
	 */
	MAXX("DBV_LINE2D_MAXX", "Double", "String firstLine", "\n"
			+ "			if (firstLine == null) {\n"
			+ "				return null;\n"
			+ "			} else {\n"
			+ "				Double maxX = null;\n"
			+ "				String[] split = firstLine.split(\"[ (),]+\");\n"
			+ "				for (int i = 1; i < split.length; i += 2) {\n"
			+ "					double x = Double.parseDouble(split[i]);\n"
			+ "					if (maxX==null || maxX<x){\n"
			+ "						maxX = x;\n"
			+ "					}\n"
			+ "				}\n"
			+ "				return maxX;\n"
			+ "			}"),
	/**
	 *
	 */
	MAXY("DBV_LINE2D_MAXY", "Double", "String firstLine", "\n"
			+ "			if (firstLine == null) {\n"
			+ "				return null;\n"
			+ "			} else {\n"
			+ "				Double maxY = null;\n"
			+ "				String[] split = firstLine.split(\"[ (),]+\");\n"
			+ "				for (int i = 1; i < split.length; i += 2) {\n"
			+ "					double y = Double.parseDouble(split[i + 1]);\n"
			+ "					if (maxY==null || maxY<y){\n"
			+ "						maxY = y;\n"
			+ "					}\n"
			+ "				}\n"
			+ "				return maxY;\n"
			+ "			}"),
	/**
	 *
	 */
	MINX("DBV_LINE2D_MINX", "Double", "String firstLine", "\n"
			+ "			if (firstLine == null) {\n"
			+ "				return null;\n"
			+ "			} else {\n"
			+ "				Double maxX = null;\n"
			+ "				String[] split = firstLine.split(\"[ (),]+\");\n"
			+ "				for (int i = 1; i < split.length; i += 2) {\n"
			+ "					double x = Double.parseDouble(split[i]);\n"
			+ "					if (maxX==null || maxX>x){\n"
			+ "						maxX = x;\n"
			+ "					}\n"
			+ "				}\n"
			+ "				return maxX;\n"
			+ "			}"),
	/**
	 *
	 */
	MINY("DBV_LINE2D_MINY", "Double", "String firstLine", "\n"
			+ "			if (firstLine == null) {\n"
			+ "				return null;\n"
			+ "			} else {\n"
			+ "				Double maxY = null;\n"
			+ "				String[] split = firstLine.split(\"[ (),]+\");\n"
			+ "				for (int i = 1; i < split.length; i += 2) {\n"
			+ "					double y = Double.parseDouble(split[i + 1]);\n"
			+ "					if (maxY==null || maxY>y){\n"
			+ "						maxY = y;\n"
			+ "					}\n"
			+ "				}\n"
			+ "				return maxY;\n"
			+ "			}"),
	/**
	 *
	 */
	BOUNDINGBOX("DBV_LINE2D_BOUNDINGBOX", "String", "String firstLine", "\n"
			+ "			if (firstLine == null) {\n"
			+ "				return null;\n"
			+ "			} else {\n"
			+ "				Double maxX = null;\n"
			+ "				Double maxY = null;\n"
			+ "				Double minX = null;\n"
			+ "				Double minY = null;\n"
			+ "				String[] split = firstLine.split(\"[ (),]+\");\n"
			+ "				for (int i = 1; i < split.length; i += 2) {\n"
			+ "					double x = Double.parseDouble(split[i]);\n"
			+ "					double y = Double.parseDouble(split[i + 1]);\n"
			+ "					if (maxX==null || maxX<x){\n"
			+ "						maxX = x;\n"
			+ "					}\n"
			+ "					if (maxY==null || maxY<y){\n"
			+ "						maxY = y;\n"
			+ "					}\n"
			+ "					if (minX==null || minX>x){\n"
			+ "						minX = x;\n"
			+ "					}\n"
			+ "					if (minY==null || minY>y){\n"
			+ "						minY = y;\n"
			+ "					}\n"
			+ "				}\n"
			+ "				String resultString = \"POLYGON ((\" + minX+\" \"+minY + \", \" + maxX+\" \"+minY + \", \" + maxX+\" \"+maxY + \", \" + minX+\" \"+maxY + \", \" + minX+\" \"+minY + \"))\";\n"
			+ "				return resultString;\n"
			+ "			}"),
	/**
	 *
	 */
	DIMENSION("DBV_LINE2D_DIMENSION", "Integer", "String firstLine", "return 1;"),
	/**
	 *
	 */
	ASTEXT("DBV_LINE2D_ASTEXT", "String", "String firstLine", "return firstLine;"),
	/**
	 *
	 */
	INTERSECTS_LINE2D("DBV_LINE2D_INTERSECTS_LINE2D", "Boolean", "String firstLine, String secondLine", "\n"
			+ "		if (firstLine == null || secondLine == null) {\n"
			+ "			return null;\n"
			+ "		}\n"
			+ "		String[] split1 = firstLine.split(\"[ (),]+\");\n"
			+ "		String[] split2 = secondLine.split(\"[ (),]+\");\n"
			+ "		for (int index1 = 0; index1 < split1.length - 3; index1 += 2) {\n"
			+ "			double p0x = Double.parseDouble(split1[index1 + 1]);\n"
			+ "			double p0y = Double.parseDouble(split1[index1 + 2]);\n"
			+ "			double p1x = Double.parseDouble(split1[index1 + 3]);\n"
			+ "			double p1y = Double.parseDouble(split1[index1 + 4]);\n"
			+ "\n"
			+ "			for (int index2 = 0; index2 < split2.length - 3; index2 += 2) {\n"
			+ "				double p2x = Double.parseDouble(split2[index2 + 1]);\n"
			+ "				double p2y = Double.parseDouble(split2[index2 + 2]);\n"
			+ "				double p3x = Double.parseDouble(split2[index2 + 3]);\n"
			+ "				double p3y = Double.parseDouble(split2[index2 + 4]);\n"
			+ "\n"
			+ "				double s1_x, s1_y, s2_x, s2_y;\n"
			+ "				double i_x, i_y;\n"
			+ "				s1_x = p1x - p0x;\n"
			+ "				s1_y = p1y - p0y;\n"
			+ "				s2_x = p3x - p2x;\n"
			+ "				s2_y = p3y - p2y;\n"
			+ "\n"
			+ "				double s, t;\n"
			+ "\n"
			+ "				s = (-s1_y * (p0x - p2x) + s1_x * (p0y - p2y)) / (-s2_x * s1_y + s1_x * s2_y);\n"
			+ "				t = (s2_x * (p0y - p2y) - s2_y * (p0x - p2x)) / (-s2_x * s1_y + s1_x * s2_y);\n"
			+ "\n"
			+ "				if (s >= 0 && s <= 1 && t >= 0 && t <= 1) {\n"
			+ "					// Collision detected\n"
			+ "					return true;\n"
			+ "					//i_x = p0x + (t * s1_x);\n"
			+ "					//i_y = p0y + (t * s1_y);\n"
			+ "					//pointsFound.add(\"POINT (\" + i_x + \" \" + i_y + \")\");\n"
			+ "				} else {\n"
			+ "					// No collision\n"
			+ "					//return null;\n"
			+ "				}\n"
			+ "			}\n"
			+ "		}\n"
			+ "		return false;"),
	/**
	 *
	 */
	INTERSECTIONWITH_LINE2D("DBV_LINE2D_INTERSECTIONWITH_LINE2D", "String", "String firstLine, String secondLine", "\n"
			+ "		if (firstLine == null || secondLine == null) {\n"
			+ "			return null;\n"
			+ "		}\n"
			+ "		String[] split1 = firstLine.split(\"[ (),]+\");\n"
			+ "		String[] split2 = secondLine.split(\"[ (),]+\");\n"
			+ "		for (int index1 = 0; index1 < split1.length - 3; index1 += 2) {\n"
			+ "			double p0x = Double.parseDouble(split1[index1 + 1]);\n"
			+ "			double p0y = Double.parseDouble(split1[index1 + 2]);\n"
			+ "			double p1x = Double.parseDouble(split1[index1 + 3]);\n"
			+ "			double p1y = Double.parseDouble(split1[index1 + 4]);\n"
			+ "\n"
			+ "			for (int index2 = 0; index2 < split2.length - 3; index2 += 2) {\n"
			+ "				double p2x = Double.parseDouble(split2[index2 + 1]);\n"
			+ "				double p2y = Double.parseDouble(split2[index2 + 2]);\n"
			+ "				double p3x = Double.parseDouble(split2[index2 + 3]);\n"
			+ "				double p3y = Double.parseDouble(split2[index2 + 4]);\n"
			+ "\n"
			+ "				double s1_x, s1_y, s2_x, s2_y;\n"
			+ "				double i_x, i_y;\n"
			+ "				s1_x = p1x - p0x;\n"
			+ "				s1_y = p1y - p0y;\n"
			+ "				s2_x = p3x - p2x;\n"
			+ "				s2_y = p3y - p2y;\n"
			+ "\n"
			+ "				double s, t;\n"
			+ "\n"
			+ "				s = (-s1_y * (p0x - p2x) + s1_x * (p0y - p2y)) / (-s2_x * s1_y + s1_x * s2_y);\n"
			+ "				t = (s2_x * (p0y - p2y) - s2_y * (p0x - p2x)) / (-s2_x * s1_y + s1_x * s2_y);\n"
			+ "\n"
			+ "				if (s >= 0 && s <= 1 && t >= 0 && t <= 1) {\n"
			+ "					// Collision detected\n"
			+ "					//return true;\n"
			+ "					i_x = p0x + (t * s1_x);\n"
			+ "					i_y = p0y + (t * s1_y);\n"
			+ "					return \"POINT (\" + i_x + \" \" + i_y + \")\";\n"
			+ "				} else {\n"
			+ "					// No collision\n"
			+ "					//return null;\n"
			+ "				}\n"
			+ "			}\n"
			+ "		}\n"
			+ "		return null;"),
	/**
	 *
	 */
	ALLINTERSECTIONSWITH_LINE2D("DBV_LINE2D_ALLINTERSECTIONSWITH_LINE2D", "String", "String firstLine, String secondLine", "\n"
			+ "		if (firstLine == null || secondLine == null) {\n"
			+ "			return null;\n"
			+ "		}\n"
			+ "		String result = \"\";\n"
			+ "		String pointSeparator = \"\";\n"
			+ "		String[] split1 = firstLine.split(\"[ (),]+\");\n"
			+ "		String[] split2 = secondLine.split(\"[ (),]+\");\n"
			+ "		for (int index1 = 0; index1 < split1.length - 3; index1 += 2) {\n"
			+ "			double p0x = Double.parseDouble(split1[index1 + 1]);\n"
			+ "			double p0y = Double.parseDouble(split1[index1 + 2]);\n"
			+ "			double p1x = Double.parseDouble(split1[index1 + 3]);\n"
			+ "			double p1y = Double.parseDouble(split1[index1 + 4]);\n"
			+ "\n"
			+ "			for (int index2 = 0; index2 < split2.length - 3; index2 += 2) {\n"
			+ "				double p2x = Double.parseDouble(split2[index2 + 1]);\n"
			+ "				double p2y = Double.parseDouble(split2[index2 + 2]);\n"
			+ "				double p3x = Double.parseDouble(split2[index2 + 3]);\n"
			+ "				double p3y = Double.parseDouble(split2[index2 + 4]);\n"
			+ "\n"
			+ "				double s1_x, s1_y, s2_x, s2_y;\n"
			+ "				double i_x, i_y;\n"
			+ "				s1_x = p1x - p0x;\n"
			+ "				s1_y = p1y - p0y;\n"
			+ "				s2_x = p3x - p2x;\n"
			+ "				s2_y = p3y - p2y;\n"
			+ "\n"
			+ "				double s, t;\n"
			+ "\n"
			+ "				s = (-s1_y * (p0x - p2x) + s1_x * (p0y - p2y)) / (-s2_x * s1_y + s1_x * s2_y);\n"
			+ "				t = (s2_x * (p0y - p2y) - s2_y * (p0x - p2x)) / (-s2_x * s1_y + s1_x * s2_y);\n"
			+ "\n"
			+ "				if (s >= 0 && s <= 1 && t >= 0 && t <= 1) {\n"
			+ "					// Collision detected\n"
			+ "					//return true;\n"
			+ "					i_x = p0x + (t * s1_x);\n"
			+ "					i_y = p0y + (t * s1_y);\n"
			+ "					result += pointSeparator + i_x + \" \" + i_y;\n"
			+ "					pointSeparator = \", \";\n"
			+ "				}\n"
			+ "			}\n"
			+ "		}\n"
			+ "		return result.equals(\"\")?null:\"MULTIPOINT ((\"+result+\"))\";");

	private final String functionName;
	private final String returnType;
	private final String parameters;
	private final String code;

	Line2DFunctions(String functionName, String returnType, String parameters, String code) {
		this.functionName = functionName;
		this.returnType = returnType;
		this.parameters = parameters;
		this.code = code;
	}

	@Override
	public String toString() {
		return functionName;
	}

	/**
	 *
	 * @param stmt
	 * @throws SQLException
	 */
	@Override
	public void add(Statement stmt) throws SQLException {
		try {
			stmt.execute("DROP ALIAS " + functionName + ";");
		} catch (SQLException sqlex) {
			;
		}
		stmt.execute("CREATE ALIAS IF NOT EXISTS " + functionName + " DETERMINISTIC AS $$ \n" + "@CODE " + returnType + " " + functionName + "(" + parameters + ") {\n" + code + "} $$;");
	}

//	private Boolean intersects(String firstLine, String secondLine) {
//		//List<String> pointsFound = new ArrayList<String>();
//		if (firstLine == null || secondLine == null) {
//			return null;
//		}
//		String[] split1 = firstLine.split("[ (),]+");
//		String[] split2 = secondLine.split("[ (),]+");
//		for (int index1 = 0; index1 < split1.length - 3; index1 += 2) {
//			double p0x = Double.parseDouble(split1[index1 + 1]);
//			double p0y = Double.parseDouble(split1[index1 + 2]);
//			double p1x = Double.parseDouble(split1[index1 + 3]);
//			double p1y = Double.parseDouble(split1[index1 + 4]);
//
//			for (int index2 = 0; index2 < split2.length - 3; index2 += 2) {
//				double p2x = Double.parseDouble(split2[index2 + 1]);
//				double p2y = Double.parseDouble(split2[index2 + 2]);
//				double p3x = Double.parseDouble(split2[index2 + 3]);
//				double p3y = Double.parseDouble(split2[index2 + 4]);
//
//				double s1_x, s1_y, s2_x, s2_y;
//				double i_x, i_y;
//				s1_x = p1x - p0x;
//				s1_y = p1y - p0y;
//				s2_x = p3x - p2x;
//				s2_y = p3y - p2y;
//
//				double s, t;
//
//				s = (-s1_y * (p0x - p2x) + s1_x * (p0y - p2y)) / (-s2_x * s1_y + s1_x * s2_y);
//				t = (s2_x * (p0y - p2y) - s2_y * (p0x - p2x)) / (-s2_x * s1_y + s1_x * s2_y);
//
//				if (s >= 0 && s <= 1 && t >= 0 && t <= 1) {
//					// Collision detected
//					return true;
//					//i_x = p0x + (t * s1_x);
//					//i_y = p0y + (t * s1_y);
//					//pointsFound.add("POINT (" + i_x + " " + i_y + ")");
//				} else {
//					// No collision
//					//return null;
//				}
//			}
//		}
//		return false;
//	}
	@Override
	public String alias() {
		return toString();
	}

}
