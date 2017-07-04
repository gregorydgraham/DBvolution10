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
 * @author gregorygraham
 */
public enum LineSegment3DFunctions implements DBVFeature {

	/**
	 *
	 */
	CREATE("String", "Double... coords", "\n"
			+ "			Integer numberOfArguments = coords.length;\n"
			+ "			if (numberOfArguments % 3 != 0) {\n"
			+ "				return null;\n"
			+ "			} else {\n"
			+ "				String resultStr = \"LINESTRING (\";\n"
			+ "				String sep = \"\";\n"
			+ "				for (int i = 0; i < numberOfArguments; i += 3) {\n"
			+ "					Double x = coords[i];\n"
			+ "					Double y = coords[i + 1];\n"
			+ "					Double z = coords[i + 2];\n"
			+ "					if (x == null || y == null || z == null) {\n"
			+ "						return null;\n"
			+ "					} else {\n"
			+ "						resultStr += sep + x + \" \" + y + \" \" + z;\n"
			+ "						sep = \", \";\n"
			+ "					}\n"
			+ "				}\n"
			+ "				resultStr += \")\";\n"
			+ "				return resultStr;\n"
			+ "			}"),

	/**
	 *
	 */
	EQUALS("Boolean", "String firstLine, String secondLine", "\n"
			+ "			if (firstLine == null || secondLine == null) {\n"
			+ "				return null;\n"
			+ "			} else {\n"
			+ "				return firstLine.equals(secondLine);\n"
			+ "			}"),

	/**
	 *
	 */
	MAXX("Double", "String firstLine", "\n"
			+ "			if (firstLine == null) {\n"
			+ "				return null;\n"
			+ "			} else {\n"
			+ "				Double maxX = null;\n"
			+ "				String[] split = firstLine.split(\"[ (),]+\");\n"
			+ "				for (int i = 1; i < split.length; i += 3) {\n"
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
	MAXY("Double", "String firstLine", "\n"
			+ "			if (firstLine == null) {\n"
			+ "				return null;\n"
			+ "			} else {\n"
			+ "				Double maxY = null;\n"
			+ "				String[] split = firstLine.split(\"[ (),]+\");\n"
			+ "				for (int i = 1; i < split.length; i += 3) {\n"
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
	MAXZ("Double", "String firstLine", "\n"
			+ "			if (firstLine == null) {\n"
			+ "				return null;\n"
			+ "			} else {\n"
			+ "				Double maxZ = null;\n"
			+ "				String[] split = firstLine.split(\"[ (),]+\");\n"
			+ "				for (int i = 1; i < split.length; i += 3) {\n"
			+ "					double z = Double.parseDouble(split[i + 2]);\n"
			+ "					if (maxZ==null || maxZ<z){\n"
			+ "						maxZ = z;\n"
			+ "					}\n"
			+ "				}\n"
			+ "				return maxZ;\n"
			+ "			}"),

	/**
	 *
	 */
	MINX("Double", "String firstLine", "\n"
			+ "			if (firstLine == null) {\n"
			+ "				return null;\n"
			+ "			} else {\n"
			+ "				Double maxX = null;\n"
			+ "				String[] split = firstLine.split(\"[ (),]+\");\n"
			+ "				for (int i = 1; i < split.length; i += 3) {\n"
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
	MINY("Double", "String firstLine", "\n"
			+ "			if (firstLine == null) {\n"
			+ "				return null;\n"
			+ "			} else {\n"
			+ "				Double maxY = null;\n"
			+ "				String[] split = firstLine.split(\"[ (),]+\");\n"
			+ "				for (int i = 1; i < split.length; i += 3) {\n"
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
	MINZ("Double", "String firstLine", "\n"
			+ "			if (firstLine == null) {\n"
			+ "				return null;\n"
			+ "			} else {\n"
			+ "				Double max = null;\n"
			+ "				String[] split = firstLine.split(\"[ (),]+\");\n"
			+ "				for (int i = 1; i < split.length; i += 3) {\n"
			+ "					double z = Double.parseDouble(split[i + 2]);\n"
			+ "					if (max==null || max>z){\n"
			+ "						max = z;\n"
			+ "					}\n"
			+ "				}\n"
			+ "				return max;\n"
			+ "			}"),

	/**
	 *
	 */
	BOUNDINGBOX("String", "String firstLine", "\n"
			+ "			if (firstLine == null) {\n"
			+ "				return null;\n"
			+ "			} else {\n"
			+ "				Double maxX = null;\n"
			+ "				Double maxY = null;\n"
			+ "				Double maxZ = null;\n"
			+ "				Double minX = null;\n"
			+ "				Double minY = null;\n"
			+ "				Double minZ = null;\n"
			+ "				String[] split = firstLine.split(\"[ (),]+\");\n"
			+ "				for (int i = 1; i < split.length; i += 3) {\n"
			+ "					double x = Double.parseDouble(split[i]);\n"
			+ "					double y = Double.parseDouble(split[i + 1]);\n"
			+ "					double z = Double.parseDouble(split[i + 2]);\n"
			+ "					if (maxX==null || maxX<x){\n"
			+ "						maxX = x;\n"
			+ "					}\n"
			+ "					if (maxY==null || maxY<y){\n"
			+ "						maxY = y;\n"
			+ "					}\n"
			+ "					if (maxZ==null || maxZ<z){\n"
			+ "						maxZ = z;\n"
			+ "					}\n"
			+ "					if (minX==null || minX>x){\n"
			+ "						minX = x;\n"
			+ "					}\n"
			+ "					if (minY==null || minY>y){\n"
			+ "						minY = y;\n"
			+ "					}\n"
			+ "					if (minZ==null || minZ>z){\n"
			+ "						minZ = z;\n"
			+ "					}\n"
			+ "				}\n"
			+ "				String resultString = \"POLYGON ((\" + minX+\" \"+minY+\" \"+minZ + \", \" + maxX+\" \"+minY+\" \"+minZ + \", \" + maxX+\" \"+maxY+\" \"+minZ + \", \" + maxX+\" \"+maxY+\" \"+maxZ + \", \" + minX+\" \"+maxY+\" \"+maxZ + \", \" + minX+\" \"+minY+\" \"+maxZ + \", \" + minX+\" \"+minY+\" \"+minZ + \"))\";\n"
			+ "				return resultString;\n"
			+ "			}"),

	/**
	 *
	 */
	DIMENSION("Integer", "String firstLine", "return 1;"),

	/**
	 *
	 */
	ASTEXT("String", "String firstLine", "return firstLine;"),

	/**
	 *
	 */
	INTERSECTS_LINESEGMENT3D("Boolean", "String firstLine, String secondLine", ""
			+ "		if (firstLine == null || secondLine == null) {\n"
			+ "			return null;\n"
			+ "		}\n"
			+ "		String[] split = firstLine.split(\"[ (),]+\");\n"
			+ "		double p0x = Double.parseDouble(split[1]);\n"
			+ "		double p0y = Double.parseDouble(split[2]);\n"
			+ "		double p0z = Double.parseDouble(split[3]);\n"
			+ "		double p1x = Double.parseDouble(split[4]);\n"
			+ "		double p1y = Double.parseDouble(split[5]);\n"
			+ "		double p1z = Double.parseDouble(split[6]);\n"
			+ "		\n"
			+ "		split = secondLine.split(\"[ (),]+\");\n"
			+ "		double p2x = Double.parseDouble(split[1]);\n"
			+ "		double p2y = Double.parseDouble(split[2]);\n"
			+ "		double p2z = Double.parseDouble(split[3]);\n"
			+ "		double p3x = Double.parseDouble(split[4]);\n"
			+ "		double p3y = Double.parseDouble(split[5]);\n"
			+ "		double p3z = Double.parseDouble(split[6]);\n"
			+ "\n"
			+ "		double s1_x, s1_y, s2_x, s2_y;\n"
			+ "		double i_x, i_y;\n"
			+ "		s1_x = p1x - p0x;\n"
			+ "		s1_y = p1y - p0y;\n"
			+ "		s2_x = p3x - p2x;\n"
			+ "		s2_y = p3y - p2y;\n"
			+ "\n"
			+ "		double s, t;\n"
			+ "\n"
			+ "		s = (-s1_y * (p0x - p2x) + s1_x * (p0y - p2y)) / (-s2_x * s1_y + s1_x * s2_y);\n"
			+ "		t = (s2_x * (p0y - p2y) - s2_y * (p0x - p2x)) / (-s2_x * s1_y + s1_x * s2_y);\n"
			+ "\n"
			+ "		if (s >= 0 && s <= 1 && t >= 0 && t <= 1) {\n"
			+ "			double t_z = p0z + (t * s1_z);\n"
			+ "			double s_z = p2z + (s * s2_z);\n"
			+ "			if (t_z == s_z) {\n"
			+ "				// t and s create the same z so there is an inersection\n"
			+ "				// Collision detected\n"
			+ "				//i_x = p0x + (t * s1_x);\n"
			+ "				//i_y = p0y + (t * s1_y);\n"
			+ "				return true;\n"
			+ "			} else {\n"
			+ "				return false;\n"
			+ "			}\n"
			+ "		} else {\n"
			+ "			// No collision\n"
			+ "			return false;\n"
			+ "		} "),

	/**
	 *
	 */
	INTERSECTIONPOINT_LINESEGMENT3D("String", "String firstLine, String secondLine", ""
			+ "		if (firstLine == null || secondLine == null) {\n"
			+ "			return null;\n"
			+ "		}\n"
			+ "		String[] split = firstLine.split(\"[ (),]+\");\n"
			+ "		double p0x = Double.parseDouble(split[1]);\n"
			+ "		double p0y = Double.parseDouble(split[2]);\n"
			+ "		double p0z = Double.parseDouble(split[3]);\n"
			+ "		double p1x = Double.parseDouble(split[4]);\n"
			+ "		double p1y = Double.parseDouble(split[5]);\n"
			+ "		double p1z = Double.parseDouble(split[6]);\n"
			+ "\n"
			+ "		split = secondLine.split(\"[ (),]+\");\n"
			+ "		double p2x = Double.parseDouble(split[1]);\n"
			+ "		double p2y = Double.parseDouble(split[2]);\n"
			+ "		double p2z = Double.parseDouble(split[3]);\n"
			+ "		double p3x = Double.parseDouble(split[4]);\n"
			+ "		double p3y = Double.parseDouble(split[5]);\n"
			+ "		double p3z = Double.parseDouble(split[6]);\n"
			+ "\n"
			+ "		double s1_x, s1_y, s1_z, s2_x, s2_y, s2_z;\n"
			+ "		double i_x, i_y;\n"
			+ "		s1_x = p1x - p0x;\n"
			+ "		s1_y = p1y - p0y;\n"
			+ "		s1_z = p1z - p0z;\n"
			+ "		s2_x = p3x - p2x;\n"
			+ "		s2_y = p3y - p2y;\n"
			+ "		s2_z = p3z - p2z;\n"
			+ "\n"
			+ "		double s, t;\n"
			+ "\n"
			+ "		s = (-s1_y * (p0x - p2x) + s1_x * (p0y - p2y)) / (-s2_x * s1_y + s1_x * s2_y);\n"
			+ "		t = (s2_x * (p0y - p2y) - s2_y * (p0x - p2x)) / (-s2_x * s1_y + s1_x * s2_y);\n"
			+ "\n"
			+ "		if (s >= 0 && s <= 1 && t >= 0 && t <= 1) {\n"
			+ "			double t_z = p0z + (t * s1_z);\n"
			+ "			double s_z = p2z + (s * s2_z);\n"
			+ "			if (t_z == s_z) {\n"
			+ "				// t and s create the same z so there is an inersection\n"
			+ "				// Collision detected\n"
			+ "				i_x = p0x + (t * s1_x);\n"
			+ "				i_y = p0y + (t * s1_y);\n"
			+ "				return \"POINT (\"+i_x+\" \"+i_y+\" \"+t_z+\")\";\n"
			+ "			} else {\n"
			+ "				// No collision\n"
			+ "				return null;\n"
			+ "			}\n"
			+ "		} else {\n"
			+ "			// No collision\n"
			+ "			return null;\n"
			+ "		}");

//	private final String functionName;
	private final String returnType;
	private final String parameters;
	private final String code;

	LineSegment3DFunctions(String returnType, String parameters, String code) {
//		this.functionName = functionName;
		this.returnType = returnType;
		this.parameters = parameters;
		this.code = code;
	}

	@Override
	public String toString() {
		return "DBV_LINESEGMENT3D_" + name();
	}

	/**
	 *
	 * @param stmt
	 * @throws SQLException
	 */
	@Override
	public void add(Statement stmt) throws SQLException {
		try {
			stmt.execute("DROP ALIAS " + this + ";");
		} catch (SQLException sqlex) {
			;
		}
		final String createFunctionStatement = "CREATE ALIAS IF NOT EXISTS " + this + " DETERMINISTIC AS $$ \n" + "@CODE " + returnType + " " + this + "(" + parameters + ") {\n" + code + "} $$;";
		stmt.execute(createFunctionStatement);
	}

	private String intersection(String firstLine, String secondLine) {
		if (firstLine == null || secondLine == null) {
			return null;
		}
		String[] split = firstLine.split("[ (),]+");
		double p0x = Double.parseDouble(split[1]);
		double p0y = Double.parseDouble(split[2]);
		double p1x = Double.parseDouble(split[3]);
		double p1y = Double.parseDouble(split[4]);

		split = secondLine.split("[ (),]+");
		double p2x = Double.parseDouble(split[1]);
		double p2y = Double.parseDouble(split[2]);
		double p3x = Double.parseDouble(split[3]);
		double p3y = Double.parseDouble(split[4]);

		double s1_x, s1_y, s2_x, s2_y;
		double i_x, i_y;
		s1_x = p1x - p0x;
		s1_y = p1y - p0y;
		s2_x = p3x - p2x;
		s2_y = p3y - p2y;

		double s, t;

		s = (-s1_y * (p0x - p2x) + s1_x * (p0y - p2y)) / (-s2_x * s1_y + s1_x * s2_y);
		t = (s2_x * (p0y - p2y) - s2_y * (p0x - p2x)) / (-s2_x * s1_y + s1_x * s2_y);

		if (s >= 0 && s <= 1 && t >= 0 && t <= 1) {
			// Collision detected
			i_x = p0x + (t * s1_x);
			i_y = p0y + (t * s1_y);
			return "POINT (" + i_x + " " + i_y + ")";
		} else {
			// No collision
			return null;
		}
	}

	@Override
	public String alias() {
		return toString();
	}
}
