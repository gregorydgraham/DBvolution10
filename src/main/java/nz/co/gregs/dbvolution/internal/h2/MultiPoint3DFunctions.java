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
 * a MultiPoint in H2 is a String formatted as MULTIPOINT ((1 2 3, 4 5 6, 7 8
 * 9)) where each triple of numbers is a point and the entire string is less
 * than 2000 characters
 *
 * @author gregorygraham
 */
public enum MultiPoint3DFunctions implements DBVFeature {

// MULTIPOINT ((1 2 3, 4 5 6, 7 8 9))
	/**
	 *
	 */
	CREATE("String", "Double... coords", "\n"
			+ "			Integer numberOfArguments = coords.length;\n"
			+ "			if (numberOfArguments % 3 != 0) {\n"
			+ "				return null;\n"
			+ "			} else {\n"
			+ "				String resultStr = \"MULTIPOINT (\";\n"
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
	EQUALS("Boolean", "String firstMP, String secondMP",
			"		if (firstMP == null || secondMP == null) {\n"
			+ "			return false;\n"
			+ "		}\n"
			+ "		String[] split1 = firstMP.split(\"[ (),]+\");\n"
			+ "		String[] split2 = secondMP.split(\"[ (),]+\");\n"
			+ "		if (split1.length != split2.length) {\n"
			+ "			return false;\n"
			+ "		} else {\n"
			+ "			for (int i = 1; i < split1.length; i++) {\n"
			+ "				double value1 = Double.parseDouble(split1[i]);\n"
			+ "				double value2 = Double.parseDouble(split2[i]);\n"
			+ "				if (value1 != value2) {\n"
			+ "					return false;\n"
			+ "				}\n"
			+ "			}\n"
			+ "		}\n"
			+ "		return true;\n"
	),
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
			+ "				Double max = null;\n"
			+ "				String[] split = firstLine.split(\"[ (),]+\");\n"
			+ "				for (int i = 1; i < split.length; i += 3) {\n"
			+ "					double z = Double.parseDouble(split[i + 2]);\n"
			+ "					if (max==null || max<z){\n"
			+ "						max = z;\n"
			+ "					}\n"
			+ "				}\n"
			+ "				return max;\n"
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
	DIMENSION("Integer", "String firstLine", "return 0;"),
	/**
	 *
	 */
	ASTEXT("String", "String firstLine", "return firstLine;"),
	/**
	 *
	 */
	ASLINE3D("String", "String multipoint", "return multipoint.replace(\"(\",\"\").replace(\")\",\"\").replace(\"MULTIPOINT \", \"LINESTRING (\")+\")\";"),
	/**
	 *
	 */
	GETNUMBEROFPOINTS_FUNCTION("Integer", "String multipoint", "\n"
			+ "			if (multipoint == null||multipoint.equals(\"\")) {\n"
			+ "				return null;\n"
			+ "			} else {\n"
			+ "				String[] split = multipoint.trim().split(\"[ (),]+\");\n"
			+ "				return (split.length - 1)/3;\n"
			+ "			}"),
	/**
	 *
	 */
	GETPOINTATINDEX_FUNCTION("String", "String multipoint, Integer index", "\n"
			+ "			final int indexInMPoint = (index-1) * 3+1;\n"
			+ "			if (multipoint == null||indexInMPoint<=0) {\n"
			+ "				return null;\n"
			+ "			} else {\n"
			+ "				String[] split = multipoint.split(\"[ (),]+\");\n"
			+ "				if (indexInMPoint >= (split.length)) {\n"
			+ "					return null;\n"
			+ "				} else {\n"
			+ "					String x = split[indexInMPoint];\n"
			+ "					String y = split[indexInMPoint+1];\n"
			+ "					String z = split[indexInMPoint+2];\n"
			+ "					return \"POINT (\" + x + \" \" + y  + \" \" + z + \")\";\n"
			+ "				}\n"
			+ "			}");

//	private final String functionName;
	private final String returnType;
	private final String parameters;
	private final String code;
//	public static int CURRENTVERSION=2;

	MultiPoint3DFunctions(String returnType, String parameters, String code) {
//		this.functionName = functionName;
		this.returnType = returnType;
		this.parameters = parameters;
		this.code = code;
	}

	/**
	 *
	 * @return the integer version number of the function to be used
	 */
	static public int getCurrentVersion() {
		return 1;
	}

	@Override
	public String toString() {
		return "DBV_MULTIPOINT3D_" + name();
	}

	@Override
	public String alias() {
		return toString();
	}

	/**
	 *
	 * @param stmt
	 * @throws SQLException
	 */
	@Override
	public void add(Statement stmt) throws SQLException {
		try {
			final String dropStatement = "DROP ALIAS " + this + ";";
			stmt.execute(dropStatement);
		} catch (SQLException sqlex) {
			;// Not an issue.
		}
		final String createFunctionStatement = "CREATE ALIAS IF NOT EXISTS " + this + " DETERMINISTIC AS $$ \n" + "@CODE " + returnType + " " + this + "(" + parameters + ", Integer version) throws org.h2.jdbc.JdbcSQLException {\n"
				+ "if (version!=" + getCurrentVersion() + "){\n"
				+ "	throw new org.h2.jdbc.JdbcSQLException(\"Function " + this + " not found\", \"Function " + this + " not found\", \"Function " + this + " not found\", version, null, \"Function " + this + " not found\"); \n"
				+ "}else{\n"
				+ code
				+ "}\n"
				+ "} $$;";
		stmt.execute(createFunctionStatement);
	}

	private Boolean testEquals(String firstMP, String secondMP) {
		if (firstMP == null || secondMP == null) {
			return null;
		}
		String[] split1 = firstMP.split("[ (),]+");
		String[] split2 = secondMP.split("[ (),]+");
		if (split1.length != split2.length) {
			return false;
		} else {
			for (int i = 1; i < split1.length; i++) {
				double value1 = Double.parseDouble(split1[i]);
				double value2 = Double.parseDouble(split2[i]);
				if (value1 != value2) {
					return false;
				}
			}
		}
		return true;
	}
}
