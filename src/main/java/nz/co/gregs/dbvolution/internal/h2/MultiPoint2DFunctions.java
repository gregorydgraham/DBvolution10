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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * a MultiPoint in H2 is a String formatted as MULTIPOINT ((1 2, 3 4, 5 6))
 * where each pair of numbers is a point and the entire string is less than 2000
 * characters
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public enum MultiPoint2DFunctions implements DBVFeature {

	/**
	 *
	 */
	CREATE("String", "Double... coords", "\n"
			+ "			Integer numberOfArguments = coords.length;\n"
			+ "			if (numberOfArguments % 2 != 0) {\n"
			+ "				return null;\n"
			+ "			} else {\n"
			+ "				String resultStr = \"MULTIPOINT (\";\n"
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
	MAXY("Double", "String firstLine", "\n"
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
	MINX("Double", "String firstLine", "\n"
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
	MINY("Double", "String firstLine", "\n"
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
	BOUNDINGBOX("String", "String firstLine", "\n"
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
	DIMENSION("Integer", "String firstLine", "return 0;"),
	/**
	 *
	 */
	ASTEXT("String", "String firstLine", "return firstLine;"),
	/**
	 *
	 */
	ASLINE2D("String", "String multipoint", "return multipoint.replace(\"(\",\"\").replace(\")\",\"\").replace(\"MULTIPOINT \", \"LINESTRING (\")+\")\";"),
	/**
	 *
	 */
	GETNUMBEROFPOINTS_FUNCTION("Integer", "String multipoint", "\n"
			+ "			if (multipoint == null||multipoint.equals(\"\")) {\n"
			+ "				return null;\n"
			+ "			} else {\n"
			+ "				Double maxX = null;\n"
			+ "				String[] split = multipoint.trim().split(\"[ (),]+\");\n"
			+ "				return (split.length - 1)/2;\n"
			+ "			}"),

	/**
	 *
	 */
	GETPOINTATINDEX_FUNCTION("String", "String multipoint, Integer index", "\n"
			+ "			final int indexInMPoint = index * 2;\n"
			+ "			if (multipoint == null||indexInMPoint<=0) {\n"
			+ "				return null;\n"
			+ "			} else {\n"
			+ "				String[] split = multipoint.split(\"[ (),]+\");\n"
			+ "				if (indexInMPoint > split.length) {\n"
			+ "					return null;\n"
			+ "				} else {\n"
			+ "					String x = split[indexInMPoint - 1];\n"
			+ "					String y = split[indexInMPoint];\n"
			+ "					return \"POINT (\" + x + \" \" + y + \")\";\n"
			+ "				}\n"
			+ "			}");

	private final String returnType;
	private final String parameters;
	private final String code;

	MultiPoint2DFunctions(String returnType, String parameters, String code) {
		this.returnType = returnType;
		this.parameters = parameters;
		this.code = code;
	}

	/**
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the integer version number of the function to be used
	 */
	static public int getCurrentVersion() {
		return 2;
	}

	@Override
	public String toString() {
		return "DBV_MULTIPOINT2D_" + name();
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
	@SuppressFBWarnings(value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE",
			justification = "The strings are actually constant but made dynamically")
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
}
