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
public enum Line2DFunctions {

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
	EQUALS("DBV_LINE2D_EQUALS", "Boolean", "String firstLine, String secondLine", "\n"
			+ "			if (firstLine == null || secondLine == null) {\n"
			+ "				return null;\n"
			+ "			} else {\n"
			+ "				return firstLine.equals(secondLine);\n"
			+ "			}"),
	MAXX("DBV_LINE2D_MAXX","Double", "String firstLine", "\n" +
"			if (firstLine == null) {\n" +
"				return null;\n" +
"			} else {\n" +
"				Double maxX = null;\n" +
"				String[] split = firstLine.split(\"[ (),]+\");\n" +
"				for (int i = 1; i < split.length; i += 2) {\n" +
"					double x = Double.parseDouble(split[i]);\n" +
"					if (maxX==null || maxX<x){\n" +
"						maxX = x;\n" +
"					}\n" +
"				}\n" +
"				return maxX;\n" +
"			}"),
	MAXY("DBV_LINE2D_MAXY","Double", "String firstLine","\n" +
"			if (firstLine == null) {\n" +
"				return null;\n" +
"			} else {\n" +
"				Double maxY = null;\n" +
"				String[] split = firstLine.split(\"[ (),]+\");\n" +
"				for (int i = 1; i < split.length; i += 2) {\n" +
"					double y = Double.parseDouble(split[i + 1]);\n" +
"					if (maxY==null || maxY<y){\n" +
"						maxY = y;\n" +
"					}\n" +
"				}\n" +
"				return maxY;\n" +
"			}"),
	MINX("DBV_LINE2D_MINX","Double", "String firstLine", "\n" +
"			if (firstLine == null) {\n" +
"				return null;\n" +
"			} else {\n" +
"				Double maxX = null;\n" +
"				String[] split = firstLine.split(\"[ (),]+\");\n" +
"				for (int i = 1; i < split.length; i += 2) {\n" +
"					double x = Double.parseDouble(split[i]);\n" +
"					if (maxX==null || maxX>x){\n" +
"						maxX = x;\n" +
"					}\n" +
"				}\n" +
"				return maxX;\n" +
"			}"),
	MINY("DBV_LINE2D_MINY","Double", "String firstLine","\n" +
"			if (firstLine == null) {\n" +
"				return null;\n" +
"			} else {\n" +
"				Double maxY = null;\n" +
"				String[] split = firstLine.split(\"[ (),]+\");\n" +
"				for (int i = 1; i < split.length; i += 2) {\n" +
"					double y = Double.parseDouble(split[i + 1]);\n" +
"					if (maxY==null || maxY>y){\n" +
"						maxY = y;\n" +
"					}\n" +
"				}\n" +
"				return maxY;\n" +
"			}"),
	BOUNDINGBOX("DBV_LINE2D_BOUNDINGBOX", "String", "String firstLine","\n" +
"			if (firstLine == null) {\n" +
"				return null;\n" +
"			} else {\n" +
"				Double maxX = null;\n" +
"				Double maxY = null;\n" +
"				Double minX = null;\n" +
"				Double minY = null;\n" +
"				String[] split = firstLine.split(\"[ (),]+\");\n" +
"				for (int i = 1; i < split.length; i += 2) {\n" +
"					double x = Double.parseDouble(split[i]);\n" +
"					double y = Double.parseDouble(split[i + 1]);\n" +
"					if (maxX==null || maxX<x){\n" +
"						maxX = x;\n" +
"					}\n" +
"					if (maxY==null || maxY<y){\n" +
"						maxY = y;\n" +
"					}\n" +
"					if (minX==null || minX>x){\n" +
"						minX = x;\n" +
"					}\n" +
"					if (minY==null || minY>y){\n" +
"						minY = y;\n" +
"					}\n" +
"				}\n" +
"				String resultString = \"POLYGON ((\" + minX+\" \"+minY + \", \" + maxX+\" \"+minY + \", \" + maxX+\" \"+maxY + \", \" + minX+\" \"+maxY + \", \" + minX+\" \"+minY + \"))\";\n" +
"				return resultString;\n" +
"			}"),
	DIMENSION("DBV_LINE2D_DIMENSION","Integer","String firstLine","return 1;"),
	ASTEXT("DBV_LINE2D_ASTEXT","String","String firstLine","return firstLine;")
	;
	
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

	public void add(Statement stmt) throws SQLException {
//		try {
//			stmt.execute("DROP ALIAS " + functionName + ";");
//		} catch (SQLException sqlex) {
//			;
//		}
		stmt.execute("CREATE ALIAS IF NOT EXISTS " + functionName + " DETERMINISTIC AS $$ \n" + "@CODE " + returnType + " " + functionName + "(" + parameters + ") {\n" + code + "} $$;");
	}

}
