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
package nz.co.gregs.dbvolution.internal.oracle;

import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * @author gregory.graham
 */
public enum Line2DFunctions {
	
	MAXX("NUMBER", "@poly VARCHAR", "DECLARE \n" +
" result NUMBER;\n" +
" numPoints NUMBER;\n" +
" currentcoord NUMBER;\n" +
" pnt VARCHAR;\n" +
" textPoly VARCHAR;\n" +
" textCoord VARCHAR;\n" +
"BEGIN\n" +
" IF poly is null THEN return null; \n" +
" ELSE\n" +
"  textPoly := poly;\n" +
"  textPoly := "+StringFunctions.SUBSTRINGAFTER+"(textPoly, '[');\n" +
"  textPoly :=  "+StringFunctions.SUBSTRINGBEFORE+"(textPoly, ']');\n" +
"  numPoints := length(regexp_replace(textPoly, '[^(]', ''));\n" +
"  result:=null;\n" +
"  FOR i IN 1 .. numPoints LOOP\n" +
"   textPoly :=  "+StringFunctions.SUBSTRINGAFTER+"(textPoly, '(');\n" +
"   textCoord :=  "+StringFunctions.SUBSTRINGAFTER+"( "+StringFunctions.SUBSTRINGBEFORE+"(textPoly, ')'), ',');\n" +
"   IF char_length(textCoord) > 0 THEN \n" +
"    currentcoord := 0.0+textCoord;\n" +
"    IF result is null or result < currentcoord THEN\n" +
"     result := currentcoord;\n" +
"    END IF;\n" +
"   END IF;\n" +
"   textPoly :=  "+StringFunctions.SUBSTRINGAFTER+"(textPoly, ')');\n" +
"   textPoly :=  "+StringFunctions.SUBSTRINGAFTER+"(textPoly, ',');\n" +
"  END LOOP;\n" +
"  return result;\n" +
" END IF;\n" +
"END;"),
	MAXY("numeric(15,10)", "@poly VARCHAR", ""),
	MINX("numeric(15,10)", "@poly VARCHAR", ""),
	MINY("number", "poly IN VARCHAR(2001)", ""
			+ "DECLARE \n"
			+ " resultVal number;\n"
			+ " num number;\n"
			+ " i number;\n"
			+ " currentcoord number;\n"
			+ " pnt VARCHAR(2000);\n"
			+ "BEGIN\n"
			+ " IF poly is null \n"
			+ " THEN \n"
			+ "  return(null); \n"
			+ " ELSE\n"
			+ "  num := poly.STNumPoints();\n"
			+ "  resultVal := null;\n"
			+ "  i := 1;\n"
			+ "  WHILE i <= num \n"
			+ "  begin\n"
			+ "   pnt := poly.STPointN(i);\n"
			+ "   IF pnt is not null\n"
			+ "   THEN \n"
			+ "    currentcoord := pnt.STY;\n"
			+ "    IF resultVal is null OR resultVal > currentcoord \n"
			+ "    THEN\n"
			+ "     resultVal := currentcoord;\n"
			+ "    END IF;\n"
			+ "   END IF;\n"
			+ "   i := i + 1;\n"
			+ "  END WHILE\n"
			+ " END IF\n"
			+ "END"
			+ " return(resultVal);"),
	BOUNDINGBOX("VARCHAR(2002)", "poly IN VARCHAR(2001)", "");

	private final String returnType;
	private final String parameters;
	private final String code;

	Line2DFunctions(String returnType, String parameters, String code) {
		this.returnType = returnType;
		this.parameters = parameters;
		this.code = code;
	}

	@Override
	public String toString() {
		return "DBV_LINE2DFN_" + name();
	}

	public void add(Statement stmt) throws SQLException {
		try {
			stmt.execute("DROP FUNCTION " + this + ";");
		} catch (SQLException sqlex) {
			;
		}
		if (!this.code.isEmpty()) {
			final String createFn = "CREATE OR REPLACE FUNCTION " + this + "(" + this.parameters + ")\n"
					+ "    RETURN " + this.returnType
					+ " AS \n" + "\n" + this.code
					+ "\n/";
			System.out.println("" + createFn);
			stmt.execute(createFn);
		}
	}
}
