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
package nz.co.gregs.dbvolution.internal.oracle.aws;

import nz.co.gregs.dbvolution.internal.oracle.StringFunctions;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregory.graham
 */
public enum MultiPoint2DFunctions {

	/**
	 *
	 */
	MAXX("number", "aLine VARCHAR", ""
			+ "   result       NUMBER;\n"
			+ "   coord        NUMBER;\n"
			+ "   pnt          VARCHAR (4000);\n"
			+ "   polyAsText   VARCHAR (4000);\n"
			+ "   textCoord    VARCHAR (4000);BEGIN\n"
			+ "   result := NULL;\n"
			+ "\n"
			+ "   IF aLine IS NOT NULL\n"
			+ "   THEN\n"
			+ "      polyAsText := aLine;\n"
			+ "      --DBMS_OUTPUT.PUT_LINE ('START: ' || polyAsText);\n"
			+ "      --'MULTIPOINT ((2 3), (3 4), (4 5))'\n"
			+ "      polyAsText :=  " + StringFunctions.SUBSTRINGAFTER + " (polyAsText, '(');\n"
			+ "\n"
			+ "      --'(2 3), (3 4), (4 5))'\n"
			+ "      --'(3 4), (4 5))'\n"
			+ "      --'(4 5))'\n"
			+ "      --NULL\n"
			+ "      WHILE polyAsText IS NOT NULL\n"
			+ "      LOOP\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('POLYASTEXT: ' || polyAsText);\n"
			+ "\n"
			+ "         textCoord :=  " + StringFunctions.SUBSTRINGAFTER + " (polyAsText, '(');\n"
			+ "         textCoord :=  " + StringFunctions.SUBSTRINGBEFORE + " (textCoord, ')');\n"
			+ "\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('TEXTCOORD: ' || textCoord);\n"
			+ "\n"
			+ "         IF textCoord IS NULL\n"
			+ "         THEN\n"
			+ "            textCoord := polyAsText;\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('TEXTCOORD: ' || textCoord);\n"
			+ "         --4 5\n"
			+ "         END IF;\n"
			+ "\n"
			+ "         --'2 3'\n"
			+ "         textCoord :=  " + StringFunctions.SUBSTRINGBEFORE + " (textCoord, ' ');\n"
			+ "\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('TONUMBER: ' || textCoord);\n"
			+ "\n"
			+ "         --'3'\n"
			+ "\n"
			+ "         IF LENGTH (textCoord) > 0\n"
			+ "         THEN\n"
			+ "            coord := NULL;\n"
			+ "            coord := TO_NUMBER (textCoord);\n"
			+ "\n"
			+ "            --DBMS_OUTPUT.PUT_LINE ('COORD: ' || coord);\n"
			+ "\n"
			+ "            IF coord IS NOT NULL\n"
			+ "            THEN\n"
			+ "               IF result IS NULL OR result < coord\n"
			+ "               THEN\n"
			+ "                  result := coord;\n"
			+ "               END IF;\n"
			+ "            END IF;\n"
			+ "         END IF;\n"
			+ "\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('RESULT: ' || result);\n"
			+ "\n"
			+ "         polyAsText :=  " + StringFunctions.SUBSTRINGAFTER + " (polyAsText, ', ');\n"
			+ "      --DBMS_OUTPUT.PUT_LINE ('polyAsText: ' || polyAsText);\n"
			+ "      --'(3 4), (4 5))'\n"
			+ "      --'(4 5))'\n"
			+ "      --NULL\n"
			+ "      END LOOP;\n"
			+ "   END IF;\n"
			+ "\n"
			+ "   --   END IF;\n"
			+ "\n"
			+ "   RETURN result;\n"
			+ "END;"),
	/**
	 *
	 */
	MINX("NUMBER", "aLine VARCHAR", ""
			+ "   result       NUMBER;\n"
			+ "   coord        NUMBER;\n"
			+ "   pnt          VARCHAR (4000);\n"
			+ "   polyAsText   VARCHAR (4000);\n"
			+ "   textCoord    VARCHAR (4000);BEGIN\n"
			+ "   result := NULL;\n"
			+ "\n"
			+ "   IF aLine IS NOT NULL\n"
			+ "   THEN\n"
			+ "      polyAsText := aLine;\n"
			+ "      --DBMS_OUTPUT.PUT_LINE ('START: ' || polyAsText);\n"
			+ "      --'MULTIPOINT ((2 3), (3 4), (4 5))'\n"
			+ "      polyAsText :=  " + StringFunctions.SUBSTRINGAFTER + " (polyAsText, '(');\n"
			+ "\n"
			+ "      --'(2 3), (3 4), (4 5))'\n"
			+ "      --'(3 4), (4 5))'\n"
			+ "      --'(4 5))'\n"
			+ "      --NULL\n"
			+ "      WHILE polyAsText IS NOT NULL\n"
			+ "      LOOP\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('POLYASTEXT: ' || polyAsText);\n"
			+ "\n"
			+ "         textCoord :=  " + StringFunctions.SUBSTRINGAFTER + " (polyAsText, '(');\n"
			+ "         textCoord :=  " + StringFunctions.SUBSTRINGBEFORE + " (textCoord, ')');\n"
			+ "\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('TEXTCOORD: ' || textCoord);\n"
			+ "\n"
			+ "         IF textCoord IS NULL\n"
			+ "         THEN\n"
			+ "            textCoord := polyAsText;\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('TEXTCOORD: ' || textCoord);\n"
			+ "         --4 5\n"
			+ "         END IF;\n"
			+ "\n"
			+ "         --'2 3'\n"
			+ "         textCoord :=  " + StringFunctions.SUBSTRINGBEFORE + " (textCoord, ' ');\n"
			+ "\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('TONUMBER: ' || textCoord);\n"
			+ "\n"
			+ "         --'3'\n"
			+ "\n"
			+ "         IF LENGTH (textCoord) > 0\n"
			+ "         THEN\n"
			+ "            coord := NULL;\n"
			+ "            coord := TO_NUMBER (textCoord);\n"
			+ "\n"
			+ "            --DBMS_OUTPUT.PUT_LINE ('COORD: ' || coord);\n"
			+ "\n"
			+ "            IF coord IS NOT NULL\n"
			+ "            THEN\n"
			+ "               IF result IS NULL OR result > coord\n"
			+ "               THEN\n"
			+ "                  result := coord;\n"
			+ "               END IF;\n"
			+ "            END IF;\n"
			+ "         END IF;\n"
			+ "\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('RESULT: ' || result);\n"
			+ "\n"
			+ "         polyAsText :=  " + StringFunctions.SUBSTRINGAFTER + " (polyAsText, ', ');\n"
			+ "      --DBMS_OUTPUT.PUT_LINE ('polyAsText: ' || polyAsText);\n"
			+ "      --'(3 4), (4 5))'\n"
			+ "      --'(4 5))'\n"
			+ "      --NULL\n"
			+ "      END LOOP;\n"
			+ "   END IF;\n"
			+ "\n"
			+ "   --   END IF;\n"
			+ "\n"
			+ "   RETURN result;\n"
			+ "END;"),
	/**
	 *
	 */
	MAXY("NUMBER", "aLine VARCHAR", ""
			+ "   result       NUMBER;\n"
			+ "   coord        NUMBER;\n"
			+ "   pnt          VARCHAR (4000);\n"
			+ "   polyAsText   VARCHAR (4000);\n"
			+ "   textCoord    VARCHAR (4000);BEGIN\n"
			+ "   result := NULL;\n"
			+ "\n"
			+ "   IF aLine IS NOT NULL\n"
			+ "   THEN\n"
			+ "      polyAsText := aLine;\n"
			+ "      --DBMS_OUTPUT.PUT_LINE ('START: ' || polyAsText);\n"
			+ "      --'MULTIPOINT ((2 3), (3 4), (4 5))'\n"
			+ "      polyAsText :=  " + StringFunctions.SUBSTRINGAFTER + " (polyAsText, '(');\n"
			+ "\n"
			+ "      --'(2 3), (3 4), (4 5))'\n"
			+ "      --'(3 4), (4 5))'\n"
			+ "      --'(4 5))'\n"
			+ "      --NULL\n"
			+ "      WHILE polyAsText IS NOT NULL\n"
			+ "      LOOP\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('POLYASTEXT: ' || polyAsText);\n"
			+ "\n"
			+ "         textCoord :=  " + StringFunctions.SUBSTRINGAFTER + " (polyAsText, '(');\n"
			+ "         textCoord :=  " + StringFunctions.SUBSTRINGBEFORE + " (textCoord, ')');\n"
			+ "\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('TEXTCOORD: ' || textCoord);\n"
			+ "\n"
			+ "         IF textCoord IS NULL\n"
			+ "         THEN\n"
			+ "            textCoord := polyAsText;\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('TEXTCOORD: ' || textCoord);\n"
			+ "         --4 5\n"
			+ "         END IF;\n"
			+ "\n"
			+ "         --'2 3'\n"
			+ "         textCoord :=  " + StringFunctions.SUBSTRINGAFTER + " (textCoord, ' ');"
			+ "\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('TONUMBER: ' || textCoord);\n"
			+ "\n"
			+ "         --'3'\n"
			+ "\n"
			+ "         IF LENGTH (textCoord) > 0\n"
			+ "         THEN\n"
			+ "            coord := NULL;\n"
			+ "            coord := TO_NUMBER (textCoord);\n"
			+ "\n"
			+ "            --DBMS_OUTPUT.PUT_LINE ('COORD: ' || coord);\n"
			+ "\n"
			+ "            IF coord IS NOT NULL\n"
			+ "            THEN\n"
			+ "               IF result IS NULL OR result < coord\n"
			+ "               THEN\n"
			+ "                  result := coord;\n"
			+ "               END IF;\n"
			+ "            END IF;\n"
			+ "         END IF;\n"
			+ "\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('RESULT: ' || result);\n"
			+ "\n"
			+ "         polyAsText :=  " + StringFunctions.SUBSTRINGAFTER + " (polyAsText, ', ');\n"
			+ "      --DBMS_OUTPUT.PUT_LINE ('polyAsText: ' || polyAsText);\n"
			+ "      --'(3 4), (4 5))'\n"
			+ "      --'(4 5))'\n"
			+ "      --NULL\n"
			+ "      END LOOP;\n"
			+ "   END IF;\n"
			+ "\n"
			+ "   --   END IF;\n"
			+ "\n"
			+ "   RETURN result;\n"
			+ "END;"),
	/**
	 *
	 */
	MINY("NUMBER", "aLine VARCHAR", ""
			+ "   result       NUMBER;\n"
			+ "   coord        NUMBER;\n"
			+ "   pnt          VARCHAR (4000);\n"
			+ "   polyAsText   VARCHAR (4000);\n"
			+ "   textCoord    VARCHAR (4000);BEGIN\n"
			+ "   result := NULL;\n"
			+ "\n"
			+ "   IF aLine IS NOT NULL\n"
			+ "   THEN\n"
			+ "      polyAsText := aLine;\n"
			+ "      --DBMS_OUTPUT.PUT_LINE ('START: ' || polyAsText);\n"
			+ "      --'MULTIPOINT ((2 3), (3 4), (4 5))'\n"
			+ "      polyAsText :=  " + StringFunctions.SUBSTRINGAFTER + " (polyAsText, '(');\n"
			+ "\n"
			+ "      --'(2 3), (3 4), (4 5))'\n"
			+ "      --'(3 4), (4 5))'\n"
			+ "      --'(4 5))'\n"
			+ "      --NULL\n"
			+ "      WHILE polyAsText IS NOT NULL\n"
			+ "      LOOP\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('POLYASTEXT: ' || polyAsText);\n"
			+ "\n"
			+ "         textCoord :=  " + StringFunctions.SUBSTRINGAFTER + " (polyAsText, '(');\n"
			+ "         textCoord :=  " + StringFunctions.SUBSTRINGBEFORE + " (textCoord, ')');\n"
			+ "\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('TEXTCOORD: ' || textCoord);\n"
			+ "\n"
			+ "         IF textCoord IS NULL\n"
			+ "         THEN\n"
			+ "            textCoord := polyAsText;\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('TEXTCOORD: ' || textCoord);\n"
			+ "         --4 5\n"
			+ "         END IF;\n"
			+ "\n"
			+ "         --'2 3'\n"
			+ "         textCoord :=  " + StringFunctions.SUBSTRINGAFTER + " (textCoord, ' ');"
			+ "\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('TONUMBER: ' || textCoord);\n"
			+ "\n"
			+ "         --'3'\n"
			+ "\n"
			+ "         IF LENGTH (textCoord) > 0\n"
			+ "         THEN\n"
			+ "            coord := NULL;\n"
			+ "            coord := TO_NUMBER (textCoord);\n"
			+ "\n"
			+ "            --DBMS_OUTPUT.PUT_LINE ('COORD: ' || coord);\n"
			+ "\n"
			+ "            IF coord IS NOT NULL\n"
			+ "            THEN\n"
			+ "               IF result IS NULL OR result > coord\n"
			+ "               THEN\n"
			+ "                  result := coord;\n"
			+ "               END IF;\n"
			+ "            END IF;\n"
			+ "         END IF;\n"
			+ "\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('RESULT: ' || result);\n"
			+ "\n"
			+ "         polyAsText :=  " + StringFunctions.SUBSTRINGAFTER + " (polyAsText, ', ');\n"
			+ "      --DBMS_OUTPUT.PUT_LINE ('polyAsText: ' || polyAsText);\n"
			+ "      --'(3 4), (4 5))'\n"
			+ "      --'(4 5))'\n"
			+ "      --NULL\n"
			+ "      END LOOP;\n"
			+ "   END IF;\n"
			+ "\n"
			+ "   --   END IF;\n"
			+ "\n"
			+ "   RETURN result;\n"
			+ "END;"),
	/**
	 *
	 */
	BOUNDINGBOX("VARCHAR", "multiPoint IN VARCHAR", ""
			+ "   result   VARCHAR (2002);\n"
			+ "   maxx     NUMBER;\n"
			+ "   minx     NUMBER;\n"
			+ "   maxy     NUMBER;\n"
			+ "   miny     NUMBER;\n"
			+ "BEGIN\n"
			+ "   IF multiPoint IS NULL\n"
			+ "   THEN\n"
			+ "      RETURN NULL;\n"
			+ "   ELSE\n"
			+ "      maxx := " + MultiPoint2DFunctions.MAXX + " (multiPoint);\n"
			+ "      minx := " + MultiPoint2DFunctions.MINX + "  (multiPoint);\n"
			+ "      maxy := " + MultiPoint2DFunctions.MAXY + "  (multiPoint);\n"
			+ "      miny := " + MultiPoint2DFunctions.MINY + "  (multiPoint);\n"
			+ "      result := NULL;\n"
			+ "      result :=\n"
			+ "            'POLYGON (('\n"
			+ "         || minx\n"
			+ "         || ' '\n"
			+ "         || miny\n"
			+ "         || ', '\n"
			+ "         || maxx\n"
			+ "         || ' '\n"
			+ "         || miny\n"
			+ "         || ', '\n"
			+ "         || maxx\n"
			+ "         || ' '\n"
			+ "         || maxy\n"
			+ "         || ', '\n"
			+ "         || minx\n"
			+ "         || ' '\n"
			+ "         || maxy\n"
			+ "         || ', '\n"
			+ "         || minx\n"
			+ "         || ' '\n"
			+ "         || miny\n"
			+ "         || '))';\n"
			+ "      RETURN result;\n"
			+ "   END IF;\n"
			+ "END;"),
	/**
	 *
	 */
	EQUALS("NUMBER", "multiPoint1 IN VARCHAR, multiPoint2 IN VARCHAR", ""
			+ "BEGIN\n"
			+ "   RETURN CASE WHEN (multiPoint1 = multiPoint2) THEN 1 ELSE 0 END;\n"
			+ "END;"),
	/**
	 *
	 */
	GETFROMINDEX("VARCHAR", "mpoint IN VARCHAR, indx IN INTEGER", ""
			+ "   result         VARCHAR (4000);\n"
			+ "   currentindex   NUMBER := 1;\n"
			+ "   pnt            VARCHAR (4000);\n"
			+ "   polyAsText     VARCHAR (4000);\n"
			+ "   textCoord      VARCHAR (4000);\n"
			+ "BEGIN\n"
			+ "   result := NULL;\n"
			+ "\n"
			+ "   IF mpoint IS NOT NULL\n"
			+ "   THEN\n"
			+ "      polyAsText := mpoint;\n"
			+ "      --DBMS_OUTPUT.PUT_LINE ('START: ' || polyAsText);\n"
			+ "      --'MULTIPOINT ((2 3), (3 4), (4 5))'\n"
			+ "      polyAsText := DBV_STRINGFN_SUBSTRINGAFTER (polyAsText, '(');\n"
			+ "\n"
			+ "      --'(2 3), (3 4), (4 5))'\n"
			+ "      --'(3 4), (4 5))'\n"
			+ "      --'(4 5))'\n"
			+ "      --NULL\n"
			+ "      WHILE polyAsText IS NOT NULL AND currentindex <= indx\n"
			+ "      LOOP\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('POLYASTEXT: ' || polyAsText);\n"
			+ "\n"
			+ "         IF (currentindex = indx)\n"
			+ "         THEN\n"
			+ "            textCoord := DBV_STRINGFN_SUBSTRINGAFTER (polyAsText, '(');\n"
			+ "            textCoord := DBV_STRINGFN_SUBSTRINGBEFORE (textCoord, ')');\n"
			+ "\n"
			+ "            --DBMS_OUTPUT.PUT_LINE ('TEXTCOORD: ' || textCoord);\n"
			+ "\n"
			+ "            IF textCoord IS NOT NULL\n"
			+ "            THEN\n"
			+ "               result := 'POINT (' || textcoord || ')';\n"
			+ "            END IF;\n"
			+ "         END IF;\n"
			+ "\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('RESULT: ' || result);\n"
			+ "\n"
			+ "         polyAsText := DBV_STRINGFN_SUBSTRINGAFTER (polyAsText, ', ');\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('polyAsText: ' || polyAsText);\n"
			+ "         currentindex := currentindex + 1;\n"
			+ "      END LOOP;\n"
			+ "   END IF;\n"
			+ "\n"
			+ "   RETURN result;\n"
			+ "END;"),
	/**
	 *
	 */
	POINTCOUNT("INTEGER", "mpoint IN VARCHAR", ""
			+ "   result       NUMBER;\n"
			+ "   pnt          VARCHAR (4000);\n"
			+ "   polyAsText   VARCHAR (4000);\n"
			+ "BEGIN\n"
			+ "   result := NULL;\n"
			+ "\n"
			+ "   IF mpoint IS NOT NULL\n"
			+ "   THEN\n"
			+ "      result := 1;\n"
			+ "      polyAsText := mpoint;\n"
			+ "      --DBMS_OUTPUT.PUT_LINE ('START: ' || polyAsText);\n"
			+ "      --'MULTIPOINT ((2 3), (3 4), (4 5))'\n"
			+ "      polyAsText := DBV_STRINGFN_SUBSTRINGAFTER (polyAsText, ', ');\n"
			+ "\n"
			+ "      --'(2 3), (3 4), (4 5))'\n"
			+ "      --'(3 4), (4 5))'\n"
			+ "      --'(4 5))'\n"
			+ "      --NULL\n"
			+ "      WHILE polyAsText IS NOT NULL\n"
			+ "      LOOP\n"
			+ "         result := result + 1;\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('POLYASTEXT: ' || polyAsText);\n"
			+ "         polyAsText := DBV_STRINGFN_SUBSTRINGAFTER (polyAsText, ', ');\n"
			+ "      END LOOP;\n"
			+ "   END IF;\n"
			+ "\n"
			+ "   RETURN result;\n"
			+ "END;"),
	/**
	 *
	 */
	DIMENSION("INTEGER", "mpoint IN VARCHAR", "BEGIN\n"
			+ "RETURN 0;\n"
			+ "END;"),
	/**
	 *
	 */
	ASTEXT("VARCHAR", "mpoint IN VARCHAR", "BEGIN RETURN mpoint; END;"),
	/**
	 *
	 */
	ASLINE2D("VARCHAR", "mpoint IN VARCHAR", "BEGIN\n"
			+ "   --'MULTIPOINT ((2 3), (3 4), (4 5))'\n"
			+ "   --'LINESTRING (2 3, 3 4, 4 5)'\n"
			+ "   RETURN REPLACE (\n"
			+ "             REPLACE (REPLACE (mpoint, 'MULTIPOINT ((', 'LINESTRING ('),\n"
			+ "                      '), (',\n"
			+ "                      ', '),\n"
			+ "             '))',\n"
			+ "             ')');\n"
			+ "END;");

	private final String returnType;
	private final String parameters;
	private final String code;

	MultiPoint2DFunctions(String returnType, String parameters, String code) {
		this.returnType = returnType;
		this.parameters = parameters;
		this.code = code;
	}

	@Override
	public String toString() {
		return "DBV_MPOINT2D_" + name();
	}

	/**
	 *
	 * @param stmt
	 * @throws SQLException
	 */
	public void add(Statement stmt) throws SQLException {
		try {
			if (!this.code.isEmpty()) {
				final String createFn = "CREATE OR REPLACE FUNCTION " + this + "(" + this.parameters + ")\n"
						+ "    RETURN " + this.returnType
						+ " AS \n" + "\n" + this.code;
				stmt.execute(createFn);
			}
		} catch (SQLException ex) {
			;
		}
	}
}
