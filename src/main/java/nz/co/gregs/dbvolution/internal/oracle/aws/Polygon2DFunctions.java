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
package nz.co.gregs.dbvolution.internal.oracle.aws;

import nz.co.gregs.dbvolution.internal.oracle.StringFunctions;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public enum Polygon2DFunctions {

	/**
	 *
	 */
	CREATE_WKTPOLY2D("VARCHAR", "wktOfPolygon2D VARCHAR", ""
			+ "BEGIN\n"
			+ "RETURN wktOfPolygon2D;"
			+ "END;"),
	/**
	 *
	 */
	MAXY("NUMBER", "aPoly VARCHAR", ""
			+ "   result       NUMBER;\n"
			+ "   coord        NUMBER;\n"
			+ "   pnt          VARCHAR (4000);\n"
			+ "   polyAsText   VARCHAR (4000);\n"
			+ "   textCoord    VARCHAR (4000);\n"
			+ "BEGIN\n"
			+ "   result := NULL;\n"
			+ "\n"
			+ "   IF aPoly IS NOT NULL\n"
			+ "   THEN\n"
			+ "      polyAsText := aPoly;\n"
			+ "      --DBMS_OUTPUT.PUT_LINE ('START: ' || polyAsText);\n"
			+ "      --'LINESTRING (2 3, 3 4, 4 5)'\n"
			+ "      polyAsText := " + StringFunctions.SUBSTRINGAFTER + "(polyAsText, '(');\n"
			+ "      polyAsText := " + StringFunctions.SUBSTRINGBEFORE + "(polyAsText, ')');\n"
			+ "\n"
			+ "      --'2 3, 3 4, 4 5'\n"
			+ "      --'3 4, 4 5'\n"
			+ "      --'4 5'\n"
			+ "      --NULL\n"
			+ "      WHILE polyAsText IS NOT NULL\n"
			+ "      LOOP\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('POLYASTEXT: ' || polyAsText);\n"
			+ "\n"
			+ "         textCoord := " + StringFunctions.SUBSTRINGBEFORE + " (polyAsText, ',');\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('TEXTCOORD: ' || textCoord);\n"
			+ "\n"
			+ "         IF textCoord IS NULL\n"
			+ "         THEN\n"
			+ "            textCoord := polyAsText;\n"
			+ "            --DBMS_OUTPUT.PUT_LINE ('TEXTCOORD: ' || textCoord);\n"
			+ "         --4 5\n"
			+ "         END IF;\n"
			+ "\n"
			+ "         --'2 3'\n"
			+ "         textCoord := " + StringFunctions.SUBSTRINGAFTER + " (textCoord, ' ');\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('TONUMBER: ' || textCoord);\n"
			+ "\n"
			+ "         --'3'\n"
			+ "\n"
			+ "         IF LENGTH (textCoord) > 0\n"
			+ "         THEN\n"
			+ "            coord := NULL;\n"
			+ "            coord := TO_NUMBER (textCoord);\n"
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
			+ "         polyAsText := " + StringFunctions.SUBSTRINGAFTER + " (polyAsText, ', ');\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('polyAsText: ' || polyAsText);\n"
			+ "      --'3 4, 4 5'\n"
			+ "      --'4 5'\n"
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
	MAXX("number", "aPoly VARCHAR", ""
			+ "   result       NUMBER;\n"
			+ "   coord        NUMBER;\n"
			+ "   pnt          VARCHAR (4000);\n"
			+ "   polyAsText   VARCHAR (4000);\n"
			+ "   textCoord    VARCHAR (4000);\n"
			+ "BEGIN\n"
			+ "   result := NULL;\n"
			+ "\n"
			+ "   IF aPoly IS NOT NULL\n"
			+ "   THEN\n"
			+ "      polyAsText := aPoly;\n"
			+ "      --DBMS_OUTPUT.PUT_LINE ('START: ' || polyAsText);\n"
			+ "      --'LINESTRING (2 3, 3 4, 4 5)'\n"
			+ "      polyAsText := " + StringFunctions.SUBSTRINGAFTER + "(" + StringFunctions.SUBSTRINGAFTER + " (polyAsText, '('), '(');\n"
			+ "      polyAsText := " + StringFunctions.SUBSTRINGBEFORE + " (polyAsText, ')');\n"
			+ "\n"
			+ "      --'2 3, 3 4, 4 5'\n"
			+ "      --'3 4, 4 5'\n"
			+ "      --'4 5'\n"
			+ "      --NULL\n"
			+ "      WHILE polyAsText IS NOT NULL\n"
			+ "      LOOP\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('POLYASTEXT: ' || polyAsText);\n"
			+ "\n"
			+ "         textCoord := " + StringFunctions.SUBSTRINGBEFORE + " (polyAsText, ',');\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('TEXTCOORD: ' || textCoord);\n"
			+ "\n"
			+ "         IF textCoord IS NULL\n"
			+ "         THEN\n"
			+ "            textCoord := polyAsText;\n"
			+ "            --DBMS_OUTPUT.PUT_LINE ('TEXTCOORD: ' || textCoord);\n"
			+ "         --4 5\n"
			+ "         END IF;\n"
			+ "\n"
			+ "         --'2 3'\n"
			+ "         textCoord := " + StringFunctions.SUBSTRINGBEFORE + " (textCoord, ' ');\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('TONUMBER: ' || textCoord);\n"
			+ "\n"
			+ "         --'3'\n"
			+ "\n"
			+ "         IF LENGTH (textCoord) > 0\n"
			+ "         THEN\n"
			+ "            coord := NULL;\n"
			+ "            coord := TO_NUMBER (textCoord);\n"
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
			+ "         polyAsText := " + StringFunctions.SUBSTRINGAFTER + " (polyAsText, ', ');\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('polyAsText: ' || polyAsText);\n"
			+ "      --'3 4, 4 5'\n"
			+ "      --'4 5'\n"
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
	MINX("NUMBER", "aPoly VARCHAR", ""
			+ "   result       NUMBER;\n"
			+ "   coord        NUMBER;\n"
			+ "   pnt          VARCHAR (4000);\n"
			+ "   polyAsText   VARCHAR (4000);\n"
			+ "   textCoord    VARCHAR (4000);\n"
			+ "BEGIN\n"
			+ "   result := NULL;\n"
			+ "\n"
			+ "   IF aPoly IS NOT NULL\n"
			+ "   THEN\n"
			+ "      polyAsText := aPoly;\n"
			+ "      --DBMS_OUTPUT.PUT_LINE ('START: ' || polyAsText);\n"
			+ "      --'LINESTRING (2 3, 3 4, 4 5)'\n"
			+ "      polyAsText := " + StringFunctions.SUBSTRINGAFTER + " (" + StringFunctions.SUBSTRINGAFTER + " (polyAsText, '('), '(');\n"
			+ "      polyAsText := " + StringFunctions.SUBSTRINGBEFORE + " (polyAsText, ')');\n"
			+ "\n"
			+ "      --'2 3, 3 4, 4 5'\n"
			+ "      --'3 4, 4 5'\n"
			+ "      --'4 5'\n"
			+ "      --NULL\n"
			+ "      WHILE polyAsText IS NOT NULL\n"
			+ "      LOOP\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('POLYASTEXT: ' || polyAsText);\n"
			+ "\n"
			+ "         textCoord := " + StringFunctions.SUBSTRINGBEFORE + " (polyAsText, ',');\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('TEXTCOORD: ' || textCoord);\n"
			+ "\n"
			+ "         IF textCoord IS NULL\n"
			+ "         THEN\n"
			+ "            textCoord := polyAsText;\n"
			+ "            --DBMS_OUTPUT.PUT_LINE ('TEXTCOORD: ' || textCoord);\n"
			+ "         --4 5\n"
			+ "         END IF;\n"
			+ "\n"
			+ "         --'2 3'\n"
			+ "         textCoord := " + StringFunctions.SUBSTRINGBEFORE + " (textCoord, ' ');\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('TONUMBER: ' || textCoord);\n"
			+ "\n"
			+ "         --'3'\n"
			+ "\n"
			+ "         IF LENGTH (textCoord) > 0\n"
			+ "         THEN\n"
			+ "            coord := NULL;\n"
			+ "            coord := TO_NUMBER (textCoord);\n"
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
			+ "         polyAsText := " + StringFunctions.SUBSTRINGAFTER + " (polyAsText, ', ');\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('polyAsText: ' || polyAsText);\n"
			+ "      --'3 4, 4 5'\n"
			+ "      --'4 5'\n"
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
	MINY("number", "aPoly VARCHAR", ""
			+ "   result       NUMBER;\n"
			+ "   coord        NUMBER;\n"
			+ "   pnt          VARCHAR (4000);\n"
			+ "   polyAsText   VARCHAR (4000);\n"
			+ "   textCoord    VARCHAR (4000);\n"
			+ "BEGIN\n"
			+ "   result := NULL;\n"
			+ "\n"
			+ "   IF aPoly IS NOT NULL\n"
			+ "   THEN\n"
			+ "      polyAsText := aPoly;\n"
			+ "      --DBMS_OUTPUT.PUT_LINE ('START: ' || polyAsText);\n"
			+ "      --'LINESTRING (2 3, 3 4, 4 5)'\n"
			+ "      polyAsText := " + StringFunctions.SUBSTRINGAFTER + " (polyAsText, '(');\n"
			+ "      polyAsText := " + StringFunctions.SUBSTRINGBEFORE + " (polyAsText, ')');\n"
			+ "\n"
			+ "      --'2 3, 3 4, 4 5'\n"
			+ "      --'3 4, 4 5'\n"
			+ "      --'4 5'\n"
			+ "      --NULL\n"
			+ "      WHILE polyAsText IS NOT NULL\n"
			+ "      LOOP\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('POLYASTEXT: ' || polyAsText);\n"
			+ "\n"
			+ "         textCoord := " + StringFunctions.SUBSTRINGBEFORE + " (polyAsText, ',');\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('TEXTCOORD: ' || textCoord);\n"
			+ "\n"
			+ "         IF textCoord IS NULL\n"
			+ "         THEN\n"
			+ "            textCoord := polyAsText;\n"
			+ "            --DBMS_OUTPUT.PUT_LINE ('TEXTCOORD: ' || textCoord);\n"
			+ "         --4 5\n"
			+ "         END IF;\n"
			+ "\n"
			+ "         --'2 3'\n"
			+ "         textCoord := " + StringFunctions.SUBSTRINGAFTER + " (textCoord, ' ');\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('TONUMBER: ' || textCoord);\n"
			+ "\n"
			+ "         --'3'\n"
			+ "\n"
			+ "         IF LENGTH (textCoord) > 0\n"
			+ "         THEN\n"
			+ "            coord := NULL;\n"
			+ "            coord := TO_NUMBER (textCoord);\n"
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
			+ "         polyAsText := " + StringFunctions.SUBSTRINGAFTER + " (polyAsText, ', ');\n"
			+ "      --DBMS_OUTPUT.PUT_LINE ('polyAsText: ' || polyAsText);\n"
			+ "      --'3 4, 4 5'\n"
			+ "      --'4 5'\n"
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
	BOUNDINGBOX("VARCHAR", "polygon IN VARCHAR", ""
			+ "   result   VARCHAR (2002);\n"
			+ "   maxx     NUMBER;\n"
			+ "   minx     NUMBER;\n"
			+ "   maxy     NUMBER;\n"
			+ "   miny     NUMBER;\n"
			+ "BEGIN\n"
			+ "   IF polygon IS NULL\n"
			+ "   THEN\n"
			+ "      RETURN NULL;\n"
			+ "   ELSE\n"
			+ "      maxx := " + MAXX + " (polygon);\n"
			+ "      minx := " + MINX + " (polygon);\n"
			+ "      maxy := " + MAXY + " (polygon);\n"
			+ "      miny := " + MINY + " (polygon);\n"
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
	EXTERIORRING("VARCHAR", "polygon IN VARCHAR", ""
			+ "   polyAsText   VARCHAR (4000);\n"
			+ "   BEGIN\n"
			+ "       polyAsText :=\n"
			+ "         'LINESTRING '\n"
			+ "         || DBV_STRINGFN_SUBSTRINGBEFORE (\n"
			+ "            DBV_STRINGFN_SUBSTRINGAFTER (polygon, '('),\n"
			+ "            ')')\n"
			+ "         || ')';\n"
			+ "   RETURN polyastext;"
			+ "END;"),
	/**
	 *
	 */
	AREA("NUMBER", "polygon IN VARCHAR", ""
			+ "   sumXtoY                NUMBER := 0;\n"
			+ "   sumYtoX                NUMBER := 0;\n"
			+ "   XtoYminusYtoX          NUMBER := 0;\n"
			+ "   everythingDividedBy2   NUMBER := 0;\n"
			+ "   result                 NUMBER := NULL;\n"
			+ "   polyAsText             VARCHAR (4000);\n"
			+ "   currentX               NUMBER;\n"
			+ "   currentY               NUMBER;\n"
			+ "   nextX                  NUMBER;\n"
			+ "   nextY                  NUMBER;\n"
			+ "   currentXtext           VARCHAR (4000);\n"
			+ "   currentYtext           VARCHAR (4000);\n"
			+ "   nextXtext              VARCHAR (4000);\n"
			+ "   nextYtext              VARCHAR (4000);\n"
			+ "   currentCoord           VARCHAR (4000);\n"
			+ "   nextCoord              VARCHAR (4000);\n"
			+ "BEGIN\n"
			+ "   result := NULL;\n"
			+ "\n"
			+ "   IF polygon IS NOT NULL\n"
			+ "   THEN\n"
			+ "      polyAsText := polygon;\n"
			+ "      --DBMS_OUTPUT.PUT_LINE ('START: ' || polyAsText);\n"
			+ "      --'LINESTRING (2 3, 3 4, 4 5)'\n"
			+ "      polyAsText :=\n"
			+ "         DBV_STRINGFN_SUBSTRINGAFTER (\n"
			+ "            DBV_STRINGFN_SUBSTRINGAFTER (polyAsText, '('),\n"
			+ "            '(');\n"
			+ "      polyAsText := DBV_STRINGFN_SUBSTRINGBEFORE (polyAsText, ')');\n"
			+ "\n"
			+ "      --'2 3, 3 4, 4 5'\n"
			+ "      --'3 4, 4 5'\n"
			+ "      --'4 5'\n"
			+ "      --NULL\n"
			+ "      WHILE polyAsText IS NOT NULL\n"
			+ "      LOOP\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('POLYASTEXT: ' || polyAsText);\n"
			+ "\n"
			+ "         currentCoord := DBV_STRINGFN_SUBSTRINGBEFORE (polyAsText, ',');\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('CURRENTCOORD: ' || currentCoord);\n"
			+ "\n"
			+ "         nextCoord :=\n"
			+ "            DBV_STRINGFN_SUBSTRINGBEFORE (\n"
			+ "               DBV_STRINGFN_SUBSTRINGAFTER (polyAsText, ', '),\n"
			+ "               ',');\n"
			+ "\n"
			+ "         IF nextCoord IS NULL\n"
			+ "         THEN\n"
			+ "            nextCoord := DBV_STRINGFN_SUBSTRINGAFTER (polyAsText, ', ');\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('TEXTCOORD: ' || textCoord);\n"
			+ "         --4 5\n"
			+ "         END IF;\n"
			+ "\n"
			+ "         DBMS_OUTPUT.PUT_LINE ('NEXTCOORD: ' || nextCoord);\n"
			+ "\n"
			+ "         IF nextCoord IS NOT NULL\n"
			+ "         THEN\n"
			+ "            currentXtext := DBV_STRINGFN_SUBSTRINGBEFORE (currentCoord, ' ');\n"
			+ "            currentYtext := DBV_STRINGFN_SUBSTRINGAFTER (currentCoord, ' ');\n"
			+ "            nextXtext := DBV_STRINGFN_SUBSTRINGBEFORE (nextCoord, ' ');\n"
			+ "            nextYtext := DBV_STRINGFN_SUBSTRINGAFTER (nextCoord, ' ');\n"
			+ "\n"
			+ "            --DBMS_OUTPUT.PUT_LINE ('currentX: ' || currentXtext);\n"
			+ "            --DBMS_OUTPUT.PUT_LINE ('currentY: ' || currentYtext);\n"
			+ "            --DBMS_OUTPUT.PUT_LINE ('nextX: ' || nextXtext);\n"
			+ "            --DBMS_OUTPUT.PUT_LINE ('nextY: ' || nextYtext);\n"
			+ "\n"
			+ "            --'3'\n"
			+ "\n"
			+ "            IF LENGTH (nextXtext) > 0\n"
			+ "            THEN\n"
			+ "               currentX := NULL;\n"
			+ "               currentX := TO_NUMBER (currentXtext);\n"
			+ "               currentY := NULL;\n"
			+ "               currentY := TO_NUMBER (currentYtext);\n"
			+ "               nextX := NULL;\n"
			+ "               nextX := TO_NUMBER (nextXtext);\n"
			+ "               nextY := NULL;\n"
			+ "               nextY := TO_NUMBER (nextYtext);\n"
			+ "\n"
			+ "               --DBMS_OUTPUT.PUT_LINE ('COORD: ' || coord);\n"
			+ "\n"
			+ "               IF currentX IS NOT NULL\n"
			+ "               THEN\n"
			+ "                  sumXtoY := sumXtoY + (currentX * nextY);\n"
			+ "                  sumYtoX := sumYtoX + (currentY * nextX);\n"
			+ "               END IF;\n"
			+ "            END IF;\n"
			+ "         END IF;\n"
			+ "\n"
			+ "         --DBMS_OUTPUT.PUT_LINE ('RESULT: ' || result);\n"
			+ "\n"
			+ "         polyAsText := DBV_STRINGFN_SUBSTRINGAFTER (polyAsText, ', ');\n"
			+ "      --DBMS_OUTPUT.PUT_LINE ('polyAsText: ' || polyAsText);\n"
			+ "      --'3 4, 4 5'\n"
			+ "      --'4 5'\n"
			+ "      --NULL\n"
			+ "      END LOOP;\n"
			+ "\n"
			+ "      XtoYminusYtoX := sumXtoY - sumYtoX;\n"
			+ "      everythingDividedBy2 := XtoYminusYtoX / 2;\n"
			+ "      result := abs(everythingdividedby2);\n"
			+ "   END IF;\n"
			+ "\n"
			+ "   --   END IF;\n"
			+ "\n"
			+ "   RETURN result;"
			+ "END;"),
	/**
	 *
	 */
	DIMENSION("NUMBER", "firstPoly IN VARCHAR", "BEGIN RETURN 2; END;"),
	/**
	 *
	 */
	EQUALS("NUMBER", "firstPoly IN VARCHAR, secondPoly IN VARCHAR", "BEGIN \n"
			+ "   RETURN CASE WHEN (firstpoly = secondPoly) THEN 1 ELSE 0 END;\n"
			+ "   END;"),
	/**
	 *
	 */
	INTERSECTION("VARCHAR", "firstPoly IN VARCHAR, secondPoly IN VARCHAR", "BEGIN RETURN 2; END;"),
	/**
	 *
	 */
	INTERSECTS("BOOLEAN", "firstPoly IN VARCHAR, secondPoly IN VARCHAR", "BEGIN RETURN 2; END;"),
	/**
	 *
	 */
	CONTAINS("BOOLEAN", "firstPoly IN VARCHAR, secondPoly IN VARCHAR", "BEGIN RETURN 2; END;"),
	/**
	 *
	 */
	DISJOINT("BOOLEAN", "firstPoly IN VARCHAR, secondPoly IN VARCHAR", "BEGIN RETURN 2; END;"),
	/**
	 *
	 */
	OVERLAPS("BOOLEAN", "firstPoly IN VARCHAR, secondPoly IN VARCHAR", "BEGIN RETURN 2; END;"),
	/**
	 *
	 */
	TOUCHES("BOOLEAN", "firstPoly IN VARCHAR, secondPoly IN VARCHAR", "BEGIN RETURN 2; END;"),
	/**
	 *
	 */
	WITHIN("BOOLEAN", "firstPoly IN VARCHAR, secondPoly IN VARCHAR", "BEGIN RETURN 2; END;");

	private final String returnType;
	private final String parameters;
	private final String code;

	Polygon2DFunctions(String returnType, String parameters, String code) {
		this.returnType = returnType;
		this.parameters = parameters;
		this.code = code;
	}

	@Override
	public String toString() {
		return "DBVPOLY2D_" + name();
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
