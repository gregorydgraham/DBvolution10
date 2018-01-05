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
package nz.co.gregs.dbvolution.internal.postgres;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public enum Line2DFunctions {

	/**
	 *
	 */
	MAXY(Language.plpgsql, "decimal", "poly path", "DECLARE \n"
			+ " result decimal;\n"
			+ " num integer;\n"
			+ " currentcoord decimal;\n"
			+ " pnt point;\n"
			+ " textPoly text;\n"
			+ " textCoord text;\n"
			+ "BEGIN\n"
			+ " if poly is null then return null; \n"
			+ " else\n"
			+ "  num := npoints(poly);\n"
			+ "  textPoly := poly::text;\n"
			+ "  textPoly := " + StringFunctions.SUBSTRINGAFTER + "(textPoly, $$[$$);\n"
			+ "  textPoly :=  " + StringFunctions.SUBSTRINGBEFORE + "(textPoly, $$]$$);\n"
			+ "  result:=null;\n"
			+ "  FOR i IN 1 .. num LOOP\n"
			+ "   textPoly :=  " + StringFunctions.SUBSTRINGAFTER + "(textPoly, $$($$);\n"
			+ "   textCoord :=  " + StringFunctions.SUBSTRINGAFTER + "( " + StringFunctions.SUBSTRINGBEFORE + "(textPoly, $$)$$), $$,$$);\n"
			+ "   if char_length(textCoord) > 0 then \n"
			+ "    currentcoord := textCoord::decimal;\n"
			+ "    IF result is null or result < currentcoord THEN\n"
			+ "     result := currentcoord;\n"
			+ "    END IF;\n"
			+ "   END IF;\n"
			+ "   textPoly :=  " + StringFunctions.SUBSTRINGAFTER + "(textPoly, $$)$$);\n"
			+ "   textPoly :=  " + StringFunctions.SUBSTRINGAFTER + "(textPoly, $$,$$);\n"
			+ "  END LOOP;\n"
			+ "  return result;\n"
			+ " END IF;\n"
			+ "END;"),
	/**
	 *
	 */
	MAXX(Language.plpgsql, "decimal", "poly path", "DECLARE \n"
			+ " result decimal;\n"
			+ " num integer;\n"
			+ " currentcoord decimal;\n"
			+ " pnt point;\n"
			+ " textPoly text;\n"
			+ " textCoord text;\n"
			+ "BEGIN\n"
			+ " if poly is null then return null; \n"
			+ " else\n"
			+ "  num := npoints(poly);\n"
			+ "  textPoly := poly::text;\n"
			+ "  textPoly := " + StringFunctions.SUBSTRINGAFTER + "(textPoly, $$[$$);\n"
			+ "  textPoly :=  " + StringFunctions.SUBSTRINGBEFORE + "(textPoly, $$]$$);\n"
			+ "  result:=null;\n"
			+ "  FOR i IN 1 .. num LOOP\n"
			+ "   textPoly :=  " + StringFunctions.SUBSTRINGAFTER + "(textPoly, $$($$);\n"
			+ "   textCoord :=  " + StringFunctions.SUBSTRINGBEFORE + "( " + StringFunctions.SUBSTRINGBEFORE + "(textPoly, $$)$$), $$,$$);\n"
			+ "   if char_length(textCoord) > 0 then \n"
			+ "    currentcoord := textCoord::decimal;\n"
			+ "    IF result is null or result < currentcoord THEN\n"
			+ "     result := currentcoord;\n"
			+ "    END IF;\n"
			+ "   END IF;\n"
			+ "   textPoly :=  " + StringFunctions.SUBSTRINGAFTER + "(textPoly, $$)$$);\n"
			+ "   textPoly :=  " + StringFunctions.SUBSTRINGAFTER + "(textPoly, $$,$$);\n"
			+ "  END LOOP;\n"
			+ "  return result;\n"
			+ " END IF;\n"
			+ "END;"),
	/**
	 *
	 */
	MINX(Language.plpgsql, "decimal", "poly path", "DECLARE \n"
			+ " result decimal;\n"
			+ " num integer;\n"
			+ " currentcoord decimal;\n"
			+ " pnt point;\n"
			+ " textPoly text;\n"
			+ " textCoord text;\n"
			+ "BEGIN\n"
			+ " if poly is null then return null; \n"
			+ " else\n"
			+ "  num := npoints(poly);\n"
			+ "  textPoly := poly::text;\n"
			+ "  textPoly := " + StringFunctions.SUBSTRINGAFTER + "(textPoly, $$[$$);\n"
			+ "  textPoly :=  " + StringFunctions.SUBSTRINGBEFORE + "(textPoly, $$]$$);\n"
			+ "  result:=null;\n"
			+ "  FOR i IN 1 .. num LOOP\n"
			+ "   textPoly :=  " + StringFunctions.SUBSTRINGAFTER + "(textPoly, $$($$);\n"
			+ "   textCoord :=  " + StringFunctions.SUBSTRINGBEFORE + "( " + StringFunctions.SUBSTRINGBEFORE + "(textPoly, $$)$$), $$,$$);\n"
			+ "   if char_length(textCoord) > 0 then \n"
			+ "    currentcoord := textCoord::decimal;\n"
			+ "    IF result is null or result > currentcoord THEN\n"
			+ "     result := currentcoord;\n"
			+ "    END IF;\n"
			+ "   END IF;\n"
			+ "   textPoly :=  " + StringFunctions.SUBSTRINGAFTER + "(textPoly, $$)$$);\n"
			+ "   textPoly :=  " + StringFunctions.SUBSTRINGAFTER + "(textPoly, $$,$$);\n"
			+ "  END LOOP;\n"
			+ "  return result;\n"
			+ " END IF;\n"
			+ "END;"),
	/**
	 *
	 */
	MINY(Language.plpgsql, "decimal", "poly path", "DECLARE \n"
			+ " result decimal;\n"
			+ " num integer;\n"
			+ " currentcoord decimal;\n"
			+ " pnt point;\n"
			+ " textPoly text;\n"
			+ " textCoord text;\n"
			+ "BEGIN\n"
			+ " if poly is null then return null; \n"
			+ " else\n"
			+ "  num := npoints(poly);\n"
			+ "  textPoly := poly::text;\n"
			+ "  textPoly := " + StringFunctions.SUBSTRINGAFTER + "(textPoly, $$[$$);\n"
			+ "  textPoly :=  " + StringFunctions.SUBSTRINGBEFORE + "(textPoly, $$]$$);\n"
			+ "  result:=null;\n"
			+ "  FOR i IN 1 .. num LOOP\n"
			+ "   textPoly :=  " + StringFunctions.SUBSTRINGAFTER + "(textPoly, $$($$);\n"
			+ "   textCoord :=  " + StringFunctions.SUBSTRINGAFTER + "( " + StringFunctions.SUBSTRINGBEFORE + "(textPoly, $$)$$), $$,$$);\n"
			+ "   if char_length(textCoord) > 0 then \n"
			+ "    currentcoord := textCoord::decimal;\n"
			+ "    IF result is null or result > currentcoord THEN\n"
			+ "     result := currentcoord;\n"
			+ "    END IF;\n"
			+ "   END IF;\n"
			+ "   textPoly :=  " + StringFunctions.SUBSTRINGAFTER + "(textPoly, $$)$$);\n"
			+ "   textPoly :=  " + StringFunctions.SUBSTRINGAFTER + "(textPoly, $$,$$);\n"
			+ "  END LOOP;\n"
			+ "  return result;\n"
			+ " END IF;\n"
			+ "END;"),
	/**
	 *
	 */
	BOUNDINGBOX(Language.plpgsql, "polygon", "poly path", "DECLARE \n"
			+ " result polygon;\n"
			+ " maxx decimal;\n"
			+ " minx decimal;\n"
			+ " maxy decimal;\n"
			+ " miny decimal;\n"
			+ "BEGIN\n"
			+ " if poly is null then return null; \n"
			+ " else\n"
			+ "  maxx:= " + MAXX + "(poly);\n"
			+ "  minx:= " + MINX + "(poly);\n"
			+ "  maxy:= " + MAXY + "(poly);\n"
			+ "  miny:= " + MINY + "(poly);\n"
			+ "  result:=null;\n"
			+ "  result:= polygon ($$($$||minx||$$,$$||miny||$$),($$||maxx||$$, $$||miny||$$),($$||maxx||$$,$$||maxy||$$),($$||minx||$$,$$||maxy||$$)$$);\n"
			+ "  return result;\n"
			+ " END IF;\n"
			+ "END;"),
	/**
	 *
	 */
	INTERSECTIONWITHLINE2D(Language.plpgsql, "point", "path1 path, path2 path", "DECLARE \n"
			+ " intersectionPoly GEOMETRY;\n"
			+ "BEGIN\n"
			+ " intersectionPoly = ST_INTERSECTION(path1::GEOMETRY, path2::GEOMETRY)::GEOMETRY; \n"
			+ " if ST_ISEMPTY(intersectionPoly) then\n"
			+ "  RETURN NULL;\n"
			+ " else\n"
			+ "  if ST_ASTEXT(intersectionPoly) like $$POINT%$$ then \n"
			+ "   RETURN intersectionPoly::POINT;\n"
			+ "  else\n"
			+ "   RETURN ST_POINTN(intersectionPoly,1)::POINT; \n"
			+ "  END IF;\n"
			+ " END IF;\n"
			+ "END;"),
	/**
	 *
	 */
	INTERSECTIONPOINTSWITHLINE2D(Language.plpgsql, "geometry", "path1 path, path2 path", "DECLARE \n"
			+ " intersectionPoly GEOMETRY;\n"
			+ "BEGIN\n"
			+ " intersectionPoly = ST_INTERSECTION(path1::GEOMETRY, path2::GEOMETRY)::GEOMETRY; \n"
			+ " if ST_ISEMPTY(intersectionPoly) then\n"
			+ "  RETURN NULL;\n"
			+ " else\n"
			+ "  if ST_ASTEXT(intersectionPoly) like $$POINT%$$ then \n"
			+ "   RETURN intersectionPoly;\n"
			+ "  else\n"
			+ "   if ST_ASTEXT(intersectionPoly) like $$MULTIPOINT%$$ then \n"
			+ "    RETURN intersectionPoly; \n"
			+ "   END IF;\n"
			+ "  END IF;\n"
			+ " END IF;\n"
			+ "END;");

//	private final String functionName;
	private final String returnType;
	private final String parameters;
	private final String code;
	private final Language language;

	Line2DFunctions(Language language, String returnType, String parameters, String code) {
		this.language = language;
		this.returnType = returnType;
		this.parameters = parameters;
		this.code = code;
	}

	@Override
	public String toString() {
		return "DBV_LINE2DFN_" + name();
	}

	/**
	 *
	 * @param stmt
	 * @throws SQLException
	 */
	@SuppressFBWarnings(value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE",
			justification = "The strings are actually constant but made dynamically")
	public void add(Statement stmt) throws SQLException {
		Log LOG = LogFactory.getLog(Line2DFunctions.class);
		try {
			final String drop = "DROP FUNCTION " + this + "(" + this.parameters + ");";
			stmt.execute(drop);
		} catch (SQLException sqlex) {
		}
		try {
			final String createFunctionStatement = "CREATE OR REPLACE FUNCTION " + this + "(" + this.parameters + ")\n" + "    RETURNS " + this.returnType + " AS\n" + "'\n" + this.code + "'\n" + "LANGUAGE '" + language + "' IMMUTABLE;";
			stmt.execute(createFunctionStatement);
		} catch (SQLException sqlex) {
			LOG.warn("" + this + " INSTALL FAILED", sqlex);
		}
	}

}
