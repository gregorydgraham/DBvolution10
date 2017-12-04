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
package nz.co.gregs.dbvolution.internal.oracle.xe;

import java.sql.SQLException;
import java.sql.Statement;

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
	GETPOINTATINDEX(GeometryFunctions.GETPOINTATINDEX),
	/**
	 *
	 */
	NUMPOINTS("NUMBER", "p_geometry     IN MDSYS.SDO_GEOMETRY", " begin return p_geometry.sdo_ordinates.count/2; end;"),
	/**
	 *
	 */
	ASPOLY2D("MDSYS.SDO_GEOMETRY", "p_geometry     IN MDSYS.SDO_GEOMETRY", ""
			+ "   BEGIN\n"
			+ "    return MDSYS.SDO_GEOMETRY(\n"
			+ "      2003,\n"
			+ "      p_geometry.SDO_SRID,\n"
			+ "      p_geometry.SDO_POINT,\n"
			+ "      p_geometry.SDO_ELEM_INFO,\n"
			+ "      p_geometry.SDO_ORDINATES\n"
			+ "    );\n"
			+ "END;"),
	/**
	 *
	 */
	GETLNSEGMENT2DATINDEX("MDSYS.SDO_GEOMETRY", "p_geometry     IN MDSYS.SDO_GEOMETRY, p_index IN INTEGER", ""
			+ "     v_dims  PLS_INTEGER;    -- Number of dimensions in geometry\n"
			+ "     v_gtype NUMBER;         -- SDO_GTYPE of returned geometry\n"
			+ "     v_p     NUMBER;         -- Index into ordinates array\n"
			+ "     v_px1    NUMBER;         -- X of extracted point\n"
			+ "     v_py1    NUMBER;         -- Y of extracted point\n"
			+ "     v_pz1    NUMBER;         -- Z of extracted point\n"
			+ "     v_pm1    NUMBER;         -- M of extracted point\n"
			+ "     v_px2    NUMBER;         -- X of extracted point\n"
			+ "     v_py2    NUMBER;         -- Y of extracted point\n"
			+ "     v_pz2    NUMBER;         -- Z of extracted point\n"
			+ "     v_pm2    NUMBER;         -- M of extracted point\n"
			+ "     \n"
			+ "     Function ST_isMeasured( p_gtype in number )\n"
			+ "     return boolean\n"
			+ "     is\n"
			+ "     Begin\n"
			+ "       Return CASE WHEN MOD(trunc(p_gtype/100),10) = 0\n"
			+ "                   THEN False\n"
			+ "                   ELSE True\n"
			+ "                END;\n"
			+ "     End ST_isMeasured;\n"
			+ "\n"
			+ "   BEGIN\n"
			+ "     -- Get the number of dimensions from the gtype\n"
			+ "     v_dims := SUBSTR (p_geometry.SDO_GTYPE, 1, 1);\n"
			+ "     -- Verify that the point exists\n"
			+ "     -- and set index in ordinates array\n"
			+ "     IF p_index = 0\n"
			+ "        OR ABS(p_index) > p_geometry.SDO_ORDINATES.COUNT()/v_dims THEN\n"
			+ "       RETURN NULL;\n"
			+ "     ELSIF p_index <= -1 THEN\n"
			+ "       v_p := ( (p_geometry.SDO_ORDINATES.COUNT() / v_dims) + p_index ) * v_dims + 1;\n"
			+ "     ELSE\n"
			+ "       v_p := (p_index - 1) * v_dims + 1;\n"
			+ "     END IF;\n"
			+ "     IF v_p+3 > p_geometry.SDO_ORDINATES.COUNT() THEN\n"
			+ "        RETURN NULL;\n"
			+ "     END IF;\n"
			+ "     -- Extract the X and Y coordinates of the desired point\n"
			+ "     v_gtype := (v_dims*1000) + 2;\n"
			+ "     v_px1 := p_geometry.SDO_ORDINATES(v_p);\n"
			+ "     v_py1 := p_geometry.SDO_ORDINATES(v_p+1);\n"
			+ "     v_px2 := p_geometry.SDO_ORDINATES(v_p+2);\n"
			+ "     v_py2 := p_geometry.SDO_ORDINATES(v_p+3);\n"
			+ "     IF ( v_dims > 3 ) THEN\n"
			+ "       IF v_p+7 > p_geometry.SDO_ORDINATES.COUNT() THEN\n"
			+ "         RETURN NULL;\n"
			+ "       END IF;\n"
			+ "       v_pz1 := p_geometry.SDO_ORDINATES(v_p+2);\n"
			+ "       v_pm1 := p_geometry.SDO_ORDINATES(v_p+3);\n"
			+ "       v_px2 := p_geometry.SDO_ORDINATES(v_p+4);\n"
			+ "       v_py2 := p_geometry.SDO_ORDINATES(v_p+5);\n"
			+ "       v_pz2 := p_geometry.SDO_ORDINATES(v_p+6);\n"
			+ "       v_pm2 := p_geometry.SDO_ORDINATES(v_p+7);\n"
			+ "     ELSIF ( v_dims = 3 ) THEN\n"
			+ "         IF v_p+5 > p_geometry.SDO_ORDINATES.COUNT() THEN\n"
			+ "           RETURN NULL;\n"
			+ "         END IF;\n"
			+ "         IF ( ST_isMeasured(p_geometry.SDO_GTYPE) ) THEN\n"
			+ "           v_pm1 := p_geometry.SDO_ORDINATES(v_p+2);\n"
			+ "           v_px2 := p_geometry.SDO_ORDINATES(v_p+3);\n"
			+ "           v_py2 := p_geometry.SDO_ORDINATES(v_p+4);\n"
			+ "           v_pm2 := p_geometry.SDO_ORDINATES(v_p+5);\n"
			+ "         ELSE\n"
			+ "           v_pz1 := p_geometry.SDO_ORDINATES(v_p+2);\n"
			+ "           v_px2 := p_geometry.SDO_ORDINATES(v_p+3);\n"
			+ "           v_py2 := p_geometry.SDO_ORDINATES(v_p+4);\n"
			+ "           v_pz2 := p_geometry.SDO_ORDINATES(v_p+5);\n"
			+ "         END IF;\n"
			+ "     END IF;\n"
			+ "     -- Construct and return the point\n"
			+ "     RETURN CASE WHEN v_dims > 3\n"
			+ "                 THEN MDSYS.SDO_GEOMETRY(v_gtype,\n"
			+ "                                         p_geometry.SDO_SRID,\n"
			+ "                                         NULL,\n"
			+ "                                         MDSYS.SDO_ELEM_INFO_ARRAY(1,2,1),\n"
			+ "                                         MDSYS.SDO_ORDINATE_ARRAY(v_px1, v_py1, v_pz1, v_pm1,v_px2, v_py2, v_pz2, v_pm2))\n"
			+ "                 WHEN v_dims = 3\n"
			+ "                 THEN MDSYS.SDO_GEOMETRY(v_gtype,\n"
			+ "                                         p_geometry.SDO_SRID,\n"
			+ "                                         NULL,\n"
			+ "                                         MDSYS.SDO_ELEM_INFO_ARRAY(1,2,1),\n"
			+ "                                         MDSYS.SDO_ORDINATE_ARRAY(v_px1, v_py1, CASE WHEN ST_isMeasured(p_geometry.sdo_gtype) THEN v_pm1 ELSE v_pz1 END\n"
			+ "                                         ,v_px2, v_py2, CASE WHEN ST_isMeasured(p_geometry.sdo_gtype) THEN v_pm2 ELSE v_pz2 END)\n"
			+ "                                         )\n"
			+ "                 ELSE MDSYS.SDO_GEOMETRY(v_gtype,\n"
			+ "                                         p_geometry.SDO_SRID,\n"
			+ "                                         NULL,\n"
			+ "                                         MDSYS.SDO_ELEM_INFO_ARRAY(1,2,1),\n"
			+ "                                         MDSYS.SDO_ORDINATE_ARRAY(v_px1, v_py1\n"
			+ "                                         ,v_px2, v_py2)\n"
			+ "                                         )\n"
			+ "             END;\n"
			+ "END;") //	INTERSECTSLINE2D("NUMBER", "firstLine IN MDSYS.SDO_GEOMETRY, secondLine IN MDSYS.SDO_GEOMETRY", ""
	//			+ "   ln1           MDSYS.SDO_GEOMETRY;\n"
	//			+ "   ln2           MDSYS.SDO_GEOMETRY;\n"
	//			+ "   ln1_index     INTEGER := 1;\n"
	//			+ "   ln2_index     INTEGER := 1;\n"
	//			+ "BEGIN\n"
	//			+ "   --   DBMS_OUTPUT.PUT_LINE ('STARTING INTERSECTS... ');\n"
	//			+ "\n"
	//			+ "   IF (firstLine IS NULL OR secondLine IS NULL)\n"
	//			+ "   THEN\n"
	//			+ "      RETURN NULL;\n"
	//			+ "   END IF;\n"
	//			+ "   \n"
	//			+ "   ln1 := DBV_LN2D_GETLNSEGMENT2DATINDEX(firstLine, ln1_index);\n"
	//			+ "   \n"
	//			+ "   --'2 3, 3 4'\n"
	//			+ "   WHILE ln1 is not null\n"
	//			+ "   LOOP\n"
	//			+ "      ln2:= DBV_LN2D_GETLNSEGMENT2DATINDEX(secondLine, ln2_index);\n"
	//			+ "      WHILE ln2 is not null\n"
	//			+ "      LOOP\n"
	//			+ "  \n"
	//			+ "        --   DBMS_OUTPUT.PUT_LINE ('SECOND POINT: ' || pointAsText);\n"
	//			+ "  \n"
	//			+ "        IF ln2 is not null\n"
	//			+ "        THEN\n"
	//			+ "           if SDO_GEOM.RELATE(ln1, 'ANYINTERACT', ln2, 0.0000005) <> 'FALSE'\n"
	//			+ "           then\n"
	//			+ "             return 1;\n"
	//			+ "           end if;\n"
	//			+ "        END IF;\n"
	//			+ "        ln2_index := ln2_index+1;\n"
	//			+ "        ln2:= DBV_LN2D_GETLNSEGMENT2DATINDEX(secondLine, ln2_index);\n"
	//			+ "      END LOOP;\n"
	//			+ "\n"
	//			+ "      ln1_index := ln1_index+1;\n"
	//			+ "      ln1:= DBV_LN2D_GETLNSEGMENT2DATINDEX(firstLine, ln1_index);\n"
	//			+ "   END LOOP;\n"
	//			+ "\n"
	//			+ "   -- No Collision\n"
	//			+ "   RETURN 0;\n"
	//			+ "END;");
	;

	private final String returnType;
	private final String parameters;
	private final String code;

	Line2DFunctions(String returnType, String parameters, String code) {
		this.returnType = returnType;
		this.parameters = parameters;
		this.code = code;
	}

	Line2DFunctions(GeometryFunctions function) {
		this.returnType = function.returnType;
		this.parameters = function.parameters;
		this.code = function.code;
	}

	@Override
	public String toString() {
		return "DBV_LN2D_" + name();
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
