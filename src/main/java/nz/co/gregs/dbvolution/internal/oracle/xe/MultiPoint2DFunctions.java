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
public enum MultiPoint2DFunctions {

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
	ASLINE2D("MDSYS.SDO_GEOMETRY", "p_geometry in MDSYS.SDO_GEOMETRY", ""
			+ "   BEGIN\n"
			+ "    return MDSYS.SDO_GEOMETRY(\n"
			+ "      2002,\n"
			+ "      p_geometry.SDO_SRID,\n"
			+ "      p_geometry.SDO_POINT,\n"
			+ "      MDSYS.SDO_ELEM_INFO_ARRAY(1,2,1),\n"
			+ "      p_geometry.SDO_ORDINATES\n"
			+ "    );\n"
			+ "END;"),
	/**
	 *
	 */
	ASTEXT("VARCHAR", "p_geometry     IN MDSYS.SDO_GEOMETRY",
			"     v_dims  PLS_INTEGER;    -- Number of dimensions in geometry\n"
			+ "     v_gtype NUMBER;         -- SDO_GTYPE of returned geometry\n"
			+ "     v_count NUMBER;         -- SDO_GTYPE of returned geometry\n"
			+ "     v_p     NUMBER := 1;    -- Index into ordinates array\n"
			+ "     v_px    NUMBER;         -- X of extracted point\n"
			+ "     v_py    NUMBER;         -- Y of extracted point\n"
			+ "     v_pz    NUMBER;         -- Z of extracted point\n"
			+ "     v_pm    NUMBER;         -- M of extracted point\n"
			+ "     v_sep   VARCHAR(10):='';\n"
			+ "     -- separator between the points\n"
			+ "     v_wkt   VARCHAR(20000) := 'MULTIPOINT (';\n"
			+ "     -- the Well Known Text to return\n"
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
			+ "     v_gtype := (v_dims*1000) + 1;\n"
			+ "     v_count := p_geometry.sdo_ordinates.count;\n"
			+ "     \n"
			+ "     WHILE (v_p <= v_count)\n"
			+ "     LOOP\n"
			+ "       v_px := p_geometry.SDO_ORDINATES(v_p);\n"
			+ "       v_py := p_geometry.SDO_ORDINATES(v_p+1);\n"
			+ "       IF ( v_dims > 3 ) THEN\n"
			+ "         v_pm := p_geometry.SDO_ORDINATES(v_p+3);\n"
			+ "         v_pz := p_geometry.SDO_ORDINATES(v_p+2);\n"
			+ "         v_wkt := v_wkt||v_sep||'('||v_px||' '||v_py||' '||v_pz||' '||v_pm||')';\n"
			+ "       ELSIF ( v_dims = 3 ) THEN\n"
			+ "           IF ( ST_isMeasured(p_geometry.SDO_GTYPE) ) THEN\n"
			+ "             v_pm := p_geometry.SDO_ORDINATES(v_p+2);\n"
			+ "             v_wkt := v_wkt||v_sep||'('||v_px||' '||v_py||' '||v_pm||')';\n"
			+ "           ELSE\n"
			+ "             v_pz := p_geometry.SDO_ORDINATES(v_p+2);\n"
			+ "             v_wkt := v_wkt||v_sep||'('||v_px||' '||v_py||' '||v_pz||')';\n"
			+ "           END IF;\n"
			+ "       ELSE\n"
			+ "           v_wkt := v_wkt||v_sep||'('||v_px||' '||v_py||')';\n"
			+ "       END IF;\n"
			+ "      \n"
			+ "      v_sep := ', ';\n"
			+ "      v_p := v_p+v_dims;\n"
			+ "    END LOOP;\n"
			+ "    return v_wkt||')';\n"
			+ "-- return 'MULTIPOINT ((), (), ...)';\n"
			+ "end;"
	);

	private final String returnType;
	private final String parameters;
	private final String code;

	MultiPoint2DFunctions(String returnType, String parameters, String code) {
		this.returnType = returnType;
		this.parameters = parameters;
		this.code = code;
	}

	MultiPoint2DFunctions(GeometryFunctions function) {
		this.returnType = function.returnType;
		this.parameters = function.parameters;
		this.code = function.code;
	}

	@Override
	public String toString() {
		return "DBV_MP2D_" + name();
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
