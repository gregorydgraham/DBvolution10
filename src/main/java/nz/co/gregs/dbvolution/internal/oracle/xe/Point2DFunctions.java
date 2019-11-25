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

import nz.co.gregs.dbvolution.internal.FeatureAdd;

/**
 *
 *
 * @author gregorygraham
 */
public enum Point2DFunctions implements FeatureAdd {

	ASTEXT("VARCHAR", "p_geometry     IN MDSYS.SDO_GEOMETRY",
			""
			+ "     v_dims  PLS_INTEGER;    -- Number of dimensions in geometry\n"
			+ "     v_gtype NUMBER;         -- SDO_GTYPE of returned geometry\n"
			+ "     v_px    NUMBER;         -- X of extracted point\n"
			+ "     v_py    NUMBER;         -- Y of extracted point\n"
			+ "     v_pz    NUMBER;         -- Z of extracted point\n"
			+ "     v_pm    NUMBER;         -- M of extracted point\n"
			+ "     v_wkt   VARCHAR(20000);\n"
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
			+ "     IF (p_geometry IS NULL) THEN       \n"
			+ "       RETURN NULL;\n"
			+ "     END IF;\n"
			+ "     -- Get the number of dimensions from the gtype\n"
			+ "     v_dims := SUBSTR (p_geometry.SDO_GTYPE, 1, 1);\n"
			+ "     v_gtype := (v_dims*1000) + 1;\n"
			+ "     if (p_geometry.sdo_ordinates IS NOT NULL) THEN\n"
			+ "       v_px := p_geometry.SDO_ORDINATES(1);\n"
			+ "       v_py := p_geometry.SDO_ORDINATES(2);\n"
			+ "       IF ( v_dims > 3 ) THEN\n"
			+ "         v_pz := p_geometry.SDO_ORDINATES(3);\n"
			+ "         v_pm := p_geometry.SDO_ORDINATES(4);\n"
			+ "         v_wkt := 'POINT ('||v_px||' '||v_py||' '||v_pz||' '||v_pm||')';\n"
			+ "       ELSIF ( v_dims = 3 ) THEN\n"
			+ "           IF ( ST_isMeasured(p_geometry.SDO_GTYPE) ) THEN\n"
			+ "             v_pm := p_geometry.SDO_ORDINATES(3);\n"
			+ "             v_wkt := 'POINT ('||v_px||' '||v_py||' '||v_pm||')';\n"
			+ "           ELSE\n"
			+ "             v_pz := p_geometry.SDO_ORDINATES(3);\n"
			+ "             v_wkt := 'POINT ('||v_px||' '||v_py||' '||v_pz||')';\n"
			+ "           END IF;\n"
			+ "       ELSE\n"
			+ "           v_wkt := 'POINT ('||v_px||' '||v_py||')';\n"
			+ "       END IF;\n"
			+ "    ELSIF (p_geometry.sdo_point IS NOT NULL) THEN\n"
			+ "      v_px := p_geometry.SDO_POINT.X;\n"
			+ "      v_py := p_geometry.SDO_POINT.Y;\n"
			+ "      IF ( v_dims = 3 ) THEN\n"
			+ "           IF ( ST_isMeasured(p_geometry.SDO_GTYPE) ) THEN\n"
			+ "             v_pm := p_geometry.SDO_ORDINATES(3);\n"
			+ "             v_wkt := 'POINT ('||v_px||' '||v_py||' '||v_pm||')';\n"
			+ "           ELSE\n"
			+ "             v_pz := p_geometry.SDO_ORDINATES(3);\n"
			+ "             v_wkt := 'POINT ('||v_px||' '||v_py||' '||v_pz||')';\n"
			+ "           END IF;\n"
			+ "      ELSE\n"
			+ "           v_wkt := 'POINT ('||v_px||' '||v_py||')';\n"
			+ "      END IF;\n"
			+ "    END IF;\n"
			+ "    return v_wkt;\n"
			+ "-- return 'POINT (x y)';\n"
			+ "end;"
	);

	private final String returnType;
	private final String parameters;
	private final String code;

	Point2DFunctions(String returnType, String parameters, String code) {
		this.returnType = returnType;
		this.parameters = parameters;
		this.code = code;
	}

	Point2DFunctions(GeometryFunctions function) {
		this.returnType = function.returnType;
		this.parameters = function.parameters;
		this.code = function.code;
	}

	@Override
	public String toString() {
		return "DBV_PT2D_" + name();
	}

	public String toSQLString(String parameters) {
		return toString() + "(" + parameters + ")";
	}

	@Override
	public String[] createSQL() {
		if (!this.code.isEmpty()) {
			final String create = "CREATE OR REPLACE FUNCTION " + this + "(" + this.parameters + ")\n"
					+ "    RETURN " + this.returnType
					+ " AS \n" + "\n" + this.code;
			return new String[]{create};
		}
		return new String[]{};
	}
}
