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
package nz.co.gregs.dbvolution.internal.oracle.xe;

import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregory.graham
 */
public enum GeometryFunctions {

	/**
	 *
	 */
	ASTEXT("VARCHAR", "p_geometry     IN MDSYS.SDO_GEOMETRY", ""
			+ "     v_dims  PLS_INTEGER;    -- Number of dimensions in geometry\n"
			+ "     v_spatialtype  VARCHAR(2);\n"
			+ "     -- The subtype of the geometry\n"
			+ "     v_gtype NUMBER;         -- SDO_GTYPE of returned geometry\n"
			+ "     v_count NUMBER;         -- SDO_GTYPE of returned geometry\n"
			+ "     v_p     NUMBER := 1;    -- Index into ordinates array\n"
			+ "     v_px    NUMBER;         -- X of extracted point\n"
			+ "     v_py    NUMBER;         -- Y of extracted point\n"
			+ "     v_pz    NUMBER;         -- Z of extracted point\n"
			+ "     v_pm    NUMBER;         -- M of extracted point\n"
			+ "     v_sep   VARCHAR(10):='';\n"
			+ "     -- separator between the points\n"
			+ "     v_pointstart   VARCHAR(10):='(';\n"
			+ "     -- the beginning wrapper around points\n"
			+ "     v_wktend   VARCHAR(10):=')';\n"
			+ "     -- the finishing wrapper around the geometry\n"
			+ "     v_pointend   VARCHAR(10):=')';\n"
			+ "     -- the finishing wrapper around points\n"
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
			+ "   BEGIN\n     "
			+ "     IF p_geometry is NULL THEN\n"
			+ "       RETURN NULL;\n"
			+ "     END IF;"
			+ "     -- Get the number of dimensions from the gtype\n"
			+ "     v_dims := SUBSTR (p_geometry.SDO_GTYPE, 1, 1);\n"
			+ "     v_spatialtype := SUBSTR (p_geometry.SDO_GTYPE, 3, 2);\n"
			+ "     v_gtype := (v_dims*1000) + 1;\n"
			+ "     v_count := p_geometry.sdo_ordinates.count;\n"
			+ "     \n"
			+ "     case v_spatialtype\n"
			+ "     when '01' then v_wkt := 'POINT (';v_pointstart:='';v_pointend:='';\n"
			+ "     when '02' then v_wkt := 'LINESTRING (';v_pointstart:='';v_pointend:='';\n"
			+ "     when '03' then v_wkt := 'POLYGON ((';v_pointstart:='';v_pointend:='';v_wktend:='))';\n"
			+ "     when '04' then v_wkt := 'COLLECTION (';\n"
			+ "     when '05' then v_wkt := 'MULTIPOINT (';v_pointstart:='(';v_pointend:=')';\n"
			+ "     when '06' then v_wkt := 'MULTILINE (';v_pointstart:='';v_pointend:='';\n"
			+ "     when '07' then v_wkt := 'MULTIPOLYGON (';v_pointstart:='';v_pointend:='';\n"
			+ "     else v_wkt := 'UNKNOWN_GEOMETRY (';\n"
			+ "     end case;\n"
			+ "     \n"
			+ "     WHILE (v_p <= v_count)\n"
			+ "     LOOP\n"
			+ "       v_px := p_geometry.SDO_ORDINATES(v_p);\n"
			+ "       v_py := p_geometry.SDO_ORDINATES(v_p+1);\n"
			+ "       IF ( v_dims > 3 ) THEN\n"
			+ "         v_pm := p_geometry.SDO_ORDINATES(v_p+3);\n"
			+ "         v_pz := p_geometry.SDO_ORDINATES(v_p+2);\n"
			+ "         v_wkt := v_wkt||v_sep||v_pointstart||v_px||' '||v_py||' '||v_pz||' '||v_pm||v_pointend;\n"
			+ "       ELSIF ( v_dims = 3 ) THEN\n"
			+ "           IF ( ST_isMeasured(p_geometry.SDO_GTYPE) ) THEN\n"
			+ "             v_pm := p_geometry.SDO_ORDINATES(v_p+2);\n"
			+ "             v_wkt := v_wkt||v_sep||v_pointstart||v_px||' '||v_py||' '||v_pm||v_pointend;\n"
			+ "           ELSE\n"
			+ "             v_pz := p_geometry.SDO_ORDINATES(v_p+2);\n"
			+ "             v_wkt := v_wkt||v_sep||v_pointstart||v_px||' '||v_py||' '||v_pz||v_pointend;\n"
			+ "           END IF;\n"
			+ "       ELSE\n"
			+ "           v_wkt := v_wkt||v_sep||v_pointstart||v_px||' '||v_py||v_pointend;\n"
			+ "       END IF;\n"
			+ "      \n"
			+ "      v_sep := ', ';\n"
			+ "      v_p := v_p+v_dims;\n"
			+ "    END LOOP;\n"
			+ "    return v_wkt||v_wktend;\n"
			+ "-- return 'MULTIPOINT ((), (), ...)';\n"
			+ "end;"),
	/**
	 *
	 */
	GETPOINTATINDEX("MDSYS.SDO_GEOMETRY", "p_geometry     IN MDSYS.SDO_GEOMETRY,\n"
			+ "                          p_point_number IN NUMBER DEFAULT 1", ""
			+ "      v_dims  PLS_INTEGER;    -- Number of dimensions in geometry\n"
			+ "     v_gtype NUMBER;         -- SDO_GTYPE of returned geometry\n"
			+ "     v_p     NUMBER;         -- Index into ordinates array\n"
			+ "     v_px    NUMBER;         -- X of extracted point\n"
			+ "     v_py    NUMBER;         -- Y of extracted point\n"
			+ "     v_pz    NUMBER;         -- Z of extracted point\n"
			+ "     v_pm    NUMBER;         -- M of extracted point\n"
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
			+ "     IF p_point_number = 0\n"
			+ "        OR ABS(p_point_number) > p_geometry.SDO_ORDINATES.COUNT()/v_dims THEN\n"
			+ "       RETURN NULL;\n"
			+ "     ELSIF p_point_number <= -1 THEN\n"
			+ "       v_p := ( (p_geometry.SDO_ORDINATES.COUNT() / v_dims) + p_point_number ) * v_dims + 1;\n"
			+ "     ELSE\n"
			+ "       v_p := (p_point_number - 1) * v_dims + 1;\n"
			+ "     END IF;\n"
			+ "     -- Extract the X and Y coordinates of the desired point\n"
			+ "     v_gtype := (v_dims*1000) + 1;\n"
			+ "     v_px := p_geometry.SDO_ORDINATES(v_p);\n"
			+ "     v_py := p_geometry.SDO_ORDINATES(v_p+1);\n"
			+ "     IF ( v_dims > 3 ) THEN\n"
			+ "       v_pm := p_geometry.SDO_ORDINATES(v_p+3);\n"
			+ "       v_pz := p_geometry.SDO_ORDINATES(v_p+2);\n"
			+ "     ELSIF ( v_dims = 3 ) THEN\n"
			+ "         IF ( ST_isMeasured(p_geometry.SDO_GTYPE) ) THEN\n"
			+ "           v_pm := p_geometry.SDO_ORDINATES(v_p+2);\n"
			+ "         ELSE\n"
			+ "           v_pz := p_geometry.SDO_ORDINATES(v_p+2);\n"
			+ "         END IF;\n"
			+ "     END IF;\n"
			+ "     -- Construct and return the point\n"
			+ "     RETURN CASE WHEN v_dims > 3\n"
			+ "                 THEN MDSYS.SDO_GEOMETRY(v_gtype,\n"
			+ "                                         p_geometry.SDO_SRID,\n"
			+ "                                         NULL,\n"
			+ "                                         MDSYS.SDO_ELEM_INFO_ARRAY(1,1,1),\n"
			+ "                                         MDSYS.SDO_ORDINATE_ARRAY(v_px, v_py, v_pz, v_pm))\n"
			+ "                 ELSE MDSYS.SDO_GEOMETRY(v_gtype,\n"
			+ "                                         p_geometry.SDO_SRID,\n"
			+ "                                         MDSYS.SDO_POINT_TYPE (v_px, v_py,\n"
			+ "                                         CASE WHEN ST_isMeasured(p_geometry.sdo_gtype) THEN v_pm ELSE v_pz END),\n"
			+ "                                         NULL,\n"
			+ "                                         NULL)\n"
			+ "             END;\n"
			+ "END;\n");

	final String returnType;
	final String parameters;
	final String code;

	GeometryFunctions(String returnType, String parameters, String code) {
		this.returnType = returnType;
		this.parameters = parameters;
		this.code = code;
	}

	@Override
	public String toString() {
		return "DBV_GEOM_" + name();
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
