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
package nz.co.gregs.dbvolution.internal.sqlserver;

import java.sql.SQLException;
import java.sql.Statement;
import nz.co.gregs.dbvolution.datatypes.DBNumber;

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
	MAXX("numeric(" + DBNumber.getNumericPrecision() + "," + DBNumber.getNumericScale() + ")", "@poly GEOMETRY", " DECLARE \n"
			+ " @resultVal numeric(" + DBNumber.getNumericPrecision() + "," + DBNumber.getNumericScale() + "),\n"
			+ " @num integer,\n"
			+ " @i integer,\n"
			+ " @currentcoord numeric(" + DBNumber.getNumericPrecision() + "," + DBNumber.getNumericScale() + "),\n"
			+ " @pnt GEOMETRY\n"
			+ " if @poly is null begin \n"
			+ "  return(null) \n"
			+ " end\n"
			+ " else begin\n"
			+ "  set @num = @poly.STNumPoints()\n"
			+ "  set @resultVal = null\n"
			+ "  set @i = 1\n"
			+ "  WHILE @i <= @num \n"
			+ "  begin\n"
			+ "   set @pnt = @poly.STPointN(@i)\n"
			+ "   if @pnt is not null \n"
			+ "   begin \n"
			+ "    set @currentcoord = @pnt.STX\n"
			+ "    IF @resultVal is null or @resultVal < @currentcoord BEGIN\n"
			+ "     set @resultVal = @currentcoord\n"
			+ "    END\n"
			+ "   END\n"
			+ "   set @i = @i + 1\n"
			+ "  END\n"
			+ " END\n"
			+ " return(@resultVal)"),
	/**
	 *
	 */
	MAXY("numeric(" + DBNumber.getNumericPrecision() + "," + DBNumber.getNumericScale() + ")", "@poly GEOMETRY", " DECLARE \n"
			+ " @resultVal numeric(" + DBNumber.getNumericPrecision() + "," + DBNumber.getNumericScale() + "),\n"
			+ " @num integer,\n"
			+ " @i integer,\n"
			+ " @currentcoord numeric(" + DBNumber.getNumericPrecision() + "," + DBNumber.getNumericScale() + "),\n"
			+ " @pnt GEOMETRY\n"
			+ " if @poly is null begin \n"
			+ "  return(null) \n"
			+ " end\n"
			+ " else begin\n"
			+ "  set @num = @poly.STNumPoints()\n"
			+ "  set @resultVal = null\n"
			+ "  set @i = 1\n"
			+ "  WHILE @i <= @num \n"
			+ "  begin\n"
			+ "   set @pnt = @poly.STPointN(@i)\n"
			+ "   if @pnt is not null \n"
			+ "   begin \n"
			+ "    set @currentcoord = @pnt.STY\n"
			+ "    IF @resultVal is null or @resultVal < @currentcoord BEGIN\n"
			+ "     set @resultVal = @currentcoord\n"
			+ "    END\n"
			+ "   END\n"
			+ "   set @i = @i + 1\n"
			+ "  END\n"
			+ " END\n"
			+ " return(@resultVal)"),
	/**
	 *
	 */
	MINX("numeric(" + DBNumber.getNumericPrecision() + "," + DBNumber.getNumericScale() + ")", "@poly GEOMETRY", " DECLARE \n"
			+ " @resultVal numeric(" + DBNumber.getNumericPrecision() + "," + DBNumber.getNumericScale() + "),\n"
			+ " @num integer,\n"
			+ " @i integer,\n"
			+ " @currentcoord numeric(" + DBNumber.getNumericPrecision() + "," + DBNumber.getNumericScale() + "),\n"
			+ " @pnt GEOMETRY\n"
			+ " if @poly is null begin \n"
			+ "  return(null) \n"
			+ " end\n"
			+ " else begin\n"
			+ "  set @num = @poly.STNumPoints()\n"
			+ "  set @resultVal = null\n"
			+ "  set @i = 1\n"
			+ "  WHILE @i <= @num \n"
			+ "  begin\n"
			+ "   set @pnt = @poly.STPointN(@i)\n"
			+ "   if @pnt is not null \n"
			+ "   begin \n"
			+ "    set @currentcoord = @pnt.STX\n"
			+ "    IF @resultVal is null or @resultVal > @currentcoord BEGIN\n"
			+ "     set @resultVal = @currentcoord\n"
			+ "    END\n"
			+ "   END\n"
			+ "   set @i = @i + 1\n"
			+ "  END\n"
			+ " END\n"
			+ " return(@resultVal)"),
	/**
	 *
	 */
	MINY("numeric(" + DBNumber.getNumericPrecision() + "," + DBNumber.getNumericScale() + ")", "@poly GEOMETRY", " DECLARE \n"
			+ " @resultVal numeric(" + DBNumber.getNumericPrecision() + "," + DBNumber.getNumericScale() + "),\n"
			+ " @num integer,\n"
			+ " @i integer,\n"
			+ " @currentcoord numeric(" + DBNumber.getNumericPrecision() + "," + DBNumber.getNumericScale() + "),\n"
			+ " @pnt GEOMETRY\n"
			+ " if @poly is null begin \n"
			+ "  return(null) \n"
			+ " end\n"
			+ " else begin\n"
			+ "  set @num = @poly.STNumPoints()\n"
			+ "  set @resultVal = null\n"
			+ "  set @i = 1\n"
			+ "  WHILE @i <= @num \n"
			+ "  begin\n"
			+ "   set @pnt = @poly.STPointN(@i)\n"
			+ "   if @pnt is not null \n"
			+ "   begin \n"
			+ "    set @currentcoord = @pnt.STY\n"
			+ "    IF @resultVal is null or @resultVal > @currentcoord BEGIN\n"
			+ "     set @resultVal = @currentcoord\n"
			+ "    END\n"
			+ "   END\n"
			+ "   set @i = @i + 1\n"
			+ "  END\n"
			+ " END\n"
			+ " return(@resultVal)");

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
		return "dbo.DBV_LINE2DFN_" + name();
	}

	/**
	 *
	 * @param stmt
	 * @throws SQLException
	 */
	public void add(Statement stmt) throws SQLException {
		try {
			stmt.execute("DROP FUNCTION " + this + ";");
		} catch (SQLException sqlex) {
			;
		}
		if (!this.code.isEmpty()) {
			final String createFn = "CREATE FUNCTION " + this + "(" + this.parameters + ")\n"
					+ "    RETURNS " + this.returnType
					+ " AS BEGIN\n" + "\n" + this.code
					+ "\n END;";
			stmt.execute(createFn);
		}
	}

}
