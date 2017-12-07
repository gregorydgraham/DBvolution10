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

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public enum MigrationFunctions {

	/**
	 *
	 */
	FINDFIRSTNUMBER("nvarchar(max)", "@string as NVARCHAR(max)", "declare @numberSign as nvarchar(1) = '';\n"
			+ "declare @decimalPoint as nvarchar(1) = '';\n"
			+ "declare @numberAndRight as nvarchar(max) = '';\n"
			+ "declare @beforeTheDecimal as nvarchar(max) = '';\n"
			+ "declare @afterTheDecimal as nvarchar(max) = '';\n"
			+ "declare @startOfDigits int = patindex('%[0-9]%',@string);\n"
			+ "declare @firstNonNumber as int = 0;\n"
			+ "declare @return as nvarchar(max) = '';\n"
			+ "\n"
			+ "if (@startOfDigits<>0) \n"
			+ "BEGIN\n"
			+ "	if (@startOfDigits > 1 and substring(@string, @startOfDigits-1, 1) = '-')\n"
			+ "	BEGIN\n"
			+ "		set @numberSign = '-';\n"
			+ "	END\n"
			+ "	\n"
			+ "	set @numberAndRight = substring(@string, @startOfDigits, len(@string));\n"
			+ "	set @firstNonNumber = patindex('%[^0-9]%', @numberAndRight);\n"
			+ "	set @beforeTheDecimal = left(@numberAndRight, @firstNonNumber-1);\n"
			+ "	set @afterTheDecimal = substring(@numberAndRight, @firstNonNumber, len(@numberAndRight));\n"
			+ "\n"
			+ "	if (left(@afterTheDecimal, 1)='.' AND patindex('%[0-9]%', @afterTheDecimal)=2)\n"
			+ "	BEGIN\n"
			+ "		set @decimalPoint = '.';\n"
			+ "		set @afterTheDecimal = substring(@afterTheDecimal, 2, len(@afterTheDecimal));\n"
			+ "		set @firstNonNumber = patindex('%[^0-9]%', @afterTheDecimal)\n"
			+ "		set @afterTheDecimal = substring(@afterTheDecimal, 1, case when @firstNonNumber > 0 then @firstNonNumber-1 else len(@afterTheDecimal) end);\n"
			+ "	END\n"
			+ "	ELSE\n"
			+ "	BEGIN\n"
			+ "		set @decimalPoint = '';\n"
			+ "		set @afterTheDecimal = '';\n"
			+ "	END\n"
			+ "\n"
			+ "END\n"
			+ "	if (@beforeTheDecimal is not null and @beforeTheDecimal <> '')\n"
			+ "		set @return = @numberSign+@beforeTheDecimal+@decimalPoint+@afterTheDecimal;\n"
			+ "	else \n"
			+ "		set @return = null;\n"
			+ "	return @return;"),
	FINDFIRSTINTEGER("nvarchar(max)", "@string as NVARCHAR(max)", "declare @numberSign as nvarchar(1) = '';\n"
			+ "declare @decimalPoint as nvarchar(1) = '';\n"
			+ "declare @numberAndRight as nvarchar(max) = '';\n"
			+ "declare @beforeTheDecimal as nvarchar(max) = '';\n"
			+ "declare @afterTheDecimal as nvarchar(max) = '';\n"
			+ "declare @startOfDigits int = patindex('%[0-9]%',@string);\n"
			+ "declare @firstNonNumber as int = 0;\n"
			+ "declare @return as nvarchar(max) = '';\n"
			+ "\n"
			+ "if (@startOfDigits<>0) \n"
			+ "BEGIN\n"
			+ "	if (@startOfDigits > 1 and substring(@string, @startOfDigits-1, 1) = '-')\n"
			+ "	BEGIN\n"
			+ "		set @numberSign = '-';\n"
			+ "	END\n"
			+ "	\n"
			+ "	set @numberAndRight = substring(@string, @startOfDigits, len(@string));\n"
			+ "	set @firstNonNumber = patindex('%[^0-9]%', @numberAndRight);\n"
			+ "	set @beforeTheDecimal = left(@numberAndRight, @firstNonNumber-1);\n"
			+ "END\n"
			+ "	if (@beforeTheDecimal is not null and @beforeTheDecimal <> '')\n"
			+ "		set @return = @numberSign+@beforeTheDecimal;\n"
			+ "	else \n"
			+ "		set @return = null;\n"
			+ "	return @return;");

	private final String returnType;
	private final String parameters;
	private final String code;

	MigrationFunctions(String returnType, String parameters, String code) {
		this.returnType = returnType;
		this.parameters = parameters;
		this.code = code;
	}

	@Override
	public String toString() {
		return "dbo.DBV_MIGRATIONFN_" + name();
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
