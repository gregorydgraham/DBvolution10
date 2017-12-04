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
package nz.co.gregs.dbvolution.internal.oracle;

import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public enum StringFunctions {

	/**
	 *
	 */
	SUBSTRINGBEFORE("VARCHAR", "sourceText VARCHAR, rightHandSide VARCHAR",
			"BEGIN\n"
			+ "   CASE\n"
			+ "      WHEN INSTR (sourceText, rightHandSide) > 0\n"
			+ "      THEN\n"
			+ "         RETURN SUBSTR (sourceText, 1, INSTR (sourceText, rightHandSide) - 1);\n"
			+ "      ELSE\n"
			+ "         RETURN '';\n"
			+ "   END CASE;\n"
			+ "END;"),
	/**
	 *
	 */
	SUBSTRINGAFTER("VARCHAR", "sourceText VARCHAR, leftHandSide VARCHAR",
			"BEGIN\n"
			+ "   CASE\n"
			+ "      WHEN INSTR (sourceText, (leftHandSide)) > 0\n"
			+ "      THEN\n"
			+ "         RETURN SUBSTR (\n"
			+ "                   (sourceText),\n"
			+ "                   INSTR (sourceText, (leftHandSide)) + LENGTH (leftHandSide),\n"
			+ "                     LENGTH ( (sourceText))\n"
			+ "                   - INSTR (sourceText, (leftHandSide))\n"
			+ "                   + LENGTH (leftHandSide));\n"
			+ "      ELSE\n"
			+ "         RETURN NULL;\n"
			+ "   END CASE;\n"
			+ "END;");

//	private final String functionName;
	private final String returnType;
	private final String parameters;
	private final String code;

	StringFunctions(String returnType, String parameters, String code) {
		this.returnType = returnType;
		this.parameters = parameters;
		this.code = code;
	}

	@Override
	public String toString() {
		return "DBV_STRINGFN_" + name();
	}

	/**
	 *
	 * @param stmt
	 * @throws SQLException
	 */
	public void add(Statement stmt) throws SQLException {
		try {
			stmt.execute("DROP FUNCTION " + this + "(" + parameters + ");");
		} catch (SQLException sqlex) {
			;
		}
		stmt.execute("CREATE OR REPLACE FUNCTION " + this + "(" + this.parameters + ")\n" + "    RETURN " + this.returnType + " AS\n" + this.code);

	}

}
