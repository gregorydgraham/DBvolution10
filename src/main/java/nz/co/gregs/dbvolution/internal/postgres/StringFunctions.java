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
	SUBSTRINGBEFORE(Language.sql, "text", "sourceText text, rightHandSide text", "select (CASE WHEN POSITION(rightHandSide IN (sourceText)::VARCHAR) > 0 THEN  SUBSTRING((sourceText)::VARCHAR FROM 0 + 1 FOR POSITION(rightHandSide IN (sourceText)::VARCHAR) - 1 - 0)  ELSE $$$$ END);"),
	/**
	 *
	 */
	SUBSTRINGAFTER(Language.sql, "text", "sourceText text, leftHandSide text", " select (CASE WHEN POSITION(leftHandSide IN (sourceText)::VARCHAR) > 0 THEN  SUBSTRING((sourceText)::VARCHAR FROM POSITION(leftHandSide IN (sourceText)::VARCHAR) + 1 FOR  CHAR_LENGTH( (sourceText)::VARCHAR )  - POSITION(leftHandSide IN (sourceText)::VARCHAR))  ELSE $$$$ END);");

//	private final String functionName;
	private final Language language;
	private final String returnType;
	private final String parameters;
	private final String code;

	StringFunctions(Language language, String returnType, String parameters, String code) {
//		this.functionName = functionName;
		this.language = language;
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
	@SuppressFBWarnings(value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE",
			justification = "The strings are actually constant but made dynamically")
	public void add(Statement stmt) throws SQLException {
		try {
			final String drop = "DROP FUNCTION " + this + "(" + parameters + ");";
			stmt.execute(drop);
		} catch (SQLException sqlex) {
			;
		}
		final String add = "CREATE OR REPLACE FUNCTION " + this + "(" + this.parameters + ")\n" + "    RETURNS " + this.returnType + " AS\n" + "'\n" + this.code + "'\n" + "LANGUAGE '" + this.language.name() + "';";
		stmt.execute(add);

	}

}
