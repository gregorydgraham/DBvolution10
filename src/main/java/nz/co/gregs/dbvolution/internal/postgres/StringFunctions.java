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

import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * @author gregorygraham
 */
public enum StringFunctions {

	/**
	 *
	 */
	SUBSTRINGBEFORE(Language.sql, "text", "sourcetext text, righthandside text", "select (CASE WHEN POSITION(righthandside IN (sourcetext)::VARCHAR) > 0 THEN  SUBSTRING((sourcetext)::VARCHAR FROM 0 + 1 FOR POSITION(righthandside IN (sourcetext)::VARCHAR) - 1 - 0)  ELSE $$$$ END);"),

	/**
	 *
	 */
	SUBSTRINGAFTER(Language.sql, "text", "sourcetext text, lefthandside text", " select (CASE WHEN POSITION(lefthandside IN (sourcetext)::VARCHAR) > 0 THEN  SUBSTRING((sourcetext)::VARCHAR FROM POSITION(lefthandside IN (sourcetext)::VARCHAR) + 1 FOR  CHAR_LENGTH( (sourcetext)::VARCHAR )  - POSITION(lefthandside IN (sourcetext)::VARCHAR))  ELSE $$$$ END);");

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
