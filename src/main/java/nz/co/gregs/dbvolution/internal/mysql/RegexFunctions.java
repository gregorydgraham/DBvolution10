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
package nz.co.gregs.dbvolution.internal.mysql;

import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * @author gregorygraham
 */
public enum RegexFunctions {

	/**
	 *
	 */
	STARTPOS("int(11)", "p_regex varchar(250),p_str TEXT", ""
			+ "declare v_endpos int;\n"
			+ "declare v_startpos int;\n"
			+ "declare v_len int;\n"
			+ "\n"
			+ "set v_endpos=1;\n"
			+ "set v_len=1+char_length( p_str );\n"
			+ "while (( substr( p_str, 1, v_endpos) REGEXP p_regex)=0 and (v_endpos<v_len))\n"
			+ "do\n"
			+ "set v_endpos = v_endpos + 1;\n"
			+ "end while;\n"
			+ "\n"
			+ "return v_endpos;\n");
	/**
	 *
	 */

	private final String returnType;
	private final String parameters;
	private final String code;

	RegexFunctions(String returnType, String parameters, String code) {
		this.returnType = returnType;
		this.parameters = parameters;
		this.code = code;
	}

	@Override
	public String toString() {
		return "DBV_REGEX_" + name();
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
					+ "\n  BEGIN\n" + "\n" + this.code
					+ "\n END;";
			System.out.println(""+createFn);
			stmt.execute(createFn);
		}
	}

}
