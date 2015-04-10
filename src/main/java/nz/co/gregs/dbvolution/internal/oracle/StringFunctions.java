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
 * @author gregorygraham
 */
public enum StringFunctions {
	
	SUBSTRINGBEFORE("VARCHAR", "sourceText VARCHAR, rightHandSide VARCHAR", "BEGIN CASE WHEN POSITION(rightHandSide IN (sourceText)) > 0 THEN  RETURN SUBSTRING((sourceText) FROM 0 + 1 FOR POSITION(rightHandSide IN (sourceText)) - 1 - 0);  ELSE RETURN NULL; END CASE; END;"),
	SUBSTRINGAFTER("VARCHAR", "sourceText VARCHAR, leftHandSide VARCHAR", " BEGIN CASE WHEN POSITION(leftHandSide IN (sourceText)) > 0 THEN  RETURN SUBSTRING((sourceText) FROM POSITION(leftHandSide IN (sourceText)) + 1 FOR  CHAR_LENGTH( (sourceText) )  - POSITION(leftHandSide IN (sourceText)))  ELSE RETURN NULL END CASE; END;");
	
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
		return "DBV_STRINGFN_"+name();
	}

	public void add(Statement stmt) throws SQLException {
		try {
			stmt.execute("DROP FUNCTION " + this + "("+parameters+");");
		} catch (SQLException sqlex) {
			;
		}
		stmt.execute("CREATE OR REPLACE FUNCTION "+this+"("+this.parameters+")\n" +"    RETURN "+this.returnType+" AS\n" + this.code+"\n;");
		
	}

	
}
