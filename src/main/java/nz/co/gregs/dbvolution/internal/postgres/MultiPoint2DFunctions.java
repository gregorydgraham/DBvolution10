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

import nz.co.gregs.dbvolution.internal.FeatureAdd;

/**
 *
 *
 * @author gregorygraham
 */
public enum MultiPoint2DFunctions implements FeatureAdd {
	//MULTIPOINT(2 3,3 4)
	//[ ( 2, 3 ), ( 3 , 4 ) ]

	/**
	 *
	 */
	ASLINE2D(Language.plpgsql, "path", "path1 geometry", "DECLARE \n"
			+ " tempText text;\n"
			+ "BEGIN\n"
			+ " tempText = regexp_replace(ST_ASTEXT(path1), $$MULTIPOINT$$, $$$$, $$g$$); \n"
			+ " tempText = regexp_replace(tempText, $$[ ]*,[ ]*$$, $$), ($$, $$g$$); \n"
			+ " tempText = regexp_replace(tempText, $$([ -+0-9.]*)[ ]+([-+0-9.]*)$$, $$\\1, \\2$$, $$g$$); \n"
			+ " tempText = $$[$$||tempText||$$]$$; \n"
			+ "   RETURN tempText;\n"
			+ "END;");

//	private final String functionName;
	private final String returnType;
	private final String parameters;
	private final String code;
	private final Language language;

	MultiPoint2DFunctions(Language language, String returnType, String parameters, String code) {
		this.language = language;
		this.returnType = returnType;
		this.parameters = parameters;
		this.code = code;
	}

	@Override
	public String toString() {
		return "DBV_MPOINT2DFN_" + name();
	}

//	/**
//	 *
//	 * @param stmt the database
//	 * @throws ExceptionDuringDatabaseFeatureSetup database errors
//	 */
//	@SuppressFBWarnings(value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE",
//			justification = "The strings are actually constant but made dynamically")
//	public void add(Statement stmt) throws ExceptionDuringDatabaseFeatureSetup {
//		try {
//			final String drop = "DROP FUNCTION " + this + "(" + this.parameters + ");";
//			stmt.execute(drop);
//		} catch (SQLException sqlex) {
//			;
//		}
//		final String createFunctionStatement = "CREATE OR REPLACE FUNCTION " + this + "(" + this.parameters + ")\n" + "    RETURNS " + this.returnType + " AS\n" + "'\n" + this.code + "'\n" + "LANGUAGE '" + language + "' IMMUTABLE;";
//		try {
//			stmt.execute(createFunctionStatement);
//		} catch (Exception ex) {
//			throw new ExceptionDuringDatabaseFeatureSetup("FAILED TO ADD FEATURE: " + name(), ex);
//		}
//	}

	@Override
	public String[] dropAndCreateSQL() {
		if (!this.code.isEmpty()) {
			return new String[]{
				"DROP FUNCTION " + this + "(" + this.parameters + ");",
				"CREATE OR REPLACE FUNCTION " + this + "(" + this.parameters + ")\n" + "    RETURNS " + this.returnType + " AS\n" + "'\n" + this.code + "'\n" + "LANGUAGE '" + language + "' IMMUTABLE;"
			};
		}
		return new String[]{};
	}

}
