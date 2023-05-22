/*
 * Copyright 2013 Gregory Graham.
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
package nz.co.gregs.dbvolution.exceptions;

import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;

/**
 * Thrown when no conditions are detectable within the query and blank queries
 * have not been explicitly set with {@link DBQuery#setBlankQueryAllowed(boolean)
 * } or similar.
 *
 * @author Gregory Graham
 */
public class AccidentalBlankQueryException extends DBRuntimeException {

	static final long serialVersionUID = 1l;

	/**
	 * Thrown when a DBQuery or DBTable attempts to run a query without any
	 * parameters and without explicitly allowing blank queries.
	 *
	 */
	public AccidentalBlankQueryException() {
		super("Accidental Blank Query Aborted: ensure you have added all the required tables, defined all the criteria, and are using the correct allowBlankQueries() setting.");
		super.fillInStackTrace();
	}

	public AccidentalBlankQueryException(boolean blankQueryAllowed, boolean willCreateBlankQuery, boolean hasNoRawSQL, List<String> sqlOptions) {
		super("Accidental Blank Query Aborted: ensure you have added all the required tables, "
				+ "defined all the criteria, and are using the correct allowBlankQueries() setting: "
				+ "BlankQueryAllowed?" + blankQueryAllowed 
				+ " willCreateBlankQuery?" + willCreateBlankQuery 
				+ " hasNoRawSQL?" + hasNoRawSQL 
				+ " SQL: "+System.lineSeparator() + sqlOptions.get(0));
		super.fillInStackTrace();
	}

}
