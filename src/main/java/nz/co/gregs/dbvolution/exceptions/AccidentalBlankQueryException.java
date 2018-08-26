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

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class AccidentalBlankQueryException extends RuntimeException {

	static final long serialVersionUID = 1l;

	/**
	 * Thrown when a DBQuery or DBTable attempts to run a query without any
	 * parameters and without explicitly allowing blank queries.
	 *
	 */
	public AccidentalBlankQueryException() {
		super("Accidental Blank Query Aborted: ensure you have added all the required tables, defined all the criteria, and are using the correct allowBlankQueries() setting.");
	}

	public AccidentalBlankQueryException(boolean blankQueryAllowed, boolean willCreateBlankQuery, boolean hasNoRawSQL) {
		super("Accidental Blank Query Aborted: ensure you have added all the required tables, defined all the criteria, and are using the correct allowBlankQueries() setting: BlankQueryAllowed?" + blankQueryAllowed + " willCreateBlankQuery?" + willCreateBlankQuery + " hasNoRawSQL?" + hasNoRawSQL);
	}

}
