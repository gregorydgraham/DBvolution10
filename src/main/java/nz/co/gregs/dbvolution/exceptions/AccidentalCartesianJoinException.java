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
@SuppressWarnings("serial")
public class AccidentalCartesianJoinException extends RuntimeException {

	/**
	 * Thrown when a query will create a Cartesian Join and cartesian joins have
	 * not been explicitly permitted.
	 *
	 * <p>
	 * Cartesian joins are generally a mistake.
	 */
	public AccidentalCartesianJoinException() {
		super("Accidental Cartesian Join Aborted: ensure you have added all the required tables, defined all primary and foreign keys, and are using the correct allowCartesianJoin() setting.");
	}

	/**
	 * Thrown when a query will create a Cartesian Join and cartesian joins have
	 * not been explicitly permitted.
	 *
	 * <p>
	 * Cartesian joins are generally a mistake.
	 *
	 * @param sqlString	sqlString
	 */
	public AccidentalCartesianJoinException(String sqlString) {
		super("Accidental Cartesian Join Aborted: ensure you have added all the required tables, defined all primary and foreign keys, and are using the correct allowCartesianJoin() setting. SQL => " + sqlString);
	}
}
