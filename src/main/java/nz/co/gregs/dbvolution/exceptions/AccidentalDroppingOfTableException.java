/*
 * Copyright 2014 Gregory Graham.
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
public class AccidentalDroppingOfTableException extends DBRuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * Thrown when attempting to drop a table without first explicitly allowing
	 * it.
	 *
	 */
	public AccidentalDroppingOfTableException() {
		super("Accidental Dropping Of Table Detected: Dropping a table is virtually never the solution to your problem.  If, however, you really want to do this, enable dropping of tables with the DBDatabase.preventDroppingOfTables(bool) method.");
	}
}
