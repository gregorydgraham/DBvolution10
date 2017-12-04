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

import nz.co.gregs.dbvolution.DBRow;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class AccidentalUpdateOfUndefinedRowException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * Thrown when attempting to update an undefined DBRow.
	 *
	 * @param row row
	 */
	public AccidentalUpdateOfUndefinedRowException(DBRow row) {
		super("Accidental Update Of Undefined Row: Only rows that exist on the database already can be updated. Please use only rows from the database or insert the row and retreive it before updating.");
	}

}
