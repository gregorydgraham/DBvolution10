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

import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 * Thrown when DBvolution is unable to access a field it needs.
 *
 * <p>
 * A lot of reflection is used in DBV, please ensure that the fields are
 * publicly accessible and non-null.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class UnableToCopyQueryableDatatypeException extends DBRuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * Thrown when DBvolution is unable to access a field it needs.
	 *
	 * <p>
	 * A lot of reflection is used in DBV, please ensure that the fields are
	 * publicly accessible and non-null.
	 *
	 * @param qdt qdt
	 * @param ex ex
	 */
	public UnableToCopyQueryableDatatypeException(QueryableDatatype<?> qdt, IllegalAccessException ex) {
		super("Unable To Copy " + qdt.getClass().getSimpleName() + " Due To " + ex.getClass().getSimpleName() + ": Please ensure that all fields are accessible.", ex);
	}

}
