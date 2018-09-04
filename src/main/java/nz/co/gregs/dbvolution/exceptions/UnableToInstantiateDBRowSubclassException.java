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
 * DBvolution needed to create an instance of your DBRow but was unable to do
 * so.
 *
 * <p>
 * Please ensure all DBReports have a public, argument-less, default
 * constructor.
 *
 */
public class UnableToInstantiateDBRowSubclassException extends DBRuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * DBvolution needed to create an instance of your DBRow but was unable to do
	 * so.
	 *
	 * <p>
	 * Please ensure all DBReports have a public, argument-less, default
	 * constructor.
	 *
	 * @param requiredDBRow requiredDBRow
	 * @param cause cause
	 */
	public UnableToInstantiateDBRowSubclassException(Class<? extends DBRow> requiredDBRow, Throwable cause) {
		super("Unable To Create " + requiredDBRow.getSimpleName() + ": Please ensure that the constructor of  " + requiredDBRow.getSimpleName() + " has no arguments, throws no exceptions, and is public. If you are using an Inner Class, make sure the inner class is \"public static\" as well. Also check that all field names are unique.", cause);
	}

}
