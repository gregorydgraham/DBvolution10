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

import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;

/**
 * Primary keys are sometimes required, this is one of those times.
 *
 * <p>
 * Indicate the Primary Key of a table by adding the
 * {@link DBPrimaryKey &#64;DBPrimaryKey} annotation to the appropriate field.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
@SuppressWarnings("serial")
public class UndefinedPrimaryKeyException extends RuntimeException {

	/**
	 * Primary Keys are sometimes required, this is one of those times.
	 *
	 * <p>
	 * Indicate the Primary Key of a table by adding the
	 * {@link DBPrimaryKey &#64;DBPrimaryKey} annotation to the appropriate field.
	 *
	 */
	public UndefinedPrimaryKeyException() {
	}

	/**
	 * Primary keys are sometimes required.
	 *
	 * <p>
	 * Indicate the Primary Key of a table by adding the
	 * {@link DBPrimaryKey &#64;DBPrimaryKey} annotation to the appropriate field.
	 *
	 * @param <E> a DBRow type
	 * @param thisClass thisClass
	 */
	public <E extends DBRow> UndefinedPrimaryKeyException(Class<E> thisClass) {
		super("Primary Key Field Not Defined: Please define the primary key field of " + thisClass.getSimpleName() + " using the @" + DBPrimaryKey.class.getSimpleName() + " annotation.");
	}

	/**
	 * Primary keys are sometimes required.
	 *
	 * <p>
	 * Indicate the Primary Key of a table by adding the
	 * {@link DBPrimaryKey &#64;DBPrimaryKey} annotation to the appropriate field.
	 *
	 * @param <E> A DBRow type
	 * @param thisRow thisRow
	 */
	public <E extends DBRow> UndefinedPrimaryKeyException(E thisRow) {
		this(thisRow.getClass());
	}
}
