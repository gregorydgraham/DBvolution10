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
package nz.co.gregs.dbvolution.exceptions;

import nz.co.gregs.dbvolution.columns.ColumnProvider;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class ColumnProvidedMustBeAForeignKey extends DBRuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * Creates an exception to indicate that the column provided is not a Foreign
	 * Key and it should be.
	 *
	 * @param keyToFollow
	 */
	public ColumnProvidedMustBeAForeignKey(ColumnProvider keyToFollow) {
		super("Please Provide A Foreign key Column: the column provided, " + keyToFollow.getColumn().getPropertyWrapper().javaName() + "/" + keyToFollow.getColumn().getPropertyWrapper().columnName() + ", is not a foreign key.  Please provide a foreign key or add an appropriate foreign key to the definition of this field.");
	}

}
