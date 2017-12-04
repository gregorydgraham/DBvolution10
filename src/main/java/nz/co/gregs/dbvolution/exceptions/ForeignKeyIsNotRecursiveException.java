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
public class ForeignKeyIsNotRecursiveException extends DBRuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * The column needs to both be a foreign key and a foreign key to the
	 * table/DBRow that this column belongs to.
	 *
	 * @param keyToFollow
	 */
	public ForeignKeyIsNotRecursiveException(ColumnProvider keyToFollow) {
		super("The Foreign Key Provided Should be Recursive: the field \"" + keyToFollow.getColumn().getPropertyWrapper().javaName() + "\" should be a foreign key to this DBRow class or a subclass of it.  Please provide a different Foreign Key or correct the @DBForeignKey annotation.");
	}

}
