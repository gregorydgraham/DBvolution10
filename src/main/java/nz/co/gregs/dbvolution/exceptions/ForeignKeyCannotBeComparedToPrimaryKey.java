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

import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 * Thrown when there is a mismatch between the datatype of a DBRow's primary key
 * and another's foreign key reference to it.
 *
 * <p>
 * That is to say a DBPrimaryKey field has been declared as, for instance,
 * DBInteger but the DBForeignKey elsewhere is associated with a, for instance,
 * DBString.
 *
 * <p>
 * Generally this means the foreign key field needs to be changed to the correct
 * datatype.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class ForeignKeyCannotBeComparedToPrimaryKey extends DBRuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * Thrown when there is a mismatch between the datatype of a DBRow's primary
	 * key and another's foreign key reference to it.
	 *
	 * @param ex ex
	 * @param targetPK targetPK
	 * @param source source
	 * @param target target
	 * @param sourceFK sourceFK
	 */
	public ForeignKeyCannotBeComparedToPrimaryKey(Exception ex, RowDefinition source, PropertyWrapper sourceFK, RowDefinition target, PropertyWrapper targetPK) {
		super("Unable To Construct An Expression Representing The Foreign Key Relationship From "
				+ source.getClass().getSimpleName() + ":" + sourceFK.javaName()
				+ " To " + target.getClass().getSimpleName() + ":" + targetPK.javaName() + ": Check that the 2 fields have similar and comparable datatypes or remove the @DBForeignKey annotation",
				ex);
	}

}
