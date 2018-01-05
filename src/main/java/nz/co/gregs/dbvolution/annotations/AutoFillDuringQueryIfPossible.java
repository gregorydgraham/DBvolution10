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
package nz.co.gregs.dbvolution.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import nz.co.gregs.dbvolution.DBRow;

/**
 * Designates a DBRow derived field that should be connected to related objects
 * during query execution.
 *
 * <p>
 * This attempts to restore the usual Java hierarchy model after completing the
 * query, rather than using DBvolutions builtin facilities. It does this by
 * post-processing the created DBRow and fills the AutoFill fields with the
 * objects that are related.
 *
 * <p>
 * There are several limitations to this method:
 * <ol>
 * <li>The annotated field must be a {@link DBRow} subclass, an array of the
 * same, or a Collection of the same.</li>
 * <li>If the annotated field is a List or other Collection, then the
 * annotation's {@link #requiredClass()
 * } value must be set to a DBRow-derived class.</li>
 * <li>The field will only be filled if there are instances of the designated
 * type in the query, and there are
 * {@link DBRow#getRelatedInstancesFromQuery(nz.co.gregs.dbvolution.actions.DBQueryable, nz.co.gregs.dbvolution.DBRow) instances related to the current object}.</li>
 * <li>Filling is only performed based on the class of the field/requiredClass
 * and not associated with any foreign key or other relationship. Use a unique
 * subclass of the original DBRow subclass to avoid confusion where there are
 * multiple FKs or versions of the designated types.</li>
 * </ol>
 *
 * <p>
 * If Java supported annotations with Object fields, some of these issues may
 * have been avoided.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface AutoFillDuringQueryIfPossible {

	/**
	 * Specifies the class required to fill this field.
	 *
	 * <p>
	 * Only used for Collections like List due to type-erasure.
	 *
	 * <p>
	 * Required to resolve the expected type for the field. Be careful that the
	 * annotation and the field use compatible types.
	 *
	 * <p>
	 * Must be a DBRow subclass.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the Class of DBRow to be filled.
	 */
	Class<? extends DBRow> requiredClass() default DBRow.class;

}
