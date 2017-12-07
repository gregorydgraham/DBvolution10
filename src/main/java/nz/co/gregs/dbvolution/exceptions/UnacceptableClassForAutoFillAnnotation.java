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

import nz.co.gregs.dbvolution.annotations.AutoFillDuringQueryIfPossible;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;

/**
 * Thrown when the developer has attempted to use a POJO rather than a DBRow in
 * the {@link AutoFillDuringQueryIfPossible} annotation.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class UnacceptableClassForAutoFillAnnotation extends DBRuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 *
	 * Thrown when the developer has attempted to use a POJO rather than a DBRow
	 * in the {@link AutoFillDuringQueryIfPossible} annotation.
	 *
	 * @param field
	 * @param requiredClass
	 */
	public UnacceptableClassForAutoFillAnnotation(PropertyWrapper field, Class<?> requiredClass) {
		super("Unable To AutoFill Given Type: field " + field.qualifiedJavaName() + " is a " + requiredClass.getCanonicalName() + " but needs to be a DBRow sub-class.");
	}

}
