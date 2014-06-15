/*
 * Copyright 2013 gregorygraham.
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

/**
 * A Placeholder DBType Used During DBRow Generation To Capture Unknown Types.
 *
 * Occasionally DBvolution encounters an unusual or undocumented datatype that
 * isn't supported. This annotation helps capture the information required to
 * add support for the datatype.
 *
 * Please report this annotation with the associated value, the database engine
 * used, and the actual datatype in the database if known and I will add support if possible.
 *
 * @author gregorygraham
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface DBUnknownJavaSQLType {

	/**
	 *
	 * @return the java.sql.Types constant that has not been recognised
	 */
	int value();
}
