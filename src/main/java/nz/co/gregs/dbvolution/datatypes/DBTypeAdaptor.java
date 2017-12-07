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
package nz.co.gregs.dbvolution.datatypes;

/**
 * Translates between a target object's property type and the type used by
 * DBvolution.
 *
 * <p>
 * On the DBvolution side, the following types are supported:
 * <ul>
 * <li> String
 * <li> Integer, Long
 * <li> Float, Double
 * <li> Boolean
 * <li> arbitrary object type (requires the use of DBJavaObject)
 * </ul>
 *
 * <p>
 * If the target object's property is a DBvolution type, then the supported
 * types are the same as above. Otherwise any object type can be used that is
 * assignable with the type of the property this adaptor is used on.
 *
 * @param <J> the Java side type of the property on the target Java object
 * @param <D> the database side type: the type of the property once translated
 * for DBvolution use
 */
public interface DBTypeAdaptor<J, D> {

	/**
	 * Converts a value from the database into the datatype expected by the Java.
	 * <p>
	 * Null values must be handled correctly.</p>
	 *
	 * @param dbValue	dbValue
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return The database value transformed into the Java value
	 */
	public J fromDatabaseValue(D dbValue);

	/**
	 * Translates the Java value into the database equivalent.
	 *
	 * <p>
	 * Null values must be handled correctly.</p>
	 *
	 * @param javaValue	javaValue
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return The Java value transformed into the database value
	 */
	public D toDatabaseValue(J javaValue);
}
