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
 * Translates between a target object's property type
 * and the type used by DBvolution.
 * 
 * <p> On the DBvolution side, the following types are supported:
 * <ul>
 * <li> String
 * <li> Integer, Long
 * <li> Float, Double
 * <li> arbitrary object type (requires the use of DBJavaObject)
 * </ul>
 * 
 * <p> If the target object's property is a DBvolution type, then
 * the supported types are the same as above. Otherwise
 * any object type can be used that is assignable with the type of the
 * property this adaptor is used on. 
 * 
 * @param <T> the type of the property on the target object
 * @param <D> the database side type: the type of the property once translated for DBvolution use
 */
public interface DBTypeAdaptor<T, D> {
	/**
	 * Null values must be handled correctly.
	 * @param dbvValue
	 * @return
	 */
	public T fromDatabaseValue(D dbvValue);

	/**
	 * Null values must be handled correctly.
	 * @param objectValue
	 * @return
	 */
	public D toDatabaseValue(T objectValue);
}