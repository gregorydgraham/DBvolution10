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
package nz.co.gregs.dbvolution.reflection;

import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBIntegerEnum;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.DBStringEnum;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public interface EncodingInterpreter {

	/**
	 * Set the supplied QueryableDatatype to the value supplied.
	 *
	 * <p>
	 * Depending on the QueryableDatatype, you may be able to accomplish this by
	 * calling {@link QueryableDatatype#setValue(java.lang.Object) }.
	 *
	 * @param qdt the DBRow field that needs to be set
	 * @param value the encoded string version of the value required.
	 */
	public void setValue(QueryableDatatype<?> qdt, String value);

	/**
	 * Given the entire encoded string, split the string into the individual
	 * parameters and return an array of string, one for each parameter.
	 *
	 * <p>
	 * For instance if the encoded string is
	 * "myclass-myfield=myvalue&amp;otherclass-fieldb=valueb" and the parameter
	 * separator is "&amp;", then this method should return the equivalent of
	 * {@code new String[]{"myclass-myfield=myvalue","otherclass-fieldb=valueb"}}
	 *
	 * @param encodedTablesPropertiesAndValues the entire encoded string
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the entire encoded string separated into individuals parameter
	 * strings in an array.
	 */
	public String[] splitParameters(String encodedTablesPropertiesAndValues);

	/**
	 * From the entire parameter string separate out the name of the DBRow class.
	 *
	 * <p>
	 * For instance, if the encoded parameter is "myclass-myproperty=myvalue" and
	 * the separator between class and field is "-", then this method should
	 * return {@code "myclass"}.
	 *
	 * <p>
	 * You may need to supply the full canonical name of the the class as the
	 * ClassLoader may not be able to find it by name. In the example above it
	 * might be better to return "com.mycompany.mypackage.myclass" instead.
	 *
	 * @param parameter the entire encoded parameter including class, property,
	 * and value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the preferably full, canonical name of the class.
	 */
	public String getDBRowClassName(String parameter);

	/**
	 * From the entire parameter string separate out the name of the DBRow's
	 * property that needs to be set.
	 *
	 * <p>
	 * For instance, if the encoded parameter is "myclass-myproperty=myvalue", the
	 * class/property separator is "-", and the property/value separator is "=",
	 * then this method should return {@code "myproperty"}.
	 *
	 * <p>
	 * The property is the Java field or bean used within the DBRow class for a
	 * database column value.
	 *
	 * @param parameter the entire encoded parameter including class, property,
	 * and value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the Java name of the relevant field or bean..
	 */
	public String getPropertyName(String parameter);

	/**
	 * From the entire parameter string separate out the value that needs to be
	 * set.
	 *
	 * <p>
	 * For instance, if the encoded parameter is "myclass-myproperty=myvalue", the
	 * class/property separator is "-", and the property/value separator is "=",
	 * then this method should return {@code "myvalue"}.
	 *
	 * @param parameter the entire encoded parameter including class, property,
	 * and value
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the value to set the classes property to.
	 */
	public String getPropertyValue(String parameter);

	/**
	 * Encode the rows in the collection.
	 *
	 * <p>
	 * The encoding should allow the interpreter to retrieve a fully qualified
	 * DBRow class name, a relevant property on that class, and the value the
	 * class should be set to.
	 *
	 * <p>
	 * For example an implementation that uses "&amp;", "-", and "=" to separate
	 * the parts might produce
	 * {@code "myclass-myfield=myvalue&amp;otherclass-fieldb=valueb"}.
	 *
	 * @param rows
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an encoded string of the rows
	 */
	String encode(DBRow... rows);

	/**
	 * Decode the value and store it into the field
	 *
	 * <p>
	 * Used by
	 * {@link #setValue(nz.co.gregs.dbvolution.datatypes.QueryableDatatype, java.lang.String) setValues}
	 * to set boolean values used in the query.
	 *
	 * @param value
	 * @param field
	 */
	void decodeValue(String value, DBBoolean field);

	/**
	 * Decode the value and store it into the field
	 *
	 * <p>
	 * Used by
	 * {@link #setValue(nz.co.gregs.dbvolution.datatypes.QueryableDatatype, java.lang.String) setValues}
	 * to set boolean values used in the query.
	 *
	 * @param value
	 * @param field
	 */
	void decodeValue(String value, DBDate field);

	/**
	 * Decode the value and store it into the field
	 *
	 * <p>
	 * Used by
	 * {@link #setValue(nz.co.gregs.dbvolution.datatypes.QueryableDatatype, java.lang.String) setValues}
	 * to set boolean values used in the query.
	 *
	 * @param value
	 * @param field
	 */
	void decodeValue(String value, DBIntegerEnum<?> field) throws NumberFormatException;

	/**
	 * Decode the value and store it into the field
	 *
	 * <p>
	 * Used by
	 * {@link #setValue(nz.co.gregs.dbvolution.datatypes.QueryableDatatype, java.lang.String) setValues}
	 * to set boolean values used in the query.
	 *
	 * @param value
	 * @param field
	 */
	void decodeValue(String value, DBInteger field) throws NumberFormatException;

	/**
	 * Decode the value and store it into the field
	 *
	 * <p>
	 * Used by
	 * {@link #setValue(nz.co.gregs.dbvolution.datatypes.QueryableDatatype, java.lang.String) setValues}
	 * to set boolean values used in the query.
	 *
	 * @param value
	 * @param field
	 */
	void decodeValue(String value, DBNumber field) throws NumberFormatException;

	/**
	 * Decode the value and store it into the field
	 *
	 * <p>
	 * Used by
	 * {@link #setValue(nz.co.gregs.dbvolution.datatypes.QueryableDatatype, java.lang.String) setValues}
	 * to set boolean values used in the query.
	 *
	 * @param value
	 * @param field
	 */
	void decodeValue(String value, DBStringEnum<?> field);

	/**
	 * Decode the value and store it into the field
	 *
	 * <p>
	 * Used by
	 * {@link #setValue(nz.co.gregs.dbvolution.datatypes.QueryableDatatype, java.lang.String) setValues}
	 * to set boolean values used in the query.
	 *
	 * @param value
	 * @param field
	 */
	void decodeValue(String value, DBString field);

}
