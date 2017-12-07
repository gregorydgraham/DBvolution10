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

/**
 * Sometimes a database has a datatype that DBvolution has not yet supported,
 * this is one of those times.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class UnknownJavaSQLTypeException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	private int unknownJavaSQLType;

	/**
	 * Sometimes a database has a datatype that DBvolution has not yet supported,
	 * this is one of those times.
	 *
	 * @param string	string
	 */
	public UnknownJavaSQLTypeException(String string) {
		super(string);
	}

	/**
	 * Sometimes a database has a datatype that DBvolution has not yet supported,
	 * this is one of those times.
	 *
	 * @param string string
	 * @param columnType columnType
	 */
	public UnknownJavaSQLTypeException(String string, int columnType) {
		this(string);
		unknownJavaSQLType = columnType;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the unknownJavaSQLType
	 */
	public int getUnknownJavaSQLType() {
		return unknownJavaSQLType;
	}

}
