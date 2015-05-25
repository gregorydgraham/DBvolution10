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

/**
 * Thrown when a type is encountered that is not valid in the context in which
 * it is found.
 */
public class InvalidDeclaredTypeException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * Thrown when a type is encountered that is not valid in the context in which
	 * it is found.
	 */
	public InvalidDeclaredTypeException() {
		super();
	}

	/**
	 * Thrown when a type is encountered that is not valid in the context in which
	 * it is found.
	 *
	 * @param string	string
	 */
	public InvalidDeclaredTypeException(String string) {
		super(string);
	}

	/**
	 * Thrown when a type is encountered that is not valid in the context in which
	 * it is found.
	 *
	 * @param cause	cause
	 */
	public InvalidDeclaredTypeException(Throwable cause) {
		super(cause);
	}

	/**
	 * Thrown when a type is encountered that is not valid in the context in which
	 * it is found.
	 *
	 * @param message message
	 * @param cause cause
	 */
	public InvalidDeclaredTypeException(String message, Throwable cause) {
		super(message, cause);
	}
}
