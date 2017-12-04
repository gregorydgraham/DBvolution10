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
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class UnexpectedNumberOfRowsException extends Exception {

	private static final long serialVersionUID = 1;
	private long expectedRows;
	private long actualRows;

	/**
	 * The method requires an exact number of rows to be returned, and the actual
	 * number was wrong.
	 *
	 * <p>
	 * There has probably been a mistake.
	 *
	 * @param expected expected
	 * @param actual actual
	 * @param message message
	 * @param cause cause
	 */
	public UnexpectedNumberOfRowsException(long expected, long actual, String message, Exception cause) {
		super(message, cause);
		this.expectedRows = expected;
		this.actualRows = actual;
	}

	/**
	 * The method requires an exact number of rows to be returned, and the actual
	 * number was wrong.
	 *
	 * <p>
	 * There has probably been a mistake.
	 *
	 * @param expected expected
	 * @param message message
	 * @param actual actual
	 */
	public UnexpectedNumberOfRowsException(long expected, long actual, String message) {
		this(expected, actual, message, null);

	}

	/**
	 * The method requires an exact number of rows to be returned, and the actual
	 * number was wrong.
	 *
	 * <p>
	 * There has probably been a mistake.
	 *
	 * @param expected expected
	 * @param actual actual
	 */
	public UnexpectedNumberOfRowsException(long expected, long actual) {
		this(expected, actual, "Unexpected Number Of Rows Found: expected " + expected + " but found " + actual, null);

	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the expectedRows
	 */
	public long getExpectedRows() {
		return expectedRows;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the actualRows
	 */
	public long getActualRows() {
		return actualRows;
	}

}
