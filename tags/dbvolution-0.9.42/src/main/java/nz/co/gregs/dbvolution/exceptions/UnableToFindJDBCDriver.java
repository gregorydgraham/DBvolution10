/*
 * Copyright 2014 greg.
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
 * Please supply a JDBC Driver for your database on the classpath.
 *
 * @author Gregory Graham
 */
public class UnableToFindJDBCDriver extends DBRuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * Please supply a JDBC Driver for your database on the classpath.
	 *
	 * @param driverName
	 * @param noDriver
	 */
	public UnableToFindJDBCDriver(String driverName, ClassNotFoundException noDriver) {
		super("No Driver Found: please check the driver name is correct and the appropriate libaries have been supplied: DRIVERNAME=" + driverName, noDriver);
	}

}
