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

import java.sql.SQLException;
import javax.sql.DataSource;

/**
 * Thrown when the database is inaccessible due to a myriad of reasons.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class UnableToCreateDatabaseConnectionException extends DBRuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * Thrown when the database is inaccessible due to a myriad of reasons.
	 *
	 * @param jdbcURL jdbcURL
	 * @param noConnection noConnection
	 * @param username username
	 */
	public UnableToCreateDatabaseConnectionException(String jdbcURL, String username, SQLException noConnection) {
		super("Unable to create a Database Connection: please check the database URL, username, and password, and that the appropriate libaries have been supplied: URL=" + jdbcURL + " USERNAME=" + username, noConnection);
	}

	/**
	 * Thrown when the database is inaccessible due to a myriad of reasons.
	 *
	 * @param dataSource dataSource
	 * @param noConnection noConnection
	 */
	public UnableToCreateDatabaseConnectionException(DataSource dataSource, SQLException noConnection) {
		super("Unable to create a Database Connection: please check the database URL, username, and password, and that the appropriate libaries have been supplied: DATASOURCE=" + dataSource.toString(), noConnection);
	}

}
