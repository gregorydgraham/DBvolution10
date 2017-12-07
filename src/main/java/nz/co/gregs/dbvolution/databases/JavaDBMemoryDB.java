/*
 * Copyright 2014 gregory.graham.
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
package nz.co.gregs.dbvolution.databases;

import java.sql.SQLException;
import nz.co.gregs.dbvolution.exceptions.UnableToCreateDatabaseConnectionException;
import nz.co.gregs.dbvolution.exceptions.UnableToFindJDBCDriver;

/**
 * Use this class to work with an in-memory JavaDB.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregory.graham
 */
public class JavaDBMemoryDB extends JavaDB {

	public static final long serialVersionUID = 1l;

//	private Connection storedConnection;
	/**
	 * Creates or connects to a JavaDB in-memory instance.
	 *
	 * @param host host
	 * @param username username
	 * @param port port
	 * @param database database
	 * @param password password
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public JavaDBMemoryDB(String host, int port, String database, String username, String password) throws SQLException, UnableToCreateDatabaseConnectionException, UnableToFindJDBCDriver {
		super("jdbc:derby://" + host + ":" + port + "/memory:" + database + ";create=true", username, password);
	}

	@Override
	public JavaDBMemoryDB clone() throws CloneNotSupportedException {
		return (JavaDBMemoryDB) super.clone(); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	protected boolean persistentConnectionRequired() {
		return true;
	}

}
