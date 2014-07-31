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
package nz.co.gregs.dbvolution.databases;

import nz.co.gregs.dbvolution.DBDatabase;
import java.sql.SQLException;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.databases.definitions.InformixDBDefinition;

/**
 * A version of DBDatabase tweaked for Informix 7 and higher.
 *
 * @author Gregory Graham
 */
public class InformixDB extends DBDatabase {

    private final static String INFORMIXDRIVERNAME = "com.informix.jdbc.IfxDriver";

	/**
	 * Creates  a DBDatabase configured for Informix with the given JDBC URL, username, and password.
	 * 
	 * <p>
	 * Remember to include the Informix JDBC driver in your classpath.
	 *
	 * @param jdbcURL
	 * @param username
	 * @param password
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public InformixDB(String jdbcURL, String username, String password) throws ClassNotFoundException, SQLException {
        super(new InformixDBDefinition(), INFORMIXDRIVERNAME, jdbcURL, username, password);
        // Informix causes problems when using batched statements :(
        setBatchSQLStatementsWhenPossible(false);
    }
	/**
	 * Creates  a DBDatabase configured for Informix for the given data source.
	 * 
	 * <p>
	 * Remember to include the Informix JDBC driver in your classpath.
	 *
	 * @param dataSource
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public InformixDB(DataSource dataSource) throws ClassNotFoundException, SQLException {
        super(new InformixDBDefinition(), dataSource);
        // Informix causes problems when using batched statements :(
        setBatchSQLStatementsWhenPossible(false);
    }

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); //To change body of generated methods, choose Tools | Templates.
	}
}
