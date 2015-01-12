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
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.databases.definitions.MSSQLServerDBDefinition;

/**
 * A DBDatabase object tweaked to work with Microsoft SQL Server.
 * 
 * <p>
 * Remember to include the MS SQL Server JDBC driver in your classpath.
 * 
 * @author Malcolm Lett
 * @author Gregory Graham
 */
public class MSSQLServerDB extends DBDatabase {
    public final static String SQLSERVERDRIVERNAME = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    public final static String JTDSDRIVERNAME = "net.sourceforge.jtds.jdbc.Driver";
	public final static int DEFAULT_PORT_NUMBER = 1433;

	/**
	 * Creates a {@link DBDatabase } instance for the MS SQL Server data source.
	 *
	 * @param ds	 ds	
	 */
	public MSSQLServerDB(DataSource ds) {
        super(new MSSQLServerDBDefinition(), ds);
    }

	/**
	 * Creates a {@link DBDatabase } instance for MS SQL Server using the driver, JDBC URL, username, and password.
	 *
	 * @param driverName driverName
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 */
    public MSSQLServerDB(String driverName, String jdbcURL, String username, String password) {
        super(new MSSQLServerDBDefinition(), driverName, jdbcURL, username, password);
    }
    
	/**
	 * Creates a {@link DBDatabase } instance for MS SQL Server using the JDBC URL, username, and password.
	 * 
	 * <p>
	 * The default driver will be used for the connection.
	 *
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 */
    public MSSQLServerDB(String jdbcURL, String username, String password) {
        super(new MSSQLServerDBDefinition(), SQLSERVERDRIVERNAME, jdbcURL, username, password);
    }
	
    public MSSQLServerDB(String hostname, String instanceName, String databaseName, int portNumber, String username, String password) {
        super(
				new MSSQLServerDBDefinition(), 
				SQLSERVERDRIVERNAME, 
				"jdbc:sqlserver://"+hostname+(instanceName!=null?"\\"+instanceName:"")+":"+portNumber+";"+(databaseName==null?"":"databaseName="+databaseName+";"), 
				username, 
				password
		);
    }
	
	//jdbc:sqlserver://[serverName[\instanceName][:portNumber]][;property=value[;property=value]]

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); //To change body of generated methods, choose Tools | Templates.
	}

}
