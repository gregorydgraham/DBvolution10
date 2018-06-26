/*
 * Copyright 2018 gregorygraham.
 *
 * This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. 
 * To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/4.0/ 
 * or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.
 * 
 * You are free to:
 *     Share - copy and redistribute the material in any medium or format
 *     Adapt - remix, transform, and build upon the material
 * 
 *     The licensor cannot revoke these freedoms as long as you follow the license terms.               
 *     Under the following terms:
 *                 
 *         Attribution - 
 *             You must give appropriate credit, provide a link to the license, and indicate if changes were made. 
 *             You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.
 *         NonCommercial - 
 *             You may not use the material for commercial purposes.
 *         ShareAlike - 
 *             If you remix, transform, or build upon the material, 
 *             you must distribute your contributions under the same license as the original.
 *         No additional restrictions - 
 *             You may not apply legal terms or technological measures that legally restrict others from doing anything the 
 *             license permits.
 * 
 * Check the Creative Commons website for any details, legalese, and updates.
 */
package nz.co.gregs.dbvolution.databases;

import java.sql.SQLException;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.databases.definitions.MSSQLServer2012DBDefinition;

public class MSSQLServer2012DB extends MSSQLServerDB {

	public static final long serialVersionUID = 1l;

	public MSSQLServer2012DB(DataSource ds) throws SQLException {
		super(new MSSQLServer2012DBDefinition(), ds);
	}

	public MSSQLServer2012DB(String driverName, String jdbcURL, String username, String password) throws SQLException {
		super(new MSSQLServer2012DBDefinition(), driverName, jdbcURL, username, password);
	}

	public MSSQLServer2012DB(String jdbcURL, String username, String password) throws SQLException {
		super(new MSSQLServer2012DBDefinition(), jdbcURL, username, password);
	}

	public MSSQLServer2012DB(String hostname, String instanceName, String databaseName, int portNumber, String username, String password) throws SQLException {
		super(new MSSQLServer2012DBDefinition(), hostname, instanceName, databaseName, portNumber, username, password);
	}

	public MSSQLServer2012DB(String driverName, String hostname, String instanceName, String databaseName, int portNumber, String username, String password) throws SQLException {
		super(new MSSQLServer2012DBDefinition(), driverName, hostname, instanceName, databaseName, portNumber, username, password);
	}

}
