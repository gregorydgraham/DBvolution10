/*
 * Copyright 2019 Gregory Graham.
 *
 * Commercial licenses are available, please contact info@gregs.co.nz for details.
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
package nz.co.gregs.dbvolution.generic;

import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.databases.MySQLDB;
import org.testcontainers.containers.MySQLContainer;

/**
 *
 * @author gregorygraham
 */
public class MySQLContainerDB extends MySQLDB{
	
	private static final long serialVersionUID = 1l;
	protected final MySQLContainer storedContainer;

	public static MySQLContainerDB getInstance() {
		String instance = "";
		String database = "";
		/*
		'TZ=Pacific/Auckland' sets the container timezone to where I do my test (TODO set to server location)
		 */
		MySQLContainer container = new MySQLContainer<>();
		container.withEnv("TZ", "Pacific/Auckland");
		//			container.withEnv("TZ", ZoneId.systemDefault().getId());
		container.start();
		String password = container.getPassword();
		String username = container.getUsername();
		String url = container.getJdbcUrl();
		String host = container.getContainerIpAddress();
		Integer port = container.getFirstMappedPort();
		System.out.println("nz.co.gregs.dbvolution.generic.AbstractTest.MSSQLServerContainerDB.getInstance()");
		System.out.println("URL: " + url);
		System.out.println("" + host + " : " + instance + " : " + database + " : " + port + " : " + username + " : " + password);
		try {
			MySQLContainerDB dbdatabase = new MySQLContainerDB(container);
			return dbdatabase;
		} catch (SQLException ex) {
			Logger.getLogger(MySQLContainerDB.class.getName()).log(Level.SEVERE, null, ex);
			throw new RuntimeException("Unable To Create MySQL Database in Docker Container", ex);
		}
	}

	public MySQLContainerDB(MySQLContainer container) throws SQLException {
		super(container.getJdbcUrl(), container.getUsername(), container.getPassword());
		this.storedContainer = container;
	}
	
}
