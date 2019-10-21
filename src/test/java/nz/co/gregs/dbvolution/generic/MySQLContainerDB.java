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
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.databases.DatabaseConnectionSettings;
import nz.co.gregs.dbvolution.databases.MySQLDB;
import nz.co.gregs.dbvolution.databases.jdbcurlinterpreters.MySQLURLInterpreter;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.utility.MountableFile;

/**
 *
 * @author gregorygraham
 */
public class MySQLContainerDB extends MySQLDB {

	private static final long serialVersionUID = 1l;
	protected final MySQLContainer storedContainer;

	public static MySQLContainerDB getInstance() {
		/*
		'TZ=Pacific/Auckland' sets the container timezone to where I do my test (TODO set to server location)
		 */
		MySQLContainer container = (MySQLContainer) new MySQLContainer()
				.withDatabaseName("some_database")
				// there is a problem with the config file in the image so add our own
				.withCopyFileToContainer(MountableFile.forClasspathResource("testMySQL.cnf"), "/etc/mysql/conf.d/tstMySQL.cnf")
				// set the log consumer so we see some output
				.withLogConsumer(new Consumer<OutputFrame>() {
					// use an anonymous inner class because otherwise we get only an Object no an OutputFrame
					@Override
					public void accept(OutputFrame t) {
						System.out.print("MYSQL CONTAINER: " + t.getUtf8String());
					}
				});
		container.withEnv("TZ", "Pacific/Auckland");
		//			container.withEnv("TZ", ZoneId.systemDefault().getId());
		container.start();

		String url = container.getJdbcUrl();
		System.out.println("nz.co.gregs.dbvolution.generic.AbstractTest.MSSQLServerContainerDB.getInstance()");
		System.out.println("URL: " + url);

		// The test container doesn't use SSL so we need to turn that off
		MySQLURLInterpreter interpreter = new MySQLURLInterpreter();
		DatabaseConnectionSettings settings = interpreter.generateSettings(url);
		settings.addExtra("useSSL", "false");

		// set the database name because apparently it's not in the URL
		settings.setDatabaseName(container.getDatabaseName());

		// set the username and password so we can log in.
		settings.setUsername(container.getUsername());
		settings.setPassword(container.getPassword());

		System.out.println("FINAL URL: " + interpreter.generateJDBCURL(settings));
		try {
			// create the actual dbdatabase 
			MySQLContainerDB dbdatabase = new MySQLContainerDB(container, settings);
			return dbdatabase;
		} catch (SQLException ex) {
			Logger.getLogger(MySQLContainerDB.class.getName()).log(Level.SEVERE, null, ex);
			throw new RuntimeException("Unable To Create MySQL Database in Docker Container", ex);
		}
	}

	public MySQLContainerDB(MySQLContainer storedContainer, DatabaseConnectionSettings dcs) throws SQLException {
		super(dcs);
		this.storedContainer = storedContainer;
	}

	@Override
	public synchronized void stop() {
		super.stop();
		storedContainer.stop();
	}

}
