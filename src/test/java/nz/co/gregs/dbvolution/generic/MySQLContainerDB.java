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
import java.time.Duration;
import java.time.ZoneId;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.databases.MySQLDB;
import nz.co.gregs.dbvolution.databases.settingsbuilders.MySQLSettingsBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.OutputFrame;
//import org.testcontainers.utility.MountableFile;

/**
 *
 * @author gregorygraham
 */
public class MySQLContainerDB extends MySQLDB {

	static final Log LOG = LogFactory.getLog(MySQLContainerDB.class);

	private static final long serialVersionUID = 1l;
	protected final MySQLContainer storedContainer;

	public static MySQLContainerDB getInstance() {
		final String username = "dbvuser";
		final String password = "dbvtest";
		/*
		'TZ=Pacific/Auckland' sets the container timezone to where I do my test (TODO set to server location)
		 */
		MySQLContainer container = (MySQLContainer) new MySQLContainer("mysql:latest")
				.withDatabaseName("some_database")
				// set the log consumer so we see some output
				.withLogConsumer(new ConsumerImpl())
				// there is a problem with the config file in the image so add our own
				//.withCopyFileToContainer(MountableFile.forClasspathResource("testMySQL.cnf"), "/etc/mysql/conf.d/")
				.withStartupTimeout(Duration.ofMinutes(2))
				;
		container.withEnv("MYSQL_USER", username);
		container.withEnv("MYSQL_PASSWORD", password);
		container.withEnv("TZ", ZoneId.systemDefault().toString());
//		container.withEnv("TZ", "Pacific/Auckland");
//		container.withEnv("TZ", ZoneId.systemDefault().getId());
		container.start();

		try {
			// create the actual dbdatabase 
			MySQLContainerDB dbdatabase = new MySQLContainerDB(container);
			return dbdatabase;
		} catch (SQLException ex) {
			Logger.getLogger(MySQLContainerDB.class.getName()).log(Level.SEVERE, null, ex);
			throw new RuntimeException("Unable To Create MySQL Database in Docker Container", ex);
		}
	}

	public MySQLContainerDB(MySQLContainer storedContainer, MySQLSettingsBuilder dcs) throws SQLException {
		super(dcs);
		this.storedContainer = storedContainer;
	}

	public MySQLContainerDB(MySQLContainer container) throws SQLException {
		this(container,
				new MySQLSettingsBuilder()
						.fromJDBCURL(container.getJdbcUrl(), "root", "test")
						// set the database name because apparently it's not in the URL
						.setDatabaseName(container.getDatabaseName())
						// The test container doesn't use SSL so we need to turn that off
						.setUseSSL(false)
						// allowPublicKeyRetrieval=true
						.setAllowPublicKeyRetrieval(true)
		);
	}

	@Override
	public synchronized void stop() {
		super.stop();
		storedContainer.stop();
	}

	private static class ConsumerImpl implements Consumer<OutputFrame> {

		@Override
		public void accept(OutputFrame t) {
			LOG.info("" + t.getUtf8String().replaceAll("\n$", ""));
		}
	}

}
