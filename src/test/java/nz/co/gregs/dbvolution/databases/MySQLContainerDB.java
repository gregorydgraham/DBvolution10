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
package nz.co.gregs.dbvolution.databases;

import java.sql.SQLException;
import java.time.Duration;
import java.time.ZoneId;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.databases.settingsbuilders.MySQLSettingsBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testcontainers.containers.JdbcDatabaseContainerProvider;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.jdbc.ConnectionUrl;

/**
 *
 * @author gregorygraham
 */
public class MySQLContainerDB extends MySQLDB {

	static final Log LOG = LogFactory.getLog(MySQLContainerDB.class);

	private static final long serialVersionUID = 1l;
	protected final FinalisedMySQLContainer storedContainer;

	public static MySQLContainerDB getInstance() {
		return getLabelledInstance("Unlabelled");
	}

	public static MySQLContainerDB getLabelledInstance(String label) {
		final String username = "dbvuser";
		final String password = "dbvtest";
		final FinalisedMySQLContainer newInstance = new FinalisedMySQLContainerProvider().newInstance("latest");
		FinalisedMySQLContainer container = newInstance;
		container.withDatabaseName("some_database");
		container.withLogConsumer(new ConsumerImpl());
		container.withStartupTimeout(Duration.ofMinutes(5));
//		MySQLContainer container = (MySQLContainer) new MySQLContainer("mysql:latest")
//				.withDatabaseName("some_database")
		// set the log consumer so we see some output
//				.withLogConsumer(new ConsumerImpl())
		// there is a problem with the config file in the image so add our own
		//.withCopyFileToContainer(MountableFile.forClasspathResource("testMySQL.cnf"), "/etc/mysql/conf.d/")
//				.withStartupTimeout(Duration.ofMinutes(2))
//				;
		container.withEnv("MYSQL_USER", username);
		container.withEnv("MYSQL_PASSWORD", password);
		container.withEnv("TZ", ZoneId.systemDefault().toString());
		/*
		'TZ=Pacific/Auckland' sets the container timezone to where I do my test
		 */
//		container.withEnv("TZ", "Pacific/Auckland");
//		container.withEnv("TZ", ZoneId.systemDefault().getId());
		container.withConnectTimeoutSeconds(300);
		container.start();

		try {
			// create the actual dbdatabase 
			MySQLContainerDB dbdatabase = new MySQLContainerDB(container, label);
			return dbdatabase;
		} catch (SQLException ex) {
			Logger.getLogger(MySQLContainerDB.class.getName()).log(Level.SEVERE, null, ex);
			throw new RuntimeException("Unable To Create MySQL Database in Docker Container", ex);
		}
	}

	@Override
	public synchronized void stop() {
		super.stop();
		storedContainer.stop();
	}

	protected MySQLContainerDB(FinalisedMySQLContainer storedContainer, MySQLSettingsBuilder settings) throws SQLException {
		super(settings);
		this.storedContainer = storedContainer;
	}

	public MySQLContainerDB(MySQLContainer<?> storedContainer, MySQLSettingsBuilder settings) throws SQLException {
		this((FinalisedMySQLContainer) storedContainer, settings);
	}

	public MySQLContainerDB(MySQLContainer<?> container, String label) throws SQLException {
		this((FinalisedMySQLContainer) container,
				new MySQLSettingsBuilder()
						.fromJDBCURL(container.getJdbcUrl(), "root", "test")
						// set the database name because apparently it's not in the URL
						.setDatabaseName(container.getDatabaseName())
						// The test container doesn't use SSL so we need to turn that off
						.setUseSSL(false)
						// allowPublicKeyRetrieval=true
						.setAllowPublicKeyRetrieval(true)
						.setLabel(label)
		);
	}

	private static class ConsumerImpl implements Consumer<OutputFrame> {

		@Override
		public void accept(OutputFrame t) {
			LOG.info("" + t.getUtf8String().replaceAll("\n$", ""));
		}
	}

	protected static class FinalisedMySQLContainer extends MySQLContainer<FinalisedMySQLContainer> {

		public FinalisedMySQLContainer(String dockerImageName) {
			super(dockerImageName);
		}
	}

	protected static class FinalisedMySQLContainerProvider extends JdbcDatabaseContainerProvider {

		private static final String USER_PARAM = "user";

		private static final String PASSWORD_PARAM = "password";

		@Override
		public boolean supports(String databaseType) {
			return databaseType.equals(MySQLContainer.NAME);
		}

		@Override
		public FinalisedMySQLContainer newInstance() {
			return newDefaultInstance();
		}

		private FinalisedMySQLContainer newDefaultInstance() {
			return new FinalisedMySQLContainer(MySQLContainer.DEFAULT_TAG);
		}

		@Override
		public FinalisedMySQLContainer newInstance(String tag) {
			if (tag != null) {
				return new FinalisedMySQLContainer(MySQLContainer.IMAGE + ":" + tag);
			} else {
				return newDefaultInstance();
			}
		}

		@Override
		public FinalisedMySQLContainer newInstance(ConnectionUrl connectionUrl) {
			return (FinalisedMySQLContainer) newInstanceFromConnectionUrl(connectionUrl, USER_PARAM, PASSWORD_PARAM);
		}

	}

}
