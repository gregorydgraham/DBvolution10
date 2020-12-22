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
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.databases.MySQL8ContainerDB.Versions;
import nz.co.gregs.dbvolution.databases.settingsbuilders.MySQLSettingsBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testcontainers.containers.JdbcDatabaseContainerProvider;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.jdbc.ConnectionUrl;

/**
 *
 * @author gregorygraham
 */
public class MySQL8ContainerDB extends MySQLDB {

	private static final long serialVersionUID = 1l;
	private static MySQL8ContainerDB staticDatabase = null;
	protected final MySQLContainer<?> storedContainer;

	static final private Log LOG = LogFactory.getLog(MySQL8ContainerDB.class);

	public static MySQL8ContainerDB getInstance() {
		if (staticDatabase == null) {
			staticDatabase = createNewInstance("Unlabelled");
		}
		return staticDatabase;
	}

	public static MySQL8ContainerDB createNewInstance(String label) {
		return createNewInstance(label, Versions.v8_0);
	}

	public static MySQL8ContainerDB createNewInstance(String label, Versions tag) {
		MySQL8ContainerDB dbdatabase = null;

		final FinalisedMySQLContainerProvider provider = new FinalisedMySQLContainerProvider();
		FinalisedMySQLContainer container = null;
		try {
			LOG.info("Trying to create MySQL:" + tag);
			container = provider.newInstance(tag);
			ContainerUtils.startContainer(container);
			// create the actual dbdatabase
			dbdatabase = new MySQL8ContainerDB(container,
					ContainerUtils.getContainerSettings(new MySQLSettingsBuilder(), container, label)
			);
		} catch (Throwable exc) {
			LOG.warn("FAILED TO CREATE MySQL:" + tag);
			exc.printStackTrace();
			container = null;
			dbdatabase = null;
		}
		if (dbdatabase == null) {
			throw new RuntimeException("FAILED TO CREATE MYSQL CONTAINER");
		} else {
			LOG.info("CREATED MySQL " + tag);
		}

		return dbdatabase;
	}

	@Override
	public synchronized void stop() {
		super.stop();
		storedContainer.stop();
	}

	protected MySQL8ContainerDB(MySQLContainer<?> storedContainer, MySQLSettingsBuilder settings) throws SQLException {
		super(settings);
		this.storedContainer = storedContainer;
	}

	protected static class FinalisedMySQLContainer extends MySQLContainer<FinalisedMySQLContainer> {

		public FinalisedMySQLContainer(String dockerImageName) {
			super(dockerImageName);
		}
	}

	protected static class FinalisedMySQLContainerProvider extends JdbcDatabaseContainerProvider {

		protected static final String DEFAULT_IMAGE = "mysql";
		protected static final String DEFAULT_TAG = "8.0";
		protected static final String DEFAULT_LATEST_TAG = "latest";

		@Override
		public boolean supports(String databaseType) {
			return databaseType.equals(MySQLContainer.NAME);
		}

		@Override
		public FinalisedMySQLContainer newInstance() {
			return newDefaultInstance();
		}

		private FinalisedMySQLContainer newDefaultInstance() {
			return new FinalisedMySQLContainer(DEFAULT_TAG);
		}

		@Override
		public FinalisedMySQLContainer newInstance(String tag) {
			if (tag != null) {
				return new FinalisedMySQLContainer(DEFAULT_IMAGE + ":" + tag);
			} else {
				return newDefaultInstance();
			}
		}

		public FinalisedMySQLContainer newInstance(Versions tag) {
			if (tag != null) {
				return newInstance(tag.toString());
			} else {
				return newDefaultInstance();
			}
		}

		public FinalisedMySQLContainer newInstanceOfLatestVersion() {
			return new FinalisedMySQLContainer(DEFAULT_IMAGE + ":"+DEFAULT_LATEST_TAG);
		}

		@Override
		public FinalisedMySQLContainer newInstance(ConnectionUrl connectionUrl) {
			return (FinalisedMySQLContainer) super.newInstance(connectionUrl);
		}

	}

	public static enum Versions {
		v5,
		v5_6,
		v5_6_50, // This is likely to change as Oracle seems to like removing 3rd tier versions
		v5_7,
		v5_7_32, // This is likely to change as Oracle seems to like removing 3rd tier versions
		v8,
		v8_0,
		v8_0_22, // This is likely to change as Oracle seems to like removing 3rd tier versions
		latest;

		@Override
		public String toString() {
			return super.toString().replaceAll("_", ".").replaceFirst("v", "");
		}

		public static Iterator<Versions> getIteratorLatestFirst() {
			List<Versions> versions = Arrays.asList(Versions.values());
			Collections.reverse(versions);
			Iterator<Versions> versionsIterator = versions.iterator();
			return versionsIterator;
		}

		public static Iterator<Versions> getIteratorLatestLast() {
			List<Versions> versions = Arrays.asList(Versions.values());
			Iterator<Versions> versionsIterator = versions.iterator();
			return versionsIterator;
		}

	}
}
