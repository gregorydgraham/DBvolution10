/*
 * Copyright 2020 Gregory Graham.
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

import java.time.Duration;
import java.time.ZoneId;
import java.util.function.Consumer;
import nz.co.gregs.dbvolution.databases.settingsbuilders.AbstractMSSQLServerSettingsBuilder;
import nz.co.gregs.dbvolution.databases.settingsbuilders.AbstractMySQLSettingsBuilder;
import nz.co.gregs.dbvolution.databases.settingsbuilders.AbstractOracleSettingsBuilder;
import nz.co.gregs.dbvolution.databases.settingsbuilders.AbstractVendorSettingsBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.output.OutputFrame;

/**
 *
 * @author gregorygraham
 */
public class ContainerUtils {

	static final Log LOG = LogFactory.getLog(ContainerUtils.class);

	private static void setStandardContainerSettings(JdbcDatabaseContainer<?> container) {
		container.withEnv("TZ", ZoneId.systemDefault().toString());
		container.setStartupAttempts(3);
		container.withStartupTimeout(Duration.ofMinutes(5));
		container.withConnectTimeoutSeconds(300);
	}

	protected static void startContainer(JdbcDatabaseContainer<?> container) throws org.testcontainers.containers.ContainerLaunchException {
		setStandardContainerSettings(container);
		container.start();
	}

	protected static void startContainer(MySQLContainer<?> container) {
		setStandardContainerSettings(container);
		container.withDatabaseName("some_database");
		container.withLogConsumer(new ConsumerImpl());
		final String username = "dbvuser";
		final String password = "dbvtest";
		container.withEnv("MYSQL_USER", username);
		container.withEnv("MYSQL_PASSWORD", password);
		container.start();
	}

	private static <CONTAINER extends JdbcDatabaseContainer<?>, BUILDER extends AbstractVendorSettingsBuilder<?, ?>> BUILDER getStandardSettings(BUILDER builder, CONTAINER container, String label) {
		builder
				.fromJDBCURL(
						container.getJdbcUrl(),
						container.getUsername(),
						container.getPassword()
				)
				.setLabel(label);
		return builder;
	}

	/**
	 *
	 * @param <BUILDER>
	 * @param <CONTAINER>
	 * @param builder
	 * @param container
	 * @param label
	 * @return
	 */
	protected static <BUILDER extends AbstractVendorSettingsBuilder<?, ?>, CONTAINER extends JdbcDatabaseContainer<?>> BUILDER getContainerSettings(BUILDER builder, CONTAINER container, String label) {
		return getStandardSettings(builder, container, label);
	}

	protected static <BUILDER extends AbstractOracleSettingsBuilder<?, ?>, CONTAINER extends OracleContainer> BUILDER getContainerSettings(BUILDER builder, CONTAINER container, String label) {
		getStandardSettings(builder, container, label)
				.setHost(container.getContainerIpAddress())
				.setPort(container.getOraclePort())
				.setSID(container.getSid());
		return builder;
	}

	/**
	 *
	 * @param <BUILDER>
	 * @param <CONTAINER>
	 * @param builder
	 * @param container
	 * @param label
	 * @return
	 */
	protected static <BUILDER extends AbstractMySQLSettingsBuilder<?, ?>, CONTAINER extends MySQLContainer<?>> BUILDER getContainerSettings(BUILDER builder, CONTAINER container, String label) {
		getStandardSettings(builder, container, label)
				.setDatabaseName(container.getDatabaseName())
				// we require super user access
				.setUsername("root")
				.setPassword("test")
				// The test container doesn't use SSL so we need to turn that off
				.setUseSSL(false)
				// allowPublicKeyRetrieval=true
				.setAllowPublicKeyRetrieval(true);
		return builder;
	}

	protected static <BUILDER extends AbstractMSSQLServerSettingsBuilder<?, ?>, CONTAINER extends MSSQLServerContainer<?>> BUILDER getContainerSettings(BUILDER builder, CONTAINER container, String label) {
		getStandardSettings(builder, container, label)
				.setHost(container.getContainerIpAddress())
				.setPort(container.getFirstMappedPort());
		return builder;
	}

	private ContainerUtils() {
	}

	private static class ConsumerImpl implements Consumer<OutputFrame> {

		@Override
		public void accept(OutputFrame t) {
			LOG.info("" + t.getUtf8String().replaceAll("\n$", ""));
		}
	}
}
