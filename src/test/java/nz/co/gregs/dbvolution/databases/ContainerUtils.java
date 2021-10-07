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
import org.testcontainers.containers.*;
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

	/**
	 * Simplified method to start the container.
	 *
	 * <p>
	 * This fairly simple but the MySQL version is a bit more complicated.</p>
	 *
	 * @param container the container to start
	 * @throws org.testcontainers.containers.ContainerLaunchException  an exception thrown by TestContainers when the launch fails
	 */
	protected static void startContainer(JdbcDatabaseContainer<?> container) throws ContainerLaunchException {
		setStandardContainerSettings(container);
		container.start();
	}

	/**
	 * Simplified method to start the container.
	 *
	 * <p>
	 * This fairly simple but the MySQL version is a bit more complicated.</p>
	 *
	 * @param container the container to start
	 * @throws org.testcontainers.containers.ContainerLaunchException an exception thrown by TestContainers when the launch fails
	 */
	protected static void startContainer(MySQLContainer<?> container) throws ContainerLaunchException{
		setStandardContainerSettings(container);
		container.withDatabaseName("some_database");
		container.withLogConsumer(new ConsumerImpl());
		final String username = "dbvuser";
		final String password = "dbvtest";
		container.withEnv("MYSQL_USER", username);
		container.withEnv("MYSQL_PASSWORD", password);
		container.start();
	}

	/**
	 * Adds the standard settings thar used for all databases.
	 *
	 * @param <BUILDER> the type of builder to be used
	 * @param <CONTAINER> the type of container to be configured
	 * @param builder the builder to be used
	 * @param container the container to be configured
	 * @param label the label used to identify this container within the
	 * application
	 * @return a properly configured builder
	 */
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
	 * @param <BUILDER> the type of builder to be used
	 * @param <CONTAINER> the type of container to be configured
	 * @param builder the builder to be used
	 * @param container the container to be configured
	 * @param label the label used to identify this container within the
	 * application
	 * @return a properly configured builder
	 */
	protected static <BUILDER extends AbstractVendorSettingsBuilder<?, ?>, CONTAINER extends JdbcDatabaseContainer<?>> BUILDER getContainerSettings(BUILDER builder, CONTAINER container, String label) {
		return getStandardSettings(builder, container, label);
	}

	/**
	 *
	 * @param <BUILDER> the type of builder to be used
	 * @param <CONTAINER> the type of container to be configured
	 * @param builder the builder to be used
	 * @param container the container to be configured
	 * @param label the label used to identify this container within the
	 * application
	 * @return a properly configured builder
	 */
	protected static <BUILDER extends AbstractOracleSettingsBuilder<?, ?>, CONTAINER extends OracleContainer> BUILDER getContainerSettings(BUILDER builder, CONTAINER container, String label) {
		getStandardSettings(builder, container, label)
				.setHost(container.getContainerIpAddress())
				.setPort(container.getOraclePort())
				.setSID(container.getSid());
		return builder;
	}

	/**
	 *
	 * @param <BUILDER> the type of builder to be used
	 * @param <CONTAINER> the type of container to be configured
	 * @param builder the builder to be used
	 * @param container the container to be configured
	 * @param label the label used to identify this container within the
	 * application
	 * @return a properly configured builder
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

	/**
	 *
	 * @param <BUILDER> the type of builder to be used
	 * @param <CONTAINER> the type of container to be configured
	 * @param builder the builder to be used
	 * @param container the container to be configured
	 * @param label the label used to identify this container within the
	 * application
	 * @return a properly configured builder
	 */
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
