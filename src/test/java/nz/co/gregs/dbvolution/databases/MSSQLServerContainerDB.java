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
import java.time.ZoneId;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.databases.settingsbuilders.MSSQLServerSettingsBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.MSSQLServerContainerProvider;

/**
 *
 * @author gregorygraham
 */
public class MSSQLServerContainerDB extends MSSQLServerDB {

	static final Log LOG = LogFactory.getLog(MSSQLServerContainerDB.class);

	private static final long serialVersionUID = 1l;

	protected final MSSQLServerContainer<?> mssqlServerContainer;
	
	public static MSSQLServerContainerDB getInstance(){
		return getLabelledInstance("Unlabelled");
	}

	public static MSSQLServerContainerDB getLabelledInstance(String label) {
		/*
		'TZ=Pacific/Auckland' sets the container timezone to where I do my test (TODO set to server location)
		 */
		JdbcDatabaseContainer<?> container = new MSSQLServerContainerProvider().newInstance();
		container.addEnv("TZ", ZoneId.systemDefault().getId());
		container.withConnectTimeoutSeconds(300);

//		MSSQLServerContainer container = new MSSQLServerContainer<>();
////		container.withEnv("TZ", "Pacific/Auckland");
//		container.withEnv("TZ", ZoneId.systemDefault().getId());
		container.start();
		try {
			MSSQLServerContainerDB staticDatabase = new MSSQLServerContainerDB((MSSQLServerContainer) container, label);
			return staticDatabase;
		} catch (SQLException ex) {
			Logger.getLogger(MSSQLServerContainerDB.class.getName()).log(Level.SEVERE, null, ex);
			throw new RuntimeException("Unable To Create MSSQLServer Database in Docker Container", ex);
		}
	}

	public MSSQLServerContainerDB(MSSQLServerContainer<?> container, MSSQLServerSettingsBuilder interpreter) throws SQLException {
		super(interpreter);
		this.mssqlServerContainer = container;
	}

	public MSSQLServerContainerDB(MSSQLServerContainer<?> container, String label) throws SQLException {
		this(container,
				new MSSQLServerSettingsBuilder()
						.fromJDBCURL(
								container.getJdbcUrl(),
								container.getUsername(),
								container.getPassword()
						)
						.setHost(container.getContainerIpAddress())
						.setPort(container.getFirstMappedPort())
						.setLabel(label)
		);
	}

	@Override
	public synchronized void stop() {
		super.stop();
		final String containerId = mssqlServerContainer.getContainerId();
		mssqlServerContainer.stop();
		LOG.info("CONTAINER STOPPED: " + containerId);
	}
}
