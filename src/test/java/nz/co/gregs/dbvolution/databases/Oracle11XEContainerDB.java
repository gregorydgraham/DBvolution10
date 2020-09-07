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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.databases.settingsbuilders.Oracle11XESettingsBuilder;
import org.testcontainers.containers.OracleContainer;

/**
 *
 * @author gregorygraham
 */
public class Oracle11XEContainerDB extends Oracle11XEDB {

	private static final long serialVersionUID = 1l;
	private OracleContainer container;
	private static ExecutorService executor = Executors.newSingleThreadExecutor();

	public static Oracle11XEContainerDB getInstance() {
		return getLabelledInstance("");
	}

	public static Oracle11XEContainerDB getLabelledInstance(String label) {
		OracleContainer container = new OracleContainer("oracleinanutshell/oracle-xe-11g");
		container.start();

		try {
			return new Oracle11XEContainerDB(container, label);
		} catch (SQLException ex) {
			Logger.getLogger(Oracle11XEContainerDB.class.getName()).log(Level.SEVERE, null, ex);
			throw new RuntimeException("Unable To Create Oracle Database in Docker Container", ex);
		}
	}

	public static Future<Oracle11XEContainerDB> getLabelledInstanceInFuture(String label) {
		return executor.submit(() -> {
			return getLabelledInstance(label);
		});
	}

	protected Oracle11XEContainerDB(OracleContainer container) throws SQLException {
		this(container, "", container.getContainerIpAddress(), container.getOraclePort(), container.getSid(), container.getUsername(), container.getPassword());
	}

	protected Oracle11XEContainerDB(OracleContainer container, String label) throws SQLException {
		this(container, label, container.getContainerIpAddress(), container.getOraclePort(), container.getSid(), container.getUsername(), container.getPassword());
	}

	protected Oracle11XEContainerDB(OracleContainer container, String label, String host, int port, String sid, String username, String password) throws SQLException {
		this(container, new Oracle11XESettingsBuilder()
				.setLabel(label)
				.setHost(host)
				.setPort(port)
				.setInstance(sid)
				.setUsername(username)
				.setPassword(password)
		);
	}

	protected Oracle11XEContainerDB(OracleContainer container, Oracle11XESettingsBuilder settings) throws SQLException {
		super(settings);
		this.container = container;
		Logger.getLogger(Oracle11XEContainerDB.class.getName()).log(Level.INFO, "ORACLE: {0}", container.getJdbcUrl());
	}

	@Override
	public synchronized void stop() {
		super.stop();
		container.stop();
	}

}
