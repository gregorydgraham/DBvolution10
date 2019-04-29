/*
 * Copyright 2018 Gregory Graham.
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

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.h2.tools.Server;

public class H2SharedDB extends H2DB {

	private final static long serialVersionUID = 1l;

	Server server = null;

	public H2SharedDB(File file, String username, String password) throws IOException, SQLException {
		this(file.getAbsoluteFile().toString(), username, password);
	}

	public H2SharedDB(DataSource dataSource) throws SQLException {
		super(dataSource);
	}

	private H2SharedDB(String jdbcURL, String username, String password, boolean dummy) throws SQLException {
		super(jdbcURL, username, password);
	}

	public H2SharedDB(String databaseName, String username, String password) throws SQLException {
		this("localhost", databaseName, username, password);
	}

	public H2SharedDB(String serverName, String databaseName, String username, String password) throws SQLException {
		this("jdbc:h2:tcp://" + serverName + "/" + databaseName, username, password, false);
		this.setDatabaseName(databaseName);
	}

	public H2SharedDB(DatabaseConnectionSettings settings) throws SQLException {
		super(settings);
	}

	@Override
	protected synchronized void addDatabaseSpecificFeatures(Statement stmt) throws SQLException {
		super.addDatabaseSpecificFeatures(stmt);
	}

	@Override
	protected String getUrlFromSettings(DatabaseConnectionSettings settings) {
		String hostname = settings.getHost() == null || settings.getHost().isEmpty() ? "localhost" : settings.getHost();
		String port = settings.getPort() == null || settings.getPort().isEmpty() ? "9123" : settings.getPort();
		String url = settings.getUrl();
		return url != null && !url.isEmpty() ? url : "jdbc:h2:tcp://" + hostname + ":" + port + "/" + settings.getDatabaseName();
	}

	@Override
	protected void startServerIfRequired() {
		if (isLocalhostServer()) {
			if (server == null || !server.isRunning(false)) {
				if (serverIsUnreachable()) {
					try {
						if (getPort().isEmpty()) {
							server = Server.createTcpServer("-tcpAllowOthers", "-tcpDaemon").start();
						} else {
							server = Server.createTcpServer("-tcpAllowOthers", "-tcpDaemon", "-tcpPort", getPort()).start();
						}
						if (server != null && server.isRunning(false)) {
						} else {
						}
					} catch (SQLException ex) {
						Logger.getLogger(H2SharedDB.class.getName()).log(Level.SEVERE, null, ex);
					}
				} else {
				}
			} else {
			}
		} else {
		}
	}

	public boolean isLocalhostServer() {
		return getHost() == null || getHost().equals("") || getHost().equalsIgnoreCase("localhost") || getHost().startsWith("127.0.0");
	}

	@Override
	public synchronized void stop() {
		super.stop();
		if (server != null && server.isRunning(false)) {
			server.stop();
		}
	}

	private boolean serverIsUnreachable() {
		try {
			getConnectionFromDriverManager().close();
			return false;
		} catch (Exception ex) {
			LOG.error("Unable to reach server", ex);
		}
		return true;
	}

}
