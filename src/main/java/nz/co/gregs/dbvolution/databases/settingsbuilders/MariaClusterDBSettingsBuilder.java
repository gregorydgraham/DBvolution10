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
package nz.co.gregs.dbvolution.databases.settingsbuilders;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import nz.co.gregs.dbvolution.databases.DatabaseConnectionSettings;
import nz.co.gregs.dbvolution.databases.MariaClusterDB;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.MariaDBDefinition;

/**
 *
 * @author gregorygraham
 */
public class MariaClusterDBSettingsBuilder
		extends AbstractVendorSettingsBuilder<MariaClusterDBSettingsBuilder, MariaClusterDB>
		implements InstanceCapableSettingsBuilder<MariaClusterDBSettingsBuilder, MariaClusterDB>,
		RemoteCapableSettingsBuilder<MariaClusterDBSettingsBuilder, MariaClusterDB>,
		NamedDatabaseCapableSettingsBuilder<MariaClusterDBSettingsBuilder, MariaClusterDB>,
		ClusterCapableSettingsBuilder<MariaClusterDBSettingsBuilder, MariaClusterDB> {

	private final static HashMap<String, String> DEFAULT_EXTRAS_MAP = new HashMap<>();
	private static final long serialVersionUID = 1L;

	@Override
	public String getDefaultDriverName() {
		return MariaClusterDB.MARIADBDRIVERNAME;
	}

	@Override
	public DBDefinition getDefaultDefinition() {
		return new MariaDBDefinition();
	}

	@Override
	public Map<String, String> getDefaultConfigurationExtras() {
		return DEFAULT_EXTRAS_MAP;
	}

	@Override
	public DatabaseConnectionSettings generateSettingsInternal(String jdbcURL, DatabaseConnectionSettings set) {
//		DatabaseConnectionSettings set = getEmptySettings();
		String noPrefix = jdbcURL.replaceAll("^" + getJDBCURLPreamble(), "");
		set.setHost(noPrefix
				.split("/", 2)[0]
				.split(":")[0]);
		if (jdbcURL.matches(";")) {
			String extrasString = jdbcURL.split(";", 2)[1];
			set.setExtras(DatabaseConnectionSettings.decodeExtras(extrasString, "", "=", ";", ""));
		}
		set.setInstance(set.getExtras().get("instance"));
		set.setSchema("");
		return set;
	}

	@Override
	protected String getJDBCURLPreamble(DatabaseConnectionSettings settings) {
		return getJDBCURLPreamble();
	}

	protected String getJDBCURLPreamble() {
		return "jdbc:mariadb://";
	}

//	@Override
//	public String generateJDBCURLInternal(DatabaseConnectionSettings settings) {
//		String url = settings.getUrl();
//		return url != null && !url.isEmpty() ? url : getJDBCURLPreamble()
//				+ settings.getHost() + ":"
//				+ settings.getPort() + "/"
//				+ settings.getDatabaseName();
//	}
	@Override
	public DatabaseConnectionSettings setDefaultsInternal(DatabaseConnectionSettings settings) {
		return settings;
	}

	@Override
	public Class<MariaClusterDB> generatesURLForDatabase() {
		return MariaClusterDB.class;
	}

	@Override
	public Integer getDefaultPort() {
		return 3306;
	}

	@Override
	public String encodeHost(DatabaseConnectionSettings settings) {
		return settings.getHost() + ":"
				+ settings.getPort() + "/"
				+ settings.getDatabaseName();
	}

	@Override
	public MariaClusterDB getDBDatabase() throws Exception {
		return new MariaClusterDB(this);
	}

	private final List<DatabaseConnectionSettings> clusterHost = new ArrayList<>(0);

	@Override
	public MariaClusterDBSettingsBuilder setClusterHosts(List<DatabaseConnectionSettings> hosts) {
		this.clusterHost.clear();
		this.clusterHost.addAll(hosts);
		return this;
	}

	@Override
	public List<DatabaseConnectionSettings> getClusterHosts() {
		return this.clusterHost;
	}

	public MariaClusterDBSettingsBuilder addHosts(List<String> hosts, List<Long> ports) {
		for (int i = 0; i < hosts.size(); i++) {
			clusterHost.add(
					new MariaClusterDBSettingsBuilder()
							.setHost(hosts.get(i))
							.setPort(ports.get(i))
							.toSettings()
			);
		}
		return this;
	}
}
