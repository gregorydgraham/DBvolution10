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
import nz.co.gregs.dbvolution.databases.NuoDB;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.NuoDBDefinition;

/**
 *
 * @author gregorygraham
 */
public class NuoDBSettingsBuilder extends AbstractVendorSettingsBuilder<NuoDBSettingsBuilder, NuoDB>
		implements
		InstanceCapableSettingsBuilder<NuoDBSettingsBuilder, NuoDB>,
		RemoteCapableSettingsBuilder<NuoDBSettingsBuilder, NuoDB>,
		NamedDatabaseCapableSettingsBuilder<NuoDBSettingsBuilder, NuoDB>,
		SchemaCapableSettingsBuilder<NuoDBSettingsBuilder, NuoDB>,
		ClusterCapableSettingsBuilder<NuoDBSettingsBuilder, NuoDB> {

	private final static HashMap<String, String> DEFAULT_EXTRAS_MAP = new HashMap<>();
	private static final long serialVersionUID = 1L;
	private final List<DatabaseConnectionSettings> clusterHost = new ArrayList<>(0);

	@Override
	public String getDefaultDriverName() {
		return NuoDB.NUODB_DRIVER;
	}

	@Override
	public DBDefinition getDefaultDefinition() {
		return new NuoDBDefinition();
	}

	@Override
	public Map<String, String> getDefaultConfigurationExtras() {
		return DEFAULT_EXTRAS_MAP;
	}

	@Override
	public DatabaseConnectionSettings generateSettingsInternal(String jdbcURL, DatabaseConnectionSettings set) {
		String noPrefix = jdbcURL.replaceAll("^" + getJDBCURLPreamble(), "");
		if (jdbcURL.matches(";")) {
			String extrasString = jdbcURL.split("?", 2)[1];
			set.setExtras(DatabaseConnectionSettings.decodeExtras(extrasString, "", "=", ";", ""));
		}
		set.setPort(noPrefix
				.split("/", 2)[0]
				.replaceAll("^[^:]*:+", ""));
		set.setHost(noPrefix
				.split("/", 2)[0]
				.split(":")[0]);
		set.setInstance(set.getExtras().get("instance"));
		set.setSchema("");
		return set;
	}

	@Override
	protected String getJDBCURLPreamble(DatabaseConnectionSettings settings) {
		return getJDBCURLPreamble();
	}

	protected String getJDBCURLPreamble() {
		return "jdbc:com.nuodb://";
	}

//	@Override
//	public String generateJDBCURLInternal(DatabaseConnectionSettings settings) {
//		String url = settings.getUrl();
//		return url != null && !url.isEmpty() ? url : getJDBCURLPreamble()
//				+ settings.getHost() + "/"
//				+ settings.getDatabaseName() + "?schema=" + settings.getSchema();
//	}
	@Override
	public DatabaseConnectionSettings setDefaultsInternal(DatabaseConnectionSettings settings) {
		return settings;
	}

	@Override
	public Class<NuoDB> generatesURLForDatabase() {
		return NuoDB.class;
	}

	@Override
	public Integer getDefaultPort() {
		return 8888;// possibly 48004???
	}

	@Override
	public String encodeHost(DatabaseConnectionSettings settings) {
		return settings.getHost() + "/"
				+ settings.getDatabaseName()
				+ "?schema=" + settings.getSchema();
	}

	@Override
	public NuoDB getDBDatabase() throws Exception {
		List<String> brokers = new ArrayList<>(0);
		List<Long> ports = new ArrayList<>(0);
		for (DatabaseConnectionSettings dcs : getClusterHosts()) {
			brokers.add(dcs.getHost());
			ports.add(Long.getLong(dcs.getPort()));
		}
		return new NuoDB(brokers, ports, this.getDatabaseName(), getSchema(), getUsername(), getPassword());
	}

	@Override
	public NuoDBSettingsBuilder setClusterHosts(List<DatabaseConnectionSettings> hosts) {
		this.clusterHost.clear();
		this.clusterHost.addAll(hosts);
		return this;
	}

	@Override
	public List<DatabaseConnectionSettings> getClusterHosts() {
		return this.clusterHost;
	}

	public NuoDBSettingsBuilder addBrokers(List<String> brokers) {
		brokers.stream().forEachOrdered(broker
				-> clusterHost.add(
						new NuoDBSettingsBuilder().setHost(broker).toSettings()
				)
		);
		return this;
	}

	public NuoDBSettingsBuilder addBrokers(List<String> brokers, List<Long> ports) {
		for(int i = 0; i<brokers.size();i++){
			clusterHost.add(
					new NuoDBSettingsBuilder().setHost(brokers.get(i)).setPort(ports.get(i)).toSettings()
			);
		}
		return this;
	}
}
