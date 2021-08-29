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
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DatabaseConnectionSettings;
import nz.co.gregs.dbvolution.databases.MySQLDB;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.MySQLDBDefinition;

/**
 *
 * @author gregorygraham
 * @param <SELF> the type returned by all SELF methods
 * @param <DATABASE>
 */
public abstract class AbstractMySQLSettingsBuilder<SELF extends AbstractMySQLSettingsBuilder<SELF, DATABASE>, DATABASE extends DBDatabase>
		extends AbstractVendorSettingsBuilder<SELF, DATABASE>
		implements ClusterCapableSettingsBuilder<SELF, DATABASE>,
		InstanceCapableSettingsBuilder<SELF, DATABASE>,
		RemoteCapableSettingsBuilder<SELF, DATABASE>,
		NamedDatabaseCapableSettingsBuilder<SELF, DATABASE>,
		SchemaCapableSettingsBuilder<SELF, DATABASE>,
		ExtrasCapableSettingsBuilder<SELF, DATABASE> {

	protected static final HashMap<String, String> DEFAULT_EXTRAS_MAP = new HashMap<>() {
		{
			put("autoReconnect", "true");
			put("characterEncoding", "utf8");
			put("characterSetResults", "utf8");
			put("createDatabaseIfNotExist", "true");
			put("verifyServerCertificate", "false");
			put("useUnicode", "yes");
			put("useSSL", "false");
		}
	};

	@Override
	public String getDefaultDriverName() {
		return MySQLDB.MYSQLDRIVERNAME;
	}

	@Override
	public DBDefinition getDefaultDefinition() {
		return new MySQLDBDefinition();
	}

	@Override
	protected Map<String, String> getDefaultConfigurationExtras() {
		return DEFAULT_EXTRAS_MAP;
	}

	@Override
	protected DatabaseConnectionSettings generateSettingsInternal(String jdbcURL, DatabaseConnectionSettings settings) {
		String noPrefix = jdbcURL.replaceAll("^" + getJDBCURLPreamble(), "");
		settings.setPort(noPrefix.split("/", 2)[0].replaceAll("^[^:]*:+", ""));
		settings.setHost(noPrefix.split("/", 2)[0].split(":")[0]);
		if (jdbcURL.matches(";")) {
			String extrasString = jdbcURL.split("\\?", 2)[1];
			settings.addExtras(DatabaseConnectionSettings.decodeExtras(extrasString, "", "=", ";", ""));
		}
		settings.setInstance(settings.getExtras().get("instance"));
		settings.setSchema("");
		return settings;
	}

	protected String getJDBCURLPreamble() {
		return "jdbc:mysql://";
	}

	@Override
	protected String getJDBCURLPreamble(DatabaseConnectionSettings settings) {
		return getJDBCURLPreamble();
	}

	@Override
	public String encodeHost(DatabaseConnectionSettings settings) {
		return settings.getHost() + ":" + settings.getPort() + "/" + settings.getDatabaseName() + encodeExtras(settings, "?", "=", "&", "");
	}

	@Override
	protected DatabaseConnectionSettings setDefaultsInternal(DatabaseConnectionSettings settings) {
		settings.setPort("3306");
		return settings;
	}

	@Override
	public Integer getDefaultPort() {
		return 3306;
	}

	@SuppressWarnings("unchecked")
	public SELF setUseSSL(boolean b) {
		getStoredSettings().addExtra("useSSL", "" + b);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setAllowPublicKeyRetrieval(boolean b) {
		getStoredSettings().addExtra("allowPublicKeyRetrieval", "" + b);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setCreateDatabaseIfNotExist(boolean b) {
		getStoredSettings().addExtra("createDatabaseIfNotExist", "" + b);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setUseUnicode(boolean b) {
		getStoredSettings().addExtra("useUnicode", b ? "yes" : "no");
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setCharacterEncoding(String encoding) {
		getStoredSettings().addExtra("characterEncoding", encoding);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setCharacterSetResults(String encoding) {
		getStoredSettings().addExtra("characterSetResults", encoding);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setVerifyServerCertificate(boolean b) {
		getStoredSettings().addExtra("verifyServerCertificate", "" + b);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setAutoReconnect(boolean b) {
		getStoredSettings().addExtra("autoReconnect", "" + b);
		return (SELF) this;
	}

	private final List<DatabaseConnectionSettings> clusterHost = new ArrayList<>(0);

	@Override
	@SuppressWarnings("unchecked")
	public SELF setClusterHosts(List<DatabaseConnectionSettings> hosts) {
		this.clusterHost.clear();
		this.clusterHost.addAll(hosts);
		return (SELF) this;
	}

	@Override
	public List<DatabaseConnectionSettings> getClusterHosts() {
		return this.clusterHost;
	}

}
