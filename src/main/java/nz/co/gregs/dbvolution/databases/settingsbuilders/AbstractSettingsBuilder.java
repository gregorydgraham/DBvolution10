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

import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DatabaseConnectionSettings;
import nz.co.gregs.dbvolution.utility.SeparatedString;

/**
 *
 * @author gregorygraham
 * @param <SELF>
 */
public abstract class AbstractSettingsBuilder<SELF extends AbstractSettingsBuilder<SELF>> implements JDBCSettingsBuilder<SELF> {

	private DatabaseConnectionSettings storedSettingsInAbstractURLInterpreter;

	protected abstract Map<String, String> getDefaultConfigurationExtras();

	protected abstract DatabaseConnectionSettings generateSettingsInternal(String jdbcURL, DatabaseConnectionSettings settings);

	protected abstract String getJDBCURLPreamble(DatabaseConnectionSettings settings);

	protected abstract DatabaseConnectionSettings setDefaultsInternal(DatabaseConnectionSettings settings);

	protected abstract String encodeHost(DatabaseConnectionSettings settings) ;

	protected final String generateJDBCURLInternal(DatabaseConnectionSettings settings) {
		return this.getJDBCURLPreamble(settings) + encodeHostAbstract(settings);
	}
	
	protected String encodeHostAbstract(DatabaseConnectionSettings settings){
		return encodeHost(settings);
	}
	
	public final DatabaseConnectionSettings parseURL(String jdbcURL) {
		DatabaseConnectionSettings settings = getDefaultSettings();
		return generateSettingsInternal(jdbcURL, settings);
	}

	@Override
	public final String generateJDBCURL(DatabaseConnectionSettings settings) {
		String url = settings.getUrl();
		return url != null && !url.isEmpty() ? url : generateJDBCURLInternal(settings);
	}

	@Override
	public final boolean canProcessesURLsFor(DBDatabase otherdb) {
		Class<? extends DBDatabase> db = generatesURLForDatabase();
		return db.isAssignableFrom(otherdb.getClass());
	}

	protected final String encodeExtras(DatabaseConnectionSettings settings, String prefix, String nameValueSeparator, String nameValuePairSeparator, String suffix) {
		Map<String, String> extras = settings.getExtras();
		SeparatedString sep = SeparatedString
				.of(extras, nameValueSeparator)
				.withPrefix(prefix)
				.withSuffix(suffix)
				.separatedBy(nameValuePairSeparator);
		return sep.toString();
	}

	private DatabaseConnectionSettings getDefaultSettings() {
		DatabaseConnectionSettings settings = new DatabaseConnectionSettings();
		settings.setDbdatabaseClass(generatesURLForDatabase().getCanonicalName());
		settings.setPort("" + getDefaultPort());
		settings.setDefaultExtras(getDefaultConfigurationExtras());
		setDefaultsInternal(settings);
		return settings;
	}

	@Override
	@SuppressWarnings("unchecked")
	public final SELF fromJDBCURL(String jdbcURL) {
		this.storedSettingsInAbstractURLInterpreter = parseURL(jdbcURL);
		return (SELF) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public final SELF fromJDBCURL(String jdbcURL, String username, String password) {
		fromJDBCURL(jdbcURL);
		setUsername(username);
		setPassword(password);
		return (SELF) this;
	}

	@Override
	public final DatabaseConnectionSettings toSettings() {
		DatabaseConnectionSettings newSettings = new DatabaseConnectionSettings();
		newSettings.copy(storedSettingsInAbstractURLInterpreter);
		return newSettings;
	}

	@Override
	public final String toJDBCURL() {
		return generateJDBCURL(getStoredSettings());
	}

	protected final DatabaseConnectionSettings getStoredSettings() {
		if (storedSettingsInAbstractURLInterpreter == null) {
			storedSettingsInAbstractURLInterpreter = getDefaultSettings();
		}
		return storedSettingsInAbstractURLInterpreter;
	}

	@SuppressWarnings("unchecked")
	public final SELF setDataSource(DataSource dataSource) {
		getStoredSettings().setDataSource(dataSource);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public final SELF setDatabaseName(String databaseName) {
		getStoredSettings().setDatabaseName(databaseName);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public final SELF setDBDatabaseName(String dbDatabaseName) {
		getStoredSettings().setDbdatabaseClass(dbDatabaseName);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public final SELF setExtras(Map<String, String> extras) {
		getStoredSettings().setExtras(extras);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public final SELF addExtra(String key, String value) {
		getStoredSettings().addExtra(key, value);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public final SELF addExtras(Map<String, String> extras) {
		getStoredSettings().addExtras(extras);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public final SELF setFilename(String filename) {
		getStoredSettings().setFilename(filename);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public final SELF setHost(String host) {
		getStoredSettings().setHost(host);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public final SELF setInstance(String instance) {
		getStoredSettings().setInstance(instance);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public final SELF setLabel(String label) {
		getStoredSettings().setLabel(label);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public final SELF setPassword(String password) {
		getStoredSettings().setPassword(password);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public final SELF setPort(int port) {
		getStoredSettings().setPort("" + port);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public final SELF setPort(long port) {
		getStoredSettings().setPort("" + port);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public final SELF setProtocol(String protocol) {
		getStoredSettings().setProtocol(protocol);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public final SELF setSchema(String schema) {
		getStoredSettings().setSchema(schema);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public final SELF setUrl(String url) {
		getStoredSettings().setUrl(url);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public final SELF setUsername(String username) {
		getStoredSettings().setUsername(username);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public final SELF addClusterHost(DatabaseConnectionSettings clusterHost) {
		getStoredSettings().addClusterHost(clusterHost);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public final SELF addAllClusterHosts(List<DatabaseConnectionSettings> clusterHosts) {
		getStoredSettings().addAllClusterHosts(clusterHosts);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public final SELF setClusterHosts(List<DatabaseConnectionSettings> clusterHosts) {
		getStoredSettings().setClusterHosts(clusterHosts);
		return (SELF) this;
	}
}
