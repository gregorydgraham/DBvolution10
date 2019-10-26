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

import java.util.Map;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DatabaseConnectionSettings;
import nz.co.gregs.dbvolution.utility.SeparatedString;

/**
 *
 * @author gregorygraham
 * @param <SELF>
 */
public abstract class AbstractSettingsBuilder<SELF extends AbstractSettingsBuilder<SELF>> implements SettingsBuilder<SELF> {

	private DatabaseConnectionSettings storedSettingsInAbstractURLInterpreter;

	protected abstract Map<String, String> getDefaultConfigurationExtras();

	protected abstract DatabaseConnectionSettings generateSettingsInternal(String jdbcURL, DatabaseConnectionSettings settings);

	protected abstract String getJDBCURLPreamble(DatabaseConnectionSettings settings);

	protected abstract DatabaseConnectionSettings setDefaultsInternal(DatabaseConnectionSettings settings);

	protected abstract String encodeHost(DatabaseConnectionSettings settings);

	protected final String generateJDBCURLInternal(DatabaseConnectionSettings settings) {
		return this.getJDBCURLPreamble(settings) + encodeHostAbstract(settings);
	}

	protected String encodeHostAbstract(DatabaseConnectionSettings settings) {
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
	@SuppressWarnings("unchecked")
	public final SELF fromSettings(DatabaseConnectionSettings settingsfromSystemUsingPrefix) {
		getStoredSettings().copy(settingsfromSystemUsingPrefix);
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

	@Override
	public final DatabaseConnectionSettings getStoredSettings() {
		if (storedSettingsInAbstractURLInterpreter == null) {
			storedSettingsInAbstractURLInterpreter = getDefaultSettings();
		}
		return storedSettingsInAbstractURLInterpreter;
	}

//	@SuppressWarnings("unchecked")
//	@Override
//	public final SELF setDataSource(DataSource dataSource) {
//		getStoredSettings().setDataSource(dataSource);
//		return (SELF) this;
//	}

	@SuppressWarnings("unchecked")
	@Override
	public final SELF setDatabaseName(String databaseName) {
		getStoredSettings().setDatabaseName(databaseName);
		return (SELF) this;
	}

//	@SuppressWarnings("unchecked")
//	@Override
//	public final SELF setDBDatabaseClass(String dbDatabaseName) {
//		getStoredSettings().setDbdatabaseClass(dbDatabaseName);
//		return (SELF) this;
//	}

	@SuppressWarnings("unchecked")
	@Override
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

//	@SuppressWarnings("unchecked")
//	@Override
//	public final SELF setFilename(String filename) {
//		getStoredSettings().setFilename(filename);
//		final String databaseName = getStoredSettings().getDatabaseName();
//		if (databaseName == null || databaseName.isEmpty()) {
//			getStoredSettings().setDatabaseName(filename);
//		}
//		return (SELF) this;
//	}

//	@SuppressWarnings("unchecked")
//	@Override
//	public final SELF setInstance(String instance) {
//		getStoredSettings().setInstance(instance);
//		return (SELF) this;
//	}

	@SuppressWarnings("unchecked")
	@Override
	public final SELF setLabel(String label) {
		getStoredSettings().setLabel(label);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public final SELF setPassword(String password) {
		getStoredSettings().setPassword(password);
		return (SELF) this;
	}

//	@SuppressWarnings("unchecked")
//	@Override
//	public final SELF setSchema(String schema) {
//		getStoredSettings().setSchema(schema);
//		return (SELF) this;
//	}

//	@SuppressWarnings("unchecked")
//	@Override
//	public final SELF setUrl(String url) {
//		getStoredSettings().setUrl(url);
//		return (SELF) this;
//	}

	@SuppressWarnings("unchecked")
	@Override
	public final SELF setUsername(String username) {
		getStoredSettings().setUsername(username);
		return (SELF) this;
	}

//	@SuppressWarnings("unchecked")
//	public final SELF addClusterHost(DatabaseConnectionSettings clusterHost) {
//		getStoredSettings().addClusterHost(clusterHost);
//		return (SELF) this;
//	}
//
//	@SuppressWarnings("unchecked")
//	public final SELF addAllClusterHosts(List<DatabaseConnectionSettings> clusterHosts) {
//		getStoredSettings().addAllClusterHosts(clusterHosts);
//		return (SELF) this;
//	}
//
//	@SuppressWarnings("unchecked")
//	@Override
//	public final SELF setClusterHosts(List<DatabaseConnectionSettings> clusterHosts) {
//		getStoredSettings().setClusterHosts(clusterHosts);
//		return (SELF) this;
//	}

//	@SuppressWarnings("unchecked")
//	@Override
//	public final DataSource getDataSource() {
//		return getStoredSettings().getDataSource();
//	}

	@SuppressWarnings("unchecked")
	@Override
	public final String getDatabaseName() {
		return getStoredSettings().getDatabaseName();
	}

//	@SuppressWarnings("unchecked")
//	@Override
//	public final String getDBDatabaseClass() {
//		return getStoredSettings().getDbdatabaseClass();
//	}

	@SuppressWarnings("unchecked")
	@Override
	public final Map<String, String> getExtras() {
		return getStoredSettings().getExtras();
	}

//	@SuppressWarnings("unchecked")
//	@Override
//	public final String getFilename() {
//		return getStoredSettings().getFilename();
//	}

//	@SuppressWarnings("unchecked")
//	@Override
//	public final String getInstance() {
//		return getStoredSettings().getInstance();
//	}

	@SuppressWarnings("unchecked")
	@Override
	public final String getLabel() {
		return getStoredSettings().getLabel();
	}

	@SuppressWarnings("unchecked")
	@Override
	public final String getPassword() {
		return getStoredSettings().getPassword();
	}

//	@SuppressWarnings("unchecked")
//	@Override
//	public final String getSchema() {
//		return getStoredSettings().getSchema();
//	}

//	@SuppressWarnings("unchecked")
//	@Override
//	public final String getUrl() {
//		return getStoredSettings().getUrl();
//	}

	@SuppressWarnings("unchecked")
	@Override
	public final String getUsername() {
		return getStoredSettings().getUsername();
	}

//	@SuppressWarnings("unchecked")
//	@Override
//	public final List<DatabaseConnectionSettings> getClusterHosts() {
//		return getStoredSettings().getClusterHosts();
//	}
}
