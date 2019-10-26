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

import java.util.HashMap;
import java.util.Map;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DatabaseConnectionSettings;
import nz.co.gregs.dbvolution.databases.MSSQLServerDB;

/**
 *
 * @author gregorygraham
 * @param <SELF>
 */
public abstract class AbstractMSSQLServerSettingsBuilder<SELF extends AbstractMSSQLServerSettingsBuilder<SELF>> extends AbstractSettingsBuilder<SELF>
		implements InstanceCapableSettingsBuilder<SELF>,
		RemoteCapableSettingsBuilder<SELF>,
		NamedDatabaseCapableSettingsBuilder<SELF>,
		ExtrasCapableSettingsBuilder<SELF> {

	protected static final HashMap<String, String> DEFAULT_EXTRAS_MAP = new HashMap<>();

	@Override
	public Map<String, String> getDefaultConfigurationExtras() {
		return DEFAULT_EXTRAS_MAP;
	}

	@Override
	public DatabaseConnectionSettings generateSettingsInternal(String jdbcURL, DatabaseConnectionSettings set) {
		//		DatabaseConnectionSettings set = getEmptySettings();
		String noPrefix = jdbcURL.replaceAll("^" + getJDBCURLPreamble(), "");
		final String[] splitOnBackslash = noPrefix.split("\\\\", 2);
		final String host = splitOnBackslash[0];
		set.setHost(host);
		if (splitOnBackslash.length > 1) {
			final String[] splitOnColon = splitOnBackslash[1].split(":");
			if (splitOnColon.length > 1) {
				final String port = splitOnColon[1];
				set.setPort(port);
			}
			final String instance = splitOnColon[0];
			set.setInstance(instance);
		}
		if (jdbcURL.matches(";")) {
			String extrasString = jdbcURL.split(";", 2)[1];
			set.setExtras(DatabaseConnectionSettings.decodeExtras(extrasString, "", "=", ";", ""));
		}
		set.setSchema("");
		return set;
	}

	protected String getJDBCURLPreamble() {
		return "jdbc:sqlserver://";
	}

	@Override
	protected String getJDBCURLPreamble(DatabaseConnectionSettings settings) {
		return getJDBCURLPreamble();
	}

	@Override
	public String encodeHost(DatabaseConnectionSettings settings) {
		final String databaseName = settings.getDatabaseName();
		final String instance = settings.getInstance();
		final String urlFromSettings
				= settings.getHost()
				+ (instance != null && !instance.isEmpty() ? "\\" + instance : "")
				+ ":" + settings.getPort() + ";"
				+ (databaseName == null || databaseName.isEmpty() ? "" : "databaseName=" + databaseName + ";")
				+ encodeExtras(settings, ";", "=", ";", "");
		return urlFromSettings;
	}

//	@Override
//	public String generateJDBCURLInternal(DatabaseConnectionSettings settings) {
//		String url = settings.getUrl();
//		final String databaseName = settings.getDatabaseName();
//		final String instance = settings.getInstance();
//		final String urlFromSettings
//				= getJDBCURLPreamble()
//				+ settings.getHost()
//				+ (instance != null && !instance.isEmpty() ? "\\" + instance : "")
//				+ ":" + settings.getPort() + ";"
//				+ (databaseName == null || databaseName.isEmpty() ? "" : "databaseName=" + databaseName + ";");
//		return url != null && !url.isEmpty() ? url : urlFromSettings;
//	}
	@Override
	public DatabaseConnectionSettings setDefaultsInternal(DatabaseConnectionSettings settings) {
		return settings;
	}

	@Override
	public Class<? extends DBDatabase> generatesURLForDatabase() {
		return MSSQLServerDB.class;
	}

	@Override
	public Integer getDefaultPort() {
		return 1433;
	}

}
