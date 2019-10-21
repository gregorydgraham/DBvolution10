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
package nz.co.gregs.dbvolution.databases.jdbcurlinterpreters;

import java.util.Map;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DatabaseConnectionSettings;
import nz.co.gregs.dbvolution.utility.SeparatedString;

/**
 *
 * @author gregorygraham
 */
public abstract class AbstractURLInterpreter implements JDBCURLInterpreter {

	protected abstract DatabaseConnectionSettings generateSettingsInternal(String jdbcURL, DatabaseConnectionSettings settings);

	protected abstract String generateJDBCURLInternal(DatabaseConnectionSettings settings);

	protected abstract DatabaseConnectionSettings setDefaultsInternal(DatabaseConnectionSettings settings);

	@Override
	public final DatabaseConnectionSettings generateSettings() {
		DatabaseConnectionSettings settings = getEmptySettings();
		return settings;
	}
	
	@Override
	public final DatabaseConnectionSettings generateSettings(String jdbcURL) {
		DatabaseConnectionSettings settings = getEmptySettings();
		settings.setUrl(jdbcURL);
		return generateSettingsInternal(jdbcURL, settings);
	}
	
	@Override
	public final DatabaseConnectionSettings generateSettings(String jdbcURL, String username, String password) {
		DatabaseConnectionSettings settings = generateSettings(jdbcURL);
		settings.setUsername(username);
		settings.setPassword(password);
		settings = generateSettingsInternal(jdbcURL, settings);
		return settings;
	}

	@Override
	public final String generateJDBCURL(DatabaseConnectionSettings settings) {
		String url = settings.getUrl();
		return url != null && !url.isEmpty() ? url : generateJDBCURLInternal(settings);
	}

	private DatabaseConnectionSettings setPortAndDefaults(DatabaseConnectionSettings settings) {
		settings.setPort("" + getDefaultPort());
		return setDefaultsInternal(settings);
	}

	@Override
	public final boolean canProcessesURLsFor(DBDatabase otherdb) {
		Class<? extends DBDatabase> db = generatesURLForDatabase();
		return db.isAssignableFrom(otherdb.getClass());
	}

	public String encodeExtras(DatabaseConnectionSettings settings, String prefix, String nameValueSeparator, String nameValuePairSeparator, String suffix) {
		Map<String, String> extras = settings.getExtras();
		SeparatedString sep = SeparatedString
				.of(extras, nameValueSeparator)
				.withPrefix(prefix)
				.withSuffix(suffix)
				.separatedBy(nameValuePairSeparator);
		return sep.toString();
	}

	private DatabaseConnectionSettings setCommonDefaults(DatabaseConnectionSettings settings) {
		settings.setDbdatabaseClass(generatesURLForDatabase().getCanonicalName());
		settings.setDefaultExtras(getDefaultConfigurationExtras());
		setPortAndDefaults(settings);
		return settings;
	}

	private DatabaseConnectionSettings getEmptySettings() {
		DatabaseConnectionSettings settings = new DatabaseConnectionSettings();
		setCommonDefaults(settings);
		return settings;
	}
}
