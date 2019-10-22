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

import java.util.HashMap;
import java.util.Map;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DatabaseConnectionSettings;
import nz.co.gregs.dbvolution.databases.SQLiteDB;

/**
 *
 * @author gregorygraham
 */
public class SQLiteURLInterpreter extends AbstractURLInterpreter<SQLiteURLInterpreter> {

	private final static HashMap<String, String> DEFAULT_EXTRAS_MAP = new HashMap<>();

	@Override
	public Map<String, String> getDefaultConfigurationExtras() {
		return DEFAULT_EXTRAS_MAP;
	}

	public SQLiteURLInterpreter() {
	}

	@Override
	protected DatabaseConnectionSettings generateSettingsInternal(String jdbcURL, DatabaseConnectionSettings set) {
		String noPrefix = jdbcURL.replaceAll("^jdbc:sqlite://", "");
		if (jdbcURL.contains(";")) {
			String extrasString = jdbcURL.split("\\?", 2)[1];
			set.setExtras(DatabaseConnectionSettings.decodeExtras(extrasString, "", "=", ";", ""));
		}
		final String name = noPrefix.split(":", 3)[2];
		set.setDatabaseName(name);
		set.setFilename(name);
		if (noPrefix.contains("/")) {
			set.setPort(noPrefix
					.split("/", 2)[0]
					.replaceAll("^[^:]*:+", ""));
			set.setHost(noPrefix
					.split("/", 2)[0]
					.split(":")[0]);
		}
		set.setInstance(set.getExtras().get("instance"));
		set.setSchema("");
		return set;
	}

	@Override
	protected String generateJDBCURLInternal(DatabaseConnectionSettings settings) {
		final String url = "jdbc:sqlite:"
				+ (settings.getFilename() == null || settings.getFilename().isEmpty()
				? settings.getDatabaseName()
				: settings.getFilename());
		return url;
	}

	@Override
	public Class<? extends DBDatabase> generatesURLForDatabase() {
		return SQLiteDB.class;
	}

	@Override
	protected DatabaseConnectionSettings setDefaultsInternal(DatabaseConnectionSettings settings) {
		return settings;
	}

	@Override
	public Integer getDefaultPort() {
		return -1;// SQLite doesn't use ports
	}
}
