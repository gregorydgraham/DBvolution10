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
import nz.co.gregs.dbvolution.databases.DatabaseConnectionSettings;
import nz.co.gregs.dbvolution.databases.MySQLMXJDB;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.MySQLDBDefinition;

/**
 *
 * @author gregorygraham
 */
public class MySQLMXJDBSettingsBuilder extends AbstractMySQLSettingsBuilder<MySQLMXJDBSettingsBuilder, MySQLMXJDB> {

	private final static HashMap<String, String> DEFAULT_EXTRAS_MAP = new HashMap<>() {
		{
			put("createDatabaseIfNotExist", "true");
			put("server.initialize-user", "true");
		}
	};
	private static final long serialVersionUID = 1L;
	
	@Override
	public String getDefaultDriverName() {
		return MySQLMXJDB.MYSQLDRIVERNAME;
	}

	@Override
	public DBDefinition getDefaultDefinition() {
		return new MySQLDBDefinition();
	}

	@Override
	public Map<String, String> getDefaultConfigurationExtras() {
		return DEFAULT_EXTRAS_MAP;
	}

	@Override
	public DatabaseConnectionSettings generateSettingsInternal(String jdbcURL, DatabaseConnectionSettings settings) {
		String noPrefix = jdbcURL.replaceAll("^jdbc:postgresql://", "");
		if (jdbcURL.matches(";")) {
			String extrasString = jdbcURL.split("\\?", 2)[1];
			settings.setExtras(DatabaseConnectionSettings.decodeExtras(extrasString, "", "=", "&", ""));
		}
		settings.setPort(noPrefix
				.split("/", 2)[0]
				.replaceAll("^[^:]*:+", ""));
		settings.setHost(noPrefix
				.split("/", 2)[0]
				.split(":")[0]);
		settings.setInstance(settings.getExtras().get("instance"));
		settings.setSchema("");
		return settings;
	}

//	@Override
//	public String generateJDBCURLInternal(DatabaseConnectionSettings settings) {
//		String url = settings.getUrl();
//		return url != null && !url.isEmpty() ? url : "jdbc:mysql:mxj://" + settings.getHost() + ":" + settings.getPort() + "/" + settings.getDatabaseName()
//				+encodeExtras(settings, "?", "=", "&", "");
//	}
	@Override
	public String encodeHost(DatabaseConnectionSettings settings) {
		return settings.getHost()
				+ ":" + settings.getPort()
				+ "/" + settings.getDatabaseName()
				+ encodeExtras(settings, "?", "=", "&", "");
	}

	@Override
	protected String getJDBCURLPreamble(DatabaseConnectionSettings settings) {
		return "jdbc:mysql:mxj://";
	}

	@Override
	public DatabaseConnectionSettings setDefaultsInternal(DatabaseConnectionSettings settings) {
		return settings;
	}

	@Override
	public Class<MySQLMXJDB> generatesURLForDatabase() {
		return MySQLMXJDB.class;
	}

	@Override
	public Integer getDefaultPort() {
		return 3306;
	}

	public MySQLMXJDBSettingsBuilder setBanana(boolean bool) {
		return this;
	}

	@Override
	public MySQLMXJDB getDBDatabase() throws Exception {
		return new MySQLMXJDB(this);
	}
}
