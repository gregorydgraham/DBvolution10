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
import nz.co.gregs.dbvolution.databases.MariaDB;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.MariaDBDefinition;

/**
 *
 * @author gregorygraham
 */
public class MariaDBSettingsBuilder
		extends AbstractVendorSettingsBuilder<MariaDBSettingsBuilder, MariaDB>
		implements InstanceCapableSettingsBuilder<MariaDBSettingsBuilder, MariaDB>,
		RemoteCapableSettingsBuilder<MariaDBSettingsBuilder, MariaDB>,
		NamedDatabaseCapableSettingsBuilder<MariaDBSettingsBuilder, MariaDB> {

	private final static HashMap<String, String> DEFAULT_EXTRAS_MAP = new HashMap<>();
	private static final long serialVersionUID = 1L;

	@Override
	public String getDefaultDriverName() {
		return MariaDB.MARIADBDRIVERNAME;
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
		String noPrefix = jdbcURL.replaceAll("^" + getJDBCURLPreamble(), "");
		set.setPort(noPrefix
				.split("/", 2)[0]
				.replaceAll("^[^:]*:+", ""));
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
//		return getJDBCURLPreamble()
//				+ settings.getHost() + ":"
//				+ settings.getPort() + "/"
//				+ settings.getDatabaseName();
//	}
	@Override
	public DatabaseConnectionSettings setDefaultsInternal(DatabaseConnectionSettings settings) {
		return settings;
	}

	@Override
	public Class<MariaDB> generatesURLForDatabase() {
		return MariaDB.class;
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
	public MariaDB getDBDatabase() throws Exception {
		return new MariaDB(this);
	}
}
