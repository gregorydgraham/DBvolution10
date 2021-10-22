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
import nz.co.gregs.dbvolution.databases.JavaDB;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.JavaDBDefinition;

/**
 *
 * @author gregorygraham
 * @param <SELF> the class of the object returned by most methods, this should be the Class of "this"
 * @param <DATABASE> the class returned by {@link #getDBDatabase}
 */
public abstract class AbstractJavaDBSettingsBuilder<SELF extends AbstractJavaDBSettingsBuilder<SELF, DATABASE>, DATABASE extends DBDatabase> 
		extends AbstractVendorSettingsBuilder<SELF, DATABASE>
		implements InstanceCapableSettingsBuilder<SELF, DATABASE>,
		RemoteCapableSettingsBuilder<SELF, DATABASE>,
		NamedDatabaseCapableSettingsBuilder<SELF, DATABASE>,
		ExtrasCapableSettingsBuilder<SELF, DATABASE> {

	protected static final HashMap<String, String> DEFAULT_EXTRAS_MAP = new HashMap<>() {
		{
			put("create", "true");
		}
	};

	@Override
	public String getDefaultDriverName() {
		return JavaDB.DRIVER_NAME;
	}

	@Override
	public DBDefinition getDefaultDefinition() {
		return new JavaDBDefinition();
	}

	@Override
	public Map<String, String> getDefaultConfigurationExtras() {
		return DEFAULT_EXTRAS_MAP;
	}

	@Override
	public DatabaseConnectionSettings generateSettingsInternal(String jdbcURL, DatabaseConnectionSettings settings) {
		String noPrefix = jdbcURL.replaceAll("^" + getJDBCURLPreamble(), "");
		settings.setPort(noPrefix.split("/", 2)[0].replaceAll("^[^:]*:", ""));
		settings.setHost(noPrefix.split("/", 2)[0].split(":")[0]);
		if (jdbcURL.matches(";")) {
			String extrasString = jdbcURL.split(";", 2)[1];
			settings.setExtras(DatabaseConnectionSettings.decodeExtras(extrasString, "", "=", ";", ""));
		}
		settings.setInstance(settings.getExtras().get("instance"));
		settings.setSchema("");
		return settings;
	}

	protected String getJDBCURLPreamble() {
		return "jdbc:derby://";
	}

	@Override
	protected String getJDBCURLPreamble(DatabaseConnectionSettings settings) {
		return getJDBCURLPreamble();
	}

	@Override
	public String encodeHost(DatabaseConnectionSettings settings) {
		return settings.getHost()
				+ ":" + settings.getPort()
				+ "/" + ("".equals(settings.getProtocol()) ? "" : settings.getProtocol() + ":")
				+ settings.getDatabaseName()
				+ encodeExtras(settings, ";", "=", ";", "");
	}

//	@Override
//	public Class<? extends DBDatabase> generatesURLForDatabase() {
//		return JavaDB.class;
//	}
	@Override
	public DatabaseConnectionSettings setDefaultsInternal(DatabaseConnectionSettings settings) {
		return settings;
	}

	@Override
	public Integer getDefaultPort() {
		return 1527;
	}
}
