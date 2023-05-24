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
import nz.co.gregs.dbvolution.databases.OracleDB;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.OracleDBDefinition;

/**
 *
 * @author gregorygraham
 * @param <SELF> the class of the object returned by most methods, this should
 * be the Class of "this"
 * @param <DATABASE> the class returned by {@link #getDBDatabase}
 */
public abstract class AbstractOracleSettingsBuilder<SELF extends AbstractOracleSettingsBuilder<SELF, DATABASE>, DATABASE extends DBDatabase>
		extends AbstractVendorSettingsBuilder<SELF, DATABASE>
		implements
		InstanceCapableSettingsBuilder<SELF, DATABASE>,
		SchemaCapableSettingsBuilder<SELF, DATABASE>,
		RemoteCapableSettingsBuilder<SELF, DATABASE> {

	private final static HashMap<String, String> DEFAULT_EXTRAS_MAP = new HashMap<>();
	private static final long serialVersionUID = 1L;

	@Override
	public String getDefaultDriverName() {
		return OracleDB.ORACLE_JDBC_DRIVER;
	}

	@Override
	public DBDefinition getDefaultDefinition() {
		return new OracleDBDefinition();
	}

	@Override
	public Map<String, String> getDefaultConfigurationExtras() {
		return DEFAULT_EXTRAS_MAP;
	}

	@Override
	public DatabaseConnectionSettings generateSettingsInternal(String jdbcURL, DatabaseConnectionSettings set) {
		String noPrefix = jdbcURL.replaceAll("^" + "jdbc:oracle:[^:]*:@//", "");
		if (jdbcURL.matches(";")) {
			String extrasString = jdbcURL.split("\\?", 2)[1];
			set.setExtras(DatabaseConnectionSettings.decodeExtras(extrasString, "", "=", ";", ""));
		}
		set.setPort(noPrefix.split("/", 2)[0].replaceAll("^[^:]*:+", ""));
		set.setHost(noPrefix.split("/", 2)[0].split(":")[0]);
		set.setInstance(noPrefix.split("/", 2)[1]);
		set.setSchema("");
		return set;
	}

//	@Override
//	public String generateJDBCURLInternal(DatabaseConnectionSettings settings) {
//		String url = settings.getUrl();
//		return url != null && !url.isEmpty()
//				? url
//				: getJDBCURLPreamble()
//				+ settings.getHost() + ":"
//				+ settings.getPort() + "/"
//				+ settings.getInstance();
//	}
	@Override
	protected String getJDBCURLPreamble(DatabaseConnectionSettings settings) {
		return getJDBCURLPreamble();
	}

	protected String getJDBCURLPreamble() {
		return "jdbc:oracle:thin:@//";
	}

	@Override
	public DatabaseConnectionSettings setDefaultsInternal(DatabaseConnectionSettings settings) {
		return settings;
	}

	@Override
	public Integer getDefaultPort() {
		return 1521;
	}

	/**
	 * Apparently you can do this
	 *
	 * jdbc:oracle:thin:@(DESCRIPTION=(LOAD_BALANCE=on)
	 * (ADDRESS=(PROTOCOL=TCP)(HOST=host1) (PORT=1521))
	 * (ADDRESS=(PROTOCOL=TCP)(HOST=host2) (PORT=1521))
	 * (CONNECT_DATA=(SERVICE_NAME=service)))
	 *
	 * I have too much self-respect to do so (without being paid).
	 *
	 * @param settings the database connections setting to use
	 * @return a string encoding the host part of the JDBC URL
	 */
	@Override
	public String encodeHost(DatabaseConnectionSettings settings) {
		return settings.getHost() + ":"
				+ settings.getPort() + "/"
				+ settings.getInstance();
	}

	/**
	 * Synonym for setInstance.
	 *
	 * @param sid the server identifier to use
	 * @return this settings builder object
	 */
	public SELF setSID(String sid) {
		return this.setInstance(sid);
	}

	/**
	 * synonym for getInstance()
	 *
	 * @return the SID
	 */
	public String getSID() {
		return this.getInstance();
	}
}
