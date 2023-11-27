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

import javax.sql.DataSource;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DatabaseConnectionSettings;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;

/**
 * A flexible descriptor of databases that can create a DBDatabase instance, database connection settings, or a JDBC URL.
 *
 * @author gregorygraham
 * @param <SELF> the class of the object returned by most methods, this should be the Class of "this"
 * @param <DATABASE> the class returned by {@link #getDBDatabase}
 */
public interface SettingsBuilder<SELF extends SettingsBuilder<SELF, DATABASE>, DATABASE extends DBDatabase> {

	DatabaseConnectionSettings getStoredSettings();

	public String generateJDBCURL(DatabaseConnectionSettings settings);

	public Class<DATABASE> generatesURLForDatabase();

	public DATABASE getDBDatabase() throws Exception;

	public default SELF fromDBDatabase(DATABASE db) throws Exception{
		fromJDBCURL(db.getJdbcURL(), db.getUsername(), db.getPassword());
		return (SELF)this;
	}

	String encodeHost(DatabaseConnectionSettings settings);

	public boolean canProcessesURLsFor(DBDatabase otherdb);

	public Integer getDefaultPort();

	/**
	 * Part of the fluent API, this provides a quick why to parse a URL and alter
	 * it.
	 *
	 *
	 * @see #toSettings()
	 * @see #toJDBCURL()
	 * @see AbstractSettingsBuilder#setUsername(java.lang.String)
	 * @see AbstractSettingsBuilder#setPassword(java.lang.String)
	 * @param jdbcURL the JDBC URL to be parsed
	 * @return this settings builder object
	 */
	public SELF fromJDBCURL(String jdbcURL);

	/**
	 * Part of the fluent API, this provides a quick why to parse a URL and alter
	 * it.
	 *
	 * @param jdbcURL the JDBC URL to be interpreted
	 * @param username the username to use when connect to the database server
	 * @param password the password to use when connecting to the database server
	 * @see #toSettings()
	 * @see #toJDBCURL()
	 * @see AbstractSettingsBuilder#setUsername(java.lang.String)
	 * @see AbstractSettingsBuilder#setPassword(java.lang.String)
	 * @return this settings builder object
	 */
	public SELF fromJDBCURL(String jdbcURL, String username, String password);

	public SELF fromSettings(DatabaseConnectionSettings settingsfromSystemUsingPrefix);

	public SELF fromSystemUsingPrefix(String prefix);

	public DatabaseConnectionSettings toSettings();

	public String toJDBCURL();

	String getLabel();

	SELF setLabel(String label);

	String getUsername();

	SELF setUsername(String username);

	String getPassword();

	SELF setPassword(String password);

	DBDefinition getDefinition();

	DBDefinition getDefaultDefinition();

	SELF setDefinition(DBDefinition defn);
	
	DataSource getDataSource();

	SELF setDataSource(DataSource dataSource);

}
