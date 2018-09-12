/*
 * Copyright 2018 Gregory Graham.
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
package nz.co.gregs.dbvolution.databases;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import nz.co.gregs.dbvolution.annotations.DBTableName;

/**
 * A standardized collection of the database connection settings.
 *
 * <p>
 * This object is a bean to provide a consistent way of defining a the
 * connection details needed to connect to a database.</p>
 *
 * <p>
 * Connection details can be grouped as username/password, URL, settings, and
 * extras.</p>
 *
 * <p>
 * Username and password are generally required to connect to a database and are
 * provided to the connection separately from the url, settings, and extras.</p>
 *
 * <p>
 * URL, settings, and extras are used to create the JDBC connection URL to the
 * database and, with the username/password, to connection to the database.</p>
 *
 * <p>
 * If the URL is supplied it will be used as provided and settings and extras
 * will be ignored. This is reflected in the 2 standard constructors for
 * DatabaseConnectionSettings:
 * {@link #DatabaseConnectionSettings(java.lang.String, java.lang.String, java.lang.String) one for username/password/url}
 * and
 * {@link #DatabaseConnectionSettings(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.util.Map) one for username/password/settings/extras}.</p>
 *
 * <p>
 * Without an explicit URL the settings (host, port, instance, database, schema)
 * and extras will be combined to create the JDBC URL. This combination is
 * deferred to the appropriate DBDatabase class and its version of null {@link DBDatabase#getUrlFromSettings(nz.co.gregs.dbvolution.databases.DatabaseConnectionSettings)
 * }</p>
 *
 * <p>
 * Extras are any miscellaneous and database specific settings that are added to
 * the end of the JDBC URL to tweak the connection or database. Generally these
 * are very database specific and will not work as expected for any other
 * providers product.</p>
 *
 * <p>
 * There is some confusion in the Database/JDBC world as to what some of the
 * settings names (host, port, instance, database, schema) mean. For the
 * purposes of DatabaseConnectionSettings:</p>
 * <ul>
 * <li>Host is the server name or Internet address of the database server, for
 * instance db1.acme.com or 101.203.54.9.</li>
 * <li>Port is the port number on the host that the database will accept
 * connections from, for instance 1336</li>
 * <li>Instance is the particular application or service that is providing the
 * database if the database application is capable of running multiple instances
 * on one server. Many databases are not and this setting should be ignored for
 * those that cannot.</li>
 * <li>Database is the named database within the application that the connection
 * should use. Database is the central concept that all database providers
 * implement. File based databases should use this to provide the file
 * name.</li>
 * <li>Schema is the level below database. It is optional or irrelevant for many
 * RDBMSs or user setups. This is primarily where a user can create their own
 * groupings below the database that they have been assigned to. Schema can also
 * be specified using {@link DBTableName} when the schema name is
 * unchanging.</li>
 * </ul>
 *
 *
 * @author Gregory Graham
 */
public class DatabaseConnectionSettings {

	private String url = "";
	private String host = "";
	private String port = "";
	private String instance = "";
	private String database = "";
	private String username = "";
	private String password = "";
	private String schema = "";
	private final Map<String, String> extras = new HashMap<>();
	private String dbdatabase = "";

	public DatabaseConnectionSettings() {
		super();
	}

	/**
	 * A standardized collection of the database connection settings.
	 *
	 * <p>
	 * This object is a bean to provide a consistent way of defining a the
	 * connection details needed to connect to a database.</p>
	 *
	 * <p>
	 * Connection details can be grouped as username/password, URL, settings, and
	 * extras.</p>
	 *
	 * <p>
	 * Username and password are generally required to connect to a database and
	 * are provided to the connection separately from the url, settings, and
	 * extras.</p>
	 *
	 * <p>
	 * URL, settings, and extras are used to create the JDBC connection URL to the
	 * database and, with the username/password, to connection to the
	 * database.</p>
	 *
	 * <p>
	 * If the URL is supplied it will be used as provided and settings and extras
	 * will be ignored. This is reflected in the 2 standard constructors for
	 * DatabaseConnectionSettings:
	 * {@link #DatabaseConnectionSettings(java.lang.String, java.lang.String, java.lang.String) one for username/password/url}
	 * and
	 * {@link #DatabaseConnectionSettings(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.util.Map) one for username/password/settings/extras}.</p>
	 *
	 * <p>
	 * Without an explicit URL the settings (host, port, instance, database,
	 * schema) and extras will be combined to create the JDBC URL. This
	 * combination is deferred to the appropriate DBDatabase class and its version
	 * of null {@link DBDatabase#getUrlFromSettings(nz.co.gregs.dbvolution.databases.DatabaseConnectionSettings)
	 * }</p>
	 *
	 * <p>
	 * Extras are any miscellaneous and database specific settings that are added
	 * to the end of the JDBC URL to tweak the connection or database. Generally
	 * these are very database specific and will not work as expected for any
	 * other providers product.</p>
	 *
	 * <p>
	 * There is some confusion in the Database/JDBC world as to what some of the
	 * settings names (host, port, instance, database, schema) mean. For the
	 * purposes of DatabaseConnectionSettings:</p>
	 * <ul>
	 * <li>Host is the server name or Internet address of the database server, for
	 * instance db1.acme.com or 101.203.54.9.</li>
	 * <li>Port is the port number on the host that the database will accept
	 * connections from, for instance 1336</li>
	 * <li>Instance is the particular application or service that is providing the
	 * database if the database application is capable of running multiple
	 * instances on one server. Many databases are not and this setting should be
	 * ignored for those that cannot.</li>
	 * <li>Database is the named database within the application that the
	 * connection should use. Database is the central concept that all database
	 * providers implement. File based databases should use this to provide the
	 * file name.</li>
	 * <li>Schema is the level below database. It is optional or irrelevant for
	 * many RDBMSs or user setups. This is primarily where a user can create their
	 * own groupings below the database that they have been assigned to. Schema
	 * can also be specified using {@link DBTableName} when the schema name is
	 * unchanging.</li>
	 * </ul>
	 *
	 *
	 * @author Gregory Graham
	 * @param url
	 * @param username
	 * @param password
	 */
	public DatabaseConnectionSettings(String url, String username, String password) {
		super();
		this.url = url;
		this.username = username;
		this.password = password;
	}

	/**
	 * A standardized collection of the database connection settings.
	 *
	 * <p>
	 * This object is a bean to provide a consistent way of defining a the
	 * connection details needed to connect to a database.</p>
	 *
	 * <p>
	 * Connection details can be grouped as username/password, URL, settings, and
	 * extras.</p>
	 *
	 * <p>
	 * Username and password are generally required to connect to a database and
	 * are provided to the connection separately from the url, settings, and
	 * extras.</p>
	 *
	 * <p>
	 * URL, settings, and extras are used to create the JDBC connection URL to the
	 * database and, with the username/password, to connection to the
	 * database.</p>
	 *
	 * <p>
	 * If the URL is supplied it will be used as provided and settings and extras
	 * will be ignored. This is reflected in the 2 standard constructors for
	 * DatabaseConnectionSettings:
	 * {@link #DatabaseConnectionSettings(java.lang.String, java.lang.String, java.lang.String) one for username/password/url}
	 * and
	 * {@link #DatabaseConnectionSettings(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.util.Map) one for username/password/settings/extras}.</p>
	 *
	 * <p>
	 * Without an explicit URL the settings (host, port, instance, database,
	 * schema) and extras will be combined to create the JDBC URL. This
	 * combination is deferred to the appropriate DBDatabase class and its version
	 * of null {@link DBDatabase#getUrlFromSettings(nz.co.gregs.dbvolution.databases.DatabaseConnectionSettings)
	 * }</p>
	 *
	 * <p>
	 * Extras are any miscellaneous and database specific settings that are added
	 * to the end of the JDBC URL to tweak the connection or database. Generally
	 * these are very database specific and will not work as expected for any
	 * other providers product.</p>
	 *
	 * <p>
	 * There is some confusion in the Database/JDBC world as to what some of the
	 * settings names (host, port, instance, database, schema) mean. For the
	 * purposes of DatabaseConnectionSettings:</p>
	 * <ul>
	 * <li>Host is the server name or Internet address of the database server, for
	 * instance db1.acme.com or 101.203.54.9.</li>
	 * <li>Port is the port number on the host that the database will accept
	 * connections from, for instance 1336</li>
	 * <li>Instance is the particular application or service that is providing the
	 * database if the database application is capable of running multiple
	 * instances on one server. Many databases are not and this setting should be
	 * ignored for those that cannot.</li>
	 * <li>Database is the named database within the application that the
	 * connection should use. Database is the central concept that all database
	 * providers implement. File based databases should use this to provide the
	 * file name.</li>
	 * <li>Schema is the level below database. It is optional or irrelevant for
	 * many RDBMSs or user setups. This is primarily where a user can create their
	 * own groupings below the database that they have been assigned to. Schema
	 * can also be specified using {@link DBTableName} when the schema name is
	 * unchanging.</li>
	 * </ul>
	 *
	 *
	 * @author Gregory Graham
	 * @param host
	 * @param port
	 * @param instance
	 * @param database
	 * @param schema
	 * @param username
	 * @param password
	 * @param extras
	 */
	public DatabaseConnectionSettings(String host, String port, String instance, String database, String schema, String username, String password, Map<String, String> extras) {
		super();
		this.host = host;
		this.port = port;
		this.instance = instance;
		this.database = database;
		this.schema = schema;
		this.username = username;
		this.password = password;
		this.extras.putAll(extras);
	}

	public static DatabaseConnectionSettings getSettingsfromSystemUsingPrefix(String prefix) {
		DatabaseConnectionSettings settings = new DatabaseConnectionSettings();
		settings.setDbdatabase(System.getProperty(prefix + "dbdatabase"));
		settings.setUsername(System.getProperty(prefix + "username"));
		settings.setPassword(System.getProperty(prefix + "password"));
		settings.setUrl(System.getProperty(prefix + "url"));
		settings.setHost(System.getProperty(prefix + "host"));
		settings.setPort(System.getProperty(prefix + "port"));
		settings.setInstance(System.getProperty(prefix + "instance"));
		settings.setDatabaseName(System.getProperty(prefix + "database"));
		settings.setSchema(System.getProperty(prefix + "schema"));
		return settings;
	}

	public final DBDatabase createDBDatabase() throws ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		Class<?> dbDatabaseClass = Class.forName(this.getDbdatabase());
		Constructor<?> constructor = dbDatabaseClass.getConstructor(DatabaseConnectionSettings.class);
		if (constructor == null) {
			return null;
		} else {
			constructor.setAccessible(true);
			Object newInstance = constructor.newInstance(this);
			if (newInstance != null && DBDatabase.class.isInstance(newInstance)) {
				return (DBDatabase) newInstance;
			} else {
				return null;
			}
		}
	}

	/**
	 * @return the url
	 */
	public String getUrl() {
		return url;
	}

	/**
	 * @return the host
	 */
	public String getHost() {
		return host;
	}

	/**
	 * @return the port
	 */
	public String getPort() {
		return port;
	}

	/**
	 * @return the instance
	 */
	public String getInstance() {
		return instance;
	}

	/**
	 * @return the database
	 */
	public String getDatabaseName() {
		return database;
	}

	/**
	 * @return the username
	 */
	public String getUsername() {
		return username;
	}

	/**
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * @return the schema
	 */
	public String getSchema() {
		return schema;
	}

	/**
	 * @param url the url to set
	 */
	public void setUrl(String url) {
		this.url = url;
	}

	/**
	 * @param host the host to set
	 */
	public void setHost(String host) {
		this.host = host;
//		return this;
	}

	/**
	 * @param port the port to set
	 */
	public void setPort(String port) {
		this.port = port;
//		return this;
	}

	/**
	 * @param instance the instance to set
	 */
	public void setInstance(String instance) {
		this.instance = instance;
//		return this;
	}

	/**
	 * @param database the database to set
	 */
	public void setDatabaseName(String database) {
		this.database = database;
//		return this;
	}

	/**
	 * @param username the username to set
	 */
	public void setUsername(String username) {
		this.username = username;
//		return this;
	}

	/**
	 * @param password the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
//		return this;
	}

	/**
	 * @param schema the schema to set
	 */
	public void setSchema(String schema) {
		this.schema = schema;
//		return this;
	}

	/**
	 * @return the extras
	 */
	public Map<String, String> getExtras() {
		return extras;
	}

	/**
	 * @param newExtras
	 * @return the extras
	 */
	public DatabaseConnectionSettings setExtras(Map<String, String> newExtras) {
		extras.clear();
		extras.putAll(newExtras);
		return this;
	}

	public String formatExtras(String prefix, String nameValueSeparator, String nameValuePairSeparator, String suffix) {
		StringBuilder str = new StringBuilder();
		for (Entry<String, String> extra : extras.entrySet()) {
			if (str.length() > 0) {
				str.append(nameValuePairSeparator);
			}
			str.append(extra.getKey()).append(nameValueSeparator).append(extra.getValue());
		}
		if (str.length() > 0) {
			return prefix + str.toString() + suffix;
		} else {
			return "";
		}
	}

	public void setDbdatabase(String canonicalNameOfADBDatabaseSubclass) {
		this.dbdatabase = canonicalNameOfADBDatabaseSubclass;
	}

	public String getDbdatabase() {
		return this.dbdatabase;
	}
}
