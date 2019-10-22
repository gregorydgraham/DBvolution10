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
import javax.sql.DataSource;
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
	private String label = "";
	private DataSource dataSource = null;
	private String protocol;

	private static final String FIELD_SEPARATOR = "<DCS FIELD>";
	private static final String TOSTRING_SEPARATOR = ", ";
	private String filename = "";

	public DatabaseConnectionSettings() {
		super();
	}

	public DatabaseConnectionSettings(DatabaseConnectionSettings dcs) {
		super();
		this.copy(dcs);
	}

	@Override
	public String toString() {
		return "DATABASECONNECTIONSETTINGS: "
				+ getDbdatabaseClass() + TOSTRING_SEPARATOR
				+ getHost() + TOSTRING_SEPARATOR
				+ getPort() + TOSTRING_SEPARATOR
				+ getInstance() + TOSTRING_SEPARATOR
				+ getDatabaseName() + TOSTRING_SEPARATOR
				+ getSchema() + TOSTRING_SEPARATOR
				+ getUrl() + TOSTRING_SEPARATOR
				+ getUsername() + TOSTRING_SEPARATOR
				+ getLabel() + TOSTRING_SEPARATOR
				+ getFilename()+ TOSTRING_SEPARATOR;
	}

	public String encode() {
		return "DATABASECONNECTIONSETTINGS: "
				+ getDbdatabaseClass() + FIELD_SEPARATOR
				+ getHost() + FIELD_SEPARATOR
				+ getPort() + FIELD_SEPARATOR
				+ getInstance() + FIELD_SEPARATOR
				+ getDatabaseName() + FIELD_SEPARATOR
				+ getSchema() + FIELD_SEPARATOR
				+ getUrl() + FIELD_SEPARATOR
				+ getUsername() + FIELD_SEPARATOR
				+ getPassword() + FIELD_SEPARATOR
				+ getLabel() + FIELD_SEPARATOR
				+ getFilename() + FIELD_SEPARATOR;
	}

	public static DatabaseConnectionSettings decode(String encodedSettings) {
		DatabaseConnectionSettings settings = new DatabaseConnectionSettings();

		String[] data = encodedSettings.split("DATABASECONNECTIONSETTINGS: ")[1].split(FIELD_SEPARATOR);
		if (data.length > 0) {
			settings.setDbdatabaseClass(data[0]);
			if (data.length > 1) {
				settings.setHost(data[1]);
				if (data.length > 2) {
					settings.setPort(data[2]);
					if (data.length > 3) {
						settings.setInstance(data[3]);
						if (data.length > 4) {
							settings.setDatabaseName(data[4]);
							if (data.length > 5) {
								settings.setSchema(data[5]);
								if (data.length > 6) {
									settings.setUrl(data[6]);
									if (data.length > 7) {
										settings.setUsername(data[7]);
										if (data.length > 8) {
											settings.setPassword(data[8]);
											if (data.length > 9) {
												settings.setLabel(data[9]);
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
		return settings;
	}

	public boolean equals(DatabaseConnectionSettings obj) {
		return this.encode().equals(obj.encode());
	}

	public boolean notEquals(DatabaseConnectionSettings obj) {
		return !this.encode().equals(obj.encode());
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
	 * @param url the JDBC URL to connect to the database
	 * @param username the username for logging in to the database
	 * @param password the password for the database user
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
	 * @param label an arbitrary label to identify the database
	 * @param url the JDBC URL to connect to the database
	 * @param username the username for logging in to the database
	 * @param password the password for the database user
	 */
	public DatabaseConnectionSettings(String url, String username, String password, String label) {
		super();
		this.label = label;
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
	 * @param label and arbitrary name for the database
	 * @param host the hostname of the database server
	 * @param port the port used to connect to the database
	 * @param instance the database instance to use
	 * @param database the name of the database in the instance
	 * @param schema the schema to use in the database
	 * @param username the username for logging in to the database
	 * @param password the password for the database user
	 * @param extras any other database specific settings
	 */
	public DatabaseConnectionSettings(String host, String port, String instance, String database, String schema, String username, String password, Map<String, String> extras, String label) {
		super();
		this.label = label;
		this.host = host;
		this.port = port;
		this.instance = instance;
		this.database = database;
		this.schema = schema;
		this.username = username;
		this.password = password;
		this.extras.putAll(extras);
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
	 * @param host the hostname of the database server
	 * @param port the port used to connect to the database
	 * @param instance the database instance to use
	 * @param database the name of the database in the instance
	 * @param schema the schema to use in the database
	 * @param username the username for logging in to the database
	 * @param password the password for the database user
	 * @param extras any other database specific settings
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
		settings.setLabel(System.getProperty(prefix + "label"));
		settings.setDbdatabaseClass(System.getProperty(prefix + "dbdatabase"));
		settings.setUsername(System.getProperty(prefix + "username"));
		settings.setPassword(System.getProperty(prefix + "password"));
		settings.setProtocol(System.getProperty(prefix + "protocol"));
		settings.setUrl(System.getProperty(prefix + "url"));
		settings.setHost(System.getProperty(prefix + "host"));
		settings.setPort(System.getProperty(prefix + "port"));
		settings.setInstance(System.getProperty(prefix + "instance"));
		settings.setDatabaseName(System.getProperty(prefix + "database"));
		settings.setSchema(System.getProperty(prefix + "schema"));
		return settings;
	}

	public final void copy(DatabaseConnectionSettings newSettings) {
		this.setDataSource(newSettings.getDataSource());
		this.setDatabaseName(newSettings.getDatabaseName());
		this.setFilename(newSettings.getFilename());
		this.setDbdatabaseClass(newSettings.getDbdatabaseClass());
		this.setHost(newSettings.getHost());
		this.setExtras(newSettings.getExtras());
		this.setInstance(newSettings.getInstance());
		this.setLabel(newSettings.getLabel());
		this.setPassword(newSettings.getPassword());
		this.setPort(newSettings.getPort());
		this.setProtocol(newSettings.getProtocol());
		this.setSchema(newSettings.getSchema());
		this.setUrl(newSettings.getUrl());
		this.setUsername(newSettings.getUsername());
	}

	/**
	 * Create the DBDatabase described by these settings
	 *
	 * @return the DBDatabase
	 * @throws ClassNotFoundException all database need an accessible default
	 * constructor
	 * @throws NoSuchMethodException all database need an accessible default
	 * constructor
	 * @throws SecurityException all database need an accessible default
	 * constructor
	 * @throws InstantiationException all database need an accessible default
	 * constructor
	 * @throws IllegalAccessException all database need an accessible default
	 * constructor
	 * @throws IllegalArgumentException all database need an accessible default
	 * constructor
	 * @throws InvocationTargetException all database need an accessible default
	 * constructor
	 */
	public final DBDatabase createDBDatabase() throws ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		Class<?> dbDatabaseClass = Class.forName(this.getDbdatabaseClass());
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
	public final String getUrl() {
		return url;
	}

	/**
	 * @return the host
	 */
	public final String getHost() {
		return host;
	}

	/**
	 * @return the port
	 */
	public final String getPort() {
		return port;
	}

	/**
	 * @return the instance
	 */
	public final String getInstance() {
		return instance;
	}

	/**
	 * @return the database
	 */
	public final String getDatabaseName() {
		return database;
	}

	/**
	 * @return the username
	 */
	public final String getUsername() {
		return username;
	}

	/**
	 * @return the password
	 */
	public final String getPassword() {
		return password;
	}

	/**
	 * @return the schema
	 */
	public final String getSchema() {
		return schema;
	}

	/**
	 * @param url the url to set
	 */
	public final void setUrl(String url) {
		this.url = url == null ? "" : url;
	}

	/**
	 * @param host the host to set
	 */
	public final void setHost(String host) {
		this.host = host == null ? "" : host;
	}

	/**
	 * @param port the port to set
	 */
	public final void setPort(String port) {
		this.port = port == null ? "" : port;
	}

	/**
	 * @param instance the instance to set
	 */
	public final void setInstance(String instance) {
		this.instance = instance == null ? "" : instance;
	}

	/**
	 * @param database the database to set
	 */
	public final void setDatabaseName(String database) {
		this.database = database == null ? "" : database;
	}

	/**
	 * @param username the username to set
	 */
	public final void setUsername(String username) {
		this.username = username == null ? "" : username;
	}

	/**
	 * @param password the password to set
	 */
	public final void setPassword(String password) {
		this.password = password == null ? "" : password;
	}

	/**
	 * @param schema the schema to set
	 */
	public final void setSchema(String schema) {
		this.schema = schema == null ? "" : schema;
	}

	/**
	 * @return the extras
	 */
	public final Map<String, String> getExtras() {
		return extras;
	}

	/**
	 * Removes all existing extras and adds the supplied values.
	 *
	 * @param newExtras extra settings for the database
	 * @return the extras
	 */
	public final DatabaseConnectionSettings setExtras(Map<String, String> newExtras) {
		extras.clear();
		if (newExtras != null && !newExtras.isEmpty()) {
			extras.putAll(newExtras);
		}
		return this;
	}

	/**
	 * Adds or replaces the new values to the existing extras.
	 *
	 * @param newExtras extra settings for the database
	 * @return the extras
	 */
	public final DatabaseConnectionSettings addExtras(Map<String, String> newExtras) {
		if (newExtras != null && !newExtras.isEmpty()) {
			extras.putAll(newExtras);
		}
		return this;
	}

	public final String formatExtras(String prefix, String nameValueSeparator, String nameValuePairSeparator, String suffix) {
		return encodeExtras(extras, prefix, nameValueSeparator, nameValuePairSeparator, suffix);
	}

	public static String encodeExtras(Map<String, String> extras, String prefix, String nameValueSeparator, String nameValuePairSeparator, String suffix) {
		StringBuilder str = new StringBuilder();
		extras.entrySet().forEach((extra) -> {
			if (str.length() > 0) {
				str.append(nameValuePairSeparator);
			}
			str.append(extra.getKey()).append(nameValueSeparator).append(extra.getValue());
		});
		if (str.length() > 0) {
			return prefix + str.toString() + suffix;
		} else {
			return "";
		}
	}

	public static Map<String, String> decodeExtras(String extras, String prefix, String nameValueSeparator, String nameValuePairSeparator, String suffix) {
		Map<String, String> map = new HashMap<String, String>();
		String onlyValues = extras.replaceAll("$" + prefix, "");
		String[] split = onlyValues.split(nameValuePairSeparator);
		for (String string : split) {
			String[] nameAndValue = string.split(nameValueSeparator);
			map.put(nameAndValue[0], nameAndValue[1]);
		}
		return map;
	}

	public final void setDbdatabaseClass(String canonicalNameOfADBDatabaseSubclass) {
		this.dbdatabase = canonicalNameOfADBDatabaseSubclass;
	}

	public final String getDbdatabaseClass() {
		return this.dbdatabase;
	}

	/**
	 * A label for the database for reference within an application.
	 *
	 * <p>
	 * This label has no effect on the actual database connection.
	 *
	 * @param label an arbitrary name for the database
	 */
	public final void setLabel(String label) {
		this.label = label;
	}

	/**
	 * A label for the database for reference within an application.
	 *
	 * <p>
	 * This label has no effect on the actual database connection.
	 *
	 *
	 * @return the label set for the database
	 */
	public final String getLabel() {
		return this.label;
	}

	public final void setDataSource(DataSource ds) {
		dataSource = ds;
	}

	public final DataSource getDataSource() {
		return dataSource;
	}

	public final void setProtocol(String protocol) {
		this.protocol = protocol;
	}

	public final String getProtocol() {
		return protocol;
	}

	public final void setDefaultExtras(Map<String, String> defaultConfigurationExtras) {
		defaultConfigurationExtras.forEach((t, u) -> {
			this.extras.putIfAbsent(t, u);
		});
	}

	public final void addExtra(String tag, String value) {
		this.extras.put(tag, value);
	}

//	public DatabaseConnectionSettings flowHost(String server) {
//		this.setHost(server);
//		return this;
//	}
//
//	public DatabaseConnectionSettings flowPort(int port) {
//		this.setPort(""+port);
//		return this;
//	}
//
//	public DatabaseConnectionSettings flowPort(long port) {
//		this.setPort(""+port);
//		return this;
//	}
//
//	public DatabaseConnectionSettings flowPort(String port) {
//		this.setPort(port);
//		return this;
//	}
//
//	public DatabaseConnectionSettings flowDatasource(DataSource source) {
//		this.setDataSource(source);
//		return this;
//	}
//
//	public DatabaseConnectionSettings flowDatabaseName(String databaseName) {
//		this.setDatabaseName(databaseName);
//		return this;
//	}
//
//	public DatabaseConnectionSettings flowDbdatabaseClass(String canonicalName) {
//		this.setDbdatabaseClass(canonicalName);
//		return this;
//	}
//
//	public DatabaseConnectionSettings flowExtras(Map<String, String> extras) {
//		this.addExtras(extras);
//		return this;
//	}
//
//	public DatabaseConnectionSettings flowExtra(String name, String value) {
//		this.addExtra(name, value);
//		return this;
//	}
//
//	public DatabaseConnectionSettings flowInstance(String instance) {
//		this.setInstance(instance);
//		return this;
//	}
//
//	public DatabaseConnectionSettings flowLabel(String label) {
//		this.setLabel(label);
//		return this;
//	}
//
//	public DatabaseConnectionSettings flowPassword(String password) {
//		this.setPassword(password);
//		return this;
//	}
//
//	public DatabaseConnectionSettings flowProtocol(String protocol) {
//		this.setProtocol(protocol);
//		return this;
//	}
//
//	public DatabaseConnectionSettings flowSchema(String schema) {
//		this.setSchema(schema);
//		return this;
//	}
//
//	public DatabaseConnectionSettings flowURL(String url) {
//		this.setUrl(url);
//		return this;
//	}
//
//	public DatabaseConnectionSettings flowUsername(String username) {
//		this.setUsername(username);
//		return this;
//	}
//
//	public DatabaseConnectionSettings flowFilename(String filename) {
//		this.setFilename(filename);
//		return this;
//	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public String getFilename() {
		return filename;
	}
}
