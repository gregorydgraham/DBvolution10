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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 *
 * @author gregorygraham
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
	private String file = "";
	private final Map<String, String> extras = new HashMap<>();

	private DatabaseConnectionSettings() {
	}

	public DatabaseConnectionSettings(String url, String username, String password) {
		super();
		this.url = url;
		this.username = username;
		this.password = password;
	}

	public DatabaseConnectionSettings(String host, String port, String instance, String database, String schema, String username, String password, Map<String, String> extras) {
		super();
		this.url = host;
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
		settings.setUrl(System.getProperty(prefix + "url"));
		settings.setHost(System.getProperty(prefix + "host"));
		settings.setPort(System.getProperty(prefix + "port"));
		settings.setInstance(System.getProperty(prefix + "instance"));
		settings.setDatabase(System.getProperty(prefix + "database"));
		settings.setUsername(System.getProperty(prefix + "username"));
		settings.setPassword(System.getProperty(prefix + "password"));
		settings.setSchema(System.getProperty(prefix + "schema"));
		settings.setFile(System.getProperty(prefix + "file"));
		return settings;
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
	 * @return the file
	 */
	public String getFile() {
		return file;
	}

	/**
	 * @param url the url to set
	 */
	public void setUrl(String url) {
		this.url = url;
	}

	/**
	 * @param host the host to set
	 * @return 
	 */
	public DatabaseConnectionSettings setHost(String host) {
		this.host = host;
		return this;
	}

	/**
	 * @param port the port to set
	 * @return 
	 */
	public DatabaseConnectionSettings setPort(String port) {
		this.port = port;
		return this;
	}

	/**
	 * @param instance the instance to set
	 * @return 
	 */
	public DatabaseConnectionSettings setInstance(String instance) {
		this.instance = instance;
		return this;
	}

	/**
	 * @param database the database to set
	 * @return 
	 */
	public DatabaseConnectionSettings setDatabase(String database) {
		this.database = database;
		return this;
	}

	/**
	 * @param username the username to set
	 * @return 
	 */
	public DatabaseConnectionSettings setUsername(String username) {
		this.username = username;
		return this;
	}

	/**
	 * @param password the password to set
	 * @return 
	 */
	public DatabaseConnectionSettings setPassword(String password) {
		this.password = password;
		return this;
	}

	/**
	 * @param schema the schema to set
	 * @return 
	 */
	public DatabaseConnectionSettings setSchema(String schema) {
		this.schema = schema;
		return this;
	}

	/**
	 * @param file the file to set
	 * @return 
	 */
	public DatabaseConnectionSettings setFile(String file) {
		this.file = file;
		return this;
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
	public DatabaseConnectionSettings setExtras(Map<String,String> newExtras) {
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
}
