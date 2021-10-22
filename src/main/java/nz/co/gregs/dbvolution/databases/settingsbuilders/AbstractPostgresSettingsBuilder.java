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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DatabaseConnectionSettings;
import nz.co.gregs.dbvolution.databases.PostgresDB;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.PostgresDBDefinition;

/**
 *
 * @author gregorygraham
 * @param <SELF> the class of the object returned by most methods, this should be the Class of "this"
 * @param <DATABASE> the class returned by {@link #getDBDatabase}
 */
public abstract class AbstractPostgresSettingsBuilder<SELF extends AbstractPostgresSettingsBuilder<SELF, DATABASE>, DATABASE extends DBDatabase> 
		extends AbstractVendorSettingsBuilder<SELF, DATABASE>
		implements ClusterCapableSettingsBuilder<SELF, DATABASE>,
		InstanceCapableSettingsBuilder<SELF, DATABASE>,
		RemoteCapableSettingsBuilder<SELF, DATABASE>,
		NamedDatabaseCapableSettingsBuilder<SELF, DATABASE>,
		ExtrasCapableSettingsBuilder<SELF, DATABASE> {

	protected static final HashMap<String, String> DEFAULT_EXTRAS_MAP = new HashMap<>();

	@Override
	public Map<String, String> getDefaultConfigurationExtras() {
		return DEFAULT_EXTRAS_MAP;
	}

	@Override
	public String getDefaultDriverName() {
		return PostgresDB.POSTGRES_DRIVER_NAME;
	}

	@Override
	public DBDefinition getDefaultDefinition() {
		return new PostgresDBDefinition();
	}

	@Override
	protected DatabaseConnectionSettings generateSettingsInternal(String jdbcURL, DatabaseConnectionSettings settings) {
		String noPrefix = jdbcURL.replaceAll("^" + getJDBCURLPreamble(), "");
		if (noPrefix.contains("?")) {
			String extrasString = noPrefix.split("\\?", 2)[1];
			settings.setExtras(DatabaseConnectionSettings.decodeExtras(extrasString, "", "=", "&", ""));
			noPrefix = noPrefix.split("\\?", 2)[0];
		}
		settings.setPort(noPrefix.split("/", 2)[0].replaceAll("^[^:]*:+", ""));
		settings.setHost(noPrefix.split("/", 2)[0].split(":")[0]);
		settings.setDatabaseName(noPrefix.split("/", 2)[1]);
		settings.setInstance(settings.getExtras().get("instance"));
		settings.setSchema("");
		return settings;
	}

	protected String getJDBCURLPreamble() {
		return "jdbc:postgresql://";
	}

	@Override
	protected String getJDBCURLPreamble(DatabaseConnectionSettings settings) {
		return getJDBCURLPreamble();
	}

	@Override
	public String encodeHost(DatabaseConnectionSettings settings) {
		return settings.getHost() + ":" + settings.getPort() + "/" + settings.getDatabaseName() + encodeExtras(settings, "?", "=", "&", "");
	}

	@Override
	protected DatabaseConnectionSettings setDefaultsInternal(DatabaseConnectionSettings settings) {
		return settings;
	}

	@Override
	public Integer getDefaultPort() {
		return 5432;
	}

	private final List<DatabaseConnectionSettings> clusterHost = new ArrayList<>(0);

	@Override
	@SuppressWarnings("unchecked")
	public SELF setClusterHosts(List<DatabaseConnectionSettings> hosts) {
		this.clusterHost.clear();
		this.clusterHost.addAll(hosts);
		return (SELF) this;
	}

	@Override
	public List<DatabaseConnectionSettings> getClusterHosts() {
		return this.clusterHost;
	}

	@SuppressWarnings("unchecked")
	public SELF setSSL(boolean ssl) {
		getStoredSettings().addExtra("ssl", ssl ? "true" : "false");
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setSSLMode(SSLMode mode) {
		getStoredSettings().addExtra("sslmode", mode.toString());
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setSSLFactory(String factoryClassName) {
		getStoredSettings().addExtra("sslfactory", factoryClassName);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setSSLCert(String cert) {
		getStoredSettings().addExtra("sslcert", cert);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setSSLRootCert(String cert) {
		getStoredSettings().addExtra("sslrootcert", cert);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setSSLKey(String key) {
		getStoredSettings().addExtra("sslkey", key);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setSSLHostNameVerifier(String verifier) {
		getStoredSettings().addExtra("sslhostnameverifier", verifier);
		return (SELF) this;
	}

	/**
	 * Class name of the SSL password provider. Defaults to
	 * org.postgresql.ssl.jdbc4.LibPQFactory.ConsoleCallbackHandler
	 *
	 * @param callback
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public SELF setSSLPasswordCallback(String callback) {
		getStoredSettings().addExtra("sslpasswordcallback", callback);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setSSLPassword(String password) {
		getStoredSettings().addExtra("sslpassword", password);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setSendBufferSize(int bufferSize) {
		getStoredSettings().addExtra("sendBufferSize", "" + bufferSize);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setRecvBufferSize(int bufferSize) {
		getStoredSettings().addExtra("recvBufferSize", "" + bufferSize);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setProtocolVersion(int bufferSize) {
		getStoredSettings().addExtra("protocolVersion", "" + bufferSize);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setLoggerLevelOFF() {
		getStoredSettings().addExtra("loggerLevel", "off");
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setLoggerLevelDEBUG() {
		getStoredSettings().addExtra("loggerLevel", "debug");
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setLoggerLevelTRACE() {
		getStoredSettings().addExtra("loggerLevel", "trace");
		return (SELF) this;
	}

	/**
	 * File name output of the Logger.
	 *
	 * <p>
	 * If set, the Logger will use a java.util.logging.FileHandler to write to a
	 * specified file. If the parameter is not set or the file can’t be created
	 * the java.util.logging.ConsoleHandler will be used instead. This parameter
	 * should be use together with loggerLevel.</p>
	 *
	 * @param loggerFilename
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public SELF setLoggerFile(String loggerFilename) {
		getStoredSettings().addExtra("loggerFile", "" + loggerFilename);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setAllowEncodingChanges(boolean allow) {
		getStoredSettings().addExtra("allowEncodingChanges", "" + allow);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setLogUnclosedConnections(boolean allow) {
		getStoredSettings().addExtra("logUnclosedConnections", "" + allow);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setAutosaveALWAYS() {
		getStoredSettings().addExtra("autosave", "always");
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setAutosaveCONSERVATIVE() {
		getStoredSettings().addExtra("autosave", "conservative");
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setAutosaveNEVER() {
		getStoredSettings().addExtra("autosave", "never");
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setCleanupSavePoints(boolean cleanup) {
		getStoredSettings().addExtra("cleanupSavePoints", "" + cleanup);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setBinaryTransferEnable(boolean binaryTransfer) {
		getStoredSettings().addExtra("binaryTransferEnable", "" + binaryTransfer);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setBinaryTransferDisable(boolean binaryTransfer) {
		getStoredSettings().addExtra("binaryTransferDisable", "" + binaryTransfer);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setPrepareThreshold(int threshold) {
		getStoredSettings().addExtra("prepareThreshold", "" + threshold);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setPreparedStatementCacheQueries(int numberOfQueries) {
		getStoredSettings().addExtra("cleanupSavePoints", "" + numberOfQueries);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setPreparedStatementCacheSizeMiB(int sizeInMB) {
		getStoredSettings().addExtra("cleanupSavePoints", "" + sizeInMB);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setPreferQueryMode(QueryMode mode) {
		getStoredSettings().addExtra("preferQueryMode", "" + mode);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setDefaultRowFetchSize(int fetchSize) {
		getStoredSettings().addExtra("defaultRowFetchSize", "" + fetchSize);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setLoginTimeout(int timeoutInSeconds) {
		getStoredSettings().addExtra("loginTimeout", "" + timeoutInSeconds);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setConnectTimeout(int timeoutInSeconds) {
		getStoredSettings().addExtra("connectTimeout", "" + timeoutInSeconds);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setSocketTimeout(int timeoutInSeconds) {
		getStoredSettings().addExtra("socketTimeout", "" + timeoutInSeconds);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setCancelSignalTimeout(int timeoutInSeconds) {
		getStoredSettings().addExtra("cancelSignalTimeout", "" + timeoutInSeconds);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setTCPKeepAlive(boolean keepAlive) {
		getStoredSettings().addExtra("tcpKeepAlive", "" + keepAlive);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setUnknownLength(int length) {
		getStoredSettings().addExtra("unknownLength", "" + length);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setStringtype(String stringtype) {
		getStoredSettings().addExtra("stringtype", "" + stringtype);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setKerberosServerName(String name) {
		getStoredSettings().addExtra("kerberosServerName", "" + name);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setJAASApplicationName(String name) {
		getStoredSettings().addExtra("jaasApplicationName", "" + name);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setJAASLogin(boolean login) {
		getStoredSettings().addExtra("jaasLogin", "" + login);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setApplicationName(String name) {
		getStoredSettings().addExtra("ApplicationName", "" + name);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setGSSlib(String name) {
		getStoredSettings().addExtra("gsslib", "" + name);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setSSPIServiceClass(String name) {
		getStoredSettings().addExtra("sspiServiceClass", "" + name);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setUseSpnego(boolean spnego) {
		getStoredSettings().addExtra("useSpnego", "" + spnego);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setReceiveBufferSize(int size) {
		getStoredSettings().addExtra("receiveBufferSize", "" + size);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setReadOnly(boolean readonly) {
		getStoredSettings().addExtra("readonly", "" + readonly);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setDisableColumnSanitiser(boolean sanitiser) {
		getStoredSettings().addExtra("disableColumnSanitiser", "" + sanitiser);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setAssumeMinServerVersion(String version) {
		getStoredSettings().addExtra("assumeMinServerVersion", "" + version);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setCurrentSchema(String schema) {
		getStoredSettings().addExtra("currentSchema", "" + schema);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setTargetServerType(String serverType) {
		getStoredSettings().addExtra("targetServerType", "" + serverType);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setHostRecheckSeconds(int recheckInSeconds) {
		getStoredSettings().addExtra("hostRecheckSeconds", "" + recheckInSeconds);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setLoadBalanceHosts(boolean loadBalance) {
		getStoredSettings().addExtra("loadBalanceHosts", "" + loadBalance);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setSocketFactory(String socketFactory) {
		getStoredSettings().addExtra("socketFactory", "" + socketFactory);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setReWriteBatchedInserts(boolean rewrite) {
		getStoredSettings().addExtra("reWriteBatchedInserts", "" + rewrite);
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setReplicationTRUE() {
		getStoredSettings().addExtra("replication", "true");
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF setReplicationDATABASE() {
		getStoredSettings().addExtra("replication", "database");
		return (SELF) this;
	}

	public static enum SSLMode {

		DISABLE,
		ALLOW,
		PREFER,
		REQUIRE,
		VERIFY_CA,
		VERIFY_FULL,
		TRUE;

		SSLMode() {
		}

		@Override
		public String toString() {
			return super.toString().toLowerCase().replaceAll("_", "-");
		}
	}

	public static enum QueryMode {

		SIMPLE,
		EXTENDED,
		EXTENDED_FOR_PREPARED,
		EXTENDED_CACHE_EVERYTHING;

		QueryMode() {
		}

		@Override
		public String toString() {
			return super.toString().toLowerCase().replaceAll("_", "");
		}
	}
}
