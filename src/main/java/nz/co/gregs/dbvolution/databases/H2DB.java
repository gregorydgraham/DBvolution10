/*
 * Copyright 2013 greg.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nz.co.gregs.dbvolution.databases;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.databases.jdbcurlinterpreters.H2URLInterpreter;
import nz.co.gregs.dbvolution.databases.definitions.H2DBDefinition;
import nz.co.gregs.dbvolution.databases.jdbcurlinterpreters.AbstractH2URLInterpreter;
import nz.co.gregs.dbvolution.exceptions.ExceptionDuringDatabaseFeatureSetup;
import nz.co.gregs.dbvolution.internal.h2.*;
import org.h2.jdbc.JdbcException;
import nz.co.gregs.dbvolution.databases.jdbcurlinterpreters.JDBCURLInterpreter;

/**
 * Stores all the required functionality to use an H2 database.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class H2DB extends DBDatabase {

	private static final long serialVersionUID = 1l;
	public static final String DRIVER_NAME = "org.h2.Driver";
	private final static Map<String, DBVFeature> FEATURE_MAP = new HashMap<>();
	private static boolean dataTypesNotProcessed = true;

	static {
		try {
			Class.forName(DRIVER_NAME);
		} catch (ClassNotFoundException ex) {
			Logger.getLogger(H2DB.class.getName()).log(Level.SEVERE, null, ex);
		}
		for (DBVFeature function : DateRepeatFunctions.values()) {
			FEATURE_MAP.put(function.alias(), function);
		}
		for (DBVFeature function : Point2DFunctions.values()) {
			FEATURE_MAP.put(function.alias(), function);
		}
		for (DBVFeature function : LineSegment2DFunctions.values()) {
			FEATURE_MAP.put(function.alias(), function);
		}
		for (DBVFeature function : Line2DFunctions.values()) {
			FEATURE_MAP.put(function.alias(), function);
		}
		for (DBVFeature function : Polygon2DFunctions.values()) {
			FEATURE_MAP.put(function.alias(), function);
		}
		for (DBVFeature function : MultiPoint2DFunctions.values()) {
			FEATURE_MAP.put(function.alias(), function);
		}
		for (DataTypes datatype : DataTypes.values()) {
			FEATURE_MAP.put(datatype.alias(), datatype);
		}
	}

	/**
	 * Default constructor, try not to use this.
	 *
	 */
	protected H2DB() {
		super();
	}

	/**
	 * Creates a DBDatabase for a H2 database in the file supplied.
	 *
	 * <p>
	 * This creates a database connection to a local H2 database stored in the
	 * file provided.
	 *
	 * <p>
	 * If the file does not exist the database will be created in the file.
	 *
	 * Database exceptions may be thrown
	 *
	 * @param file file
	 * @param username username
	 * @param password password
	 * @throws java.io.IOException java.io.IOException
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public H2DB(File file, String username, String password) throws IOException, SQLException {
		this(
				new H2URLInterpreter()
						.setDatabaseName(file.getCanonicalFile().toString())
						.setUsername(username)
						.setPassword(password)
						.toSettings()
		);
//		this("jdbc:h2:" + file.getCanonicalFile(), username, password);
	}

	/**
	 * Creates a DBDatabase for a H2 database.
	 *
	 * <p>
	 * Database exceptions may be thrown
	 *
	 * @param dataSource dataSource
	 * @throws java.sql.SQLException database errors
	 */
	public H2DB(DataSource dataSource) throws SQLException {
		super(new H2DBDefinition(), DRIVER_NAME, dataSource);
	}

	/**
	 * Creates a DBDatabase for a H2 database.
	 *
	 * <p>
	 * Database exceptions may be thrown
	 *
	 * @param dataSource dataSource
	 * @throws java.sql.SQLException database errors
	 */
	public H2DB(DatabaseConnectionSettings dataSource) throws SQLException {
		super(new H2DBDefinition(), DRIVER_NAME, dataSource);
	}

	/**
	 * Creates a DBDatabase for a H2 database.
	 *
	 * <p>
	 * Database exceptions may be thrown
	 *
	 * @param dataSource dataSource
	 * @throws java.sql.SQLException database errors
	 */
	protected H2DB(AbstractH2URLInterpreter dataSource) throws SQLException {
		super(new H2DBDefinition(), DRIVER_NAME, dataSource);
	}

	/**
	 * Creates a DBDatabase for a H2 database.
	 *
	 *
	 *
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 * @throws java.sql.SQLException database errors
	 */
	public H2DB(String jdbcURL, String username, String password) throws SQLException {
		this(
				new H2URLInterpreter()
						.fromJDBCURL(jdbcURL)
						.setUsername(username)
						.setPassword(password)
						.toSettings()
		);
	}

	/**
	 * Creates a DBDatabase for a H2 database.
	 *
	 *
	 *
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @param databaseFilename the name and path of the database file
	 * @param username username
	 * @param password password
	 * @param dummy unused
	 * @throws java.sql.SQLException database errors
	 */
	public H2DB(String databaseFilename, String username, String password, boolean dummy) throws SQLException {
		this(new H2URLInterpreter()
				.setFilename(databaseFilename)
				.setUsername(username)
				.setPassword(password)
				.toSettings()
		);
	}

	/**
	 * Creates a DBDatabase for a H2 database.
	 * 
	 * <p>Database exceptions may be thrown</p>
	 *
	 * @param settings
	 * @throws java.sql.SQLException database errors
	 */
	public H2DB(H2URLInterpreter settings) throws SQLException {
		this(settings.toSettings());
	}

	@Override
	protected synchronized void addDatabaseSpecificFeatures(final Statement stmt) throws ExceptionDuringDatabaseFeatureSetup {
		DataTypes.addAll(stmt);
		if (dataTypesNotProcessed) {
			for (DataTypes datatype : DataTypes.values()) {
				FEATURE_MAP.put(datatype.alias(), datatype);
			}
			dataTypesNotProcessed = false;
		}
	}

	/**
	 * Clones the DBDatabase
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a clone of the database.
	 * @throws java.lang.CloneNotSupportedException
	 * java.lang.CloneNotSupportedException
	 *
	 */
	@Override
	public H2DB clone() throws CloneNotSupportedException {
		return (H2DB) super.clone();
	}

	private final static Pattern BROKEN_CONNECTION_PATTERN = Pattern.compile("Connection is broken: \"session closed\"");
	private final static Pattern ALREADY_CLOSED_PATTERN = Pattern.compile("The object is already closed");
	private final static Pattern DROPPING_NONEXISTENT_TABLE_PATTERN = Pattern.compile("Table \"([^\"]*)\" not found; SQL statement:.*DROP TABLE \\1");
	private final static Pattern CREATING_EXISTING_TABLE_PATTERN = Pattern.compile("Table \"[^\"]*\" already exists; SQL statement:");

	@Override
	public ResponseToException addFeatureToFixException(Exception exp, QueryIntention intent) throws Exception {
		boolean handledException = false;
		if ((exp instanceof JdbcException)) {
			String message = exp.getMessage();
			if (message != null) {
				if (BROKEN_CONNECTION_PATTERN.matcher(message).lookingAt()
						|| ALREADY_CLOSED_PATTERN.matcher(message).lookingAt()) {
					return ResponseToException.REPLACECONNECTION;
				} else if (DROPPING_NONEXISTENT_TABLE_PATTERN.matcher(message).lookingAt()) {
					return ResponseToException.SKIPQUERY;
				} else if (CREATING_EXISTING_TABLE_PATTERN.matcher(message).lookingAt()) {
					return ResponseToException.SKIPQUERY;
				} else {
					try (DBStatement statement = getConnection().createDBStatement()) {
						if ((message.startsWith("Function \"DBV_") && message.contains("\" not found"))
								|| (message.startsWith("Method \"DBV_") && message.contains("\" not found"))) {
							String[] split = message.split("[\" ]+");
							String functionName = split[1];
							DBVFeature functions = FEATURE_MAP.get(functionName);
							if (functions != null) {
								functions.add(statement.getInternalStatement());
								handledException = true;
							}
						} else if (message.startsWith("Unknown data type: \"DBV_")) {
							String[] split = message.split("\"");
							String functionName = split[1];
							DBVFeature datatype = FEATURE_MAP.get(functionName);
							if (datatype != null) {
								datatype.add(statement.getInternalStatement());
								handledException = true;
							}
						} else if (message.matches(": +method \"DBV_[A-Z_0-9]+")) {
							String[] split = message.split("method \"");
							split = split[1].split("\\(");
							String functionName = split[0];

							DBVFeature functions = FEATURE_MAP.get(functionName);
							if (functions != null) {
								functions.add(statement.getInternalStatement());
								handledException = true;
							}
						} else {
							for (Map.Entry<String, DBVFeature> entrySet : FEATURE_MAP.entrySet()) {
								String key = entrySet.getKey();
								DBVFeature value = entrySet.getValue();
								if (message.contains(key)) {
									value.add(statement.getInternalStatement());
									handledException = true;
								}
							}
						}
					}
				}
			}
		}
		if (!handledException) {
			throw exp;
		} else {
			return ResponseToException.REQUERY;
		}
	}

//	@Override
//	protected String getUrlFromSettings(DatabaseConnectionSettings settings) {
//		String url = settings.getUrl();
//		return url != null && !url.isEmpty() ? url : "jdbc:h2:" + settings.getDatabaseName();
//	}
	@Override
	public boolean isMemoryDatabase() {
		return getJdbcURL().contains(":mem:");
	}

	protected String getFileFromJdbcURL() {
		String jdbcURL = getJdbcURL();
		String noPrefix = jdbcURL.replaceAll("^jdbc:h2:", "").replaceAll("^mem:", "");
		if (noPrefix.startsWith("tcp") || noPrefix.startsWith("ssl")) {
			return noPrefix
					.replaceAll("tcp://", "")
					.replaceAll("ssl://", "").split("/", 2)[1];
		} else {
			return noPrefix.replaceAll("^file:", "").split(";", 2)[0];
		}
	}

//	@Override
//	protected DatabaseConnectionSettings getSettingsFromJDBCURL(String jdbcURL) {
//		DatabaseConnectionSettings set = new DatabaseConnectionSettings();
//		int protocolIndex = 2;
//		int restIndex = 4;
//		String[] firstSplit = jdbcURL.split(":", restIndex);
//		set.setProtocol(firstSplit[protocolIndex]);
//		if (!set.getProtocol().equals("tcp")
//				&& !set.getProtocol().equals("ssl")
//				&& !set.getProtocol().equals("mem")
//				&& !set.getProtocol().equals("zip")
//				&& !set.getProtocol().equals("file")) {
//			set.setProtocol("file");
//			restIndex -= 1;
//			firstSplit = jdbcURL.split(":", restIndex);
//		}
//		String restString = firstSplit[restIndex - 1];
//		if (restString.startsWith("//")) {
//			String[] secondSplit = restString.split("/", 4);
//			String hostAndPort = secondSplit[2];
//			if (hostAndPort.contains(":")) {
//				String[] thirdSplit = hostAndPort.split(":");
//				set.setHost(thirdSplit[0]);
//				set.setPort(thirdSplit[1]);
//			} else {
//				set.setHost(hostAndPort);
//				set.setPort("" + getDefaultPort());
//			}
//			restString = secondSplit[3];
//		}
//		String[] fourthSplit = restString.split(";");
//		set.setDatabaseName(fourthSplit[0]);
//		if (fourthSplit.length > 1) {
//			set.setExtras(DatabaseConnectionSettings.decodeExtras(fourthSplit[1], ";", "=", ";", ""));
//		}
//		return set;
//	}
	@Override
	public Integer getDefaultPort() {
		return 9123;
	}

//	@Override
//	protected  Class<? extends DBDatabase> getBaseDBDatabaseClass() {
//		return H2DB.class;
//	}
	private final static H2URLInterpreter URL_PROCESSOR = new H2URLInterpreter();

	@Override
	protected AbstractH2URLInterpreter<?> getURLInterpreter() {
		return URL_PROCESSOR;
	}
}
