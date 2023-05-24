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
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.databases.settingsbuilders.H2SettingsBuilder;
import nz.co.gregs.dbvolution.databases.settingsbuilders.AbstractH2SettingsBuilder;
import nz.co.gregs.dbvolution.databases.settingsbuilders.H2FileSettingsBuilder;
import nz.co.gregs.dbvolution.exceptions.ExceptionDuringDatabaseFeatureSetup;
import nz.co.gregs.dbvolution.internal.h2.*;
import nz.co.gregs.dbvolution.internal.query.StatementDetails;
import nz.co.gregs.regexi.Regex;
import org.h2.jdbc.JdbcException;

/**
 * Stores all the required functionality to use an H2 database.
 *
 * @author Gregory Graham
 */
public class H2DB extends DBDatabaseImplementation {

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
		this(new H2FileSettingsBuilder()
				.setDatabaseName(file.getCanonicalFile().toString())
				.setUsername(username)
				.setPassword(password)
				.toSettings()
		);
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
		super(
				new H2SettingsBuilder().setDataSource(dataSource)
		);
	}

	/**
	 * Creates a DBDatabase for a H2 database.
	 *
	 * <p>
	 * Database exceptions may be thrown
	 *
	 * @param dcs dataSource
	 * @throws java.sql.SQLException database errors
	 */
	public H2DB(DatabaseConnectionSettings dcs) throws SQLException {
		super(new H2SettingsBuilder().fromSettings(dcs));
	}

	/**
	 * Creates a DBDatabase for a H2 database.
	 *
	 * <p>
	 * Database exceptions may be thrown
	 *
	 * @param settings dataSource
	 * @throws java.sql.SQLException database errors
	 */
	protected H2DB(AbstractH2SettingsBuilder<?, ?> settings) throws SQLException {
		super(settings);
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
		this(new H2SettingsBuilder()
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
		this(new H2FileSettingsBuilder()
				.setFilename(databaseFilename)
				.setUsername(username)
				.setPassword(password)
				.toSettings()
		);
	}

	/**
	 * Creates a DBDatabase for a H2 database.
	 *
	 * <p>
	 * Database exceptions may be thrown</p>
	 *
	 * @param settings the settings that specify everything necessary to connect
	 * to the H2 database
	 * @throws java.sql.SQLException database errors
	 */
	public H2DB(H2SettingsBuilder settings) throws SQLException {
		this(settings.toSettings());
	}

	@Override
	public synchronized void addDatabaseSpecificFeatures(final Statement stmt) throws ExceptionDuringDatabaseFeatureSetup {
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
	 * @return a clone of the database.
	 * @throws java.lang.CloneNotSupportedException
	 * java.lang.CloneNotSupportedException
	 *
	 */
	@Override
	public H2DB clone() throws CloneNotSupportedException {
		return (H2DB) super.clone();
	}

	private final static Regex BROKEN_CONNECTION_PATTERN = Regex.startingAnywhere().literal("Connection is broken: \"session closed\"").toRegex();
	private final static Regex ALREADY_CLOSED_PATTERN = Regex.startingAnywhere().literal("The object is already closed").toRegex();
	private final static Regex DROPPING_NONEXISTENT_TABLE_PATTERN = Regex.startingAnywhere().literal("Table \"").beginNamedCapture("table").noneOfTheseCharacters("\"").oneOrMore().endNamedCapture().literal("\" not found; SQL statement:").anyCharacter().optionalMany().literal("DROP TABLE ").namedBackReference("table").toRegex();
	private final static Regex TABLE_NOT_FOUND_WHILE_CHECKING_EXISTENCE_PATTERN = Regex.startingAnywhere().literal("Table \"").noneOfTheseCharacters("\"").oneOrMore().literal("\" not found").anyCharacterIncludingLineEnd().optionalMany().literal("SQL statement:").anyCharacterIncludingLineEnd().optionalMany().literal("SELECT COUNT(").star().literal(")").toRegex();
	private final static Regex CREATING_EXISTING_TABLE_PATTERN = Regex.startingAnywhere().literal("Table \"").anythingButThis("\"").oneOrMore().literal("\" already exists; SQL statement:").toRegex();

	@Override
	public ResponseToException addFeatureToFixException(Exception exp, QueryIntention intent, StatementDetails details) throws Exception {
		boolean handledException = false;
		if ((exp instanceof JdbcException)) {
			String message = exp.getMessage();
			if (message != null) {
				if (BROKEN_CONNECTION_PATTERN.matchesWithinString(message)
						|| ALREADY_CLOSED_PATTERN.matchesWithinString(message)) {
					return ResponseToException.REPLACECONNECTION;
				} else if (DROPPING_NONEXISTENT_TABLE_PATTERN.matchesWithinString(message)) {
					return ResponseToException.SKIPQUERY;
				} else if (QueryIntention.CHECK_TABLE_EXISTS.equals(intent) && TABLE_NOT_FOUND_WHILE_CHECKING_EXISTENCE_PATTERN.matchesWithinString(message)) {
					return ResponseToException.SKIPQUERY;
				} else if (CREATING_EXISTING_TABLE_PATTERN.matchesWithinString(message)) {
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

	@Override
	public boolean isMemoryDatabase() {
		return getJdbcURL().contains(":mem:");
	}

	@Override
	public Integer getDefaultPort() {
		return 9123;
	}

	private final static H2SettingsBuilder URL_PROCESSOR = new H2SettingsBuilder();

	@Override
	public AbstractH2SettingsBuilder<?, ?> getURLInterpreter() {
		return URL_PROCESSOR;
	}
}
