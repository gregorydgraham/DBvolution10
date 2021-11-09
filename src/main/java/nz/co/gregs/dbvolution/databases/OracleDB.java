/*
 * Copyright 2014 Gregory Graham.
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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import nz.co.gregs.dbvolution.internal.oracle.StringFunctions;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.settingsbuilders.AbstractOracleSettingsBuilder;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.OracleDBDefinition;
import nz.co.gregs.dbvolution.databases.settingsbuilders.Oracle11XESettingsBuilder;
import nz.co.gregs.dbvolution.databases.supports.SupportsPolygonDatatype;
import nz.co.gregs.dbvolution.exceptions.ExceptionDuringDatabaseFeatureSetup;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import nz.co.gregs.dbvolution.internal.query.StatementDetails;

/**
 * Super class for connecting the different versions of the Oracle DB.
 *
 * <p>
 * You should probably use {@link Oracle11XEDB} or {@link Oracle12DB} instead.
 *
 * @author Gregory Graham
 * @see Oracle11XEDB
 * @see Oracle12DB
 */
public abstract class OracleDB extends DBDatabase implements SupportsPolygonDatatype {

	public static final String ORACLE_JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";
	public static final long serialVersionUID = 1l;
	public static final int DEFAULT_PORT = 1521;

	/**
	 *
	 * Provides a convenient constructor for DBDatabases that have configuration
	 * details hardwired or are able to automatically retrieve the details.
	 *
	 * <p>
	 * This constructor creates an empty DBDatabase with only the default
	 * settings, in particular with no driver, URL, username, password, or
	 * {@link DBDefinition}
	 *
	 * <p>
	 * Most programmers should not call this constructor directly. Check the
	 * subclasses in {@code nz.co.gregs.dbvolution.databases} for your particular
	 * database.
	 *
	 * <p>
	 * DBDatabase encapsulates the knowledge of the database, in particular the
	 * syntax of the database in the DBDefinition and the connection details from
	 * a DataSource.
	 *
	 * @see DBDefinition
	 * @see Oracle12DB
	 * @see Oracle11XEDB
	 * @see OracleAWS11DB
	 * @see OracleAWSDB
	 */
//	protected OracleDB() {
//
//	}
	/**
	 * Creates an Oracle connection for the DatabaseConnectionSettings.
	 *
	 * @param builder
	 * @throws java.sql.SQLException database errors
	 */
	public OracleDB(AbstractOracleSettingsBuilder<?, ?> builder) throws SQLException {
		super(builder);
	}

	/**
	 * Creates an Oracle connection for the DatabaseConnectionSettings.
	 *
	 * @param dcs	dcs
	 * @throws java.sql.SQLException database errors
	 */
	@Deprecated
	public OracleDB(DatabaseConnectionSettings dcs) throws SQLException {
		this(new OracleDBDefinition(), dcs);
	}

	/**
	 * Creates an Oracle connection for the DatabaseConnectionSettings.
	 *
	 * @param dcs	dcs
	 * @param defn the oracle database definition
	 * @throws java.sql.SQLException database errors
	 */
	@Deprecated
	public OracleDB(OracleDBDefinition defn, DatabaseConnectionSettings dcs) throws SQLException {
		super(new Oracle11XESettingsBuilder().fromSettings(dcs).setDefinition(defn));
	}

	/**
	 * Creates a DBDatabase instance for the definition and data source.
	 *
	 * <p>
	 * You should probably be using {@link Oracle11XEDB#Oracle11XEDB(java.lang.String, int, java.lang.String, java.lang.String, java.lang.String)
	 * } or
	 * {@link Oracle12DB#Oracle12DB(java.lang.String, int, java.lang.String, java.lang.String, java.lang.String)}
	 *
	 * @param definition definition
	 * @param driverName
	 * @param password password
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @throws java.sql.SQLException database errors
	 */
	@Deprecated
	public OracleDB(DBDefinition definition, String driverName, String jdbcURL, String username, String password) throws SQLException {
		this(new Oracle11XESettingsBuilder().fromJDBCURL(jdbcURL, username, password).setDefinition(definition).setDriverName(driverName));
	}

	/**
	 * Creates a DBDatabase instance.
	 *
	 * @param dbDefinition an oracle database definition instance
	 * @param dataSource a data source to an Oracle database
	 * @throws java.sql.SQLException database errors
	 */
	@Deprecated
	public OracleDB(DBDefinition dbDefinition, AbstractOracleSettingsBuilder<?, ?> dataSource) throws SQLException {
		this(dbDefinition, ORACLE_JDBC_DRIVER, dataSource);
	}

	/**
	 * Creates a DBDatabase instance.
	 *
	 * @param dbDefinition an oracle database definition instance
	 * @param driverName
	 * @param dataSource a data source to an Oracle database
	 * @throws java.sql.SQLException database errors
	 */
	@Deprecated
	public OracleDB(DBDefinition dbDefinition, String driverName, AbstractOracleSettingsBuilder<?, ?> dataSource) throws SQLException {
		super(dataSource.setDefinition(dbDefinition).setDriverName(driverName));
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone();
	}

	@Override
	public void addDatabaseSpecificFeatures(Statement statement) throws ExceptionDuringDatabaseFeatureSetup {
		for (StringFunctions fn : StringFunctions.values()) {
			try {
				fn.add(statement);
			} catch (Exception ex) {
				throw new ExceptionDuringDatabaseFeatureSetup("FAILED TO ADD FEATURE: " + fn.name(), ex);
			}
		}
	}

	@Override
	public <TR extends DBRow> void dropAnyAssociatedDatabaseObjects(DBStatement dbStatement, TR tableRow) throws SQLException {

		dropAnyTriggerBasedPrimaryKeyObject(dbStatement, tableRow);
		removeSpatialMetadata(dbStatement, tableRow);
	}

	protected <TR extends DBRow> void dropAnyTriggerBasedPrimaryKeyObject(DBStatement dbStatement, TR tableRow) throws SQLException {
		var fields = tableRow.getColumnPropertyWrappers();
		List<String> triggerBasedIdentitySQL = new ArrayList<>();
		final DBDefinition definition = this.getDefinition();
		if (definition.prefersTriggerBasedIdentities()) {
			List<PropertyWrapper<?, ?, ?>> pkFields = new ArrayList<>();
			for (var field : fields) {
				if (field.isColumn() && !field.getQueryableDatatype().hasColumnExpression()) {
					if (field.isPrimaryKey()) {
						pkFields.add(field);
					}
				}
			}
			if (pkFields.size() == 1) {
				triggerBasedIdentitySQL = definition.dropTriggerBasedIdentitySQL(this, definition.formatTableName(tableRow), definition.formatColumnName(pkFields.get(0).columnName()));
			}
		}
		for (String sql : triggerBasedIdentitySQL) {
			dbStatement.execute(new StatementDetails("Remove identity trigger", QueryIntention.CREATE_TRIGGER_BASED_IDENTITY, sql));
		}
	}

	private final static Pattern SEQUENCE_DOES_NOT_EXIST = Pattern.compile("ORA-02289: sequence does not exist");
	private final static Pattern TRIGGER_DOES_NOT_EXIST = Pattern.compile("ORA-04080: trigger .* does not exist");
	private final static Pattern TABLE_ALREADY_EXISTS = Pattern.compile("ORA-00955: name is already used by an existing object");
	private final static Pattern TABLE_DOES_NOT_EXIST = Pattern.compile("ORA-00942: table or view does not exist");
	private final static Pattern LOOP_IN_RECURSIVE_QUERY = Pattern.compile("ORA-32044: cycle detected while executing recursive WITH query");

	@Override
	public ResponseToException addFeatureToFixException(Exception exp, QueryIntention intent) throws Exception {
		final String message = exp.getMessage();
		if ((intent.is(QueryIntention.CHECK_TABLE_EXISTS) && TABLE_DOES_NOT_EXIST.matcher(message).lookingAt())) {
			return ResponseToException.SKIPQUERY;
		} else if ((intent.isOneOf(QueryIntention.DROP_SEQUENCE, QueryIntention.CREATE_TRIGGER_BASED_IDENTITY)) && SEQUENCE_DOES_NOT_EXIST.matcher(message).lookingAt()) {
			return ResponseToException.SKIPQUERY;
		} else if ((intent.is(QueryIntention.DROP_TABLE) && TABLE_DOES_NOT_EXIST.matcher(message).lookingAt())) {
			return ResponseToException.SKIPQUERY;
		} else if (TABLE_ALREADY_EXISTS.matcher(message).lookingAt()) {
			return ResponseToException.SKIPQUERY;
		} else if (TRIGGER_DOES_NOT_EXIST.matcher(message).lookingAt()) {
			return ResponseToException.SKIPQUERY;
		} else if (LOOP_IN_RECURSIVE_QUERY.matcher(message).lookingAt()) {
			return ResponseToException.EMULATE_RECURSIVE_QUERY;
		} else {
		}

		return super.addFeatureToFixException(exp, intent);
	}

	/**
	 * Allows the database to remove any spatial metadata that might exist for a
	 * table during DROP TABLE.
	 *
	 * @param <TR> the class of the object defining the table to have it's spatial
	 * meta-data removed.
	 * @param statement the statement to use
	 * @param tableRow the object defining the table to have it's spatial
	 * meta-data removed.
	 * @throws SQLException database exceptions may be thrown.
	 */
	@SuppressFBWarnings(value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE",
			justification = "Compiled DBRows are safe, for a reasonable value of safe")
	protected <TR extends DBRow> void removeSpatialMetadata(DBStatement statement, TR tableRow) throws SQLException {
		DBDefinition definition = getDefinition();
		final String formattedTableName = definition.formatTableName(tableRow);
		final String sql = "DELETE FROM USER_SDO_GEOM_METADATA WHERE TABLE_NAME = '" + formattedTableName.toUpperCase() + "'";
		statement.execute(new StatementDetails("Delete user", QueryIntention.DELETE_ROW, sql));
	}

	@Override
	public Integer getDefaultPort() {
		return 1521;
	}

}
