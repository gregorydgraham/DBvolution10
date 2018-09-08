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
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.supports.SupportsPolygonDatatype;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;

/**
 * Super class for connecting the different versions of the Oracle DB.
 *
 * <p>
 * You should probably use {@link Oracle11XEDB} or {@link Oracle12DB} instead.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 * @see Oracle11XEDB
 * @see Oracle12DB
 */
public abstract class OracleDB extends DBDatabase implements SupportsPolygonDatatype {

	public static final String ORACLE_JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";
	public static final long serialVersionUID = 1l;
	public static final int DEFAULT_PORT = 1521;
	private String derivedURL;

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
	protected OracleDB() {

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
	 * @param password password
	 * @param jdbcURL jdbcURL
	 * @param driverName driverName
	 * @param username username
	 */
	public OracleDB(DBDefinition definition, String driverName, String jdbcURL, String username, String password) throws SQLException {
		super(definition, driverName, jdbcURL, username, password);
	}

	/**
	 * Creates a DBDatabase instance.
	 *
	 * @param dbDefinition an oracle database definition instance
	 * @param dataSource a data source to an Oracle database
	 * @throws java.sql.SQLException
	 */
	public OracleDB(DBDefinition dbDefinition, DataSource dataSource) throws SQLException {
		super(dbDefinition, ORACLE_JDBC_DRIVER, dataSource);
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone();
	}

	@Override
	protected void addDatabaseSpecificFeatures(Statement statement) throws SQLException {
		for (StringFunctions fn : StringFunctions.values()) {
			fn.add(statement);
		}
	}

	@Override
	protected <TR extends DBRow> void dropAnyAssociatedDatabaseObjects(DBStatement dbStatement, TR tableRow) throws SQLException {

		dropAnyTriggerBasedPrimaryKeyObject(dbStatement, tableRow);
		removeSpatialMetadata(dbStatement, tableRow);
	}

	protected <TR extends DBRow> void dropAnyTriggerBasedPrimaryKeyObject(DBStatement dbStatement, TR tableRow) throws SQLException {
		List<PropertyWrapper> fields = tableRow.getColumnPropertyWrappers();
		List<String> triggerBasedIdentitySQL = new ArrayList<>();
		final DBDefinition definition = this.getDefinition();
		if (definition.prefersTriggerBasedIdentities()) {
			List<PropertyWrapper> pkFields = new ArrayList<>();
			for (PropertyWrapper field : fields) {
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
//		try (DBStatement dbStatement = getDBStatement()) {
		for (String sql : triggerBasedIdentitySQL) {
			dbStatement.execute(sql);
		}
//		}
	}

	@Override
	public ResponseToException addFeatureToFixException(Exception exp) throws Exception {
		throw exp;
	}

	/**
	 * Allows the database to remove any spatial metadata that might exist for a
	 * table during DROP TABLE.
	 *
	 * @param <TR> the class of the object defining the table to have it's spatial
	 * meta-data removed.
	 * @param statement
	 * @param tableRow the object defining the table to have it's spatial
	 * meta-data removed.
	 * @throws SQLException database exceptions may be thrown.
	 */
	@SuppressFBWarnings(value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE",
			justification = "Compiled DBRows are safe, for a reasonable value of safe")
	protected <TR extends DBRow> void removeSpatialMetadata(DBStatement statement, TR tableRow) throws SQLException {
		DBDefinition definition = getDefinition();
		final String formattedTableName = definition.formatTableName(tableRow);
		statement.execute("DELETE FROM USER_SDO_GEOM_METADATA WHERE TABLE_NAME = '" + formattedTableName.toUpperCase() + "'");
	}

	@Override
	protected String getUrlFromSettings(DatabaseConnectionSettings settings) {
		String url = settings.getUrl();
		return url != null && !url.isEmpty()
				? url
				: "jdbc:oracle:thin:@//"
				+ settings.getHost() + ":"
				+ settings.getPort() + "/"
				+ settings.getInstance();
	}

}
