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

import nz.co.gregs.dbvolution.internal.oracle.aws.MultiPoint2DFunctions;
import nz.co.gregs.dbvolution.internal.oracle.aws.LineSegment2DFunctions;
import nz.co.gregs.dbvolution.internal.oracle.aws.Polygon2DFunctions;
import nz.co.gregs.dbvolution.internal.oracle.aws.Line2DFunctions;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.databases.settingsbuilders.AbstractOracleSettingsBuilder;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.OracleAWSDBDefinition;
import nz.co.gregs.dbvolution.databases.settingsbuilders.OracleAWS11SettingsBuilder;
import nz.co.gregs.dbvolution.databases.supports.SupportsPolygonDatatype;
import nz.co.gregs.dbvolution.exceptions.ExceptionDuringDatabaseFeatureSetup;

/**
 * Super class for connecting the different versions of the AWS Oracle DB.
 *
 * <p>
 * You should probably use {@link OracleAWS11DB} or {@link Oracle12DB} instead.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 * @see OracleAWS11DB
 * @see Oracle12DB
 */
public abstract class OracleAWSDB extends OracleDB implements SupportsPolygonDatatype {

	public static final long serialVersionUID = 1l;

	/**
	 * Creates an Oracle connection for the DatabaseConnectionSettings.
	 *
	 * @param dcs	dcs
	 * @throws java.sql.SQLException database errors
	 */
	public OracleAWSDB(DatabaseConnectionSettings dcs) throws SQLException {
		this(new OracleAWSDBDefinition(), dcs);
	}

	/**
	 * Creates an Oracle connection for the DatabaseConnectionSettings.
	 *
	 * @param dcs	dcs
	 * @throws java.sql.SQLException database errors
	 */
	public OracleAWSDB(AbstractOracleSettingsBuilder<?,?> dcs) throws SQLException {
		this(new OracleAWSDBDefinition(), dcs);
	}

	/**
	 * Creates an Oracle connection for the DatabaseConnectionSettings.
	 *
	 * @param dcs	dcs
	 * @param defn the oracle database definition
	 * @throws java.sql.SQLException database errors
	 */
	@Deprecated
	public OracleAWSDB(OracleAWSDBDefinition defn, DatabaseConnectionSettings dcs) throws SQLException {
		super(defn, dcs);
	}

	/**
	 * Creates an Oracle connection for the DatabaseConnectionSettings.
	 *
	 * @param dcs	dcs
	 * @param defn the oracle database definition
	 * @throws java.sql.SQLException database errors
	 */
	@Deprecated
	public OracleAWSDB(OracleAWSDBDefinition defn, AbstractOracleSettingsBuilder<?,?> dcs) throws SQLException {
		super(defn, dcs);
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
	 * @throws java.sql.SQLException database errors
	 */
	@Deprecated
	public OracleAWSDB(DBDefinition definition, String driverName, String jdbcURL, String username, String password) throws SQLException {
		super(definition, driverName, jdbcURL, username, password);
	}

	/**
	 * Creates a DBDatabase instance.
	 *
	 * @param dbDefinition an oracle database definition instance
	 * @param dataSource a data source to an Oracle database
	 * @throws java.sql.SQLException database errors
	 */
	public OracleAWSDB(DBDefinition dbDefinition, DataSource dataSource) throws SQLException {
		super(
				new OracleAWS11SettingsBuilder().setDataSource(dataSource)
		);
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone();
	}

	@Override
	public void addDatabaseSpecificFeatures(Statement statement) throws ExceptionDuringDatabaseFeatureSetup {
		super.addDatabaseSpecificFeatures(statement);

		for (Line2DFunctions fn : Line2DFunctions.values()) {
			fn.add(statement);
		}
		for (LineSegment2DFunctions fn : LineSegment2DFunctions.values()) {
			fn.add(statement);
		}
		for (Polygon2DFunctions fn : Polygon2DFunctions.values()) {
			fn.add(statement);
		}
		for (MultiPoint2DFunctions fn : MultiPoint2DFunctions.values()) {
			fn.add(statement);
		}
	}

}
