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
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.supports.SupportsPolygonDatatype;

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
	 * @see OracleAWS11DB
	 * @see OracleAWSDB
	 */
	protected OracleAWSDB() {
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
	public OracleAWSDB(DBDefinition definition, String driverName, String jdbcURL, String username, String password) throws SQLException {
		super(definition, driverName, jdbcURL, username, password);
	}

	/**
	 * Creates a DBDatabase instance.
	 *
	 * @param dbDefinition an oracle database definition instance
	 * @param dataSource a data source to an Oracle database
	 */
	public OracleAWSDB(DBDefinition dbDefinition, DataSource dataSource) throws SQLException {
		super(dbDefinition, dataSource);
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone();
	}

	@Override
	protected void addDatabaseSpecificFeatures(Statement statement) throws SQLException {
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

	@Override
	protected <TR extends DBRow> void removeSpatialMetadata(DBStatement dbStatement, TR tableRow) throws SQLException {
	}

}
