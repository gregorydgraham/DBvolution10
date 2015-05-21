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
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.H2DBDefinition;
import nz.co.gregs.dbvolution.databases.supports.SupportsDateRepeatDatatypeFunctions;
import nz.co.gregs.dbvolution.databases.supports.SupportsPolygonDatatype;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.internal.h2.*;

/**
 * Stores all the required functionality to use an H2 database.
 *
 * @author Gregory Graham
 */
public class H2DB extends DBDatabase implements SupportsDateRepeatDatatypeFunctions, SupportsPolygonDatatype {

	/**
	 * Used to hold the database open
	 *
	 */
	protected Connection storedConnection;

	/**
	 * Creates a DBDatabase for a H2 database in the file supplied.
	 *
	 *
	 *
	 *
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @param file file
	 * @param username username
	 * @param password password
	 * @throws java.io.IOException java.io.IOException
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public H2DB(File file, String username, String password) throws IOException, SQLException {
		this("jdbc:h2:" + file.getCanonicalFile(), username, password);
	}

	/**
	 * Creates a DBDatabase for a H2 database.
	 *
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @param dataSource dataSource
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public H2DB(DataSource dataSource) throws SQLException {
		super(new H2DBDefinition(), dataSource);
		jamDatabaseConnectionOpen();
		final Statement stmt = getConnection().createStatement();
		addDatabaseSpecificFeatures(stmt);
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
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public H2DB(String jdbcURL, String username, String password) throws SQLException {
		super(new H2DBDefinition(), "org.h2.Driver", jdbcURL, username, password);
		jamDatabaseConnectionOpen();
//		final Statement stmt = getConnection().createStatement();
//		addDatabaseSpecificFeatures(stmt);
	}

	@Override
	protected void addDatabaseSpecificFeatures(final Statement stmt) throws SQLException {
		DateRepeatFunctions.addFunctions(stmt);
		Point2D.POINT2D.add(stmt);
		Line2D.LINE2D.add(stmt);
		Polygon2D.POLYGON2D.add(stmt);
	}

	private void jamDatabaseConnectionOpen() throws DBRuntimeException, SQLException {
		this.storedConnection = getConnection();
		this.storedConnection.createStatement();
	}

	@Override
	public boolean supportsFullOuterJoinNatively() {
		return false;
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

}
