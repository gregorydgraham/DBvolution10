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
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.H2DBDefinition;
import nz.co.gregs.dbvolution.databases.supports.SupportsDateRepeatDatatypeFunctions;
import nz.co.gregs.dbvolution.databases.supports.SupportsPolygonDatatype;
import nz.co.gregs.dbvolution.internal.h2.*;

/**
 * Stores all the required functionality to use an H2 database.
 *
 * @author Gregory Graham
 */
public class H2DB extends DBDatabase implements SupportsDateRepeatDatatypeFunctions, SupportsPolygonDatatype {

	private static Map<String, DBVFeature> featureMap = null;

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
		this("jdbc:h2:" + file.getCanonicalFile(), username, password);
	}

	/**
	 * Creates a DBDatabase for a H2 database.
	 *
	 * <p>
	 * Database exceptions may be thrown
	 *
	 * @param dataSource dataSource
	 */
	public H2DB(DataSource dataSource) {
		super(new H2DBDefinition(), dataSource);
//		jamDatabaseConnectionOpen();
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
	 */
	public H2DB(String jdbcURL, String username, String password) {
		super(new H2DBDefinition(), "org.h2.Driver", jdbcURL, username, password);
//		jamDatabaseConnectionOpen();
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
	 */
	public H2DB(String databaseFilename, String username, String password, boolean dummy) {
		super(new H2DBDefinition(), "org.h2.Driver", "jdbc:h2:" + databaseFilename, username, password);
//		jamDatabaseConnectionOpen();
	}

	@Override
	protected void addDatabaseSpecificFeatures(final Statement stmt) throws SQLException {
//		DateRepeatFunctions.addFunctions(stmt);
		DataTypes.addAll(stmt);
		if (featureMap == null) {
			featureMap = new HashMap<>();
			for (DBVFeature function : DateRepeatFunctions.values()) {
				featureMap.put(function.alias(), function);
			}
			for (DBVFeature function : Point2DFunctions.values()) {
				featureMap.put(function.alias(), function);
			}
			for (DBVFeature function : LineSegment2DFunctions.values()) {
				featureMap.put(function.alias(), function);
			}
			for (DBVFeature function : Line2DFunctions.values()) {
				featureMap.put(function.alias(), function);
			}
			for (DBVFeature function : Polygon2DFunctions.values()) {
				featureMap.put(function.alias(), function);
			}
			for (DBVFeature function : MultiPoint2DFunctions.values()) {
				featureMap.put(function.alias(), function);
			}
		}
		for (DataTypes datatype : DataTypes.values()) {
			featureMap.put(datatype.alias(), datatype);
		}
	}
//
//	private void jamDatabaseConnectionOpen() throws DBRuntimeException, SQLException {
//		this.storedConnection = getConnection();
//		this.storedConnection.createStatement();
//	}

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

	@Override
	public void addFeatureToFixException(Exception exp) throws Exception {
//		org.h2.jdbc.JdbcSQLException: Function "DBV_LINE2D_EQUALS" not found
//      org.h2.jdbc.JdbcSQLException: Unknown data type: "DBV_LINE2D"; SQL statement:
		boolean handledException = false;
		if (exp instanceof org.h2.jdbc.JdbcSQLException) {
			String message = exp.getMessage();
			if ((message.startsWith("Function \"DBV_") && message.contains("\" not found"))
					|| (message.startsWith("Method \"DBV_") && message.contains("\" not found"))) {
				String[] split = message.split("[\" ]+");
				String functionName = split[1];
				DBVFeature functions = featureMap.get(functionName);
				if (functions != null) {
					functions.add(getConnection().createStatement());
					handledException = true;
				}
			} else if (message.startsWith("Unknown data type: \"DBV_")) {
				String[] split = message.split("\"");
				String functionName = split[1];
				DBVFeature datatype = featureMap.get(functionName);
				if (datatype != null) {
					datatype.add(getConnection().createStatement());
					handledException = true;
				}
				//symbol:   method DBV_MULTIPOINT2D_BOUNDINGBOX(
			} else if (message.matches(": +method \"DBV_[A-Z_0-9]+")) {
				String[] split = message.split("method \"");
				split = split[1].split("\\(");
				String functionName = split[0];
				System.out.println("ADDING FUNCTION: " + functionName);
				DBVFeature functions = featureMap.get(functionName);
				if (functions != null) {
					functions.add(getConnection().createStatement());
					handledException = true;
				}
			} else {
				for (Map.Entry<String, DBVFeature> entrySet : featureMap.entrySet()) {
					String key = entrySet.getKey();
					DBVFeature value = entrySet.getValue();
					if (message.contains(key)) {
						value.add(getConnection().createStatement());
						handledException = true;
					}
				}
			}
		}
		if (!handledException) {
			throw exp;
		}
	}

}
