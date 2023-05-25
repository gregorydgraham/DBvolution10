/*
 * Copyright 2013 Gregory Graham.
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
package nz.co.gregs.dbvolution.generation;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import nz.co.gregs.dbvolution.databases.DBDatabase;

/**
 * Automatically generates Java files to be used in your data model.
 *
 * <p>
 * DBvolution data model classes (DBRow subclasses) are designed to be easy to
 * create and modify. However with a complex existing database it can be easier
 * to use this class to generate the data model and then add the details.
 *
 * @author Gregory Graham
 */
class DataRepoGenerator {

	private DataRepoGenerator() {
	}

	/**
	 *
	 * Creates DBTableRow classes corresponding to all the tables and views
	 * accessible to the user specified in the database supplied.
	 *
	 * <p>
	 * Classes are placed in the correct subdirectory of the base directory as
	 * defined by the package name supplied.
	 *
	 * <p>
	 * Convenience method which calls {@code generateClasses(jdbcURL, username,
	 * password, packageName, 1L, baseDirectory,new PrimaryKeyRecognisor(),new
	 * ForeignKeyRecognisor());}
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @param database database
	 * @param packageName packageName
	 * @param baseDirectory baseDirectory
	 * @return
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws java.io.FileNotFoundException java.io.FileNotFoundException
	 * @throws java.io.IOException java.io.IOException
	 */
	static public DataRepo generateClasses(DBDatabase database, String packageName) throws SQLException, FileNotFoundException, IOException {
		return generateClasses(database, packageName, new Options());
	}

	/**
	 *
	 * Creates DBTableRow classes corresponding to all the tables and views
	 * accessible to the user specified in the database supplied.
	 *
	 * <p>
	 * Classes are placed in the correct subdirectory of the base directory as
	 * defined by the package name supplied.
	 *
	 * <p>
	 * Convenience method which calls {@code
	 * generateClasses(jdbcURL,username,password,packageName,baseDirectory,new
	 * PrimaryKeyRecognisor(),new ForeignKeyRecognisor());}
	 *
	 * @param database database
	 * @param versionNumber - the value to use for serialVersionUID
	 *
	 * 1 Database exceptions may be thrown
	 * @param packageName packageName
	 * @param baseDirectory baseDirectory
	 * @return
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws java.io.FileNotFoundException java.io.FileNotFoundException
	 * @throws java.io.IOException java.io.IOException
	 *
	 *
	 */
	static public DataRepo generateClasses(DBDatabase database, String packageName, Long versionNumber) throws SQLException, FileNotFoundException, IOException {
		Options opts = new Options();
		opts.setVersionNumber(versionNumber);
		return generateClasses(database, packageName, opts);
	}

	/**
	 *
	 * Creates DBTableRow classes corresponding to all the tables and views
	 * accessible to the user specified in the database supplied.
	 *
	 * <p>
	 * Classes are placed in the correct subdirectory of the base directory as
	 * defined by the package name supplied.
	 *
	 * <p>
	 * Primary keys and foreign keys are created based on the definitions within
	 * the database and the results from the PK and FK recognisors.
	 *
	 * <p>
	 * Database exceptions may be thrown
	 *
	 * @param database database
	 * @param packageName packageName
	 * @param options preferences for the class generation
	 * @return
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws java.io.FileNotFoundException java.io.FileNotFoundException
	 * @throws java.io.IOException java.io.IOException
	 */
	static public DataRepo generateClasses(DBDatabase database, String packageName, Options options) throws SQLException, FileNotFoundException, IOException {
		DataRepo repo = new DataRepo(database, packageName);
		String viewsPackage = packageName + ".views";
		DataRepo parsedViews = parseViews(database, viewsPackage, options);
		repo.addViews(parsedViews.getTables());

		String tablesPackage = packageName + ".tables";
		DataRepo parsedTables = parseTables(database, tablesPackage, options);
		repo.addTables(parsedTables.getTables());

		repo.compile(options);

		return repo;
	}

	/**
	 * Generate the required Java classes for all the Tables on the database.
	 *
	 * <p>
	 * Connects to the database using the DBDatabase instance supplied and
	 * generates class for the tables it can find.
	 *
	 * <p>
	 * Classes will be in the package supplied, serialVersionUID will be set to
	 * the version number supplied and the supplied {@link PrimaryKeyRecognisor}
	 * and {@link ForeignKeyRecognisor} will be used.
	 *
	 *
	 * @param database database
	 * @param packageName packageName
	 * @param options
	 *
	 *
	 * @return a List of DBTableClass instances representing the tables found on
	 * the database 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	private static DataRepo parseTables(DBDatabase database, String packageName, Options options) throws SQLException {
		return parseObjectTypes(database, packageName, options, "TABLE");
	}

	/**
	 * Generate the required Java classes for all the Views on the database.
	 *
	 * <p>
	 * Connects to the database using the DBDatabase instance supplied and
	 * generates class for the views it can find.
	 *
	 * <p>
	 * Classes will be in the package supplied, serialVersionUID will be set to
	 * the version number supplied and the supplied {@link PrimaryKeyRecognisor}
	 * and {@link ForeignKeyRecognisor} will be used.
	 *
	 * @param database database
	 * @param packageName packageName
	 * @param options
	 *
	 * @return a List of DBTableClass instances representing the views found on
	 * the database 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	private static DataRepo parseViews(DBDatabase database, String packageName, Options options) throws SQLException {
		final DataRepo dataRepo = new DataRepo(database, packageName);
		dataRepo.addViews(parseObjectTypes(database, packageName, options, "VIEW").getTables());
		return dataRepo;
	}

	/**
	 * Generate the required Java classes for all the Tables and Views on the
	 * database.
	 *
	 * <p>
	 * Connects to the database using the DBDatabase instance supplied and
	 * generates class for the tables and views it can find.
	 *
	 * <p>
	 * Classes will be in the package supplied, serialVersionUID will be set to
	 * the version number supplied and the supplied {@link PrimaryKeyRecognisor}
	 * and {@link ForeignKeyRecognisor} will be used.
	 *
	 *
	 *
	 *
	 * @return a List of DBTableClass instances representing the tables and views
	 * found on the database 1 Database exceptions may be thrown
	 */
	private static DataRepo parseObjectTypes(DBDatabase db, String packageName, Options options, String... dbObjectTypes) throws SQLException {

		DataRepo datarepo = new DBDatabaseMetaData(db, dbObjectTypes, packageName, options).getDataRepo();

		return datarepo;
	}

	/**
	 *
	 * returns a good guess at the java CLASS version of a DB field name.
	 *
	 * I.e. changes "_" into an uppercase letter.
	 *
	 * @param s	s
	 *
	 *
	 * @return camel case version of the String
	 */
	static String toClassCase(String s) {
		StringBuilder classCaseString = new StringBuilder("");
		if (s == null) {
			return null;
		} else if (s.matches("[lLtT]+_[0-9]+(_[0-9]+)*")) {
			classCaseString.append(s.toUpperCase());
		} else {
			String[] parts = s.split("[^a-zA-Z0-9]");
			for (String part : parts) {
				classCaseString.append(toProperCase(part));
			}
		}
		return classCaseString.toString();
	}

	/**
	 *
	 * Capitalizes the first letter of the string
	 *
	 *
	 *
	 *
	 *
	 * @return Capitalizes the first letter of the string
	 */
	private static String toProperCase(String s) {
		switch (s.length()) {
			case 0:
				return s;
			case 1:
				return s.toUpperCase();
			default:
				String firstChar = s.substring(0, 1);
				String rest;
				if (s.replaceAll("[^A-Z]", "").length() > 0
						&& s.replaceAll("[^a-z]", "").length() > 0) {
					rest = s.substring(1);//.toLowerCase();
				} else {
					rest = s.substring(1).toLowerCase();
				}
				if (firstChar.matches("[^a-zA-Z]")) {
					return "_" + firstChar + rest;
				} else {
					return firstChar.toUpperCase() + rest;
				}
		}
	}

}
