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
package nz.co.gregs.dbvolution.generation.deprecated;

import nz.co.gregs.dbvolution.exceptions.UnknownJavaSQLTypeException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import nz.co.gregs.dbvolution.databases.connections.DBConnection;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.regexi.Regex;

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
public class DBTableClassGenerator {

	private static final String[] JAVA_RESERVED_WORDS_ARRAY = new String[]{"null", "true", "false", "abstract", "continue", "for", "new", "switch", "assert", "default", "goto", "package", "synchronized", "boolean", "do", "if", "private", "this", "break", "double", "implements", "", "protected", "throw", "byte", "else", "import", "public", "throws", "case", "enum", "instanceof", "return", "transient", "catch", "extends", "int", "short", "try", "char", "final", "interface", "static", "", "void", "class", "finally", "long", "strictfp", "volatile", "const", "float", "native", "super", "while"};
	private static final List<String> JAVA_RESERVED_WORDS = Arrays.asList(JAVA_RESERVED_WORDS_ARRAY);

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
	 * @return a data repo
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws java.io.FileNotFoundException java.io.FileNotFoundException
	 * @throws java.io.IOException java.io.IOException
	 */
	public static DataRepo generateClasses(DBDatabase database, String packageName, String baseDirectory) throws SQLException, FileNotFoundException, IOException {
		return generateClasses(database, packageName, baseDirectory, new Options());
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
	 * @return a datarepo
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws java.io.FileNotFoundException java.io.FileNotFoundException
	 * @throws java.io.IOException java.io.IOException
	 *
	 *
	 */
	public static DataRepo generateClasses(DBDatabase database, String packageName, String baseDirectory, Long versionNumber) throws SQLException, FileNotFoundException, IOException {
		Options opts = new Options();
		opts.versionNumber = versionNumber;
		return generateClasses(database, packageName, baseDirectory, opts);
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
	 * @param baseDirectory baseDirectory
	 * @param options preferences for the class generation
	 * @return a data repo
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws java.io.FileNotFoundException java.io.FileNotFoundException
	 * @throws java.io.IOException java.io.IOException
	 */
	public static DataRepo generateClasses(DBDatabase database, String packageName, String baseDirectory, Options options) throws SQLException, FileNotFoundException, IOException {
		DataRepo datastore = new DataRepo(database, packageName);
		String viewsPackage = packageName + ".views";
		String viewsPath = viewsPackage.replaceAll("[.]", "/");
		DataRepo generatedViews = DBTableClassGenerator.generateClassesOfViews(database, viewsPackage, options);
		datastore.addViews(generatedViews.getTables());

		String tablesPackage = packageName + ".tables";
		String tablesPath = tablesPackage.replaceAll("[.]", "/");
		DataRepo generatedTables = DBTableClassGenerator.generateClassesOfTables(database, tablesPackage, options);
		datastore.addTables(generatedTables.getTables());

		List<DBTableClass> allGeneratedClasses = new ArrayList<>();
		allGeneratedClasses.addAll(datastore.getViews());
		allGeneratedClasses.addAll(datastore.getTables());
		generateAllJavaSource(allGeneratedClasses, options);

		File dir = new File(baseDirectory + "/" + viewsPath);
		if (dir.mkdirs() || dir.exists()) {
			saveGeneratedClassesToDirectory(generatedViews.getViews(), dir);
		} else {
			throw new RuntimeException("Unable to Make Directories, QUITTING!");
		}

		dir = new File(baseDirectory + "/" + tablesPath);
		if (dir.mkdirs() || dir.exists()) {
			saveGeneratedClassesToDirectory(generatedTables.getTables(), dir);
		} else {
			throw new RuntimeException("Unable to Make Directories, QUITTING!");
		}
		return datastore;
	}

	/**
	 *
	 * Saves the supplied DBTableRow classes as java files in the supplied
	 * directory.
	 *
	 * <p>
	 * No database interaction nor package name checking is performed.
	 *
	 * <p>
	 * You probably want to use {@link #generateClasses(nz.co.gregs.dbvolution.databases.DBDatabase, java.lang.String, java.lang.String)
	 * }
	 *
	 *
	 *
	 * 1 Database exceptions may be thrown
	 *
	 *
	 */
	private static void saveGeneratedClassesToDirectory(List<DBTableClass> generatedClasses, File classDirectory) throws SQLException, FileNotFoundException, IOException {
		{
			File file;
			FileOutputStream fileOutputStream;
			for (DBTableClass clazz : generatedClasses) {
				file = new File(classDirectory, clazz.getClassName() + ".java");
				fileOutputStream = new FileOutputStream(file);
				try {
					fileOutputStream.write(clazz.getJavaSource().getBytes(Charset.forName("UTF8")));
					fileOutputStream.close();
				} finally {
					fileOutputStream.close();
				}
			}
		}
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
	 * @param options a collection of options
	 *
	 *
	 * @return a List of DBTableClass instances representing the tables found on
	 * the database 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public static DataRepo generateClassesOfTables(DBDatabase database, String packageName, Options options) throws SQLException {
		return generateClassesOfObjectTypes(database, packageName, options, "TABLE");
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
	 * @param options a collection of options
	 *
	 * @return a List of DBTableClass instances representing the views found on
	 * the database 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public static DataRepo generateClassesOfViews(DBDatabase database, String packageName, Options options) throws SQLException {
		final DataRepo dataRepo = new DataRepo(database, packageName);
		dataRepo.addViews(generateClassesOfObjectTypes(database, packageName, options, "VIEW").getTables());
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
	private static DataRepo generateClassesOfObjectTypes(DBDatabase db, String packageName, Options options, String... dbObjectTypes) throws SQLException {
		DataRepo datarepo = new DataRepo(db, packageName);

		DBDatabase database = db;
		if (db instanceof DBDatabaseCluster) {
			database = ((DBDatabaseCluster) db).getReadyDatabase();
		}

//		List<DBTableClass> dbTableClasses = new ArrayList<>();
		try (DBStatement dbStatement = database.getDBStatement()) {
			DBConnection connection = dbStatement.getConnection();
			String catalog = connection.getCatalog();
			String schema = null;
			try {
				//Method method = connection.getClass().getMethod("getSchema");
				//schema = (String) method.invoke(connection);
				schema = connection.getSchema();
			} catch (java.sql.SQLFeatureNotSupportedException nope) {
				// SOMEONE DIDN'T WRITE THEIR DRIVER PROPERLY
			} catch (java.lang.AbstractMethodError exp) {
				// NOT USING Java 1.7+ apparently
			} catch (IllegalArgumentException ex) {
				// NOT USING Java 1.7+ apparently
			} catch (SecurityException ex) {
				// NOT USING Java 1.7+ apparently
			}

			DatabaseMetaData metaData = connection.getMetaData();
			try (ResultSet tables = metaData.getTables(catalog, schema, null, dbObjectTypes)) {
				while (tables.next()) {
					final String tableName = tables.getString("TABLE_NAME");
					if (schema == null) {
						schema = tables.getString("TABLE_SCHEM");
					}
					DBDefinition definition = database.getDefinition();
					Regex systemTableExclusionPattern = definition.getSystemTableExclusionPattern();
					if (systemTableExclusionPattern.matchesEntireString(tableName)) {
						final String className = toClassCase(tableName);
						DBTableClass dbTableClass = new DBTableClass(tableName, schema, packageName, className);

						ResultSet primaryKeysRS = metaData.getPrimaryKeys(catalog, schema, dbTableClass.getTableName());
						List<String> pkNames = new ArrayList<>();
						try {
							while (primaryKeysRS.next()) {
								String pkColumnName = primaryKeysRS.getString("COLUMN_NAME");
								pkNames.add(pkColumnName);
							}
						} finally {
							primaryKeysRS.close();
						}

						ResultSet foreignKeysRS = metaData.getImportedKeys(catalog, schema, dbTableClass.getTableName());
						Map<String, String[]> fkNames = new HashMap<>();
						try {
							while (foreignKeysRS.next()) {
								String pkTableName = foreignKeysRS.getString("PKTABLE_NAME");
								String pkColumnName = foreignKeysRS.getString("PKCOLUMN_NAME");
								String fkColumnName = foreignKeysRS.getString("FKCOLUMN_NAME");
								fkNames.put(fkColumnName, new String[]{pkTableName, pkColumnName});
							}
						} finally {
							foreignKeysRS.close();
						}

						try (ResultSet columns = metaData.getColumns(catalog, schema, dbTableClass.getTableName(), null)) {
							while (columns.next()) {
								var dbTableField = new DBTableField();
								dbTableField.columnName = columns.getString("COLUMN_NAME");
								dbTableField.fieldName = toFieldCase(dbTableField.columnName);
								try {
									dbTableField.referencedTable = columns.getString("SCOPE_TABLE");
								} catch (SQLException exp) {
									; // MSSQLServer throws an exception on this
								}
								dbTableField.precision = columns.getInt("COLUMN_SIZE");
								dbTableField.comments = columns.getString("REMARKS");
								String isAutoIncr = null;
								try {
									isAutoIncr = columns.getString("IS_AUTOINCREMENT");
								} catch (SQLException sqlex) {
									;// SQLite-JDBC throws an exception when retrieving IS_AUTOINCREMENT
								}
								dbTableField.isAutoIncrement = isAutoIncr != null && isAutoIncr.equals("YES");
								try {
									dbTableField.sqlDataTypeInt = columns.getInt("DATA_TYPE");
									dbTableField.sqlDataTypeName = columns.getString("TYPE_NAME");
									dbTableField.columnType = getQueryableDatatypeNameOfSQLType(database, dbTableField, options.trimCharColumns);
								} catch (UnknownJavaSQLTypeException ex) {
									dbTableField.columnType = DBUnknownDatatype.class;
									dbTableField.javaSQLDatatype = ex.getUnknownJavaSQLType();
								}
								if (pkNames.contains(dbTableField.columnName) || options.pkRecog.isPrimaryKeyColumn(dbTableClass.getTableName(), dbTableField.columnName)) {
									dbTableField.isPrimaryKey = true;
								}

//								database.getDefinition().sanityCheckDBTableField(dbTableField);

								String[] pkData = fkNames.get(dbTableField.columnName);
								if (pkData != null && pkData.length == 2) {
									dbTableField.isForeignKey = true;
									dbTableField.referencesClass = toClassCase(pkData[0]);
									dbTableField.referencesField = pkData[1];
								} else if (options.fkRecog.isForeignKeyColumn(dbTableClass.getTableName(), dbTableField.columnName)) {
									dbTableField.isForeignKey = true;
									dbTableField.referencesField = options.fkRecog.getReferencedColumn(dbTableClass.getTableName(), dbTableField.columnName);
									dbTableField.referencesClass = toClassCase(options.fkRecog.getReferencedTable(dbTableClass.getTableName(), dbTableField.columnName));
								}
								if (!dbTableClass.getFields().contains(dbTableField)) {
									dbTableClass.getFields().add(dbTableField);
								}
							}
						}

						datarepo.addTable(dbTableClass);
					}
				}
			}
			generateAllJavaSource(datarepo.getTables(), options);
		}
		return datarepo;
	}

	static void generateAllJavaSource(List<DBTableClass> dbTableClasses, Options options) {
		List<String> dbTableClassNames = new ArrayList<>();

		for (DBTableClass dbt : dbTableClasses) {
			dbTableClassNames.add(dbt.getClassName());
		}
		for (DBTableClass dbt : dbTableClasses) {
			for (DBTableField dbf : dbt.getFields()) {
				if (dbf.isForeignKey) {
					if (!dbTableClassNames.contains(dbf.referencesClass)) {
						List<String> matchingNames = new ArrayList<>();
						for (String name : dbTableClassNames) {
							if (name.toLowerCase().startsWith(dbf.referencesClass.toLowerCase())) {
								matchingNames.add(name);
							}
						}
						if (matchingNames.size() == 1) {
							String properClassname = matchingNames.get(0);
							dbf.referencesClass = properClassname;
						}
					}
				}
			}
			dbt.generateJavaSource(options);
		}
	}

	/**
	 * Investigate the database schema and generate appropriate classes and return
	 * DataRepo containing the database and the new source code.
	 *
	 * @param database the database to generate a DataRepo for
	 * @param namespace the package name to use for the resulting classes
	 * @return a DataRepo containing the DBDatabase and the associated classes
	 * @throws java.sql.SQLException database errors
	 * @throws java.io.IOException IO exceptions may occur will creating the DataRepo classes
	 */
	public static DataRepo generateClassesOfViewsAndTables(DBDatabase database, final String namespace) throws SQLException, IOException {
		var generateSchema = generateClassesOfObjectTypes(database, namespace, new Options(), "VIEW", "TABLE");
		return generateSchema;
	}

	/**
	 *
	 * Returns a string of the appropriate QueryableDatatype for the specified
	 * SQLType
	 *
	 *
	 *
	 *
	 *
	 * @return a string of the appropriate QueryableDatatype for the specified
	 * SQLType
	 */
	private static Class<? extends Object> getQueryableDatatypeNameOfSQLType(DBDatabase database, DBTableField column, Boolean trimCharColumns) throws UnknownJavaSQLTypeException {
		int columnType = column.sqlDataTypeInt;
		int precision = column.precision;
		String typeName = column.sqlDataTypeName;

		Class<? extends Object> value;
		switch (columnType) {
			case Types.BIT:
				if (precision == 1) {
					value = DBBoolean.class;
				} else {
					value = DBLargeBinary.class;
				}
				break;
			case Types.TINYINT:
			case Types.INTEGER:
			case Types.BIGINT:
			case Types.BINARY:
			case Types.BOOLEAN:
			case Types.ROWID:
			case Types.SMALLINT:
				if (precision == 1) {
					value = DBBoolean.class;
				} else {
					value = DBInteger.class;
				}
				break;
			case Types.DECIMAL:
			case Types.DOUBLE:
			case Types.FLOAT:
			case Types.NUMERIC:
			case Types.REAL:
				value = DBNumber.class;
				break;
			case Types.CHAR:
			case Types.NCHAR:
				if (trimCharColumns) {
					value = DBStringTrimmed.class;
				} else {
					value = DBString.class;
				}
				break;
			case Types.VARCHAR:
			case Types.NVARCHAR:
			case Types.LONGNVARCHAR:
			case Types.LONGVARCHAR:
				value = DBString.class;
				break;
			case Types.DATE:
			case Types.TIME:
			case Types.TIMESTAMP:
				value = DBDate.class;
				break;
			case Types.OTHER:
				Class<? extends QueryableDatatype<?>> customType = database.getDefinition().getQueryableDatatypeClassForSQLDatatype(typeName);
				if (customType != null) {
					value = customType;
					break;
				} else {
					value = DBJavaObject.class;
					break;
				}
			case Types.JAVA_OBJECT:
				value = DBJavaObject.class;
				break;
			case Types.CLOB:
			case Types.NCLOB:
				value = DBLargeText.class;
				break;
			case Types.VARBINARY:
			case Types.LONGVARBINARY:
			case Types.BLOB:
			case Types.ARRAY:
			case Types.SQLXML:
				value = DBLargeBinary.class;
				break;
			default:
				throw new UnknownJavaSQLTypeException("Unknown Java SQL Type: " + columnType, columnType);
		}
		return value;
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
	public static String toClassCase(String s) {
		StringBuilder classCaseString = new StringBuilder("");
		if (s == null) {
			return null;
		} else if (s.matches("[lLtT]+_[0-9]+(_[0-9]+)*")) {
			classCaseString.append(s.toUpperCase());
		} else {
			String[] parts = s.split("[^a-zA-Z0-9]");//"[_$#]");
			for (String part : parts) {
				classCaseString.append(toProperCase(part));
			}
		}
		return classCaseString.toString();
	}

	/**
	 *
	 * returns a good guess at the java field version of a DB field name.
	 *
	 * I.e. changes "_" into an uppercase letter.
	 *
	 *
	 *
	 *
	 *
	 * @return Camel Case version of S
	 */
	private static String toFieldCase(String s) {
		String classClass = toClassCase(s);
		String camelCaseString = classClass.substring(0, 1).toLowerCase() + classClass.substring(1);
		camelCaseString = camelCaseString.replaceAll("[^a-zA-Z0-9_$]", "_");
		if (JAVA_RESERVED_WORDS.contains(camelCaseString)) {
			camelCaseString += "_";
		}
		return camelCaseString;
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
				String rest = s.substring(1).toLowerCase();
				if (firstChar.matches("[^a-zA-Z]")) {
					return "_" + firstChar + rest;
				} else {
					return firstChar.toUpperCase() + rest;
				}
		}
	}

	private DBTableClassGenerator() {
	}

	/*
	 * @param fkRecog fkRecog an object that can recognize foreign key columns by
	 * the column name and derive the related table
	 * @param versionNumber versionNumber
	 * @param pkRecog pkRecog an object that can recognize primary key columns by
	 * the column name
	 * @param trimCharColumns

	 */
	public static class Options {

		Long versionNumber = 1l;
		PrimaryKeyRecognisor pkRecog = new PrimaryKeyRecognisor();
		ForeignKeyRecognisor fkRecog = new ForeignKeyRecognisor();
		Boolean trimCharColumns = false;
		Boolean includeForeignKeyColumnName = false;

		public Options() {
		}

		public Options(Long versionNumber, PrimaryKeyRecognisor pkRecog, ForeignKeyRecognisor fkRecog, Boolean trimCharColumns) {
			this.versionNumber = versionNumber;
			this.pkRecog = pkRecog;
			this.fkRecog = fkRecog;
			this.trimCharColumns = trimCharColumns;

		}

	}
}
