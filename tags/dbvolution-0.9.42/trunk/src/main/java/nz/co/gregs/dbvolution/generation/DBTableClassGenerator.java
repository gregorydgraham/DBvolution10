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

import nz.co.gregs.dbvolution.exceptions.UnknownJavaSQLTypeException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.datatypes.*;

/**
 *
 * @author Gregory Graham
 */
public class DBTableClassGenerator {

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
	 * @param database
	 * @param packageName
	 * @param baseDirectory
	 * @throws SQLException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void generateClasses(DBDatabase database, String packageName, String baseDirectory) throws SQLException, FileNotFoundException, IOException {
		generateClasses(database, packageName, baseDirectory, 1L, new PrimaryKeyRecognisor(), new ForeignKeyRecognisor());
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
	 * @param database
	 * @param packageName
	 * @param versionNumber - the value to use for serialVersionUID
	 * @param baseDirectory
	 * @throws SQLException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void generateClasses(DBDatabase database, String packageName, String baseDirectory, Long versionNumber) throws SQLException, FileNotFoundException, IOException {
		generateClasses(database, packageName, baseDirectory, versionNumber, new PrimaryKeyRecognisor(), new ForeignKeyRecognisor());
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
	 * @param database
	 * @param packageName
	 * @param versionNumber
	 * @param baseDirectory
	 * @param pkRecog
	 * @param fkRecog
	 * @throws SQLException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void generateClasses(DBDatabase database, String packageName, String baseDirectory, Long versionNumber, PrimaryKeyRecognisor pkRecog, ForeignKeyRecognisor fkRecog) throws SQLException, FileNotFoundException, IOException {
		String viewsPackage = packageName + ".views";
		String viewsPath = viewsPackage.replaceAll("[.]", "/");
		List<DBTableClass> generatedViews = DBTableClassGenerator.generateClassesOfViews(database, viewsPackage, pkRecog, fkRecog);

		String tablesPackage = packageName + ".tables";
		String tablesPath = tablesPackage.replaceAll("[.]", "/");
		List<DBTableClass> generatedTables = DBTableClassGenerator.generateClassesOfTables(database, tablesPackage, pkRecog, fkRecog);
		List<DBTableClass> allGeneratedClasses = new ArrayList<DBTableClass>();
		allGeneratedClasses.addAll(generatedViews);
		allGeneratedClasses.addAll(generatedTables);
		generateAllJavaSource(allGeneratedClasses);

		File dir = new File(baseDirectory + "/" + viewsPath);
		if (dir.mkdirs() || dir.exists()) {
			saveGeneratedClassesToDirectory(generatedViews, dir);
		} else {
			throw new RuntimeException("Unable to Make Directories, QUITTING!");
		}

		dir = new File(baseDirectory + "/" + tablesPath);
		if (dir.mkdirs() || dir.exists()) {
			saveGeneratedClassesToDirectory(generatedTables, dir);
		} else {
			throw new RuntimeException("Unable to Make Directories, QUITTING!");
		}

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
	 * You probably want to use {@link #generateClasses(nz.co.gregs.dbvolution.DBDatabase, java.lang.String, java.lang.String)
	 * }
	 *
	 * @param generatedClasses
	 * @param classDirectory
	 * @throws SQLException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private static void saveGeneratedClassesToDirectory(List<DBTableClass> generatedClasses, File classDirectory) throws SQLException, FileNotFoundException, IOException {
		{
			File file;
			FileOutputStream fileOutputStream;
			for (DBTableClass clazz : generatedClasses) {
//                System.out.println(clazz.className + " => " + classDirectory.getAbsolutePath() + "/" + clazz.className + ".java");
				file = new File(classDirectory, clazz.getClassName() + ".java");
				fileOutputStream = new FileOutputStream(file);
//                System.out.println(clazz.javaSource);
//                System.out.println("");
				fileOutputStream.write(clazz.getJavaSource().getBytes());
				fileOutputStream.close();
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
	 * @param database
	 * @param packageName
	 * @param pkRecog
	 * @param fkRecog
	 * @return a List of DBTableClass instances representing the tables found on
	 * the database
	 * @throws SQLException
	 */
	public static List<DBTableClass> generateClassesOfTables(DBDatabase database, String packageName, PrimaryKeyRecognisor pkRecog, ForeignKeyRecognisor fkRecog) throws SQLException {
		return generateClassesOfObjectTypes(database, packageName, pkRecog, fkRecog, "TABLE");
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
	 * @param database
	 * @param packageName
	 * @param pkRecog
	 * @param fkRecog
	 * @return a List of DBTableClass instances representing the views found on
	 * the database
	 * @throws SQLException
	 */
	public static List<DBTableClass> generateClassesOfViews(DBDatabase database, String packageName, PrimaryKeyRecognisor pkRecog, ForeignKeyRecognisor fkRecog) throws SQLException {
		return generateClassesOfObjectTypes(database, packageName, pkRecog, fkRecog, "VIEW");
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
	 * @param database
	 * @param packageName
	 * @param dbObjectTypes
	 * @return a List of DBTableClass instances representing the tables and
	 * views found on the database
	 * @throws SQLException
	 */
	private static List<DBTableClass> generateClassesOfObjectTypes(DBDatabase database, String packageName, PrimaryKeyRecognisor pkRecog, ForeignKeyRecognisor fkRecog, String... dbObjectTypes) throws SQLException {
		List<DBTableClass> dbTableClasses = new ArrayList<DBTableClass>();

		DBStatement dbStatement = database.getDBStatement();
		try {
			Connection connection = dbStatement.getConnection();
			String catalog = connection.getCatalog();
			String schema = null;
			try {
				Method method = connection.getClass().getMethod("getSchema");
				schema = (String) method.invoke(connection);
				//schema = connection.getSchema();
			} catch (java.lang.AbstractMethodError exp) {
				// NOT USING Java 1.7+ apparently
			} catch (IllegalAccessException ex) {
				// NOT USING Java 1.7+ apparently
			} catch (IllegalArgumentException ex) {
				// NOT USING Java 1.7+ apparently
			} catch (NoSuchMethodException ex) {
				// NOT USING Java 1.7+ apparently
			} catch (SecurityException ex) {
				// NOT USING Java 1.7+ apparently
			} catch (InvocationTargetException ex) {
				// NOT USING Java 1.7+ apparently
			}

			DatabaseMetaData metaData = connection.getMetaData();
			ResultSet tables = metaData.getTables(catalog, schema, null, dbObjectTypes);

			try {
				while (tables.next()) {
					final String tableName = tables.getString("TABLE_NAME");
					final String className = toClassCase(tableName);
					DBTableClass dbTableClass = new DBTableClass(tableName, packageName, className);
//					dbTableClass.setPackageName(packageName);
//					dbTableClass.setTableName(tableName);
//					dbTableClass.setClassName(className);

					ResultSet primaryKeysRS = metaData.getPrimaryKeys(catalog, schema, dbTableClass.getTableName());
					List<String> pkNames = new ArrayList<String>();
					try {
						while (primaryKeysRS.next()) {
							String pkColumnName = primaryKeysRS.getString("COLUMN_NAME");
							pkNames.add(pkColumnName);
						}
					} finally {
						primaryKeysRS.close();
					}

					ResultSet foreignKeysRS = metaData.getImportedKeys(catalog, schema, dbTableClass.getTableName());
					Map<String, String[]> fkNames = new HashMap<String, String[]>();
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

					ResultSet columns = metaData.getColumns(catalog, schema, dbTableClass.getTableName(), null);
					try {
						while (columns.next()) {
							DBTableField dbTableField = new DBTableField();
							dbTableField.columnName = columns.getString("COLUMN_NAME");
							dbTableField.fieldName = toFieldCase(dbTableField.columnName);
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
								dbTableField.columnType = getQueryableDatatypeNameOfSQLType(dbTableField.sqlDataTypeInt, dbTableField.precision);
							} catch (UnknownJavaSQLTypeException ex) {
								dbTableField.columnType = DBUnknownDatatype.class;
								dbTableField.javaSQLDatatype = ex.getUnknownJavaSQLType();
							}
							if (pkNames.contains(dbTableField.columnName) || pkRecog.isPrimaryKeyColumn(dbTableClass.getTableName(), dbTableField.columnName)) {
								dbTableField.isPrimaryKey = true;
							}
							
							database.getDefinition().sanityCheckDBTableField(dbTableField);
							
							String[] pkData = fkNames.get(dbTableField.columnName);
							if (pkData != null && pkData.length == 2) {
								dbTableField.isForeignKey = true;
								dbTableField.referencesClass = toClassCase(pkData[0]);
								dbTableField.referencesField = pkData[1];
							} else if (fkRecog.isForeignKeyColumn(dbTableClass.getTableName(), dbTableField.columnName)) {
								dbTableField.isForeignKey = true;
								dbTableField.referencesField = fkRecog.getReferencedColumn(dbTableClass.getTableName(), dbTableField.columnName);
								dbTableField.referencesClass = toClassCase(fkRecog.getReferencedTable(dbTableClass.getTableName(), dbTableField.columnName));
							}
							dbTableClass.getFields().add(dbTableField);
						}
					} finally {
						columns.close();
					}

					dbTableClasses.add(dbTableClass);
				}
			} finally {
				tables.close();
			}
			generateAllJavaSource(dbTableClasses);
		} finally {
			dbStatement.close();
		}
		return dbTableClasses;
	}

	static void generateAllJavaSource(List<DBTableClass> dbTableClasses) {
		List<String> dbTableClassNames = new ArrayList<String>();

		for (DBTableClass dbt : dbTableClasses) {
			dbTableClassNames.add(dbt.getClassName());
		}
		for (DBTableClass dbt : dbTableClasses) {
			for (DBTableField dbf : dbt.getFields()) {
				if (dbf.isForeignKey) {
					if (!dbTableClassNames.contains(dbf.referencesClass)) {
						List<String> matchingNames = new ArrayList<String>();
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
			dbt.generateJavaSource();
//            System.out.println(dbt.javaSource);
		}
	}

	/**
	 *
	 * Returns a string of the appropriate QueryableDatatype for the specified
	 * SQLType
	 *
	 * @param columnType
	 * @return a string of the appropriate QueryableDatatype for the specified
	 * SQLType
	 */
	private static Class<? extends Object> getQueryableDatatypeNameOfSQLType(int columnType, int precision) throws UnknownJavaSQLTypeException {
		Class<? extends Object> value = QueryableDatatype.class;
		switch (columnType) {
			case Types.BIT:
				if (precision == 1) {
					value = DBBoolean.class;
				} else {
					value = DBByteArray.class;
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
			case Types.VARCHAR:
			case Types.CHAR:
			case Types.NCHAR:
			case Types.NVARCHAR:
			case Types.CLOB:
			case Types.NCLOB:
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
			case Types.JAVA_OBJECT:
				value = DBJavaObject.class;
				break;
			case Types.VARBINARY:
			case Types.LONGVARBINARY:
			case Types.BLOB:
			case Types.ARRAY:
			case Types.SQLXML:
				value = DBByteArray.class;
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
	 * @param s
	 * @return camel case version of the String
	 */
	public static String toClassCase(String s) {
		String classCaseString = "";
		if (s.matches("[lLtT]+_[0-9]+(_[0-9]+)*")) {
			classCaseString = s.toUpperCase();
		} else {
//            System.out.println("Splitting: " + s);
			String[] parts = s.split("[_$#]");
			for (String part : parts) {
				classCaseString += toProperCase(part);
			}
		}
		return classCaseString;
	}

	/**
	 *
	 * returns a good guess at the java field version of a DB field name.
	 *
	 * I.e. changes "_" into an uppercase letter.
	 *
	 * @param s
	 * @return Camel Case version of S
	 */
	private static String toFieldCase(String s) {
		String classClass = toClassCase(s);
		String camelCaseString = classClass.substring(0, 1).toLowerCase() + classClass.substring(1);
		return camelCaseString;
	}

	/**
	 *
	 * Capitalizes the first letter of the string
	 *
	 * @param s
	 * @return Capitalizes the first letter of the string
	 */
	private static String toProperCase(String s) {
		if (s.length() == 0) {
			return s;
		} else if (s.length() == 1) {
			return s.toUpperCase();
		} else {
			String firstChar = s.substring(0, 1);
			String rest = s.substring(1).toLowerCase();
			if (firstChar.matches("[^a-zA-Z]")) {
				return "_" + firstChar + rest;
			} else {
				return firstChar.toUpperCase() + rest;
			}
		}
	}
}
