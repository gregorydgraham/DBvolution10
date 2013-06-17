/*
 * Copyright 2013 gregorygraham.
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

import java.beans.IntrospectionException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.InformixDB;

/**
 *
 * @author gregorygraham
 */
public class DBTableClassGenerator {

    /**
     *
     * Creates DBTableRow classes corresponding to all the tables and views
     * accessible to the user specified in the database supplied.
     *
     * Classes are placed in the correct subdirectory of the base directory as
     * defined by the package name supplied.
     *
     * convenience method which calls
     * generateClassesFromJDBCURLToDirectory(jdbcURL,username,password,packageName,baseDirectory,new
     * PrimaryKeyRecognisor(),new ForeignKeyRecognisor());
     *
     * @param jdbcURL
     * @param username
     * @param password
     * @param packageName
     * @param baseDirectory
     * @throws ClassNotFoundException
     * @throws SQLException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws IntrospectionException
     * @throws InvocationTargetException
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static void generateClassesFromJDBCURLToDirectory(String jdbcURL, String username, String password, String packageName, String baseDirectory) throws ClassNotFoundException, SQLException, IllegalArgumentException, IllegalAccessException, IntrospectionException, InvocationTargetException, FileNotFoundException, IOException {
        generateClassesFromJDBCURLToDirectory(jdbcURL, username, password, packageName, baseDirectory, new PrimaryKeyRecognisor(), new ForeignKeyRecognisor());
    }

    public static void generateClassesFromJDBCURLToDirectory(String jdbcURL, String username, String password, String packageName, String baseDirectory, PrimaryKeyRecognisor pkRecog, ForeignKeyRecognisor fkRecog) throws ClassNotFoundException, SQLException, IllegalArgumentException, IllegalAccessException, IntrospectionException, InvocationTargetException, FileNotFoundException, IOException {
        InformixDB informixDB = new InformixDB(jdbcURL, username, password);

        String viewsPackage = packageName + ".views";
        String viewsPath = viewsPackage.replaceAll("[.]", "/");
        File dir = new File(baseDirectory + "/" + viewsPath);
        if (dir.mkdirs() || dir.exists()) {
            List<DBTableClass> generatedViews = DBTableClassGenerator.generateClassesOfViews(informixDB, viewsPackage, pkRecog, fkRecog);
            saveGenerateClassesToDirectory(generatedViews, dir);
        } else {
            throw new RuntimeException("Unable to Make Directories, QUITTING!");
        }

        String tablesPackage = packageName + ".tables";
        String tablesPath = tablesPackage.replaceAll("[.]", "/");
        dir = new File(baseDirectory + "/" + tablesPath);
        if (dir.mkdirs() || dir.exists()) {
            List<DBTableClass> generatedTables = DBTableClassGenerator.generateClassesOfTables(informixDB, tablesPackage, pkRecog, fkRecog);
            saveGenerateClassesToDirectory(generatedTables, dir);
        } else {
            throw new RuntimeException("Unable to Make Directories, QUITTING!");
        }

    }

    /**
     *
     * Saves the supplied DBTableRow classes as java files in the supplied
     * directory.
     *
     * No database interaction nor package name checking is performed.
     *
     * You probably want to use generateClassesFromJDBCURLToDirectory
     *
     * @param generatedClasses
     * @param classDirectory
     * @throws ClassNotFoundException
     * @throws SQLException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws IntrospectionException
     * @throws InvocationTargetException
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static void saveGenerateClassesToDirectory(List<DBTableClass> generatedClasses, File classDirectory) throws ClassNotFoundException, SQLException, IllegalArgumentException, IllegalAccessException, IntrospectionException, InvocationTargetException, FileNotFoundException, IOException {
        {
            File file;
            FileOutputStream fileOutputStream;
            for (DBTableClass clazz : generatedClasses) {
                System.out.println(clazz.className + " => " + classDirectory.getAbsolutePath() + "/" + clazz.className + ".java");
                file = new File(classDirectory, clazz.className + ".java");
                fileOutputStream = new FileOutputStream(file);
                System.out.println(clazz.javaSource);
                System.out.println("");
                fileOutputStream.write(clazz.javaSource.getBytes());
                fileOutputStream.close();
            }
        }
    }

    /**
     *
     * @param database
     * @param packageName
     * @return
     * @throws SQLException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws IntrospectionException
     * @throws InvocationTargetException
     */
    public static List<DBTableClass> generateClassesOfTables(DBDatabase database, String packageName, PrimaryKeyRecognisor pkRecog, ForeignKeyRecognisor fkRecog) throws SQLException, IllegalArgumentException, IllegalAccessException, IntrospectionException, InvocationTargetException {
        return generateClassesOfObjectTypes(database, packageName, pkRecog, fkRecog, new String[]{"TABLE"});
    }

    /**
     *
     * @param database
     * @param packageName
     * @return
     * @throws SQLException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws IntrospectionException
     * @throws InvocationTargetException
     */
    public static List<DBTableClass> generateClassesOfViews(DBDatabase database, String packageName, PrimaryKeyRecognisor pkRecog, ForeignKeyRecognisor fkRecog) throws SQLException, IllegalArgumentException, IllegalAccessException, IntrospectionException, InvocationTargetException {
        return generateClassesOfObjectTypes(database, packageName, pkRecog, fkRecog, new String[]{"VIEW"});
    }

    /**
     *
     * @param database
     * @param packageName
     * @param dbObjectTypes
     * @return
     * @throws SQLException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws IntrospectionException
     * @throws InvocationTargetException
     */
    public static List<DBTableClass> generateClassesOfObjectTypes(DBDatabase database, String packageName, PrimaryKeyRecognisor pkRecog, ForeignKeyRecognisor fkRecog, String[] dbObjectTypes) throws SQLException, IllegalArgumentException, IllegalAccessException, IntrospectionException, InvocationTargetException {
        List<DBTableClass> dbTableClasses = new ArrayList<DBTableClass>();
        List<String> dbTableClassNames = new ArrayList<String>();
        //String lineSeparator = System.getProperty("line.separator");
        //String conceptBreak = lineSeparator + lineSeparator;

        Statement dbStatement = database.getDBStatement();
        Connection connection = dbStatement.getConnection();
        String catalog = connection.getCatalog();
        String schema = null;
        try {
            schema = connection.getSchema();
        } catch (java.lang.AbstractMethodError exp) {
            // NOT USING Java 1.7+ apparently
        }
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet tables = metaData.getTables(catalog, schema, null, dbObjectTypes);

        while (tables.next()) {
            DBTableClass dbTableClass = new DBTableClass();
            dbTableClass.packageName = packageName;
            dbTableClass.tableName = tables.getString("TABLE_NAME");
            System.out.println(dbTableClass.tableName);
            dbTableClass.className = toClassCase(dbTableClass.tableName);
            dbTableClassNames.add(dbTableClass.className);

            ResultSet primaryKeysRS = metaData.getPrimaryKeys(catalog, schema, dbTableClass.tableName);
            List<String> pkNames = new ArrayList<String>();
            while (primaryKeysRS.next()) {
                String pkColumnName = primaryKeysRS.getString("COLUMN_NAME");
                pkNames.add(pkColumnName);
            }

            ResultSet foreignKeysRS = metaData.getImportedKeys(catalog, schema, dbTableClass.tableName);
            Map<String, String[]> fkNames = new HashMap<String, String[]>();
            while (foreignKeysRS.next()) {
                String pkTableName = foreignKeysRS.getString("PKTABLE_NAME");
                String pkColumnName = foreignKeysRS.getString("PKCOLUMN_NAME");
                String fkColumnName = foreignKeysRS.getString("FKCOLUMN_NAME");
                fkNames.put(fkColumnName, new String[]{pkTableName, pkColumnName});
            }

            ResultSet columns = metaData.getColumns(catalog, schema, dbTableClass.tableName, null);
            while (columns.next()) {
                DBTableField dbTableField = new DBTableField();
                dbTableField.columnName = columns.getString("COLUMN_NAME");
                dbTableField.fieldName = toFieldCase(dbTableField.columnName);
                dbTableField.columnType = getQueryableDatatypeOfSQLType(columns.getInt("DATA_TYPE"));
                if (pkNames.contains(dbTableField.columnName) || pkRecog.isPrimaryKeyColumn(dbTableClass.tableName, dbTableField.columnName)) {
                    dbTableField.isPrimaryKey = true;
                }
                String[] pkData = fkNames.get(dbTableField.columnName);
                if (pkData != null && pkData.length == 2) {
                    dbTableField.isForeignKey = true;
                    dbTableField.referencesClass = toClassCase(pkData[0]);
                    dbTableField.referencesField = pkData[1];
                } else if (fkRecog.isForeignKeyColumn(dbTableClass.tableName, dbTableField.columnName)) {
                    dbTableField.isForeignKey = true;
                    dbTableField.referencesField = fkRecog.getReferencedColumn(dbTableClass.tableName, dbTableField.columnName);
                    dbTableField.referencesClass = fkRecog.getReferencedTable(dbTableClass.tableName, dbTableField.columnName);
                }
                dbTableClass.fields.add(dbTableField);
            }
            dbTableClasses.add(dbTableClass);
        }
        for (DBTableClass dbt : dbTableClasses) {
            for (DBTableField dbf : dbt.fields) {
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
            System.out.println(dbt.javaSource);

        }
        return dbTableClasses;
    }

    /**
     *
     * Returns a string of the appropriate QueryableDatatype for the specified
     * SQLType
     *
     * @param columnType
     * @return
     */
    public static String getQueryableDatatypeOfSQLType(int columnType) {
        String value = "";
        switch (columnType) {
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.BINARY:
            case Types.BOOLEAN:
            case Types.ROWID:
            case Types.SMALLINT:
                value = "DBInteger";
                break;
            case Types.DECIMAL:
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.NUMERIC:
            case Types.REAL:
                value = "DBNumber";
                break;
            case Types.VARCHAR:
            case Types.CHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.LONGNVARCHAR:
            case Types.LONGVARCHAR:
                value = "DBString";
                break;
            case Types.DATE:
            case Types.TIME:
                value = "DBDate";
                break;
            case Types.TIMESTAMP:
                value = "DBDate";
                break;
            case Types.VARBINARY:
            case Types.JAVA_OBJECT:
            case Types.LONGVARBINARY:
                value = "DBBlob";
                break;
            default:
                throw new RuntimeException("Unknown Java SQL Type: " + columnType);
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
     * @return
     */
    public static String toClassCase(String s) {
        System.out.println("Splitting: " + s);
        String[] parts = s.split("_");
        String classCaseString = "";
        for (String part : parts) {
            classCaseString = classCaseString + toProperCase(part);
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
     * @return
     */
    public static String toFieldCase(String s) {
//        String[] parts = s.split("_");
//        String camelCaseString = "";
//        for (String part : parts) {
//            camelCaseString = camelCaseString + toProperCase(part);
//        }
        String classClass = toClassCase(s);
        String camelCaseString = classClass.substring(0, 1).toLowerCase() + classClass.substring(1);
        return camelCaseString;
    }

    /**
     *
     * Capitalizes the first letter of the string
     *
     * @param s
     * @return
     */
    public static String toProperCase(String s) {
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
