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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nz.co.gregs.dbvolution.databases.DBDatabase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Top level code generator.
 * Generates code based on database schema.
 */
public class DBTableClassGenerator {
	private static final Logger log = LoggerFactory.getLogger(DBTableClassGenerator.class);

    /**
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
     * @param database
     * @param packageName
     * @param baseDirectory
     * @throws SQLException
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static void generateClassesFromJDBCURLToDirectory(DBDatabase database, String packageName, String baseDirectory) throws SQLException, FileNotFoundException, IOException {
        generateClassesFromJDBCURLToDirectory(database, packageName, baseDirectory, new PrimaryKeyRecognisor(), new ForeignKeyRecognisor());
    }

    public static void generateClassesFromJDBCURLToDirectory(DBDatabase database, String packageName, String baseDirectory, PrimaryKeyRecognisor pkRecog, ForeignKeyRecognisor fkRecog) throws SQLException, FileNotFoundException, IOException {
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
     * No database interaction nor package name checking is performed.
     *
     * You probably want to use generateClassesFromJDBCURLToDirectory
     *
     * @param generatedClasses
     * @param classDirectory
     * @throws SQLException
     * @throws FileNotFoundException
     * @throws IOException
     */
    static void saveGeneratedClassesToDirectory(List<DBTableClass> generatedClasses, File classDirectory) throws SQLException, FileNotFoundException, IOException {
        {
            File file;
            FileOutputStream fileOutputStream;
            for (DBTableClass clazz : generatedClasses) {
                log.info(clazz.getClassName() + " => " + classDirectory.getAbsolutePath() + "/" + clazz.getClassName() + ".java");
                file = new File(classDirectory, clazz.getClassName() + ".java");
                fileOutputStream = new FileOutputStream(file);
                log.info(clazz.getJavaSource());
                log.info("");
                fileOutputStream.write(clazz.getJavaSource().getBytes());
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
     */
    public static List<DBTableClass> generateClassesOfTables(DBDatabase database, String packageName, PrimaryKeyRecognisor pkRecog, ForeignKeyRecognisor fkRecog) throws SQLException {
        return generateClassesOfObjectTypes(database, packageName, pkRecog, fkRecog, "TABLE");
    }

    /**
     *
     * @param database
     * @param packageName
     * @return
     * @throws SQLException
     */
    public static List<DBTableClass> generateClassesOfViews(DBDatabase database, String packageName, PrimaryKeyRecognisor pkRecog, ForeignKeyRecognisor fkRecog) throws SQLException {
        return generateClassesOfObjectTypes(database, packageName, pkRecog, fkRecog, "VIEW");
    }

    /**
     *
     * @param database
     * @param packageName
     * @param dbObjectTypes
     * @return
     * @throws SQLException
     */
    public static List<DBTableClass> generateClassesOfObjectTypes(DBDatabase database, String packageName, PrimaryKeyRecognisor pkRecog, ForeignKeyRecognisor fkRecog, String... dbObjectTypes) throws SQLException {
        List<DBTableClass> dbTableClasses = new ArrayList<DBTableClass>();

        Statement dbStatement = database.getDBStatement();
        Connection connection = dbStatement.getConnection();
        String catalog = connection.getCatalog();
        String schema;
        try {
            Method method = connection.getClass().getMethod("getSchema");
            schema = (String) method.invoke(connection);
            //schema = connection.getSchema();
        } catch (java.lang.AbstractMethodError exp) {
            // NOT USING Java 1.7+ apparently
        	schema = null; // fall-back to doing it without the schema name
        } catch (Exception ex) {
            // NOT USING Java 1.7+ apparently
        	schema = null; // fall-back to doing it without the schema name
        }

        // fetch names of all tables
        List<String> tableNames = new ArrayList<String>();
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet tables = metaData.getTables(catalog, schema, null, dbObjectTypes);
        while (tables.next()) {
        	tableNames.add(tables.getString("TABLE_NAME"));
        }
        
        // TODO: fetch existing tables and columns with their class names and field names
        TableNameResolver tableNameResolver = new TableNameResolver(packageName);

        // fetch schema for all tables
        for (String tableName: tableNames) {
            DBTableClass dbTableClass = new DBTableClass();
            dbTableClass.setPackageName(packageName);
            dbTableClass.setTableName(tableName);
            log.info("Retrieving details for table "+dbTableClass.getTableName());
            dbTableClass.setClassName(tableNameResolver.getSimpleClassNameFor(dbTableClass.getTableName()));
            dbTableClasses.add(dbTableClass);
            
            // TODO: initialise ColumnNameResolver with existing columns on this table

            ResultSet primaryKeysRS = metaData.getPrimaryKeys(catalog, schema, dbTableClass.getTableName());
            List<String> pkNames = new ArrayList<String>();
            while (primaryKeysRS.next()) {
                String pkColumnName = primaryKeysRS.getString("COLUMN_NAME");
                pkNames.add(pkColumnName);
            }

            ResultSet foreignKeysRS = metaData.getImportedKeys(catalog, schema, dbTableClass.getTableName());
            Map<String, String[]> fkNames = new HashMap<String, String[]>();
            while (foreignKeysRS.next()) {
                String pkTableName = foreignKeysRS.getString("PKTABLE_NAME");
                String pkColumnName = foreignKeysRS.getString("PKCOLUMN_NAME");
                String fkColumnName = foreignKeysRS.getString("FKCOLUMN_NAME");
                fkNames.put(fkColumnName, new String[]{pkTableName, pkColumnName});
            }

            // populate initial column info
            ResultSet columns = metaData.getColumns(catalog, schema, dbTableClass.getTableName(), null);
            while (columns.next()) {
                DBTableField dbTableField = new DBTableField();
                dbTableField.setColumnName(columns.getString("COLUMN_NAME"));
                dbTableField.setFieldName(toFieldCase(dbTableField.getColumnName()));
                dbTableField.setPrecision(columns.getInt("COLUMN_SIZE"));
                dbTableField.setColumnType(database.getDefinition().getQueryableDatatypeOfSQLType(columns.getInt("DATA_TYPE"), dbTableField.getPrecision()));
                if (pkNames.contains(dbTableField.getColumnName()) || pkRecog.isPrimaryKeyColumn(dbTableClass.getTableName(), dbTableField.getColumnName())) {
                    dbTableField.setIsPrimaryKey(true);
                }
                dbTableClass.getFields().add(dbTableField);
                
                // identify foreign key columns by constraint
                // (foreign keys are additionally identified by recogniser
                //  once all tables and columns are known about)
                String[] pkData = fkNames.get(dbTableField.getColumnName());
                if (pkData != null && pkData.length == 2) {
                    dbTableField.setIsForeignKey(true);
                    dbTableField.setReferencedTable(pkData[0]);
                    dbTableField.setReferencedColumn(pkData[1]);
                }
            }
        }
        
        populateForeignKeys(dbTableClasses, fkRecog);
        generateAllJavaSource(dbTableClasses);
        return dbTableClasses;
    }

    /**
     * Populates references to foreign key classes and fields from
     * known table and column names.
     * 
     * <p> This has to be done after all tables are loaded from the schema
     * so that all tables are known about.
     * @param dbTableClasses
     */
    static void populateForeignKeys(List<DBTableClass> dbTableClasses, ForeignKeyRecognisor fkRecog) {
    	// prepare lookups
    	Map<String,DBTableClass> classesByTableName = new HashMap<String, DBTableClass>();
        for (DBTableClass dbt : dbTableClasses) {
            classesByTableName.put(dbt.getTableName(), dbt);
        }
        Set<String> allTableNames = classesByTableName.keySet();
        
        // identify foreign key columns by recogniser
        // (Match up foreign keys to their target generated class and field
        //  via the foreign key recogniser)
        // (note: skip over columns already marked as foreign key by constraint)
        for (DBTableClass dbTableClass: dbTableClasses) {
	        for (DBTableField dbTableField: dbTableClass.getFields()) {
	        	if (!dbTableField.isForeignKey()) {
	                if (fkRecog.isForeignKeyColumn(dbTableClass.getTableName(), dbTableField.getColumnName())) {
	                    //dbTableField.setIsForeignKey(true);
	                    
	                    // identify referenced table and class
	                	String table = fkRecog.getReferencedTable(dbTableClass.getTableName(), dbTableField.getColumnName(), allTableNames);
	                	dbTableField.setReferencedTable(table);
	                	if (table == null) {
	                		log.warn("dropping foreign key from column "+dbTableClass.getTableName()+"."+dbTableField.getColumnName()+" - referenced table could not be inferred from column name");
	                	}
	                	// FIXME: what to do if table is null?
	                	
	                	DBTableClass referencedClass = null;
	                	if (table != null) {
		                	referencedClass = classesByTableName.get(table);
		                	dbTableField.setReferencedClass(referencedClass);
		                	if (referencedClass == null) {
		                		log.warn("dropping foreign key from column "+dbTableClass.getTableName()+"."+dbTableField.getColumnName()+" - referenced table '"+table+"' does not exist");
		                	}
		                	// FIXME: what to do if referencedClass is null?
	                	}
	                	
	                	if (referencedClass != null) {
	                		dbTableField.setIsForeignKey(true);
	                	}
		                
	                	// identify referenced column and field
	                	if (referencedClass != null) {
		                	Set<String> allColumnNames = new HashSet<String>();
		                	for (DBTableField field: dbTableClass.getFields()) {
		                		allColumnNames.add(field.getColumnName());
		                	}
		                	
		                	String column = fkRecog.getReferencedColumn(dbTableClass.getTableName(), dbTableField.getColumnName(), allColumnNames);
		                	dbTableField.setReferencedColumn(column);
		                	// null is ok here, causes default to primary key
		                	
		                	if (column != null) {
	                    		DBTableField referencedField = referencedClass.getFieldByColumnName(column);
	                    		dbTableField.setReferencedField(referencedField);
	                    		if (referencedField == null) {
	                    			log.warn("dropping foreign key target column from foreign key on "+dbTableClass.getTableName()+"."+dbTableField.getColumnName()+" - referenced column '"+column+"' does not exist");
	                    		}
	                    		// FIXME: what to do if referencedField is null?
		                	}
	                	}
	                }
	        	}
	        }
        }
        
    	// match up foreign keys to class and field where not already done
        // (still need to do this for those foreign keys that were found
        //  by foreign key constraint)
        // TODO: this is a confusing bit of code duplication, see if there's
        //       some way to reduce this code duplication.
        for (DBTableClass dbt : dbTableClasses) {
            for (DBTableField dbf : dbt.getFields()) {
                if (dbf.isForeignKey() && dbf.getReferencedTable() != null) {
                	DBTableClass referencedClass = classesByTableName.get(dbf.getReferencedTable());
                	dbf.setReferencedClass(referencedClass);
            		// FIXME: what to do if null now?
                	
                	if (referencedClass != null) {
                    	if (dbf.getReferencedColumn() != null) {
                    		DBTableField referencedField = referencedClass.getFieldByColumnName(dbf.getReferencedColumn());
                    		dbf.setReferencedField(referencedField);
                    		// FIXME: what to do if null now?
                    	}
                	}
                }
            }
        }
    }

    // populates all java sources
    static void generateAllJavaSource(List<DBTableClass> dbTableClasses) {
        for (DBTableClass dbt : dbTableClasses) {
            dbt.generateJavaSource();
            log.info(dbt.getJavaSource());
        }
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
        String classCaseString = "";
        if (s.matches("[lLtT]+_[0-9]+(_[0-9]+)*")) {
            classCaseString = s.toUpperCase();
        } else {
            String[] parts = s.split("_");
            for (String part : parts) {
                classCaseString = classCaseString + toProperCase(part);
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
