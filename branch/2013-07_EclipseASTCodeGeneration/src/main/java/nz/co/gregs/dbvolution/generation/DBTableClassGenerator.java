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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
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
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBByteArray;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBDateOnly;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBObject;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.generation.ast.ParsedBeanProperty;
import nz.co.gregs.dbvolution.generation.ast.ParsedClass;
import nz.co.gregs.dbvolution.generation.ast.ParsedTypeRef;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Top level code generator.
 * Generates code based on database schema.
 */
public class DBTableClassGenerator {
	private static final Logger log = LoggerFactory.getLogger(DBTableClassGenerator.class);
	private static final Class<?>[] KNOWN_FIELD_TYPES = new Class<?>[] {
			DBBoolean.class, DBByteArray.class, DBDate.class, DBDateOnly.class,
			DBInteger.class, DBLargeObject.class, DBNumber.class,
			DBObject.class, DBString.class
		};
	
	private File sourceRoot;
	private String packageForTables;
	private String packageForViews;
	private PrimaryKeyRecognisor pkRecogniser = new PrimaryKeyRecognisor();
	private ForeignKeyRecognisor fkRecogniser = new ForeignKeyRecognisor();
	private CodeGenerationConfiguration config = new CodeGenerationConfiguration();

	public DBTableClassGenerator(File sourceRoot, String packageName) {
		this.sourceRoot = sourceRoot;
        this.packageForTables = packageOf(packageName, "tables");
        this.packageForViews = packageOf(packageName, "views");
	}

	public DBTableClassGenerator(File sourceRoot, String packageForTables, String packageForViews) {
		this.sourceRoot = sourceRoot;
        this.packageForTables = packageForTables;
        this.packageForViews = packageForViews;
	}
	
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
    public void generateClassesFromJDBCURLToDirectory(DBDatabase database) throws SQLException, FileNotFoundException, IOException {
    	if (sourceRoot == null) {
    		throw new NullPointerException("sourceRoot must be specified");
    	}
    	if (!sourceRoot.exists()) {
    		throw new IllegalArgumentException(sourceRoot.toString()+" does not exist");
    	}
    	if (!sourceRoot.isDirectory()) {
    		throw new IllegalArgumentException(sourceRoot.toString()+" is not a directory");
    	}
    	
    	// get existing classes (top-level info only)
    	List<DBTableClass> existingClasses = identifyExistingClasses("TABLE", "VIEW");
    	
    	// generate new and update existing classes
    	// (save straight to disk)
    	List<DBTableClass> generatedTables = generateClassesOfObjectTypes(database, packageForTables, existingClasses, true, "TABLE");
        List<DBTableClass> generatedViews = generateClassesOfObjectTypes(database, packageForViews, existingClasses, true, "VIEW");
        saveGeneratedClassesToDirectory(generatedTables);
        saveGeneratedClassesToDirectory(generatedViews);
    }
    
    /**
     *
     * @param database
     * @param packageName
     * @return
     * @throws SQLException
     */
    public List<DBTableClass> generateClassesOfTables(DBDatabase database) throws SQLException {
        return generateClassesOfObjectTypes(database, packageForTables, null, false, "TABLE");
    }

    /**
     *
     * @param database
     * @param packageName
     * @return
     * @throws SQLException
     */
    public List<DBTableClass> generateClassesOfViews(DBDatabase database) throws SQLException {
        return generateClassesOfObjectTypes(database, packageForViews, null, false, "VIEW");
    }

    /**
     * Generates classes based on the database schema and optionally
     * updates existing classes.
     * If {@code writeFiles} is set it will write the output to the filesystem,
     * otherwise it will write the output to the {@code javaSource} property
     * of the {@link DBTableClass} instances returned.
     * @param database
     * @param packageName
     * @param existingClasses null if not doing updates
     * @param writeFiles writes to files if {@code true}, writes to {@code javaSource} property otherwise
     * @param dbObjectTypes
     * @return
     * @throws SQLException
     */
    private List<DBTableClass> generateClassesOfObjectTypes(DBDatabase database, String packageName,
    		List<DBTableClass> existingClasses, boolean writeFiles, String... dbObjectTypes) throws SQLException {
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
            ColumnNameResolver columnNameResolver = new ColumnNameResolver();

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
                dbTableField.setFieldName(columnNameResolver.getPropertyNameFor(dbTableField.getColumnName()));
                dbTableField.setPrecision(columns.getInt("COLUMN_SIZE"));
                dbTableField.setColumnType(database.getDefinition().getQueryableDatatypeOfSQLType(columns.getInt("DATA_TYPE"), dbTableField.getPrecision()));
                if (pkNames.contains(dbTableField.getColumnName()) || pkRecogniser.isPrimaryKeyColumn(dbTableClass.getTableName(), dbTableField.getColumnName())) {
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
        
        populateForeignKeys(dbTableClasses, fkRecogniser);
        generateAllJavaSource(tableNameResolver, dbTableClasses);
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
        for (DBTableClass clazz: dbTableClasses) {
	        for (DBTableField field: clazz.getFields()) {
	        	if (!field.isForeignKey()) {
	                if (fkRecog.isForeignKeyColumn(clazz.getTableName(), field.getColumnName())) {
	                    //dbTableField.setIsForeignKey(true);
	                    
	                    // identify referenced table and class
	                	String tableName = fkRecog.getReferencedTable(clazz.getTableName(), field.getColumnName(), allTableNames);
	                	field.setReferencedTable(tableName);
	                	if (tableName == null) {
	                		log.warn("dropping foreign key from column "+clazz.getTableName()+"."+field.getColumnName()+
	                				" - referenced table could not be inferred from column name");
	                	}
	                	// FIXME: what to do if table is null?
	                	
	                	DBTableClass referencedClass = null;
	                	if (tableName != null) {
		                	referencedClass = classesByTableName.get(tableName);
		                	field.setReferencedClass(referencedClass);
		                	if (referencedClass == null) {
		                		log.warn("dropping foreign key from column "+clazz.getTableName()+"."+field.getColumnName()+
		                				" - referenced table '"+tableName+"' does not exist");
		                	}
		                	// FIXME: what to do if referencedClass is null?
	                	}
	                	
	                	if (referencedClass != null) {
	                		field.setIsForeignKey(true);
	                	}
		                
	                	// identify referenced column and field
	                	if (referencedClass != null) {
		                	Set<String> allFKTableColumnNames = new HashSet<String>();
		                	for (DBTableField referencedClassField: referencedClass.getFields()) {
		                		allFKTableColumnNames.add(referencedClassField.getColumnName());
		                	}
		                	
		                	String columnName = fkRecog.getReferencedColumn(clazz.getTableName(), field.getColumnName(), allFKTableColumnNames);
		                	field.setReferencedColumn(columnName);
		                	// null is ok here, causes default to primary key
		                	
		                	if (columnName != null) {
	                    		DBTableField referencedField = referencedClass.getFieldByColumnName(columnName);
	                    		field.setReferencedField(referencedField);
	                    		if (referencedField == null) {
	                    			log.warn("dropping foreign key target column from foreign key on "+clazz.getTableName()+"."+
	                    					field.getColumnName()+" - referenced column '"+columnName+"' does not exist");
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
        for (DBTableClass clazz : dbTableClasses) {
            for (DBTableField field : clazz.getFields()) {
                if (field.isForeignKey() && field.getReferencedTable() != null) {
                	DBTableClass referencedClass = classesByTableName.get(field.getReferencedTable());
                	field.setReferencedClass(referencedClass);
                	if (referencedClass == null) {
                		log.warn("dropping foreign key from column "+clazz.getTableName()+"."+field.getColumnName()+
                				" - referenced table '"+field.getReferencedTable()+"' does not exist");
                	}
            		// FIXME: what to do if null now?
                	
                	if (referencedClass != null) {
                    	if (field.getReferencedColumn() != null) {
                    		DBTableField referencedField = referencedClass.getFieldByColumnName(field.getReferencedColumn());
                    		field.setReferencedField(referencedField);
                    		if (referencedField == null) {
                    			log.warn("dropping foreign key target column from foreign key on "+clazz.getTableName()+"."+
                    					field.getColumnName()+" - referenced column '"+field.getReferencedColumn()+"' does not exist");
                    		}
                    		// FIXME: what to do if null now?
                    	}
                	}
                }
            }
        }
    }

    // populates all java sources
    void generateAllJavaSource(TableNameResolver tableNameResolver, List<DBTableClass> dbTableClasses) {
        for (DBTableClass dbt : dbTableClasses) {
            //dbt.generateJavaSource();
        	ClassMaintainer classMaintainer = new ClassMaintainer(config, tableNameResolver);
        	classMaintainer.ensureAs(dbt);
        	String contents = classMaintainer.writeToString();
        	dbt.setJavaSource(contents);
        	
            log.info(dbt.getJavaSource());
        }
    }

    /**
     * Saves the supplied DBTableRow classes as java files in the configured
     * output directory.
     *
     * No database interaction nor package name checking is performed.
     *
     * You probably want to use generateClassesFromJDBCURLToDirectory
     *
     * @param generatedClasses
     * @throws FileNotFoundException
     * @throws IOException
     */
    void saveGeneratedClassesToDirectory(List<DBTableClass> generatedClasses) throws FileNotFoundException, IOException {
        for (DBTableClass clazz : generatedClasses) {
        	String packagePath = clazz.getPackageName().replace(".", "/");
        	File folder = new File(sourceRoot,  packagePath);
        	if (!folder.exists() && !folder.mkdirs()) {
        		throw new IOException("Unable to create package path "+folder);
        	}
        	
        	File file = new File(folder, clazz.getClassName() + ".java");
        	BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        	try {
        		writer.write(clazz.getJavaSource());
        	} finally {
        		try {
        			writer.close();
        		} catch (IOException dropped) {
        			// assume caused by earlier exception
        		}
        	}
            log.info("Wrote "+clazz.getClassName() + " => " + file.getAbsolutePath());
        }
    }
    
    /**
     * Scans the filesystem on the configured paths for existing table and view classes.
     * The contents of the classes are not parsed so their full content must
     * be separately parsed in a later step.
     * 
     * <p> Where tables and views are in the same package, all classes
     * will be returned.
     * @param dbObjectTypes containing one or more of {@code "TABLES"} and {@code "VIEWS"}.
     * @return list of identified classes for tables and/or views
     * @throws IOException on error reading or parsing existing source files
     */
    private List<DBTableClass> identifyExistingClasses(String... dbObjectTypes) throws IOException  {
        List<DBTableClass> tableClasses = new ArrayList<DBTableClass>();
        
        Set<String> packagesDone = new HashSet<String>();
        for (String dbObjectType: dbObjectTypes) {
        	String packageName;
        	if (dbObjectType.equals("TABLE")) {
        		packageName = packageForTables;
        	}
        	else if (dbObjectType.equals("VIEW")) {
        		packageName = packageForViews;
        	}
        	else {
        		throw new IllegalArgumentException("Unknown dbObjectType: "+dbObjectType);
        	}
        	
        	if (packageName != null && !packagesDone.contains(packageName)) {
        		packagesDone.add(packageName);
        		
        		// list all existing source files
        		File dir = new File(sourceRoot, packageName.replace(".", "/"));
                File[] files = dir.listFiles(new FileFilter() {
        			public boolean accept(File pathname) {
        				return pathname.isFile() && pathname.getName().endsWith(".java");
        			}
                });
                
                // partially parse each existing source file
                if (files != null) {
	                for (File file: files) {
	                	DBTableClass tableClass;
						try {
							tableClass = parseFileMinimallyForReadOnly(file);
		                	if (tableClass != null) {
		                		tableClasses.add(tableClass);
		                	}
						} catch (IOException e) {
							throw new IOException("Error reading file "+file+": "+e.getMessage(), e);
						}
	                }
                }
        	}
        }
        
        return tableClasses;
    }
    
    /**
     * Parses the given java source file to the top-level class
     * and annotations only.
     * Assumes that the parsed file is not going to be immediately
     * updated and thus does not populate information required for applying
     * updates.
     * 
     * <p> If the class does not have any <code>@DBTableName</code> annotations,
     * then it will still be returned but will have no column name.
     * @param file
     * @return the parsed information
     * @throws IOException 
     */
    private DBTableClass parseFileMinimallyForReadOnly(File file) throws IOException {
    	ParsedClass parsedClass = ParsedClass.parseFileMinimally(file);
    	
    	DBTableClass dbTableClass = new DBTableClass();
    	dbTableClass.setJavaFile(file);
    	dbTableClass.setPackageName(parsedClass.getPackage());
    	dbTableClass.setClassName(parsedClass.getDeclaredName());
    	dbTableClass.setTableName(parsedClass.getTableNameIfSet());
    	
    	return dbTableClass;
    }

    /**
     * Fully parses the given java source file and populates
     * the <code>ParsedClass</code> object into the returned
     * instance.
     * 
     * <p> If the class does not have any <code>@DBTableName</code> annotations,
     * then it will still be returned but will have no column name.
     * @param file
     * @return the parsed information
     * @throws IOException 
     */
    private DBTableClass parseFileForUpdate(File file) throws IOException {
    	ParsedClass parsedClass = ParsedClass.parseFile(file);
    	
    	DBTableClass dbTableClass = new DBTableClass();
    	dbTableClass.setParsedClass(parsedClass); // for applying updates
    	dbTableClass.setJavaFile(file); // for saving updates to same file
    	dbTableClass.setPackageName(parsedClass.getPackage());
    	dbTableClass.setClassName(parsedClass.getDeclaredName());
    	dbTableClass.setTableName(parsedClass.getTableNameIfSet());
    	
    	for (ParsedBeanProperty parsedProperty: parsedClass.getDBColumnProperties()) {
    		DBTableField dbTableField = new DBTableField();
    		dbTableField.setFieldName(parsedProperty.getName());
    		dbTableField.setColumnName(parsedProperty.getColumnNameIfSet());
    		dbTableField.setIsPrimaryKey(parsedProperty.isDBPrimaryKey());
    		
    		// foreign key
    		// note: referencedClassName will either match a known class,
    		//       in which case it should be linked to that class;
    		//       or it will not be recognised,
    		//       in which case we don't need to worry about it.
    		if (parsedProperty.isDBForeignKey()) {
    			ParsedTypeRef referencedType = parsedProperty.getForeignTypeIfSet();
	    		dbTableField.setIsForeignKey(true);
	    		dbTableField.setReferencedClassName(referencedType.getQualifiedTypeName());
	    		dbTableField.setReferencedColumn(parsedProperty.getForeignColumnNameIfSet());
    		}
    		
    		// column type
    		for (Class<?> fieldType: KNOWN_FIELD_TYPES) {
    			if (parsedProperty.getType().isJavaType(fieldType)) {
    				dbTableField.setColumnType((Class<? extends QueryableDatatype>) fieldType);
    				break;
    			}
    		}
    	}
    	
    	return dbTableClass;
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
        
    /**
	 * @return the sourceRoot
	 */
	public File getSourceRoot() {
		return sourceRoot;
	}

	/**
	 * @param sourceRoot the sourceRoot to set
	 */
	public void setSourceRoot(File sourceRoot) {
		this.sourceRoot = sourceRoot;
	}

	/**
	 * @return the packageForTables
	 */
	public String getPackageForTables() {
		return packageForTables;
	}

	/**
	 * @param packageForTables the packageForTables to set
	 */
	public void setPackageForTables(String packageForTables) {
		this.packageForTables = packageForTables;
	}

	/**
	 * @return the packageForViews
	 */
	public String getPackageForViews() {
		return packageForViews;
	}

	/**
	 * @param packageForViews the packageForViews to set
	 */
	public void setPackageForViews(String packageForViews) {
		this.packageForViews = packageForViews;
	}

	/**
	 * @return the pkRecogniser
	 */
	public PrimaryKeyRecognisor getPrimaryKeyRecogniser() {
		return pkRecogniser;
	}

	/**
	 * @param pkRecogniser the pkRecogniser to set
	 */
	public void setPrimaryKeyRecogniser(PrimaryKeyRecognisor pkRecogniser) {
		this.pkRecogniser = pkRecogniser;
	}

	/**
	 * @return the fkRecogniser
	 */
	public ForeignKeyRecognisor getForeignKeyRecogniser() {
		return fkRecogniser;
	}

	/**
	 * @param fkRecogniser the fkRecogniser to set
	 */
	public void setForeignKeyRecogniser(ForeignKeyRecognisor fkRecogniser) {
		this.fkRecogniser = fkRecogniser;
	}

	/**
	 * @return the config
	 */
	public CodeGenerationConfiguration getConfig() {
		return config;
	}

	/**
	 * @param config the config to set
	 */
	public void setConfig(CodeGenerationConfiguration config) {
		this.config = config;
	}

	private static boolean isBlank(String str) {
    	return (str == null) || (str.trim().length() == 0);
    }
    
    /**
     * Concatenates two packages, either of which may be null
     * @param basePackage
     * @param subPackage
     * @return
     */
    private static String packageOf(String basePackage, String subPackage) {
    	if (isBlank(basePackage)) {
    		return subPackage;
    	}
    	else if (isBlank(subPackage)) {
    		return basePackage;
    	}
    	else {
    		return basePackage+"."+subPackage;
    	}
    }
}
