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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.DBUnknownDatatype;
import nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException;
import nz.co.gregs.dbvolution.exceptions.UnknownJavaSQLTypeException;

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
		Options opts = Options.empty()
				.setVersionNumber(versionNumber)
				.setDBDatabase(database)
				.setPackageName(packageName);
		return generateClasses(opts);
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
		options.setDBDatabase(database);
		options.setPackageName(packageName);
		return generateClasses(options);
	}

	static private DataRepo generateClasses(Options options) throws SQLException, FileNotFoundException, IOException {
		DBDatabase database = options.getDBDatabase();
		String packageName = options.getPackageName();

		DataRepo repo = new DataRepo(options);
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
		Options opts = options.copy();
		opts.setDBDatabase(database);
		opts.setPackageName(packageName);
		opts.setObjectTypes("TABLE");
		return parseObjectTypes(opts);
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
	private static DataRepo parseViews(DBDatabase database, String packageName, Options originalOptions) throws SQLException {
		Options options = originalOptions.copy();
		options.setDBDatabase(database);
		options.setPackageName(packageName);
		options.setObjectTypes("VIEW");
		final DataRepo dataRepo = new DataRepo(options);
		dataRepo.addViews(parseObjectTypes(options).getTables());
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
	private static DataRepo parseObjectTypes(Options options) throws SQLException {
//		Options opts = Options
//				.copy(options)
//				.setDBDatabase(db)
//				.setPackageName(packageName)
//				.setObjectTypes(dbObjectTypes);
		DataRepo datarepo = getDataRepo(options);

		return datarepo;
	}

	public static DataRepo getDataRepo(Options opts) throws SQLException, NoAvailableDatabaseException {
		DataRepo datarepo = new DataRepo(opts);

		DBDatabase db = opts.getDBDatabase();

		if (db != null) {

			DBDatabaseMetaData metaData = db.getDBDatabaseMetaData(opts);
			metaData.loadSchema();
			String catalog = metaData.getCatalog();
			String schema = metaData.getSchema();

			List<TableMetaData> tables = metaData.getTables(catalog, schema);
			for (TableMetaData table : tables) {
				final String tableName = table.getTableName();
				if (schema == null) {
					schema = table.getSchema();
				}
				final String className = Utility.toClassCase(tableName);
				DBTableClass dbTableClass = new DBTableClass(tableName, schema, opts.getPackageName(), className);
				datarepo.addTable(dbTableClass);

				List<TableMetaData.PrimaryKey> primaryKeys = table.getPrimaryKeys();
				List<String> pkNames = new ArrayList<String>();
				for (TableMetaData.PrimaryKey primaryKey : primaryKeys) {
					pkNames.add(primaryKey.getName());
				}
				String classTableName = dbTableClass.getTableName();

				List<TableMetaData.ForeignKey> foreignKeys = table.getForeignKeys(catalog, schema, classTableName);
				Map<String, TableMetaData.ForeignKey> fkNames = new HashMap<>();
				foreignKeys.stream()
						.forEach((fk) -> {
							fkNames.put(fk.getName(), fk);
						});

				List<TableMetaData.Column> columns = table.getColumns();
				for (TableMetaData.Column col : columns) {
					DBTableField dbTableField = new DBTableField();
					dbTableClass.getFields().add(dbTableField);
					dbTableField.columnName = col.getColumnName();
					dbTableField.fieldName = Utility.toFieldCase(dbTableField.columnName);
					dbTableField.referencedTable = col.getReferencedTable();
					dbTableField.precision = col.getColumnSize();
					dbTableField.comments = col.getRemarks();
					dbTableField.isAutoIncrement = col.getIsAutoIncrement();
					try {
						dbTableField.sqlDataTypeInt = col.getDatatype();
						dbTableField.sqlDataTypeName = col.getTypeName();
						dbTableField.columnType = Utility.getQDTClassOfSQLType(db, dbTableField.sqlDataTypeName, dbTableField.sqlDataTypeInt, dbTableField.precision, opts.getTrimCharColumns());
					} catch (UnknownJavaSQLTypeException ex) {
						dbTableField.columnType = DBUnknownDatatype.class;
						dbTableField.javaSQLDatatype = ex.getUnknownJavaSQLType();
					}
					if (pkNames.contains(dbTableField.columnName) 
							|| (opts.getPkRecog()!=null &&opts.getPkRecog().isPrimaryKeyColumn(classTableName, dbTableField.columnName))
							) {
						dbTableField.isPrimaryKey = true;
					}

					TableMetaData.ForeignKey fk = fkNames.get(dbTableField.columnName);
					ForeignKeyRecognisor fkRecog = opts.getFkRecog();
					if (fk != null) {
						dbTableField.isForeignKey = true;
						dbTableField.referencesClass = Utility.toClassCase(fk.getPrimaryKeyTableName());
						dbTableField.referencesField = fk.getPrimaryKeyColumnName();
					} else if (fkRecog != null && fkRecog.isForeignKeyColumn(classTableName, dbTableField.columnName)) {
						dbTableField.isForeignKey = true;
						dbTableField.referencesField = fkRecog.getReferencedColumn(classTableName, dbTableField.columnName);
						String fkTable = fkRecog.getReferencedTable(classTableName, dbTableField.columnName);
						dbTableField.referencesClass = Utility.toClassCase(fkTable);
					}
				}
			}
		}
		return datarepo;
	}

}
