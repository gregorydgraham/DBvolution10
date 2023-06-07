/*
 * Copyright 2023 Gregory Graham.
 *
 * Commercial licenses are available, please contact info@gregs.co.nz for details.
 * 
 * This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. 
 * To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/4.0/ 
 * or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.
 * 
 * You are free to:
 *     Share - copy and redistribute the material in any medium or format
 *     Adapt - remix, transform, and build upon the material
 * 
 *     The licensor cannot revoke these freedoms as long as you follow the license terms.               
 *     Under the following terms:
 *                 
 *         Attribution - 
 *             You must give appropriate credit, provide a link to the license, and indicate if changes were made. 
 *             You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.
 *         NonCommercial - 
 *             You may not use the material for commercial purposes.
 *         ShareAlike - 
 *             If you remix, transform, or build upon the material, 
 *             you must distribute your contributions under the same license as the original.
 *         No additional restrictions - 
 *             You may not apply legal terms or technological measures that legally restrict others from doing anything the 
 *             license permits.
 * 
 * Check the Creative Commons website for any details, legalese, and updates.
 */
package nz.co.gregs.dbvolution.generation;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.databases.connections.DBConnection;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.exceptions.UnknownJavaSQLTypeException;
import nz.co.gregs.regexi.Regex;

/**
 *
 * @author gregorygraham
 */
public class DBDatabaseMetaData {

	private final DBDatabase database;
	private final String packageName;
	private final Options options;
//	private final List<TableMetaData> tablesWithinSchema = new ArrayList<>(0);
	private List<TableMetaData> finalList;
	private String catalog;
	private String schema;
	private boolean loaded = false;

	public DBDatabaseMetaData(Options options) {
		this.database = options.getDBDatabase();
		this.packageName = options.getPackageName();
		this.options = options.copy();
	}

	public void loadSchema() throws SQLException {
		ArrayList<TableMetaData> tablesFound = new ArrayList<>(0);
		DBDatabase db = this.database;

		// should be impossible but just in case
		if (db != null) {
			// we'll need these later...
			DBDefinition definition = db.getDefinition();
			Regex nonSystemTableRegex = definition.getSystemTableExclusionPattern();

			try (DBStatement dbStatement = db.getDBStatement()) {
				DBConnection connection = dbStatement.getConnection();
				catalog = connection.getCatalog();
				schema = null;
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
				try (ResultSet tables = metaData.getTables(catalog, schema, null, options.getObjectTypes())) {
					while (tables.next()) {
						final String tableName = tables.getString("TABLE_NAME");
						if (schema == null) {
							schema = tables.getString("TABLE_SCHEM");
						}
						boolean nonSystemTableCheck = nonSystemTableRegex.matchesEntireString(tableName);
						if (nonSystemTableCheck) {
							TableMetaData tableMetaData = new TableMetaData(schema, tableName);
							tablesFound.add(tableMetaData);

							List<TableMetaData.PrimaryKey> pkNames = new ArrayList<>();
							try (ResultSet primaryKeysRS = metaData.getPrimaryKeys(catalog, schema, tableName)) {
								while (primaryKeysRS.next()) {
									String pkColumnName = primaryKeysRS.getString("COLUMN_NAME");
									TableMetaData.PrimaryKey primaryKey = new TableMetaData.PrimaryKey(schema, tableName, pkColumnName);
									pkNames.add(primaryKey);
									tableMetaData.addPrimaryKey(primaryKey);
								}
							}

							Map<String, TableMetaData.ForeignKey> fkNames = new HashMap<>();
							try (ResultSet foreignKeysRS = metaData.getImportedKeys(catalog, schema, tableName)) {
								while (foreignKeysRS.next()) {
									String pkTableName = foreignKeysRS.getString("PKTABLE_NAME");
									String pkColumnName = foreignKeysRS.getString("PKCOLUMN_NAME");
									String fkColumnName = foreignKeysRS.getString("FKCOLUMN_NAME");
									fkNames.put(fkColumnName, new TableMetaData.ForeignKey(tableName, fkColumnName, pkTableName, pkColumnName));
								}
							}

							try (ResultSet columns = metaData.getColumns(catalog, schema, tableName, null)) {
								while (columns.next()) {
									TableMetaData.Column column = new TableMetaData.Column(schema, tableName);
									tableMetaData.addColumn(column);
									column.setColumnName(columns.getString("COLUMN_NAME"));
									try {
										column.setReferencedTable(columns.getString("SCOPE_TABLE"));
									} catch (SQLException exp) {
										; // MSSQLServer throws an exception on this
									}
									column.precision = columns.getInt("COLUMN_SIZE");
									column.comments = columns.getString("REMARKS");
									String isAutoIncr = null;
									try {
										isAutoIncr = columns.getString("IS_AUTOINCREMENT");
									} catch (SQLException sqlex) {
										;// SQLite-JDBC throws an exception when retrieving IS_AUTOINCREMENT
									}
									column.isAutoIncrement = isAutoIncr != null && isAutoIncr.equals("YES");
									try {
										column.sqlDataTypeInt = columns.getInt("DATA_TYPE");
										column.sqlDataTypeName = columns.getString("TYPE_NAME");
										column.columnType = Utility.getQDTClassOfSQLType(db, column.sqlDataTypeName, column.sqlDataTypeInt, column.precision, options.getTrimCharColumns());
									} catch (UnknownJavaSQLTypeException ex) {
										column.columnType = DBUnknownDatatype.class;
										column.javaSQLDatatype = ex.getUnknownJavaSQLType();
									}
									Optional<TableMetaData.PrimaryKey> pkNameFound = pkNames.stream().filter((c) -> c.getName().equals(column.columnName)).findFirst();
									boolean recogFound = options.getPkRecog().isPrimaryKeyColumn(tableName, column.columnName);
									if (pkNameFound.isPresent() || recogFound) {
										column.isPrimaryKey = true;
										TableMetaData.PrimaryKey primaryKey = pkNameFound.orElse(new TableMetaData.PrimaryKey(schema, tableName, column.columnName));
										tableMetaData.addPrimaryKey(primaryKey);
										pkNames.add(primaryKey);
									}

									TableMetaData.ForeignKey fkData = fkNames.get(column.columnName);
									if (fkData != null) {
										column.isForeignKey = true;
										column.referencesTableName = fkData.primaryKeyTableName;
										column.referencesClass = Utility.toClassCase(fkData.primaryKeyTableName);
										column.referencesField = fkData.primaryKeyColumnName;
										tableMetaData.addForeignKey(fkData);
									} else {
										if (options.getFkRecog().isForeignKeyColumn(tableName, column.columnName)) {
											column.isForeignKey = true;
											column.referencesField = options.getFkRecog().getReferencedColumn(tableName, column.columnName);
											column.referencesClass = Utility.toClassCase(options.getFkRecog().getReferencedTable(tableName, column.columnName));
											TableMetaData.ForeignKey fk = new TableMetaData.ForeignKey(tableName, column.columnName, column.referencesTableName, column.referencesField);
											tableMetaData.addForeignKey(fk);
										}
									}
								}
							}
						}
					}
				}
			}
		}
		finalList = List.copyOf(tablesFound);
	}

//	public DataRepo getDataRepo() throws SQLException, NoAvailableDatabaseException {
//		DataRepo datarepo = new DataRepo(database, packageName);
//
//		DBDatabase db = this.database;
//		while (db instanceof DBDatabaseCluster) {
//			//There's an implicit assumption here that an actual RDBMS is in the cluster
//			db = ((DBDatabaseCluster) db).getReadyDatabase();
//		}
//
//		if (db != null) {
//			// we'll need these later...
//			DBDefinition definition = db.getDefinition();
//			Regex nonSystemTableRegex = definition.getSystemTableExclusionPattern();
//			
//			try (DBStatement dbStatement = db.getDBStatement()) {
//				connection = dbStatement.getConnection();
//				String catalog = connection.getCatalog();
//				String schema = null;
//				try {
//					//Method method = connection.getClass().getMethod("getSchema");
//					//schema = (String) method.invoke(connection);
//					schema = connection.getSchema();
//				} catch (java.sql.SQLFeatureNotSupportedException nope) {
//					// SOMEONE DIDN'T WRITE THEIR DRIVER PROPERLY
//				} catch (java.lang.AbstractMethodError exp) {
//					// NOT USING Java 1.7+ apparently
//				} catch (IllegalArgumentException ex) {
//					// NOT USING Java 1.7+ apparently
//				} catch (SecurityException ex) {
//					// NOT USING Java 1.7+ apparently
//				}
//
//				DatabaseMetaData metaData = connection.getMetaData();
//				try (ResultSet tables = metaData.getTables(catalog, schema, null, objectTypes)) {
//					while (tables.next()) {
//						final String tableName = tables.getString("TABLE_NAME");
//						if (schema == null) {
//							schema = tables.getString("TABLE_SCHEM");
//						}
//						if (nonSystemTableRegex.matchesEntireString(tableName)) {
//							final String className = toClassCase(tableName);
//							DBTableClass dbTableClass = new DBTableClass(tableName, schema, packageName, className);
//
//							ResultSet primaryKeysRS = metaData.getPrimaryKeys(catalog, schema, dbTableClass.getTableName());
//							List<String> pkNames = new ArrayList<>();
//							try {
//								while (primaryKeysRS.next()) {
//									String pkColumnName = primaryKeysRS.getString("COLUMN_NAME");
//									pkNames.add(pkColumnName);
//								}
//							} finally {
//								primaryKeysRS.close();
//							}
//
//							ResultSet foreignKeysRS = metaData.getImportedKeys(catalog, schema, dbTableClass.getTableName());
//							Map<String, String[]> fkNames = new HashMap<>();
//							try {
//								while (foreignKeysRS.next()) {
//									String pkTableName = foreignKeysRS.getString("PKTABLE_NAME");
//									String pkColumnName = foreignKeysRS.getString("PKCOLUMN_NAME");
//									String fkColumnName = foreignKeysRS.getString("FKCOLUMN_NAME");
//									fkNames.put(fkColumnName, new String[]{pkTableName, pkColumnName});
//								}
//							} finally {
//								foreignKeysRS.close();
//							}
//
//							try (ResultSet columns = metaData.getColumns(catalog, schema, dbTableClass.getTableName(), null)) {
//								while (columns.next()) {
//									DBTableField dbTableField = new DBTableField();
//									dbTableField.columnName = columns.getString("COLUMN_NAME");
//									dbTableField.fieldName = toFieldCase(dbTableField.columnName);
//									try {
//										dbTableField.referencedTable = columns.getString("SCOPE_TABLE");
//									} catch (SQLException exp) {
//										; // MSSQLServer throws an exception on this
//									}
//									dbTableField.precision = columns.getInt("COLUMN_SIZE");
//									dbTableField.comments = columns.getString("REMARKS");
//									String isAutoIncr = null;
//									try {
//										isAutoIncr = columns.getString("IS_AUTOINCREMENT");
//									} catch (SQLException sqlex) {
//										;// SQLite-JDBC throws an exception when retrieving IS_AUTOINCREMENT
//									}
//									dbTableField.isAutoIncrement = isAutoIncr != null && isAutoIncr.equals("YES");
//									try {
//										dbTableField.sqlDataTypeInt = columns.getInt("DATA_TYPE");
//										dbTableField.sqlDataTypeName = columns.getString("TYPE_NAME");
//										dbTableField.columnType = getQDTClassOfSQLType(db, dbTableField, options.getTrimCharColumns());
//									} catch (UnknownJavaSQLTypeException ex) {
//										dbTableField.columnType = DBUnknownDatatype.class;
//										dbTableField.javaSQLDatatype = ex.getUnknownJavaSQLType();
//									}
//									if (pkNames.contains(dbTableField.columnName) || options.getPkRecog().isPrimaryKeyColumn(dbTableClass.getTableName(), dbTableField.columnName)) {
//										dbTableField.isPrimaryKey = true;
//									}
//
//									String[] pkData = fkNames.get(dbTableField.columnName);
//									if (pkData != null && pkData.length == 2) {
//										dbTableField.isForeignKey = true;
//										dbTableField.referencesClass = toClassCase(pkData[0]);
//										dbTableField.referencesField = pkData[1];
//									} else if (options.getFkRecog().isForeignKeyColumn(dbTableClass.getTableName(), dbTableField.columnName)) {
//										dbTableField.isForeignKey = true;
//										dbTableField.referencesField = options.getFkRecog().getReferencedColumn(dbTableClass.getTableName(), dbTableField.columnName);
//										dbTableField.referencesClass = toClassCase(options.getFkRecog().getReferencedTable(dbTableClass.getTableName(), dbTableField.columnName));
//									}
//									if (!dbTableClass.getFields().contains(dbTableField)) {
//										dbTableClass.getFields().add(dbTableField);
//									}
//								}
//							}
//							if (!db.supportsGeometryTypesFullyInSchema()) {
//								dbTableClass = getGeometryTypesFromSchemaTables(db, dbStatement, connection, dbTableClass);
//							}
//							datarepo.addTable(dbTableClass);
//						}
//					}
//				}
//				connectForeignKeyReferences(datarepo.getTables());
//			}
//		}
//		return datarepo;
//	}
	String getCatalog() {
		return catalog;
	}

	String getSchema() {
		return schema;
	}

	List<TableMetaData> getTables(String catalog, String schema) {
		return finalList;
	}

	/**
	 * @return the database
	 */
	public DBDatabase getDatabase() {
		return database;
	}

	/**
	 * @return the packageName
	 */
	public String getPackageName() {
		return packageName;
	}

	/**
	 * @return the options
	 */
	public Options getOptions() {
		return options;
	}
}
