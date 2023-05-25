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
import java.sql.Types;
import java.util.*;
import nz.co.gregs.dbvolution.databases.DBConnection;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.datatypes.spatial2D.*;
import nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException;
import nz.co.gregs.dbvolution.exceptions.UnknownJavaSQLTypeException;
import static nz.co.gregs.dbvolution.generation.DataRepoGenerator.toClassCase;

/**
 *
 * @author gregorygraham
 */
public class DBDatabaseMetaData {

	private final DBDatabase database;
	private final String[] objectTypes;
	private final String packageName;
	private final Options options;

	public DBDatabaseMetaData(DBDatabase database, String[] dbObjectTypes, String packageName, Options options) {
		this.database = database;
		this.objectTypes = dbObjectTypes;
		this.packageName = packageName;
		this.options = options;
	}

	public DataRepo getDataRepo() throws SQLException, NoAvailableDatabaseException {
		DataRepo datarepo = new DataRepo(database, packageName);
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
			try (ResultSet tables = metaData.getTables(catalog, schema, null, objectTypes)) {
				while (tables.next()) {
					final String tableName = tables.getString("TABLE_NAME");
					if (schema == null) {
						schema = tables.getString("TABLE_SCHEM");
					}
					if (tableName.matches(database.getDefinition().getSystemTableExclusionPattern())) {
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
								DBTableField dbTableField = new DBTableField();
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
									dbTableField.columnType = getQueryableDatatypeNameOfSQLType(database, dbTableField, options.getTrimCharColumns());
								} catch (UnknownJavaSQLTypeException ex) {
									dbTableField.columnType = DBUnknownDatatype.class;
									dbTableField.javaSQLDatatype = ex.getUnknownJavaSQLType();
								}
								if (pkNames.contains(dbTableField.columnName) || options.getPkRecog().isPrimaryKeyColumn(dbTableClass.getTableName(), dbTableField.columnName)) {
									dbTableField.isPrimaryKey = true;
								}

								String[] pkData = fkNames.get(dbTableField.columnName);
								if (pkData != null && pkData.length == 2) {
									dbTableField.isForeignKey = true;
									dbTableField.referencesClass = toClassCase(pkData[0]);
									dbTableField.referencesField = pkData[1];
								} else if (options.getFkRecog().isForeignKeyColumn(dbTableClass.getTableName(), dbTableField.columnName)) {
									dbTableField.isForeignKey = true;
									dbTableField.referencesField = options.getFkRecog().getReferencedColumn(dbTableClass.getTableName(), dbTableField.columnName);
									dbTableField.referencesClass = toClassCase(options.getFkRecog().getReferencedTable(dbTableClass.getTableName(), dbTableField.columnName));
								}
								if (!dbTableClass.getFields().contains(dbTableField)) {
									dbTableClass.getFields().add(dbTableField);
								}
							}
						}
						if (!database.supportsGeometryTypesFullyInSchema()) {
							dbTableClass = getGeometryTypesFromSchemaTables(database, dbStatement, connection,dbTableClass);
						}
						datarepo.addTable(dbTableClass);
					}
				}
			}
			connectForeignKeyReferences(datarepo.getTables());
		}
		return datarepo;
	}

	private static void connectForeignKeyReferences(List<DBTableClass> dbTableClasses) {
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
		}
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
	private static final String[] JAVA_RESERVED_WORDS_ARRAY = new String[]{"null", "true", "false", "abstract", "continue", "for", "new", "switch", "assert", "default", "goto", "package", "synchronized", "boolean", "do", "if", "private", "this", "break", "double", "implements", "", "protected", "throw", "byte", "else", "import", "public", "throws", "case", "enum", "instanceof", "return", "transient", "catch", "extends", "int", "short", "try", "char", "final", "interface", "static", "", "void", "class", "finally", "long", "strictfp", "volatile", "const", "float", "native", "super", "while"};
	private static final List<String> JAVA_RESERVED_WORDS = Arrays.asList(JAVA_RESERVED_WORDS_ARRAY);

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
			case Types.BINARY:
			case Types.VARBINARY:
			case Types.LONGVARBINARY:
			case Types.BLOB:
			case Types.ARRAY:
			case Types.SQLXML:
				Class<? extends QueryableDatatype<?>> customBinaryType = database.getDefinition().getQueryableDatatypeClassForSQLDatatype(typeName);
				if (customBinaryType != null) {
					value = customBinaryType;
					break;
				} else {
					value = DBLargeBinary.class;
					break;
				}
			default:
				throw new UnknownJavaSQLTypeException("Unknown Java SQL Type: " + columnType, columnType);
		}
		return value;
	}

	private DBTableClass getGeometryTypesFromSchemaTables(DBDatabase database, DBStatement dbStatement, DBConnection connection, DBTableClass inwardTable) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

}
