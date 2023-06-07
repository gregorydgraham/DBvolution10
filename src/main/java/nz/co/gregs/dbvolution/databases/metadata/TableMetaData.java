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
package nz.co.gregs.dbvolution.databases.metadata;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author gregorygraham
 */
public class TableMetaData {

	private final String tableName;
	private final String schema;
	private final List<Column> columns = new ArrayList<>(0);
	private final List<ForeignKey> foreignKeys = new ArrayList<>(0);
	private final List<PrimaryKey> primaryKeys = new ArrayList<>(0);

	public TableMetaData(String schema, String tableName) {
		this.schema = schema;
		this.tableName = tableName;
	}

	public String getTableName() {
		return tableName;
	}

	public String getSchema() {
		return schema;
	}

	public List<PrimaryKey> getPrimaryKeys() {
		return primaryKeys;
	}

	public List<ForeignKey> getForeignKeys(String catalog, String schema, String tableName) {
		return foreignKeys;
	}

	public List<Column> getColumns() {
		return columns;
	}

	void addPrimaryKey(PrimaryKey primaryKey) {
		primaryKeys.add(primaryKey);
	}

	void addForeignKey(ForeignKey fkData) {
		foreignKeys.add(fkData);
	}

	void addColumn(Column col) {
		columns.add(col);
	}

	public static class ForeignKey {

		final String primaryKeyTableName;
		final String primaryKeyColumnName;
		final String columnName;
		final String tableName;

		public String getColumnName() {
			return columnName;
		}

		public String getTableName() {
			return tableName;
		}

		public ForeignKey(String tableName, String columnName, String pkTableName, String pkColumnName) {
			this.tableName = tableName;
			this.columnName = columnName;
			this.primaryKeyTableName = pkTableName;
			this.primaryKeyColumnName = pkColumnName;
		}

		public String getName() {
			return columnName;
		}

		public String getPrimaryKeyTableName() {
			return primaryKeyTableName;
		}

		public String getPrimaryKeyColumnName() {
			return primaryKeyColumnName;
		}
	}

	public static class PrimaryKey {

		private final String columnName;
		private final String schema;
		private final String tableName;

		public PrimaryKey(String schema, String tableName, String pkColumnName) {
			this.schema = schema;
			this.tableName = tableName;
			this.columnName = pkColumnName;
		}

		public String getName() {
			return columnName;
		}

		/**
		 * @return the schema
		 */
		public String getSchema() {
			return schema;
		}

		/**
		 * @return the tableName
		 */
		public String getTableName() {
			return tableName;
		}
	}

	public static class Column {

		final String schema;
		final String tableName;
		String columnName;
		int precision;
		String comments;
		boolean isAutoIncrement;
		int sqlDataTypeInt;
		String sqlDataTypeName;
		Class<? extends Object> columnType;
		int javaSQLDatatype;
		boolean isPrimaryKey = false;
		boolean isForeignKey = false;
		String referencesClass;
		String referencesField;
		String referencesTableName;

		Column(String schema, String tableName) {
			this.schema = schema;
			this.tableName = tableName;
		}


		public String getSchema() {
			return schema;
		}

		public String getTableName() {
			return tableName;
		}
		public String getColumnName() {
			return columnName;
		}

		public String getReferencedTable() {
			return referencesTableName;
		}

		public int getColumnSize() {
			return precision;
		}

		public String getRemarks() {
			return comments;
		}

		public boolean getIsAutoIncrement() {
			return isAutoIncrement;
		}

		public int getDatatype() {
			return sqlDataTypeInt;
		}

		public String getTypeName() {
			return sqlDataTypeName;
		}

		public void setColumnName(String columnName) {
			this.columnName = columnName;
		}

		public void setReferencedTable(String scopeTableName) {
			this.referencesTableName = scopeTableName;
		}
	}

	/**
	 * @return the foreignKeys
	 */
	public List<ForeignKey> getForeignKeys() {
		return foreignKeys;
	}
	
}
