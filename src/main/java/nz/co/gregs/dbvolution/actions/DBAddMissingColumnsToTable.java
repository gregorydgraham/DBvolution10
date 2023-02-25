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
package nz.co.gregs.dbvolution.actions;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBConnection;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.databases.QueryIntention;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.exceptions.UnableToInstantiateDBRowSubclassException;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author gregorygraham
 */
public class DBAddMissingColumnsToTable extends DBAction {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(DBAddMissingColumnsToTable.class);

	public <R extends DBRow> DBAddMissingColumnsToTable(DBRow table) {
		super(table, QueryIntention.ADD_MISSING_COLUMNS_TO_TABLE);
	}

	@Override
	protected DBActionList getRevertDBActionList() {
		DBActionList reverts = new DBActionList();
		return reverts;
	}

	@Override
	public List<String> getSQLStatements(DBDatabase db) {
		List<String> result = new ArrayList<>(1);

		return result;
	}

	@Override
	public DBActionList execute(DBDatabase db) throws SQLException {
		DBActionList actions = prepareActionList(db);
		for (DBAction action : actions) {
			action.execute(db);
		}
		return actions;
	}

	@Override
	protected DBActionList prepareActionList(DBDatabase database) throws AccidentalBlankQueryException, SQLException, UnableToInstantiateDBRowSubclassException {

		DBActionList actions = new DBActionList();

		List<PropertyWrapper<?, ?, ?>> newColumns = new ArrayList<>();
		DBRow table = getRow();
		HashMap<String, ColumnStructure> existingColumns = getColumnStructureViaMetaData(database, table);

		var columnPropertyWrappers = table.getColumnPropertyWrappers();
		for (var columnPropertyWrapper : columnPropertyWrappers) {
			if (columnPropertyWrapper != null && !columnPropertyWrapper.hasColumnExpression()) {
				String columnName = columnPropertyWrapper.columnName();
				DBDefinition definition = database.getDefinition();
				String formattedColumnName = definition.formatColumnName(columnName);
				ColumnStructure got = existingColumns.get(formattedColumnName);
				if (got == null) {
					newColumns.add(columnPropertyWrapper);
				}
			}
		}
		for (var newColumn : newColumns) {
			actions.add(new DBAlterTableAddColumnIfNeeded(table, newColumn));
		}

		return actions;
	}

	@Override
	protected void prepareRollbackData(DBDatabase db, DBActionList actions) {
		// with any real database attempting this is absurd
	}

	private HashMap<String, ColumnStructure> getColumnStructureViaMetaData(DBDatabase database, DBRow table) throws SQLException {

		HashMap<String, ColumnStructure> structures = new HashMap<>(0);
		ResultSet columns = getMetaDataForTable(database, table);
		while (columns.next()) {
			String columnName = columns.getString("COLUMN_NAME");
			ColumnStructure column = new ColumnStructure(
					Integer.getInteger(columns.getString("COLUMN_SIZE")),
					columns.getString("DATA_TYPE"),
					"YES".equals(columns.getString("IS_NULLABLE")),
					"YES".equals(columns.getString("IS_AUTOINCREMENT")));
			structures.put(columnName, column);
		}
		return structures;
	}

	private ResultSet getMetaDataForTable(DBDatabase database, DBRow table) throws SQLException {
		try (DBStatement dbStatement = database.getDBStatement()) {
			DBConnection conn = dbStatement.getConnection();
			DBDefinition definition = database.getDefinition();
			String formattedTableName = definition.formatTableName(table);
			ResultSet rset = conn.getMetaData().getColumns(null, null, formattedTableName, null);
			return rset;
		}
	}

	public static class ColumnStructure {

		private final Integer size;
		private final String datatype;
		private final boolean isNullable;
		private final boolean isAutoIncrement;

		public ColumnStructure(Integer size, String datatype, boolean isNullable, boolean isAutoIncrement) {
			this.size = size;
			this.datatype = datatype;
			this.isNullable = isNullable;
			this.isAutoIncrement = isAutoIncrement;
		}

		public Integer getSize() {
			return size;
		}

		public String getDatatype() {
			return datatype;
		}

		public boolean isIsNullable() {
			return isNullable;
		}

		public boolean isIsAutoIncrement() {
			return isAutoIncrement;
		}

	};
}
