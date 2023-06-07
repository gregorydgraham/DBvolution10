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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;

/**
 *
 * @author gregorygraham
 */
public class MySQLDBDatabaseMetaData extends DBDatabaseMetaData {

	public MySQLDBDatabaseMetaData(Options options) throws SQLException {
		super(options);
	}

	@Override
	protected void postProcessing(Options options, ArrayList<TableMetaData> tablesFound) {
		DBDatabase database = options.getDBDatabase();
		List<TableMetaData.Column> reqdColumns = new ArrayList<>(0);
		for (TableMetaData table : tablesFound) {
			for (TableMetaData.Column column : table.getColumns()) {
				if (column.getTypeName().toUpperCase().equals("GEOMETRY")) {
					reqdColumns.add(column);
				}
			}
		}
		if (reqdColumns.size() > 0) {
			for (TableMetaData.Column reqdColumn : reqdColumns) {
				STGeometryColumns example = new STGeometryColumns();
				example.tableCatalog.setValue(getCatalog());
				example.tableName.setValue(reqdColumn.tableName);
				example.columnName.setValue(reqdColumn.columnName);
				try {
					database.setPrintSQLBeforeExecuting(true);
					List<STGeometryColumns> got = database.get(1l, example);
					reqdColumn.sqlDataTypeName = got.get(0).geometryTypeName.getValue();
				} catch (SQLException | UnexpectedNumberOfRowsException | AccidentalBlankQueryException | NoAvailableDatabaseException ex) {
					ex.printStackTrace();
					Logger.getLogger(MySQLDBDatabaseMetaData.class.getName()).log(Level.SEVERE, null, ex);
				}finally{
					database.setPrintSQLBeforeExecuting(false);
				}
			}
		}
	}

	@DBTableName(value = "ST_GEOMETRY_COLUMNS", schema = "information_schema")
	public static class STGeometryColumns extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn("TABLE_CATALOG")
		DBString tableCatalog = new DBString();

		@DBColumn("TABLE_SCHEMA")
		DBString tableSchema = new DBString();

		@DBColumn("TABLE_NAME")
		DBString tableName = new DBString();

		@DBColumn("COLUMN_NAME")
		DBString columnName = new DBString();

		@DBColumn("SRS_NAME")
		DBString srsName = new DBString();

		@DBColumn("SRS_ID")
		DBInteger srsID = new DBInteger();

		@DBColumn("GEOMETRY_TYPE_NAME")
		DBString geometryTypeName = new DBString();

	}

}
