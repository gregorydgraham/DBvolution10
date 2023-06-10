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
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.utility.StringCheck;

/**
 *
 * @author gregorygraham
 */
public class MySQLDBDatabaseMetaData extends DBDatabaseMetaData {

	protected static final Logger LOG = Logger.getLogger(MySQLDBDatabaseMetaData.class.getName());

	public MySQLDBDatabaseMetaData(Options options) throws SQLException {
		super(options);
	}

	@Override
	protected void postProcessing(Options options, ArrayList<TableMetaData> tablesFound) throws SQLException, DBRuntimeException {
		DBDatabase database = options.getDBDatabase();

		// find all the geometry columns
		List<TableMetaData.Column> reqdColumns = new ArrayList<>(0);
		for (TableMetaData table : tablesFound) {
			for (TableMetaData.Column column : table.getColumns()) {
				if (column.getTypeName().toUpperCase().equals("GEOMETRY")) {
					reqdColumns.add(column);
				}
			}
		}

		// query the information_schema for every column and grab the actual datatype
		if (reqdColumns.size() > 0) {
			for (TableMetaData.Column reqdColumn : reqdColumns) {
				// TODO
				// This is an inefficient query.
				// Rewrite it to retrieve all the columns at once and link them up in memory
				STGeometryColumns example = new STGeometryColumns();
				example.tableCatalog.setValue("def"); // this will always be "def" according to https://dev.mysql.com/doc/refman/8.0/en/information-schema-st-geometry-columns-table.html
				if (StringCheck.isNotEmptyNorNull(reqdColumn.getCatalog())) {
					// MySQL follows the standard but doesn't really differntiate between 
					// catalog and schema. In this instance they use the catalog as the schema
					// I expect this to change in a random MySQL update
					example.tableSchema.setValue(reqdColumn.getCatalog());
				}
				example.tableName.setValue(reqdColumn.tableName);
				example.columnName.setValue(reqdColumn.columnName);
				// if we get more than one something has gone horribly wrong so ensure it
				List<STGeometryColumns> got = database.get(1l, example);
				STGeometryColumns firstRow = got.get(0);
				String preferredType = firstRow.geometryTypeName.getValue();
				// finally set the correct datatype
				reqdColumn.sqlDataTypeName = preferredType.toUpperCase();
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
