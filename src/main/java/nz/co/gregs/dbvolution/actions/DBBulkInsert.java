/*
 * Copyright 2019 Gregory Graham.
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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.databases.QueryIntention;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 *
 * @author gregorygraham
 */
public class DBBulkInsert extends DBAction {

	public static final long serialVersionUID = 1l;

	ArrayList<DBRow> rows = new ArrayList<>();

	private <R extends DBRow> DBBulkInsert(R row) {
		super(row, QueryIntention.BULK_INSERT);
	}

	public <R extends DBRow> DBBulkInsert() {
		super(null, QueryIntention.BULK_INSERT);
	}

	public void addRow(DBRow row) {
		rows.add(row);
	}

	public synchronized DBActionList insert(DBDatabase database) throws SQLException {
		return save(database);
	}

	public DBActionList save(DBDatabase database) throws SQLException {
		DBActionList changes = new DBActionList();
		if (database.getDefinition().supportsBulkInserts()) {
			return database.executeDBAction(this);
		} else {
			for (DBRow rowToInsert : rows) {
				changes.addAll(database.insert(rowToInsert));
			}
		}
		return changes;
	}

	@Override
	protected DBActionList getRevertDBActionList() {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public DBRow getRow() {
		return DBRow.copyDBRow(rows.get(0));
	}

	@Override
	public ArrayList<String> getSQLStatements(DBDatabase db) {
		ArrayList<String> sqlStatements = new ArrayList<String>();
		List<DBRow> accumulated = new ArrayList<>();
		for (DBRow currentRow : rows) {
			QueryableDatatype<?>[] pks = currentRow.getPrimaryKeysAsArray();
			boolean pksAreSet = true;
			for (QueryableDatatype<?> pk : pks) {
				pksAreSet = pksAreSet && pk.hasBeenSet();
			}
			if (pksAreSet) {
				accumulated.add(currentRow);
			} else {
				sqlStatements.addAll(generateSQLForAccumulatedRows(db, accumulated));
				accumulated.clear();
				sqlStatements.addAll(new DBInsert(currentRow).getSQLStatements(db));
			}
		}
		sqlStatements.addAll(generateSQLForAccumulatedRows(db, accumulated));
		return sqlStatements;
	}

	private DBInsert.InsertFields processAllFieldsForInsert(DBDatabase database, DBRow row, boolean isFirstRow) {
		DBInsert.InsertFields fields = new DBInsert.InsertFields();
		StringBuilder allColumns = fields.getAllColumns();
		StringBuilder allValues = fields.getAllValues();
		StringBuilder allChangedColumns = fields.getAllChangedColumns();
		StringBuilder allSetValues = fields.getAllSetValues();
		DBDefinition defn = database.getDefinition();
		var props = row.getColumnPropertyWrappers();
		String allColumnSeparator = "";
		String columnSeparator = "";
		String valuesSeparator = isFirstRow ? defn.beginValueClause() : defn.beginValueSeparatorClause();
		String allValuesSeparator = isFirstRow ? defn.beginValueClause() : defn.beginValueSeparatorClause();
		for (var prop : props) {
			if (prop.isColumn() && !prop.hasColumnExpression()) {
				final QueryableDatatype<?> qdt = prop.getQueryableDatatype();
				if (qdt != null) {
					// BLOBS are not inserted normally so don't include them
					if (!(qdt instanceof DBLargeObject)) {
						//support for inserting empty rows in a table with an autoincrementing pk
						if (!prop.isAutoIncrement() || qdt.hasBeenSet()) {
							allColumns
									.append(allColumnSeparator)
									.append(" ")
									.append(defn.formatColumnName(prop.columnName()));
							allColumnSeparator = defn.getValuesClauseColumnSeparator();
							// add the value
							allValues.append(allValuesSeparator);
							if (!qdt.hasBeenSet() && qdt.hasDefaultInsertValue()) {
								allValues.append(
										qdt.getDefaultInsertValueSQLString(database.getDefinition())
								);
							} else {
								allValues.append(
										qdt.toSQLString(database.getDefinition())
								);
							}
							allValuesSeparator = defn.getValuesClauseValueSeparator();
						}
						if (qdt.hasBeenSet() || qdt.hasDefaultInsertValue()) {
							// nice normal columns
							// Add the column
							allChangedColumns
									.append(columnSeparator)
									.append(" ")
									.append(defn.formatColumnName(prop.columnName()));
							columnSeparator = defn.getValuesClauseColumnSeparator();
							allSetValues.append(valuesSeparator);
							// add the value
							if (qdt.hasBeenSet()) {
								allSetValues.append(
										qdt.toSQLString(database.getDefinition())
								);
							} else if (qdt.hasDefaultInsertValue()) {
								allSetValues.append(
										qdt.getDefaultInsertValueSQLString(database.getDefinition())
								);
							}
							valuesSeparator = defn.getValuesClauseValueSeparator();
						}
					}
				}
			}
		}
		allValues.append(defn.endValueClause());
		allSetValues.append(defn.endValueClause());
		return fields;
	}

	@Override
	public DBActionList execute(DBDatabase db) throws SQLException {
		DBActionList actions = new DBActionList();
		boolean allRowsCanBeBulkInserted = true;
		for (DBRow current : rows) {
			allRowsCanBeBulkInserted = allRowsCanBeBulkInserted && canBeBulkInserted(current);
		}
		if (allRowsCanBeBulkInserted) {
			try (DBStatement statement = db.getDBStatement()) {
				for (String sql : getSQLStatements(db)) {
					statement.execute("BULK INSERT", QueryIntention.BULK_INSERT, sql);
				}
			}
			for (DBRow current : rows) {
				actions.add(new DBInsert(current));
			}
		} else {
			for (DBRow current : rows) {
				actions.addAll(new DBInsert(current).execute(db));
			}
		}
		return actions;
	}

	/* In this method we need to generate the SQL and execute it */
	private ArrayList<String> generateSQLForAccumulatedRows(DBDatabase database, List<DBRow> accumulated) {
		ArrayList<String> strs = new ArrayList<>();
		StringBuilder inserts = new StringBuilder();
		if (accumulated.size() > 0) {
			//generate and execute the SQL
			DBRow table = accumulated.get(0);
			DBDefinition defn = database.getDefinition();
			DBInsert.InsertFields fields = processAllFieldsForInsert(database, table, true);

			strs.addAll(defn.getInsertPreparation(table));
			inserts.append(defn.beginInsertLine())
					.append(defn.formatTableName(table))
					.append(defn.beginInsertColumnList())
					.append(fields.getAllColumns())
					.append(defn.endInsertColumnList());
			boolean isFirstRow = true;
			for (DBRow currentRow : rows) {
				fields = processAllFieldsForInsert(database, currentRow, isFirstRow);
				inserts.append(fields.getAllValues().toString());
				isFirstRow = false;
			}
			inserts.append(defn.endInsertLine());
			strs.add(inserts.toString());
			strs.addAll(defn.getInsertCleanUp(table));
		}
		return strs;
	}

	public void addAll(DBRow[] listOfRowsToInsert) {
		rows.addAll(Arrays.asList(listOfRowsToInsert));
	}

	private boolean canBeBulkInserted(DBRow row) {
		return row.getDefined();
	}

}
