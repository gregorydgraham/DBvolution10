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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.databases.QueryIntention;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.exceptions.AccidentalCartesianJoinException;
import nz.co.gregs.dbvolution.exceptions.AutoCommitActionDuringTransactionException;
import nz.co.gregs.dbvolution.exceptions.UnableToInstantiateDBRowSubclassException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author gregorygraham
 */
public class DBDropTable extends DBAction {

	private static final long serialVersionUID = 1L;
	static final private Log LOG = LogFactory.getLog(DBDropTable.class);

	private final ArrayList<DBRow> savedRows = new ArrayList<>(0);

	public <R extends DBRow> DBDropTable(R row) {
		super(row, QueryIntention.DROP_TABLE);
	}

	@Override
	protected DBActionList getRevertDBActionList() {
		DBActionList reverts = new DBActionList();
		/* TODO: work out if the table has foreign keys or not */
		reverts.add(new DBCreateTable(getRow(), false));
		for (DBRow savedRow : savedRows) {
			reverts.add(new DBInsert(savedRow));
		}
		return reverts;
	}

	@Override
	public List<String> getSQLStatements(DBDatabase db) {
		List<String> result = new ArrayList<>(1);
		DBRow tableRow = getRow();
//		LOG.debug("DROPPING TABLE: " + tableRow.getTableName());

		String sqlString = createDropTableSQL(db, tableRow);
		result.add(sqlString);
		result.addAll(db.getDefinition().getSQLToDropAnyAssociatedDatabaseObjects(tableRow));

		return result;
	}

	protected String createDropTableSQL(DBDatabase db, DBRow tableRow) {
		DBDefinition definition = db.getDefinition();
		StringBuilder sqlScript = new StringBuilder(0);
		final String dropTableStart = definition.getDropTableStart();
		final String formatTableName = definition.formatTableName(tableRow);
		final String endSQLStatement = definition.endSQLStatement();
		sqlScript.append(dropTableStart).append(formatTableName).append(endSQLStatement);
		String sqlString = sqlScript.toString();
		return sqlString;
	}

	@Override
	public DBActionList execute(DBDatabase db) throws SQLException {

		return execute2(db);
	}

	public DBActionList execute2(DBDatabase db) throws SQLException {

		preventImproperActions(db);

		DBActionList actions = prepareActionList(db);
		prepareRollbackData(db, actions);
		executeOnStatement(db);
		return actions;
	}

	protected void preventImproperActions(DBDatabase db) throws AutoCommitActionDuringTransactionException {
		db.preventDDLDuringTransaction("DBDropTable");
		db.preventAccidentalDroppingOfTables();
	}

	protected DBActionList prepareActionList(DBDatabase db) throws AccidentalBlankQueryException, SQLException, UnableToInstantiateDBRowSubclassException {
		DBRow table = getRow();
		final DBDropTable newAction = new DBDropTable(table);
		DBActionList actions = new DBActionList(newAction);
		return actions;
	}

	protected DBActionList prepareRollbackData(DBDatabase db, DBActionList actions) throws AccidentalBlankQueryException, AccidentalCartesianJoinException, SQLException, UnableToInstantiateDBRowSubclassException {
		for (DBAction act : actions) {
			if (act instanceof DBDropTable) {
				DBDropTable drop = (DBDropTable) act;
				DBRow table = drop.getRow();
				DBRow example = DBRow.getDBRow(table.getClass());
				if (db.tableExists(example)) {
					try {
						List<DBRow> rowsToBeDeleted = db.getDBTable(example).setBlankQueryAllowed(true).getAllRows();
						for (DBRow deletingRow : rowsToBeDeleted) {
							drop.savedRows.add(DBRow.copyDBRow(deletingRow));
						}
					} catch (SQLException | AccidentalBlankQueryException | AccidentalCartesianJoinException e) {
						LOG.warn("EXCEPTION OCCURRED WHILE PREPARING ROLLBACK: rollback data will not be available.", e);
					}
				}
			}
		}
		return actions;
	}

}
