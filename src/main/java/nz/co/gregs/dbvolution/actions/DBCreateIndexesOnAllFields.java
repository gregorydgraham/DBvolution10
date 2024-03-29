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
import nz.co.gregs.dbvolution.databases.QueryIntention;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException;
import nz.co.gregs.dbvolution.exceptions.UnableToInstantiateDBRowSubclassException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author gregorygraham
 */
public class DBCreateIndexesOnAllFields extends DBAction {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(DBCreateIndexesOnAllFields.class);

	public static DBActionList createIndexes(DBDatabase db, DBRow... rows) throws NoAvailableDatabaseException, SQLException {
		DBActionList actions = new DBActionList();
		for (DBRow row : rows) {
			DBActionList updates = new DBActionList(new DBCreateIndexesOnAllFields(row));
			for (DBAction act : updates) {
				actions.addAll(db.executeDBAction(act));
			}
		}
		return actions;
	}

	public <R extends DBRow> DBCreateIndexesOnAllFields(DBRow table) {
		super(table, QueryIntention.CREATE_INDEX_ON_ALL_KEYS);
	}

	@Override
	protected DBActionList getRevertDBActionList() {
		DBActionList reverts = new DBActionList();
		return reverts;
	}

	@Override
	public List<String> getSQLStatements(DBDatabase db) {
			List<String> indexClauses = new ArrayList<>();
		DBDefinition definition = db.getDefinition();
			var fields = getRow().getColumnPropertyWrappers();
			for (var field : fields) {
				final QueryableDatatype<?> qdt = field.getQueryableDatatype();
				if (field.isColumn() && !qdt.hasColumnExpression() && !(qdt instanceof DBLargeObject)) {
					String indexClause = definition.getIndexClauseForCreateTable(field);
					if (!indexClause.isEmpty()) {
						indexClauses.add(indexClause);
					}
				}
			}
		return indexClauses;
	}

	@Override
	public DBActionList execute(DBDatabase db) throws SQLException {
		return execute2(db);
	}

	@Override
	protected DBActionList prepareActionList(DBDatabase db) throws AccidentalBlankQueryException, SQLException, UnableToInstantiateDBRowSubclassException {
		final DBCreateIndexesOnAllFields newAction = new DBCreateIndexesOnAllFields(getRow());
		DBActionList actions = new DBActionList(newAction);
		return actions;
	}

	@Override
	protected void prepareRollbackData(DBDatabase db, DBActionList actions) {
	}
}
