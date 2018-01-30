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
package nz.co.gregs.dbvolution.actions;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;

/**
 * Supplies supports for the abstract concept of deleting rows based on an
 * example row.
 *
 * <p>
 * The best way to use this is by using {@link DBDelete#getDeletes(nz.co.gregs.dbvolution.databases.DBDatabase, nz.co.gregs.dbvolution.DBRow...)
 * } to automatically use this action.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBDeleteByExample extends DBDelete {

	private static final long serialVersionUID = 1l;
	
	private final List<DBRow> savedRows = new ArrayList<DBRow>();

	/**
	 * Creates a DBDeleteByExample action for the supplied example DBRow on the
	 * supplied database.
	 *
	 * @param <R> the table affected
	 * @param row the example to be deleted
	 */
	protected <R extends DBRow> DBDeleteByExample(R row) {
		super(row);
	}

	private <R extends DBRow> DBDeleteByExample(DBDatabase db, R row) throws SQLException {
		super(row);
		List<R> gotRows = db.get(row);
		for (R gotRow : gotRows) {
			savedRows.add(DBRow.copyDBRow(gotRow));
		}
	}

	@Override
	public DBActionList execute(DBDatabase db) throws SQLException {
		DBRow table = getRow();
		final DBDeleteByExample deleteAction = new DBDeleteByExample(table);
		DBActionList actions = new DBActionList(deleteAction);
		List<DBRow> rowsToBeDeleted = db.get(table);
		for (DBRow deletingRow : rowsToBeDeleted) {
			deleteAction.savedRows.add(DBRow.copyDBRow(deletingRow));
		}
		try (DBStatement statement = db.getDBStatement()) {
			for (String str : getSQLStatements(db)) {
				statement.execute(str);
			}
		}
		return actions;
	}

	@Override
	public List<String> getSQLStatements(DBDatabase db) {
		DBDefinition defn = db.getDefinition();
		DBRow table = getRow();
		StringBuilder whereClause = new StringBuilder();
		for (String clause : table.getWhereClausesWithoutAliases(defn)) {
			whereClause.append(defn.beginAndLine()).append(clause);
		}

		ArrayList<String> strs = new ArrayList<>();
		strs.add(defn.beginDeleteLine()
				+ defn.formatTableName(table)
				+ defn.beginWhereClause()
				+ defn.getWhereClauseBeginningCondition()
				+ whereClause
				+ defn.endDeleteLine());
		return strs;
	}

	@Override
	protected DBActionList getRevertDBActionList() {
		DBActionList reverts = new DBActionList();
		for (DBRow savedRow : savedRows) {
			reverts.add(new DBInsert(savedRow));
		}
		return reverts;
	}

	@Override
	protected DBActionList getActions() {//DBRow row) {
		return new DBActionList(new DBDeleteByExample(getRow()));
	}

	/**
	 * Returns the list of actions required to delete rows matching the example
	 * supplied on the database supplied.
	 *
	 * <p>
	 * While it is unlikely that more than one action is required to delete, all
	 * actions return a list to allow for complex actions.
	 *
	 * @param db the target database
	 * @param row the row to be deleted
	 * @throws SQLException Database actions can throw SQLException
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the list of actions required to delete all the rows.
	 */
	@Override
	protected DBActionList getActions(DBDatabase db, DBRow row) throws SQLException {
		return new DBActionList(new DBDeleteByExample(db, row));
	}
}
