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
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;

/**
 * Provides support for the abstract concept of deleting rows based on a primary
 * key.
 *
 * <p>
 * The best way to use this is by using {@link DBDelete#getDeletes(nz.co.gregs.dbvolution.DBDatabase, nz.co.gregs.dbvolution.DBRow...)
 * } to automatically use this action.
 *
 * @author gregorygraham
 */
public class DBDeleteByPrimaryKey extends DBDelete {

	private final List<DBRow> savedRows = new ArrayList<DBRow>();

	/**
	 * Creates a DBDeleteByPrimaryKey action for the supplied example DBRow on the supplied
	 * database.
	 *
	 * @param <R>
	 * @param row
	 */
	protected <R extends DBRow> DBDeleteByPrimaryKey(R row) {
		super(row);
	}

	private <R extends DBRow> DBDeleteByPrimaryKey(DBDatabase db, R row) throws SQLException {
		super(row);
		DBRow example = DBRow.getDBRow(row.getClass());
		example.getPrimaryKey().setValue(row.getPrimaryKey());
		List<DBRow> gotRows = db.get(example);
		for (DBRow gotRow : gotRows) {
			savedRows.add(gotRow);
		}
	}

	@Override
	protected DBActionList execute(DBDatabase db) throws SQLException {
		DBRow row = getRow();
		final DBDeleteByPrimaryKey newDeleteAction = new DBDeleteByPrimaryKey(row);
		DBActionList actions = new DBActionList(newDeleteAction);
		DBRow example = DBRow.getDBRow(row.getClass());
		example.getPrimaryKey().setValue(row.getPrimaryKey());
		List<DBRow> rowsToBeDeleted = db.get(example);
		for (DBRow deletingRow : rowsToBeDeleted) {
			newDeleteAction.savedRows.add(DBRow.copyDBRow(deletingRow));
		}
		DBStatement statement = db.getDBStatement();
		try {
			for (String str : getSQLStatements(db)) {
				statement.execute(str);
			}
		} finally {
			statement.close();
		}
		return actions;
	}

	@Override
	public ArrayList<String> getSQLStatements(DBDatabase db) {
		DBDefinition defn = db.getDefinition();
		DBRow row = getRow();

		ArrayList<String> strs = new ArrayList<String>();
		strs.add(defn.beginDeleteLine()
				+ defn.formatTableName(row)
				+ defn.beginWhereClause()
				+ defn.formatColumnName(row.getPrimaryKeyColumnName())
				+ defn.getEqualsComparator()
				+ row.getPrimaryKey().toSQLString(db)
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
		return new DBActionList(new DBDeleteByPrimaryKey(getRow()));
	}

	/**
	 * Returns the list of actions required to delete rows with the primary key of the supplied example on the
	 * database supplied.
	 *
	 * <p>
	 * While it is unlikely that more than one action is required to delete, all
	 * actions return a list to allow for complex actions.
	 *
	 * @param db
	 * @param row
	 * @return the list of actions required to delete all the rows.
	 * @throws SQLException
	 */
	@Override
	protected DBActionList getActions(DBDatabase db, DBRow row) throws SQLException {
		return new DBActionList(new DBDeleteByPrimaryKey(db, row));
	}
}
