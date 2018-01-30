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
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 * Provides support for the abstract concept of deleting rows based on a primary
 * key.
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
public class DBDeleteByPrimaryKey extends DBDelete {

	private static final long serialVersionUID = 1l;
	
	private final List<DBRow> savedRows = new ArrayList<>();

	/**
	 * Creates a DBDeleteByPrimaryKey action for the supplied example DBRow on
	 * the supplied database.
	 *
	 * @param <R> the table affected
	 * @param row the row to be deleted
	 */
	protected <R extends DBRow> DBDeleteByPrimaryKey(R row) {
		super(row);
	}

	private <R extends DBRow> DBDeleteByPrimaryKey(DBDatabase db, R row) throws SQLException {
		super(row);
		DBRow example = DBRow.getPrimaryKeyExample(row);
		List<DBRow> gotRows = db.get(example);
		for (DBRow gotRow : gotRows) {
			savedRows.add(gotRow);
		}
	}

	@Override
	public DBActionList execute(DBDatabase db) throws SQLException {
		DBRow table = getRow();
		final DBDeleteByPrimaryKey newDeleteAction = new DBDeleteByPrimaryKey(table);
		DBActionList actions = new DBActionList(newDeleteAction);
		DBRow example = DBRow.getPrimaryKeyExample(table);
		List<DBRow> rowsToBeDeleted = db.get(example);
		for (DBRow deletingRow : rowsToBeDeleted) {
			newDeleteAction.savedRows.add(DBRow.copyDBRow(deletingRow));
		}
		try (DBStatement statement = db.getDBStatement()) {
			for (String str : getSQLStatements(db)) {
				statement.execute(str);
			}
		}
		return actions;
	}

	@Override
	public ArrayList<String> getSQLStatements(DBDatabase db) {
		DBDefinition defn = db.getDefinition();
		DBRow table = getRow();

		ArrayList<String> strs = new ArrayList<>();
		StringBuilder sql = new StringBuilder(defn.beginDeleteLine()
				+ defn.formatTableName(table)
				+ defn.beginWhereClause());
		List<QueryableDatatype<?>> primaryKeys = table.getPrimaryKeys();
		for (QueryableDatatype<?> pk : primaryKeys) {
			sql.append(defn.formatColumnName(table.getPropertyWrapperOf(pk).columnName()))
					.append(defn.getEqualsComparator())
					.append(pk.toSQLString(defn));
		}
		sql.append(defn.endDeleteLine());
		strs.add(sql.toString());
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
	 * Returns the list of actions required to delete rows with the primary key
	 * of the supplied example on the database supplied.
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
		return new DBActionList(new DBDeleteByPrimaryKey(db, row));
	}
}
