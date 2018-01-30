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
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;

/**
 * Provides support for the abstract concept of deleting rows based on a defined
 * row without a primary key.
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
public class DBDeleteUsingAllColumns extends DBDelete {

	private static final long serialVersionUID = 1l;
	
	private final List<DBRow> savedRows = new ArrayList<>();

	/**
	 * Creates a DBDeleteUsingAllColumns action for the supplied example DBRow on
	 * the supplied database.
	 *
	 * @param <R> the table affected
	 * @param row the row to be deleted
	 */
	protected <R extends DBRow> DBDeleteUsingAllColumns(R row) {
		super(row);
	}

	private <R extends DBRow> DBDeleteUsingAllColumns(DBDatabase db, R row) throws SQLException {
		super(row);
		List<R> gotRows = db.get(row);
		for (R gotRow : gotRows) {
			savedRows.add(gotRow);
		}
	}

	@Override
	public DBActionList execute(DBDatabase db) throws SQLException {
		DBRow table = getRow();
		final DBDeleteUsingAllColumns dbDeleteUsingAllColumns = new DBDeleteUsingAllColumns(table);
		DBActionList actions = new DBActionList(dbDeleteUsingAllColumns);
		List<DBRow> rowsToBeDeleted = db.get(table);
		for (DBRow deletingRow : rowsToBeDeleted) {
			dbDeleteUsingAllColumns.savedRows.add(DBRow.copyDBRow(deletingRow));
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
		DBRow table = getRow();
		DBDefinition defn = db.getDefinition();

		StringBuilder sql = new StringBuilder(defn.beginDeleteLine()
				+ defn.formatTableName(table)
				+ defn.beginWhereClause()
				+ defn.getWhereClauseBeginningCondition());
		for (PropertyWrapper prop : table.getColumnPropertyWrappers()) {
			QueryableDatatype<?> qdt = prop.getQueryableDatatype();
			sql.append(defn.beginWhereClauseLine())
					.append(prop.columnName())
					.append(defn.getEqualsComparator())
					.append(qdt.hasChanged() ? qdt.getPreviousSQLValue(defn) : qdt.toSQLString(defn));
		}
		sql.append(defn.endDeleteLine());
		ArrayList<String> strs = new ArrayList<>();
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
		return new DBActionList(new DBDeleteUsingAllColumns(getRow()));
	}

	/**
	 * Returns the list of actions required to delete rows matching all the
	 * columns of the example supplied on the database supplied.
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
		return new DBActionList(new DBDeleteUsingAllColumns(db, row));
	}

}
