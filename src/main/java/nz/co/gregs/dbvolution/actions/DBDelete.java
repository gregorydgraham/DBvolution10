/*
 * Copyright 2013 gregorygraham.
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
import java.util.Collection;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;

/**
 * Provides support for the abstract concept of deleting rows.
 *
 * @author gregorygraham
 */
public abstract class DBDelete extends DBAction {

	/**
	 * Creates a DBDelete action for the supplied row.
	 *
	 * @param <R>
	 * @param row
	 */
	protected <R extends DBRow> DBDelete(R row) {
		super(row);
	}

	/**
	 * Deletes the specified row or example from the database and returns the
	 * actions performed.
	 *
	 * @param database
	 * @param row
	 * @return the actions executed as a DBActionList
	 * @throws SQLException
	 */
	public static DBActionList delete(DBDatabase database, DBRow row) throws SQLException {
		DBActionList delete = getDeletes(database, row);
		return delete.execute(database);
	}

	/**
	 * Creates a DBActionList of delete actions for the rows.
	 * <p>
	 * You probably want to use {@link #getDeletes(nz.co.gregs.dbvolution.DBDatabase, nz.co.gregs.dbvolution.DBRow...)
	 * } instead.
	 * <p>
	 * The actions created can be applied on a particular database using
	 * {@link DBActionList#execute(nz.co.gregs.dbvolution.DBDatabase)}
	 *
	 * <p>
	 * This method cannot produce DBInsert statements for the revert action list
	 * until the actions have been executed. If you need the revert script to
	 * include insert statements use the {@link #getDeletes(nz.co.gregs.dbvolution.DBDatabase, nz.co.gregs.dbvolution.DBRow[])
	 * } method.
	 *
	 * @param rows
	 * @return a DBActionList of deletes.
	 * @throws SQLException
	 */
	public static DBActionList getDeletesWithRevertCapability(DBRow... rows) throws SQLException {
		DBActionList actions = new DBActionList();
		for (DBRow row : rows) {
			if (row.getDefined()) {
				if (row.getPrimaryKey() == null) {
					DBDeleteUsingAllColumns allCols = new DBDeleteUsingAllColumns(row);
					actions.addAll(allCols.getActions());
				} else {
					DBDeleteByPrimaryKey pk = new DBDeleteByPrimaryKey(row);
					actions.addAll(pk.getActions());
				}
			} else {
				DBDeleteByExample example = new DBDeleteByExample(row);
				actions.addAll(example.getActions());
			}
		}
		return actions;
	}

	/**
	 * Creates a DBActionList of delete actions for the rows.
	 *
	 * <p>
	 * The actions created can be applied on a particular database using
	 * {@link DBActionList#execute(nz.co.gregs.dbvolution.DBDatabase)}
	 *
	 * <p>
	 * The DBDatabase instance will be used to create DBInsert actions for the
	 * revert action list.
	 *
	 *
	 * @param db
	 * @param rows
	 * @return a DBActionList of delete actions.
	 * @throws SQLException
	 */
	public static DBActionList getDeletes(DBDatabase db, DBRow... rows) throws SQLException {
		DBActionList actions = new DBActionList();
		for (DBRow row : rows) {
			if (row.getDefined()) {
				if (row.getPrimaryKey() == null) {
					DBDeleteUsingAllColumns allCols = new DBDeleteUsingAllColumns(row);
					actions.addAll(allCols.getActions(db, row));
				} else {
					DBDeleteByPrimaryKey pk = new DBDeleteByPrimaryKey(row);
					actions.addAll(pk.getActions(db, row));
				}
			} else {
				DBDeleteByExample example = new DBDeleteByExample(row);
				actions.addAll(example.getActions(db, row));
			}
		}
		return actions;
	}

	/**
	 * Creates a DBActionList of delete actions for the rows.
	 *
	 * <p>
	 * The actions created can be applied on a particular database using
	 * {@link DBActionList#execute(nz.co.gregs.dbvolution.DBDatabase)}
	 *
	 * <p>
	 * The DBDatabase instance will be used to create DBInsert actions for the
	 * revert action list.
	 *
	 *
	 * @param db
	 * @param rows
	 * @return a DBActionList of delete actions.
	 * @throws SQLException
	 */
	public static DBActionList getDeletes(DBDatabase db, Collection<? extends DBRow> rows) throws SQLException {
		DBActionList actions = new DBActionList();
		for (DBRow row : rows) {
			if (row.getDefined()) {
				if (row.getPrimaryKey() == null) {
					DBDeleteUsingAllColumns allCols = new DBDeleteUsingAllColumns(row);
					actions.addAll(allCols.getActions(db, row));
				} else {
					DBDeleteByPrimaryKey pk = new DBDeleteByPrimaryKey(row);
					actions.addAll(pk.getActions(db, row));
				}
			} else {
				DBDeleteByExample example = new DBDeleteByExample(row);
				actions.addAll(example.getActions(db, row));
			}
		}
		return actions;
	}

	/**
	 * Returns the list of actions required to delete the row supplied on the
	 * database supplied.
	 *
	 * <p>
	 * While it is unlikely that more than one action is required to delete, all
	 * actions return a list to allow for complex actions.
	 *
	 * @param db
	 * @param row
	 * @return a DBActionList of the actions required to implement the change.
	 * @throws SQLException
	 */
	protected abstract DBActionList getActions(DBDatabase db, DBRow row) throws SQLException;
}
