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
import java.util.Collection;
import java.util.List;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 * Provides support for the abstract concept of deleting rows.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public abstract class DBDelete extends DBAction {

	private final static long serialVersionUID = 1l;

	/**
	 * Creates a DBDelete action for the supplied row.
	 *
	 * @param <R> the table affected
	 * @param row the row to delete
	 */
	protected <R extends DBRow> DBDelete(R row) {
		super(row);
	}

	/**
	 * Deletes the specified row or example from the database and returns the
	 * actions performed.
	 *
	 * @param database the target database
	 * @param row the row to be deleted
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the actions executed as a DBActionList
	 * @throws SQLException database exceptions
	 */
	public static DBActionList delete(DBDatabase database, DBRow row) throws SQLException {
		DBActionList delete = getDeletes(database, row);
		return delete.execute(database);
	}

	/**
	 * Deletes the specified row or example from the database and returns the
	 * actions performed.
	 *
	 * @param database the target database
	 * @param rows the row to be deleted
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the actions executed as a DBActionList
	 * @throws SQLException database exceptions
	 */
	public static DBActionList delete(DBDatabase database, DBRow... rows) throws SQLException {
		DBActionList delete = getDeletes(database, rows);
		return delete.execute(database);
	}

	/**
	 * Deletes the specified row or example from the database and returns the
	 * actions performed.
	 *
	 * @param database the target database
	 * @param rows the row to be deleted
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the actions executed as a DBActionList
	 * @throws SQLException database exceptions
	 */
	public static DBActionList delete(DBDatabase database, Collection<? extends DBRow> rows) throws SQLException {
		DBActionList delete = getDeletes(database, rows);
		return delete.execute(database);
	}

	/**
	 * Creates a DBActionList of delete actions for the rows.
	 *
	 * <p>
	 * The actions created can be applied on a particular database using
	 * {@link DBActionList#execute(nz.co.gregs.dbvolution.databases.DBDatabase)}
	 *
	 * <p>
	 * The DBDatabase instance will be used to create DBInsert actions for the
	 * revert action list.
	 *
	 *
	 * @param db the target database
	 * @param rows the rows to be deleted
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBActionList of delete actions.
	 * @throws SQLException Database actions can throw SQLException
	 */
	public static DBActionList getDeletes(DBDatabase db, DBRow... rows) throws SQLException {
		DBActionList actions = new DBActionList();
		for (DBRow row : rows) {
			if (row.getDefined()) {
				final List<QueryableDatatype<?>> primaryKeys = row.getPrimaryKeys();
				if (primaryKeys == null || primaryKeys.isEmpty()) {
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
	 * {@link DBActionList#execute(nz.co.gregs.dbvolution.databases.DBDatabase)}
	 *
	 * <p>
	 * The DBDatabase instance will be used to create DBInsert actions for the
	 * revert action list.
	 *
	 *
	 * @param db the target database
	 * @param rows the rows to be deleted
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBActionList of delete actions.
	 * @throws SQLException Database actions can throw SQLException
	 */
	public static DBActionList getDeletes(DBDatabase db, Collection<? extends DBRow> rows) throws SQLException {
		DBActionList actions = new DBActionList();
		for (DBRow row : rows) {
			if (row.getDefined()) {
				final List<QueryableDatatype<?>> primaryKeys = row.getPrimaryKeys();
				if (primaryKeys == null || primaryKeys.isEmpty()) {
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
	 * @param db the target database
	 * @param row the row to be deleted
	 * @throws SQLException Database actions can throw SQLException
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBActionList of the actions required to implement the change.
	 */
	protected abstract DBActionList getActions(DBDatabase db, DBRow row) throws SQLException;

	/**
	 * Returns a DBActionList of the actions required to perform this DBAction.
	 *
	 * <p>
	 * Actions are allowed to create sub-actions so all actions are returned as a
	 * DBActionList.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a DBActionList of this DBAction.
	 */
	protected abstract DBActionList getActions();
}
