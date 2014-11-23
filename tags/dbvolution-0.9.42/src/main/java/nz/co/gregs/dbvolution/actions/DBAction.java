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
import java.util.List;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;

/**
 * DBAction encapsulates the concept of permanent changes to the database.
 *
 * <p>
 * All DBActions are an immutable encapsulation of the action and row at the
 * point in time. A DBAction will perform the same action, given the same
 * DBDatabase, no matter what has happened since the creation of the DBAction.
 *
 * <p>
 * Usually you should use the methods {@link DBDatabase#delete(nz.co.gregs.dbvolution.DBRow...)  }, {@link DBDatabase#update(nz.co.gregs.dbvolution.DBRow...)  }, {@link DBDatabase#insert(nz.co.gregs.dbvolution.DBRow...)
 * } to automatically execute the DBActions, update any DBRows that need
 * updating, and return the performed DBActions as a DBActionList.
 *
 * <p>
 * However DBAction and it's subclasses provide more features for scripting
 * particularly {@link DBDelete#getDeletes(nz.co.gregs.dbvolution.DBDatabase, nz.co.gregs.dbvolution.DBRow...)  }, {@link DBUpdate#getUpdates(nz.co.gregs.dbvolution.DBRow...)
 * }, and {@link DBInsert#getInserts(nz.co.gregs.dbvolution.DBRow...) },
 * allowing a series of changes to be created then executed in a single batch.
 *
 * @author Gregory Graham
 */
public abstract class DBAction {

	private final DBRow row;

	/**
	 * Standard action constructor.
	 *
	 * <p>
	 * Saves a copy of the row to ensure immutability.</p>
	 *
	 * @param <R>
	 * @param row
	 */
	public <R extends DBRow> DBAction(R row) {
		super();
		this.row = DBRow.copyDBRow(row);
	}

	/**
	 * Returns a DBActionList containing the changes required to revert the
	 * DBAction.
	 *
	 * <p>
	 * Every action has an opposite reaction. This method supplies the actions
	 * require to revert the change enacted by the action.
	 *
	 * <p>
	 * Revert actions are tricky to implement correctly, so be sure to check
	 * that the revert will produce the desired result.
	 *
	 * @return a list of all the actions required to revert this action in the
	 * order they need to enacted.
	 */
	protected abstract DBActionList getRevertDBActionList();

	/**
	 * Returns a DBActionList of the actions required to perform this DBAction.
	 *
	 * <p>
	 * Actions are allowed to create sub-actions so all actions are returned as
	 * a DBActionList.
	 *
	 * @return a DBActionList of this DBAction.
	 */
	protected abstract DBActionList getActions();

	/**
	 * Returns a list of the SQL statements that this DBAction will produce for
	 * the specified database.
	 *
	 * <p>
	 * Actions happen all by themselves but when you want to know what will
	 * actually happen, use this method to get a complete list of all the SQL
	 * required.
	 *
	 * @param db
	 * @return the list of SQL strings that equates to this action.
	 */
	public abstract List<String> getSQLStatements(DBDatabase db);

	/**
	 * Performs the DB action and returns a list of all actions performed in the
	 * process.
	 *
	 * <p>
	 * The supplied row will be changed by the action in an appropriate way,
	 * however the Action will contain an unchanged and unchangeable copy of the
	 * row for internal use.
	 *
	 * @param db
	 * @return The complete list of all actions performed to complete this
	 * action on the database
	 * @throws SQLException
	 */
	protected abstract DBActionList execute(DBDatabase db) throws SQLException;

	/**
	 * Returns a copy of the row supplied during creation.
	 *
	 * @return the row
	 */
	protected DBRow getRow() {
		return DBRow.copyDBRow(row);
	}

}
