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
import java.util.Arrays;
import java.util.List;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.DBScript;
import nz.co.gregs.dbvolution.transactions.DBTransaction;

/**
 * Encapsulates the concept of a contiguous series of actions performed, or to
 * be performed, on a database.
 *
 * {@link DBAction} encapsulates the concept of an action, but has no concept of
 * sequence. DBActionList provides the concept of sequence and allows it to be
 * reversed.
 *
 * <p>
 * DBActionList does not provide for complex processing of rows and actions, use
 * {@link DBScript} for that.
 *
 * <p>
 * Similarly DBActionList does not provide transactional integrity, that is
 * provided by {@link DBTransaction}.
 *
 * <p>
 * DBActionList are designed to provide a revert script for actions performed or
 * to accumulate actions that are executed as a batch with {@link DBActionList#execute(nz.co.gregs.dbvolution.databases.DBDatabase)
 * }.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBActionList extends ArrayList<DBAction> {

	private static final long serialVersionUID = 1L;

	/**
	 * Creates a new DBActionList containing the DBactions provided in the order
	 * specified.
	 *
	 * @param actions the list of actions to include in this DBActionList
	 */
	public DBActionList(DBAction... actions) {
		super();
		this.addAll(Arrays.asList(actions));
	}

	/**
	 * Returns the SQL that would be executed on the database provided.
	 *
	 * @param db the target database.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a List of SQL statements appropriate to the actions of this
	 * DBActionList and the database.
	 */
	public synchronized List<String> getSQL(DBDatabase db) {
		List<String> sqlList = new ArrayList<String>();
		for (DBAction act : this) {
			sqlList.addAll(act.getSQLStatements(db));
		}
		return sqlList;
	}

	/**
	 * Executes every action in this DBActionList on the database provided.
	 *
	 * @param database the target database.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a new DBActionList containing the DBActions after execution.
	 * @throws SQLException Database actions may throw SQLException
	 */
	public synchronized DBActionList execute(DBDatabase database) throws SQLException {
		DBActionList executed = new DBActionList();
		for (DBAction action : this) {
			executed.addAll(database.executeDBAction(action));
		}
		return executed;
	}

	/**
	 * Provides a list of {@link DBAction DBActions} intended to revert changed
	 * rows to their previous state.
	 *
	 * <p>
	 * Creating revert scripts is particularly tricky in databases so be sure to
	 * check that the revert script does what you intend.
	 *
	 * <p>
	 * The actions returned will be in the correct order to, for instance,
	 * re-insert a deleted row before updating it.
	 *
	 * <p>
	 * Despite the warning above this method works well for handling simple
	 * inserts and deletes, you should watch out for complex updates that may
	 * change a different selection from the original.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A DBactionList of DBActions required to revert the actions within
	 * this DBActionList.
	 */
	public DBActionList getRevertActionList() {
		DBAction[] toArray = this.toArray(new DBAction[]{});
		DBActionList reverts = new DBActionList();
		for (int i = toArray.length - 1; i >= 0; i--) {
			reverts.addAll(toArray[i].getRevertDBActionList());
		}
		return reverts;
	}
}
