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
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.AccidentalUpdateOfUndefinedRowException;

/**
 * Provides support for the abstract concept of updating rows.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public abstract class DBUpdate extends DBAction {

	private final static long serialVersionUID = 1l;

	/**
	 * Creates a DBUpdate action for the row supplied.
	 *
	 * @param <R> the table affected
	 * @param row the row to be updated
	 */
	public <R extends DBRow> DBUpdate(R row) {
		super(row);
	}

	/**
	 * Executes required update actions for the row and returns a
	 * {@link DBActionList} of those actions.
	 *
	 * The original rows are not changed by this method, or any DBUpdate method.
	 * Use {@link DBRow#setSimpleTypesToUnchanged() } if you need to ignore the
	 * changes to the row.
	 *
	 * @param db the target database
	 * @param row the row to be updated
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBActionList of updates that have been executed.
	 * @throws SQLException database exceptions
	 */
	public static DBActionList update(DBDatabase db, DBRow row) throws SQLException {
		DBActionList updates = getUpdates(row);
		for (DBAction act : updates) {
			db.executeDBAction(act);
		}
		return updates;
	}

	/**
	 * Creates a DBActionList of update actions for the rows.
	 *
	 * <p>
	 * The actions created can be applied on a particular database using
	 * {@link DBActionList#execute(nz.co.gregs.dbvolution.databases.DBDatabase)}
	 *
	 * <p>
	 * Synonym for {@link #getUpdates(nz.co.gregs.dbvolution.DBRow...) }
	 *
	 * @param rows the rows to be updated
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBActionList of updates.
	 * @throws SQLException database exceptions
	 */
	public static DBActionList update(DBRow... rows) throws SQLException {
		return getUpdates(rows);
	}

	/**
	 * Creates a DBActionList of update actions for the rows.
	 *
	 * <p>
	 * The actions created can be applied on a particular database using
	 * {@link DBActionList#execute(nz.co.gregs.dbvolution.databases.DBDatabase)}
	 *
	 * @param rows the rows to be updated
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBActionList of updates.
	 * @throws SQLException database exceptions
	 */
	public static DBActionList getUpdates(DBRow... rows) throws SQLException {
		DBActionList updates = new DBActionList();
		for (DBRow row : rows) {
			if (row.getDefined()) {
				if (row.hasChangedSimpleTypes()) {
					final List<QueryableDatatype<?>> primaryKeys = row.getPrimaryKeys();
					if (primaryKeys == null || primaryKeys.isEmpty()) {
						updates.add(new DBUpdateSimpleTypesUsingAllColumns(row));
					} else {
						updates.add(new DBUpdateSimpleTypes(row));
					}
				}
				if (hasChangedLargeObjects(row)) {
					updates.add(new DBUpdateLargeObjects(row));
				}
			} else {
				throw new AccidentalUpdateOfUndefinedRowException(row);
			}
		}
		return updates;
	}

	private static boolean hasChangedLargeObjects(DBRow row) {
		if (row.hasLargeObjects()) {
			for (QueryableDatatype<?> qdt : row.getLargeObjects()) {
				if (qdt.hasChanged()) {
					return true;
				}
			}
		}
		return false;
	}

}
