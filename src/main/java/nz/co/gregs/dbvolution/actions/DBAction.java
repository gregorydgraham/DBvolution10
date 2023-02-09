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

import java.io.Serializable;
import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.QueryIntention;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
 * particularly {@link DBDelete#getDeletes(nz.co.gregs.dbvolution.databases.DBDatabase, nz.co.gregs.dbvolution.DBRow...)  }, {@link DBUpdate#getUpdates(nz.co.gregs.dbvolution.DBRow...)
 * }, and {@link DBInsert#getInserts(nz.co.gregs.dbvolution.DBRow...) },
 * allowing a series of changes to be created then executed in a single batch.
 *
 * @author Gregory Graham
 */
public abstract class DBAction implements Serializable {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(DBInsert.class);

	final DBRow row;
	private RefetchRequirement refetchStatus = RefetchRequirement.REFETCH;

	protected final QueryIntention intention;
	
	/**
	 * Standard action constructor.
	 *
	 * <p>
	 * Saves a copy of the row to ensure immutability.</p>
	 *
	 * @param <R> the table that this action applies to.
	 * @param row the row or example that this action applies to.
	 * @param intent the specific intention of this action, a description of what is expected to occur
	 */
	public <R extends DBRow> DBAction(R row, QueryIntention intent) {
		super();
		if (row != null) {
			this.row = DBRow.copyDBRow(row);
		} else {
			this.row = row;
		}
		this.intention = intent;
	}
	
	public QueryIntention getIntent(){
		return intention;
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
	 * Revert actions are tricky to implement correctly, so be sure to check that
	 * the revert will produce the desired result.
	 *
	 * @return a list of all the actions required to revert this action in the
	 * order they need to enacted.
	 */
	protected abstract DBActionList getRevertDBActionList();

	/**
	 * Returns a copy of the row supplied during creation.
	 *
	 * @return the row
	 */
	protected DBRow getRow() {
		return DBRow.copyDBRow(row);
	}

	/**
	 * Returns a string that can be used in the WHERE clause to identify the rows
	 * affected by this DBAction.
	 *
	 * <p>
	 * Used internally during UPDATE and INSERT.</p>
	 *
	 * @param row the row that will be used in the method
	 * @param db the database to execute the DBAction on
	 *
	 * @return a string representing the
	 */
	protected String getPrimaryKeySQL(DBDatabase db, DBRow row) {
		final DBDefinition definition = db.getDefinition();
		StringBuilder sqlString = new StringBuilder();
		List<QueryableDatatype<?>> primaryKeys = row.getPrimaryKeys();
		String separator = "(";
		for (QueryableDatatype<?> pk : primaryKeys) {
			var wrapper = row.getPropertyWrapperOf(pk);
			String pkValue = (pk.hasChanged() ? pk.getPreviousSQLValue(definition) : pk.toSQLString(definition));
			sqlString.append(separator)
					.append(definition.formatColumnName(wrapper.columnName()))
					.append(definition.getEqualsComparator())
					.append(pkValue);
			separator = definition.beginAndLine();
		}
		return sqlString.append(")").toString();
	}

	/**
	 * Returns a list of the SQL statements that this DBAction will produce for
	 * the specified database.
	 *
	 * <p>
	 * Actions happen all by themselves but when you want to know what will
	 * actually happen, use this method to get a complete list of all the SQL
	 * required.
	 *
	 * @param db the database that the SQL must be appropriate for.
	 * @return the list of SQL strings that equates to this action.
	 */
	public abstract List<String> getSQLStatements(DBDatabase db);

	/**
	 * Performs the DB execute and returns a list of all actions performed in the
	 * process.
	 *
	 * <p>
	 * The supplied row will be changed by the action in an appropriate way,
	 * however the Action will contain an unchanged and unchangeable copy of the
	 * row for internal use.
	 *
	 * @param db the target database.
	 * @return The complete list of all actions performed to complete this action
	 * on the database
	 * @throws SQLException Database operations may throw SQLExceptions
	 */
	public abstract DBActionList execute(DBDatabase db) throws SQLException;

	public boolean requiresRunOnIndividualDatabaseBeforeCluster() {
		return false;
	}

	public boolean runOnDatabaseDuringCluster(DBDatabase initialDatabase, DBDatabase next) {
		return true;
	}

	protected void refetchIfClusterRequires(DBDatabase db, DBRow originalRow) {
		try {
			if (refetchNeeded()) {
				if (originalRow.hasAutomaticValueFields()) {
					if (originalRow.getPrimaryKeys().size() > 0) {
						updateRefetchRequirementForOtherDatabases();
						DBRow example = DBRow.getPrimaryKeyExample(originalRow);
						DBRow newRow = db
								.getDBTable(example)
								.setQueryLabel("AUTOMATIC REFETCH")
								.getOnlyRow();
						List<PropertyWrapper<?, ?, ?>> props = originalRow.getColumnPropertyWrappers();
						props.stream().filter(p -> p != null).forEach(p -> p.copyFromRowToOtherRow(newRow, originalRow));
					}
				}
			}
		} catch (SQLException ex) {
			LOG.fatal(null, ex);
		} catch (UnexpectedNumberOfRowsException ex) {
			LOG.fatal(null, ex);
		} catch (AccidentalBlankQueryException ex) {
			LOG.fatal(null, ex);
		} catch (NoAvailableDatabaseException ex) {
			LOG.fatal(null, ex);
		}
	}

	private boolean refetchNeeded() {
		return RefetchRequirement.REFETCH.equals(refetchStatus);
	}

	protected void updateRefetchRequirementForOtherDatabases() {
		setRefetchStatus(RefetchRequirement.REFETCH);
	}

	protected void setRefetchStatus(RefetchRequirement refetchStatus) {
		this.refetchStatus = refetchStatus;
	}

	public static enum RefetchRequirement {
		REFETCH,
		DO_NOT_REFETCH
	}
}
