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
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.databases.QueryIntention;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.internal.query.StatementDetails;

/**
 * Provides support for the abstract concept of updating rows without primary
 * keys.
 *
 * <p>
 * The best way to use this is by using {@link DBUpdate#getUpdates(nz.co.gregs.dbvolution.DBRow...)
 * } to automatically use this action.
 *
 * @author Gregory Graham
 */
public class DBUpdateForcedOnSimpleTypesUsingPrimaryKey extends DBUpdateSimpleTypes {

	private static final long serialVersionUID = 1l;

	DBUpdateForcedOnSimpleTypesUsingPrimaryKey(DBRow row) {
		super(row);
		var props = this.row.getNonPrimaryKeyNonDynamicPropertyWrappers();
		props.forEach(prop -> prop.getQueryableDatatype().setChanged());
	}

	/**
	 * Executes required update actions for the row and returns a
	 * {@link DBActionList} of those actions.The original rows are not changed by this method, or any DBUpdate method.
	 *
	 * Use {@link DBRow#setSimpleTypesToUnchanged() } if you need to ignore the
	 * changes to the row.
	 *
	 * @param db the target database
	 * @param row the row to be updated
	 * @return a list of the actions taken to action this update
	 * @throws SQLException database exceptions
	 */
	public static DBActionList updateAnyway(DBDatabase db, DBRow row) throws SQLException {
		DBActionList updates = getUpdateAnyways(row);
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
	 * @param rows the rows to be updated
	 * @return a DBActionList of updates.
	 * @throws SQLException database exceptions
	 */
	public static DBActionList getUpdateAnyways(DBRow... rows) throws SQLException {
		DBActionList updates = new DBActionList();
		for (DBRow row : rows) {
			final List<QueryableDatatype<?>> primaryKeys = row.getPrimaryKeys();
			if (primaryKeys == null || primaryKeys.isEmpty()) {
			} else {
				updates.add(new DBUpdateForcedOnSimpleTypesUsingPrimaryKey(row));
			}
			if (row.hasLargeObjects()) {
				updates.add(new DBUpdateLargeObjects(row));
			}
		}
		return updates;
	}

	@Override
	public List<String> getSQLStatements(DBDatabase db) {
		DBRow table = getRow();
		DBDefinition defn = db.getDefinition();

		StringBuilder sql = new StringBuilder()
				.append(defn.beginUpdateLine())
				.append(defn.formatTableName(table))
				.append(defn.beginSetClause())
				.append(getSetClause(db, table))
				.append(defn.beginWhereClause())
				.append(defn.getWhereClauseBeginningCondition());
		for (var prop : table.getPrimaryKeyPropertyWrappers()) {
			QueryableDatatype<?> qdt = prop.getQueryableDatatype();
			if (qdt.isNull()) {
				sql.append(defn.beginWhereClauseLine())
						.append(BooleanExpression.isNull(table.column(qdt)).toSQLString(defn));
			} else {
				sql.append(defn.beginWhereClauseLine())
						.append(prop.columnName())
						.append(defn.getEqualsComparator())
						.append(qdt.hasChanged() ? qdt.getPreviousSQLValue(defn) : qdt.toSQLString(defn));
			}
		}
		sql.append(defn.endDeleteLine());
		List<String> sqls = new ArrayList<>();
		sqls.add(sql.toString());
		return sqls;
	}

	/**
	 * Creates the required SET clause of the UPDATE statement.
	 *
	 * @param db the target database
	 * @param row the row to be updated
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return The SET clause of the UPDATE statement.
	 */
	@Override
	protected String getSetClause(DBDatabase db, DBRow row) {
		DBDefinition defn = db.getDefinition();
		StringBuilder sql = new StringBuilder();
		var fields = row.getColumnPropertyWrappers();

		String separator = defn.getStartingSetSubClauseSeparator();
		for (var field : fields) {
			if (field.isColumn() && !field.isPrimaryKey()) {
				final QueryableDatatype<?> qdt = field.getQueryableDatatype();
				if (qdt != null) {
					if (!(qdt instanceof DBLargeObject)) {
						if (qdt.hasBeenSet()) {
							String columnName = field.columnName();
							sql.append(separator)
									.append(defn.formatColumnName(columnName))
									.append(defn.getEqualsComparator())
									.append(qdt
											.toSQLString(defn));
							separator = defn.getSubsequentSetSubClauseSeparator();
						} else if (qdt.hasDefaultUpdateValue()) {
							String columnName = field.columnName();
							sql.append(separator)
									.append(defn.formatColumnName(columnName))
									.append(defn.getEqualsComparator())
									.append(qdt.getDefaultUpdateValueSQLString(defn));
							separator = defn.getSubsequentSetSubClauseSeparator();
						}
					}
				}
			}
		}

		return sql.toString();
	}

	@Override
	protected DBActionList getRevertDBActionList() {
		DBActionList dbActionList = new DBActionList();
		dbActionList.add(new DBUpdateToPreviousValues(this.getRow()));
		return dbActionList;
	}

	@Override
	public DBActionList execute(DBDatabase db) throws SQLException {
		DBRow table = getRow();
		DBActionList actions = new DBActionList(new DBUpdateForcedOnSimpleTypesUsingPrimaryKey(table));
		try (DBStatement statement = db.getDBStatement()) {
			for (String sql : getSQLStatements(db)) {
				statement.execute(new StatementDetails("Update row", QueryIntention.UPDATE_ROW, sql));
			}
		}
		return actions;
	}

}
