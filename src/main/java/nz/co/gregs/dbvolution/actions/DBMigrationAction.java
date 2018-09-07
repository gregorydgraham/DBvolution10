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
import nz.co.gregs.dbvolution.DBMigration;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Provides support for the abstract concept of migrating rows from one or more
 * tables to another table.
 *
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 * @param <R> the resulting DBRow from this DBQueryInsertAction
 */
public class DBMigrationAction<R extends DBRow> extends DBAction {

	private static final long serialVersionUID = 1l;
	
	private static final Log LOG = LogFactory.getLog(DBQueryInsertAction.class);

//	private transient StringBuilder allChangedColumns;
//	private transient StringBuilder allSetValues;
	private final DBMigration<R> sourceMigration;
	private final DBRow[] extraExamples;
//	private StringBuilder allColumns;
//	private StringBuilder allValues;

	/**
	 * Creates a DBMigrate action for the row.
	 *
	 * @param migration the mapping to transform the source data
	 * @param resultRow the resulting DBRow produced by the mapping
	 * @param examples extra examples used to reduce the source data set.
	 */
	public DBMigrationAction(DBMigration<R> migration, DBRow resultRow, DBRow... examples) {
		super(resultRow);
		sourceMigration = migration;
		extraExamples = examples;
	}

	/**
	 * Perform the migration
	 *
	 * @param database the database used by this action
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBActionList of the migration's effects
	 * @throws SQLException SQL Exceptions may be thrown
	 */
	public DBActionList migrate(DBDatabase database) throws SQLException {
		DBMigrationAction<R> migrate = new DBMigrationAction<>(sourceMigration, getRow());
		final DBActionList executedActions = database.executeDBAction(migrate);
		return executedActions;
	}

	@Override
	@SuppressWarnings("unchecked")
	public ArrayList<String> getSQLStatements(DBDatabase db) {
		DBRow table = getRow();
		DBDefinition defn = db.getDefinition();
		String allColumns = processAllFieldsForMigration(db, (R) getRow());

		ArrayList<String> strs = new ArrayList<>();
		strs.add(defn.beginInsertLine()
				+ defn.formatTableName(table)
				+ defn.beginInsertColumnList()
				+ allColumns
				+ defn.endInsertColumnList()
				+ sourceMigration.getSQLForQuery(db, extraExamples));
		return strs;
	}

	@Override
	public DBActionList execute(DBDatabase db) throws SQLException {
		DBActionList actions = new DBActionList(new DBMigrationAction<>(sourceMigration, getRow(), extraExamples));

		try (DBStatement statement = db.getDBStatement()) {
			for (String sql : getSQLStatements(db)) {
				try {
					statement.execute(sql);
				} catch (SQLException sqlex) {
					try {
						statement.execute(sql);
					} catch (SQLException ex) {
						throw new SQLException(ex.getLocalizedMessage()+":"+sql, ex);
					}
				}
			}
		}
		return actions;
	}

	private String processAllFieldsForMigration(DBDatabase database, R row) {
		StringBuilder allColumns = new StringBuilder();
		StringBuilder allValues = new StringBuilder();
		StringBuilder allChangedColumns = new StringBuilder();
		StringBuilder allSetValues = new StringBuilder();
		DBDefinition defn = database.getDefinition();
		List<PropertyWrapper> props = row.getColumnPropertyWrappers();
		String allColumnSeparator = "";
		String columnSeparator = "";
		String valuesSeparator = defn.beginValueClause();
		String allValuesSeparator = defn.beginValueClause();
		for (PropertyWrapper prop : props) {
			// BLOBS are not inserted normally so don't include them
			if (prop.isColumn()) {
				final QueryableDatatype<?> qdt = prop.getQueryableDatatype();
				if (!(qdt instanceof DBLargeObject)) {
					//support for inserting empty rows in a table with an autoincrementing pk
					if (!prop.isAutoIncrement()) {
						allColumns
								.append(allColumnSeparator)
								.append(" ")
								.append(defn.formatColumnName(prop.columnName()));
						allColumnSeparator = defn.getValuesClauseColumnSeparator();
						// add the value
						allValues
								.append(allValuesSeparator)
								.append(qdt.toSQLString(database.getDefinition()));
						allValuesSeparator = defn.getValuesClauseValueSeparator();
					}
					if (qdt.hasBeenSet()) {
						// nice normal columns
						// Add the column
						allChangedColumns
								.append(columnSeparator)
								.append(" ")
								.append(defn.formatColumnName(prop.columnName()));
						columnSeparator = defn.getValuesClauseColumnSeparator();
						// add the value
						allSetValues
								.append(valuesSeparator)
								.append(qdt.toSQLString(database.getDefinition()));
						valuesSeparator = defn.getValuesClauseValueSeparator();
					}
				}
			}
		}
		allValues.append(defn.endValueClause());
		allSetValues.append(defn.endValueClause());
		return allColumns.toString();
	}

	@Override
	protected DBActionList getRevertDBActionList() {
		throw new UnsupportedOperationException("Reverting A Migration Is Not Possible Yet.");
	}

//	@Override
//	protected DBActionList getActions() {//DBRow row) {
//		return new DBActionList(new DBInsert(getRow()));
//	}
}
