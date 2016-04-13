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
import nz.co.gregs.dbvolution.DBDatabase;
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
 * Provides support for the abstract concept of migrating rows from one or more tables to another table.
 *
 *
 * @author Gregory Graham
 * @param <R>
 */
public class DBMigrate<R extends DBRow> extends DBAction {

	private static final Log LOG = LogFactory.getLog(DBInsert.class);

	private transient StringBuilder allChangedColumns;
	private transient StringBuilder allSetValues;
	private final DBMigration<R> sourceMigration;
	private final DBRow[] extraExamples;
	private StringBuilder allColumns;
	private StringBuilder allValues;

	/**
	 * Creates a DBMigrate action for the row.
	 *
	 * @param migration the row to insert
	 * @param source
	 * @param examples
	 */
	public DBMigrate(DBMigration<R> migration, DBRow source, DBRow... examples) {
		super(source);
		sourceMigration = migration;
		extraExamples = examples;
	}

	public DBActionList migrate(DBDatabase database) throws SQLException {
		DBMigrate<R> migrate = new DBMigrate<R>(sourceMigration, getRow());
		final DBActionList executedActions = migrate.execute(database);
		return executedActions;
	}

	@Override
	@SuppressWarnings("unchecked")
	public ArrayList<String> getSQLStatements(DBDatabase db) {
		DBRow row = getRow();
		DBDefinition defn = db.getDefinition();
		processAllFieldsForMigration(db, (R) getRow());

		ArrayList<String> strs = new ArrayList<String>();
			strs.add(defn.beginInsertLine()
					+ defn.formatTableName(row)
					+ defn.beginInsertColumnList()
					+ allColumns
					+ defn.endInsertColumnList()
					+ sourceMigration.getSQLForQuery(db, extraExamples));
		return strs;
	}

	@Override
	protected DBActionList execute(DBDatabase db) throws SQLException {
		final DBDefinition defn = db.getDefinition();
		DBActionList actions = new DBActionList(new DBMigrate<R>(sourceMigration, getRow(), extraExamples));

		DBStatement statement = db.getDBStatement();
		try {
			for (String sql : getSQLStatements(db)) {
					try {
						statement.execute(sql);
					} catch (SQLException sqlex) {
						try {
							sqlex.printStackTrace();
							statement.execute(sql);
						} catch (SQLException ex) {
							throw new RuntimeException(sql, ex);
						}
					}
			}
		} finally {
			statement.close();
		}
		return actions;
	}

	private void processAllFieldsForMigration(DBDatabase database, R row) {
		allColumns = new StringBuilder();
		allValues = new StringBuilder();
		allChangedColumns = new StringBuilder();
		allSetValues = new StringBuilder();
		DBDefinition defn = database.getDefinition();
		List<PropertyWrapper> props = row.getColumnPropertyWrappers();
		String allColumnSeparator = "";
		String columnSeparator = "";
		String valuesSeparator = defn.beginValueClause();
		String allValuesSeparator = defn.beginValueClause();
		for (PropertyWrapper prop : props) {
			// BLOBS are not inserted normally so don't include them
			if (prop.isColumn()) {
				final QueryableDatatype qdt = prop.getQueryableDatatype();
				if (!(qdt instanceof DBLargeObject)) {
					//support for inserting empty rows in a table with an autoincrementing pk
					if (!prop.isAutoIncrement()) {
						allColumns
								.append(allColumnSeparator)
								.append(" ")
								.append(defn.formatColumnName(prop.columnName()));
						allColumnSeparator = defn.getValuesClauseColumnSeparator();
						// add the value
						allValues.append(allValuesSeparator).append(qdt.toSQLString(database));
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
						allSetValues.append(valuesSeparator).append(qdt.toSQLString(database));
						valuesSeparator = defn.getValuesClauseValueSeparator();
					}
				}
			}
		}
		allValues.append(defn.endValueClause());
		allSetValues.append(defn.endValueClause());
	}

	@Override
	protected DBActionList getRevertDBActionList() {
		throw new UnsupportedOperationException("Reverting A Migration Is Not Possible Yet.");
	}

	@Override
	protected DBActionList getActions() {//DBRow row) {
		return new DBActionList(new DBInsert(getRow()));
	}
}
