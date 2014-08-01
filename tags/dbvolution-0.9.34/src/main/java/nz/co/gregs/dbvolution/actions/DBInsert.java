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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Provides support for the abstract concept of inserting rows.
 *
 * @author gregorygraham
 */
public class DBInsert extends DBAction {

	private static final Log log = LogFactory.getLog(DBInsert.class);

	private transient StringBuilder allColumns;
	private transient StringBuilder allValues;
	private final List<Long> generatedKeys = new ArrayList<Long>();
	private final DBRow originalRow;

	/**
	 * Creates a DBInsert action for the row.
	 *
	 * @param <R>
	 * @param row
	 */
	protected <R extends DBRow> DBInsert(R row) {
		super(row);
		originalRow = row;
	}

	/**
	 * Saves the row to the database.
	 *
	 * Creates the appropriate actions to save the row permanently in the database
	 * and executes them.
	 * <p>
	 * Supports automatic retrieval of the new primary key in limited cases:
	 * <ul>
	 * <li>If the database supports generated keys, </li>
	 * <li> and the primary key has not been set, </li>
	 * <li>and there is exactly one generated key</li>
	 * <li>then the primary key will be set to the generated key.</li>
	 * </ul>
	 *
	 * @param database
	 * @param row
	 * @return a DBActionList of the actions performed on the database.
	 * @throws SQLException
	 */
	public static DBActionList save(DBDatabase database, DBRow row) throws SQLException {
		DBInsert dbInsert = new DBInsert(row);
		final DBActionList executedActions = dbInsert.execute(database);
		if (dbInsert.generatedKeys.size() == 1 && !row.getPrimaryKey().hasBeenSet()) {
			row.getPrimaryKey().setValue(dbInsert.generatedKeys.get(0));
		}
		return executedActions;
	}

	@Override
	public ArrayList<String> getSQLStatements(DBDatabase db) {
		DBRow row = getRow();
		DBDefinition defn = db.getDefinition();
		processAllFieldsForInsert(db, row);

		ArrayList<String> strs = new ArrayList<String>();
		strs.add(defn.beginInsertLine()
				+ defn.formatTableName(row)
				+ defn.beginInsertColumnList()
				+ allColumns
				+ defn.endInsertColumnList()
				+ allValues
				+ defn.beginReturningClause(row)
				+ defn.endInsertLine());
		return strs;
	}

	@Override
	protected DBActionList execute(DBDatabase db) throws SQLException {
		final DBDefinition defn = db.getDefinition();
		DBRow row = getRow();
		DBActionList actions = new DBActionList(new DBInsert(row));

		DBStatement statement = db.getDBStatement();
		try {
			for (String sql : getSQLStatements(db)) {
				if (defn.supportsGeneratedKeys(null)) {
					try {
						String primaryKeyColumnName = row.getPrimaryKeyColumnName();
						Integer pkIndex = row.getPrimaryKeyIndex();
						if (pkIndex == null || primaryKeyColumnName == null) {
							statement.execute(sql);
						} else {
							if (primaryKeyColumnName == null || primaryKeyColumnName.isEmpty()) {
								statement.execute(sql, Statement.RETURN_GENERATED_KEYS);
							} else {
								statement.execute(sql, new String[]{primaryKeyColumnName});
								pkIndex = 1;
							}

							ResultSet generatedKeysResultSet = statement.getGeneratedKeys();
							try {
								while (generatedKeysResultSet.next()) {
									final long pkValue = generatedKeysResultSet.getLong(pkIndex);
									this.getGeneratedPrimaryKeys().add(pkValue);
									log.info("GENERATED KEYS: " + pkValue);
									this.originalRow.getPrimaryKey().setValue(pkValue);
								}
							} catch (SQLException ex) {
								throw new RuntimeException(ex);
							} finally {
								generatedKeysResultSet.close();
							}
						}
					} catch (SQLException sqlex) {
						try {
							sqlex.printStackTrace();
							statement.execute(sql);
						} catch (SQLException ex) {
							throw new RuntimeException(ex);
						}
					}
				} else {
					try {
						statement.execute(sql);
						if (defn.supportsRetrievingLastInsertedRowViaSQL()) {
							String retrieveSQL = defn.getRetrieveLastInsertedRowSQL();
							ResultSet rs = statement.executeQuery(retrieveSQL);
							try {
								QueryableDatatype originalPK = this.originalRow.getPrimaryKey();
								QueryableDatatype rowPK = row.getPrimaryKey();
								if ((originalPK instanceof DBInteger) && (rowPK instanceof DBInteger)) {
									DBInteger inPK = (DBInteger) originalPK;
									DBInteger inRowPK = (DBInteger) rowPK;
									inPK.setValue(rs.getLong(1));
									inRowPK.setValue(rs.getLong(1));
								} else if ((originalPK instanceof DBNumber) && (rowPK instanceof DBInteger)) {
									DBNumber inPK = (DBNumber) originalPK;
									inPK.setValue(rs.getBigDecimal(1));
									inPK = (DBNumber) rowPK;
									inPK.setValue(rs.getBigDecimal(1));
								} else if ((originalPK instanceof DBString) && (rowPK instanceof DBString)) {
									DBString inPK = (DBString) originalPK;
									inPK.setValue(rs.getString(1));
									inPK = (DBString) rowPK;
									inPK.setValue(rs.getString(1));
								}
							} finally {
								rs.close();
							}
						}
					} catch (SQLException ex) {
						ex.printStackTrace();
						throw new RuntimeException(ex);
					}
				}
			}
		} finally {
			statement.close();
		}
		DBInsertLargeObjects blobSave = new DBInsertLargeObjects(this.originalRow);
		actions.addAll(blobSave.execute(db));
		row.setDefined();
		return actions;
	}

	private void processAllFieldsForInsert(DBDatabase database, DBRow row) {
		allColumns = new StringBuilder();
		allValues = new StringBuilder();
		DBDefinition defn = database.getDefinition();
		List<PropertyWrapper> props = row.getPropertyWrappers();
		String columnSeparator = "";
		String valuesSeparator = defn.beginValueClause();
		for (PropertyWrapper prop : props) {
			// BLOBS are not inserted normally so don't include them
			if (prop.isColumn()) {
				final QueryableDatatype qdt = prop.getQueryableDatatype();
				if (!(qdt instanceof DBLargeObject) && qdt.hasBeenSet()) {
					// nice normal columns
					// Add the column
					allColumns
							.append(columnSeparator)
							.append(" ")
							.append(defn.formatColumnName(prop.columnName()));
					columnSeparator = defn.getValuesClauseColumnSeparator();
					// add the value
					allValues.append(valuesSeparator).append(qdt.toSQLString(database));
					valuesSeparator = defn.getValuesClauseValueSeparator();
				}
			}
		}
		allValues.append(defn.endValueClause());
	}

	@Override
	protected DBActionList getRevertDBActionList() {
		DBActionList reverts = new DBActionList();
		if (this.getRow().getPrimaryKey() == null) {
			reverts.add(new DBDeleteUsingAllColumns(getRow()));
		} else {
			reverts.add(new DBDeleteByPrimaryKey(getRow()));
		}
		return reverts;
	}

	@Override
	protected DBActionList getActions() {//DBRow row) {
		return new DBActionList(new DBInsert(getRow()));
	}

	/**
	 * Creates a DBActionList of inserts actions for the rows.
	 *
	 * <p>
	 * The actions created can be applied on a particular database using
	 * {@link DBActionList#execute(nz.co.gregs.dbvolution.DBDatabase)}
	 *
	 * @param rows
	 * @return a DBActionList of inserts.
	 * @throws SQLException
	 */
	public static DBActionList getInserts(DBRow... rows) throws SQLException {
		DBActionList inserts = new DBActionList();
		for (DBRow row : rows) {
			inserts.add(new DBInsert(row));
		}
		return inserts;
	}

	/**
	 * Returns all generated values created during the insert actions.
	 *
	 * @return the generatedKeys
	 */
	public List<Long> getGeneratedPrimaryKeys() {
		return generatedKeys;
	}
}
