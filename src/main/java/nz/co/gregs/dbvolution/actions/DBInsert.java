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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.InternalQueryableDatatypeProxy;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapperDefinition;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Provides support for the abstract concept of inserting rows.
 *
 * <p>
 * Inserting empty rows (meaning DBRows without any set fields) is supported for
 * any DBRow with an
 * {@link DBAutoIncrement autoincrementing} {@link DBPrimaryKey primary key}
 * field.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBInsert extends DBAction {

	private static final Log LOG = LogFactory.getLog(DBInsert.class);

	private transient StringBuilder allChangedColumns;
	private transient StringBuilder allSetValues;
	private final List<Long> generatedKeys = new ArrayList<>();
	private final DBRow originalRow;
	private StringBuilder allColumns;
	private StringBuilder allValues;

	/**
	 * Creates a DBInsert action for the row.
	 *
	 * @param <R> the table affected
	 * @param row the row to insert
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
	 * @param database the target database
	 * @param row the row to be inserted
	 * @throws SQLException Database actions can throw SQLException
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBActionList of the actions performed on the database.
	 */
	public static DBActionList save(DBDatabase database, DBRow row) throws SQLException {
		DBInsert dbInsert = new DBInsert(row);
		final DBActionList executedActions = dbInsert.execute(database);
		final List<QueryableDatatype<?>> primaryKeys = row.getPrimaryKeys();
		boolean pksHaveBeenSet = true;
		for (QueryableDatatype<?> pk : primaryKeys) {
			pksHaveBeenSet = pksHaveBeenSet && pk.hasBeenSet();
		}
		if (!dbInsert.generatedKeys.isEmpty() && !pksHaveBeenSet) {
			final QueryableDatatype<?> pkQDT = primaryKeys.get(0);
			new InternalQueryableDatatypeProxy(pkQDT).setValue(dbInsert.generatedKeys.get(0));
		}
		return executedActions;
	}

	@Override
	public ArrayList<String> getSQLStatements(DBDatabase db) {
		DBRow row = getRow();
		DBDefinition defn = db.getDefinition();
		processAllFieldsForInsert(db, row);

		ArrayList<String> strs = new ArrayList<>();
		if (allChangedColumns.length() != 0) {
			strs.add(defn.beginInsertLine()
					+ defn.formatTableName(row)
					+ defn.beginInsertColumnList()
					+ allChangedColumns
					+ defn.endInsertColumnList()
					+ allSetValues
					+ defn.endInsertLine());
		} else {
			strs.add(defn.beginInsertLine()
					+ defn.formatTableName(row)
					+ defn.beginInsertColumnList()
					+ allColumns
					+ defn.endInsertColumnList()
					+ allValues
					+ defn.endInsertLine());
		}
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
				if (defn.supportsGeneratedKeys()) {
					try {
						final List<QueryableDatatype<?>> primaryKeys = row.getPrimaryKeys();
						if (primaryKeys == null || primaryKeys.size() == 0) {
							statement.execute(sql);
						} else if (primaryKeys.size() == 1) {
							QueryableDatatype<?> primaryKey = primaryKeys.get(0);
							String primaryKeyColumnName = row.getPrimaryKeyColumnNames().get(0);
							Integer pkIndex = row.getPrimaryKeyIndexes().get(0);
							if (pkIndex == null || primaryKeyColumnName == null) {
								statement.execute(sql);
							} else {
								if (primaryKeyColumnName.isEmpty()) {
									statement.execute(sql, Statement.RETURN_GENERATED_KEYS);
								} else {
									statement.execute(sql, new String[]{db.getDefinition().formatPrimaryKeyForRetrievingGeneratedKeys(primaryKeyColumnName)});
									pkIndex = 1;
								}
								if (primaryKey.hasBeenSet() == false) {
									ResultSet generatedKeysResultSet = statement.getGeneratedKeys();
									try {
										while (generatedKeysResultSet.next()) {
											final long pkValue = generatedKeysResultSet.getLong(pkIndex);
											if (pkValue > 0) {
												this.getGeneratedPrimaryKeys().add(pkValue);
												QueryableDatatype<?> pkQDT = this.originalRow.getPrimaryKeys().get(0);
												new InternalQueryableDatatypeProxy(pkQDT).setValue(pkValue);
												pkQDT = row.getPrimaryKeys().get(0);
												new InternalQueryableDatatypeProxy(pkQDT).setValue(pkValue);
											}
										}
									} catch (SQLException ex) {
										throw new RuntimeException(ex);
									} finally {
										generatedKeysResultSet.close();
									}
								}
							}
						}
					} catch (SQLException sqlex) {
						try {
							statement.execute(sql);
						} catch (SQLException ex) {
							throw new RuntimeException(sql, ex);
						}
					}
				} else {
					try {
						statement.execute(sql);
						final List<PropertyWrapper> primaryKeyWrappers = row.getPrimaryKeyPropertyWrappers();
						if (primaryKeyWrappers.size() > 0) {
							if (defn.supportsRetrievingLastInsertedRowViaSQL()) {
								String retrieveSQL = defn.getRetrieveLastInsertedRowSQL();
								ResultSet rs = statement.executeQuery(retrieveSQL);
								try {
									for (PropertyWrapper primaryKeyWrapper : primaryKeyWrappers) {
										PropertyWrapperDefinition definition = primaryKeyWrapper.getPropertyWrapperDefinition();
										QueryableDatatype<?> originalPK = definition.getQueryableDatatype(this.originalRow);
										QueryableDatatype<?> rowPK = definition.getQueryableDatatype(row);

										if (originalPK.hasBeenSet() == false) {
											if ((originalPK instanceof DBInteger) && (rowPK instanceof DBInteger)) {
												DBInteger inPK = (DBInteger) originalPK;
												DBInteger inRowPK = (DBInteger) rowPK;
												inPK.setValue(rs.getLong(1));
												inRowPK.setValue(rs.getLong(1));
											} else if ((originalPK instanceof DBNumber) && (rowPK instanceof DBInteger)) {
												DBNumber inPK = (DBNumber) originalPK;
												inPK.setValue(rs.getBigDecimal(1));
												((DBInteger) rowPK).setValue(rs.getLong(1));
											} else if ((originalPK instanceof DBString) && (rowPK instanceof DBString)) {
												DBString inPK = (DBString) originalPK;
												inPK.setValue(rs.getString(1));
												inPK = (DBString) rowPK;
												inPK.setValue(rs.getString(1));
											}
										}
									}
								} finally {
									rs.close();
								}
							}
						}
					} catch (SQLException ex) {
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
						allValues.append(allValuesSeparator).append(qdt.toSQLString(database.getDefinition()));
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
						allSetValues.append(valuesSeparator).append(qdt.toSQLString(database.getDefinition()));
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
		DBActionList reverts = new DBActionList();
		DBRow row = DBRow.copyDBRow(originalRow);
		if (this.getRow().getPrimaryKeys() == null) {
			reverts.add(new DBDeleteUsingAllColumns(row));
		} else {
			reverts.add(new DBDeleteByPrimaryKey(row));
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
	 * @param rows the rows to be inserted
	 * @throws SQLException Database actions can throw SQLException
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBActionList of inserts.
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the generatedKeys
	 */
	public List<Long> getGeneratedPrimaryKeys() {
		return generatedKeys;
	}

	@Override
	protected String getPrimaryKeySQL(DBDatabase db, DBRow row) {
		StringBuilder sqlString = new StringBuilder();
		DBDefinition defn = db.getDefinition();
		List<QueryableDatatype<?>> primaryKeys = row.getPrimaryKeys();
		String separator = "";
		for (QueryableDatatype<?> pk : primaryKeys) {
			PropertyWrapper wrapper = row.getPropertyWrapperOf(pk);
			String pkValue = pk.toSQLString(db.getDefinition());
			//String pkValue = (pk.hasChanged() ? pk.getPreviousSQLValue(db) : pk.toSQLString(db));
			sqlString.append(separator)
					.append(defn.formatColumnName(wrapper.columnName()))
					.append(defn.getEqualsComparator())
					.append(pkValue);
			separator = defn.beginAndLine();
		}
		return sqlString.toString();
	}
}
