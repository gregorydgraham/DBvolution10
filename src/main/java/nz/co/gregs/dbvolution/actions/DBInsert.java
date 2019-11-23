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
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.databases.QueryIntention;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.InternalQueryableDatatypeProxy;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.DBSQLException;
import nz.co.gregs.dbvolution.exceptions.LoopDetectedInRecursiveSQL;
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

	private static final long serialVersionUID = 1l;

	private static final Log LOG = LogFactory.getLog(DBInsert.class);

	private final List<Long> generatedKeys = new ArrayList<>();
	private final DBRow originalRow;
	private boolean primaryKeyWasGenerated = false;
	private Long primaryKeyGenerated = null;

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
	 * <p>
	 * For clustered databases, any primary key generated by the first database is
	 * used in all databases.
	 * </p>
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
		final DBActionList executedActions = database.executeDBAction(dbInsert);
		final List<QueryableDatatype<?>> primaryKeys = row.getPrimaryKeys();
		boolean pksHaveBeenSet = true;
		for (QueryableDatatype<?> pk : primaryKeys) {
			pksHaveBeenSet = pksHaveBeenSet && pk.hasBeenSet();
		}
		if (!dbInsert.generatedKeys.isEmpty() && !pksHaveBeenSet) {
			final QueryableDatatype<?> pkQDT = primaryKeys.get(0);
			new InternalQueryableDatatypeProxy(pkQDT).setValueFromDatabase(dbInsert.generatedKeys.get(0));
		}
		row.setSimpleTypesToUnchanged();
		return executedActions;
	}

	/**
	 * Returns a copy of the row supplied during creation.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the row
	 */
	@Override
	protected DBRow getRow() {
		return DBRow.copyDBRow(originalRow);
	}

	@Override
	public ArrayList<String> getSQLStatements(DBDatabase db) {
		DBRow table = getRow();
		DBDefinition defn = db.getDefinition();
		InsertFields fields = processAllFieldsForInsert(db, table);

		ArrayList<String> strs = new ArrayList<>();
		strs.addAll(defn.getInsertPreparation(table));
		final StringBuilder allChangedColumns = fields.getAllChangedColumns();
		if (allChangedColumns.length() != 0) {
			strs.add(defn.beginInsertLine()
					+ defn.formatTableName(table)
					+ defn.beginInsertColumnList()
					+ allChangedColumns
					+ defn.endInsertColumnList()
					+ fields.getAllSetValues()
					+ defn.endInsertLine());
		} else {
			strs.add(defn.beginInsertLine()
					+ defn.formatTableName(table)
					+ defn.beginInsertColumnList()
					+ fields.getAllColumns()
					+ defn.endInsertColumnList()
					+ fields.getAllValues()
					+ defn.endInsertLine());
		}
		strs.addAll(defn.getInsertCleanUp(table));
		return strs;
	}

	@Override
	public DBActionList execute(DBDatabase db) throws SQLException {
		final DBDefinition defn = db.getDefinition();
		DBRow table = originalRow;
		final DBInsert newInsert = new DBInsert(table);
		DBActionList actions = new DBActionList(newInsert);

		try (DBStatement statement = db.getDBStatement()) {
			for (String sql : getSQLStatements(db)) {
				if (defn.supportsGeneratedKeys()) {
					try {
						final List<QueryableDatatype<?>> primaryKeys = table.getPrimaryKeys();
						if (primaryKeys == null || primaryKeys.isEmpty()) {
							// There are no primary keys so execute and move on.
							statement.execute(sql, QueryIntention.INSERT_ROW);
						} else {
							boolean allPKsHaveBeenSet = true;
							for (QueryableDatatype<?> primaryKey : primaryKeys) {
								allPKsHaveBeenSet &= primaryKey.hasBeenSet();
							}
							if (allPKsHaveBeenSet) {
								// The primary key has already been sorted for us so execute and move on.
								statement.execute(sql,QueryIntention.INSERT_ROW);
							} else {
								if (primaryKeys.size() == 1) {
									QueryableDatatype<?> primaryKey = primaryKeys.get(0);
									String primaryKeyColumnName = table.getPrimaryKeyColumnNames().get(0);
									Integer pkIndex = table.getPrimaryKeyIndexes().get(0);
									if (pkIndex == null || primaryKeyColumnName == null) {
										// We can't find the PK so just execute and move on.
										statement.execute(sql, QueryIntention.INSERT_ROW);
									} else {
										// There is a PK, it's not set, and we can find it, so we need to get it's value...
										if (primaryKeyColumnName.isEmpty()) {
											// Not sure of the column name, so ask for the keys and cross fingers.
											statement.execute(sql, Statement.RETURN_GENERATED_KEYS);
										} else {
											// execute and ask for the column specifically, also cross fingers.
											statement.execute(sql, new String[]{db.getDefinition().formatPrimaryKeyForRetrievingGeneratedKeys(primaryKeyColumnName)}, QueryIntention.INSERT_ROW);
											pkIndex = 1;
										}
										if (primaryKey.hasBeenSet() == false) {
											try (ResultSet generatedKeysResultSet = statement.getGeneratedKeys()) {
												while (generatedKeysResultSet.next()) {
													final Long pkValue = generatedKeysResultSet.getLong(pkIndex);
													if (pkValue > 0) {
														setPrimaryKeyGenerated(pkValue);
														this.getGeneratedPrimaryKeys().add(pkValue);
														setPrimaryKeyOfStoredRows(pkValue, table, newInsert);
													}
												}
											} catch (SQLException ex) {
												throw new RuntimeException(ex);
											}
										}
									}
								} else {
									throw new UnsupportedOperationException("Multiple auto-increment primary keys on a row are not yet supported:" + sql);
								}
							}
						}
						updateSequenceIfNecessary(defn, db, sql, table, statement);
					} catch (SQLException sqlex) {
						try {
							statement.execute(sql, QueryIntention.INSERT_ROW);
						} catch (SQLException ex) {
							throw new DBSQLException(db, sql, sqlex);
						}
					}
				} else {
					try {
						statement.execute(sql, QueryIntention.INSERT_ROW);
						final List<PropertyWrapper> primaryKeyWrappers = table.getPrimaryKeyPropertyWrappers();
						if (primaryKeyWrappers.size() > 0) {
							if (defn.supportsRetrievingLastInsertedRowViaSQL()) {
								String retrieveSQL = defn.getRetrieveLastInsertedRowSQL();
								try (ResultSet rs = statement.executeQuery(retrieveSQL, "RETRIEVE LAST INSERT", QueryIntention.RETRIEVE_LAST_INSERT)) {
									for (PropertyWrapper primaryKeyWrapper : primaryKeyWrappers) {
										PropertyWrapperDefinition definition = primaryKeyWrapper.getPropertyWrapperDefinition();
										QueryableDatatype<?> originalPK = definition.getQueryableDatatype(this.originalRow);
										QueryableDatatype<?> rowPK = definition.getQueryableDatatype(table);

										if (originalPK.hasBeenSet() == false) {
											if ((originalPK instanceof DBInteger) && (rowPK instanceof DBInteger)) {
												final long generatedPK = rs.getLong(1);
												setPrimaryKeyGenerated(generatedPK);
												DBInteger inPK = (DBInteger) originalPK;
												DBInteger inRowPK = (DBInteger) rowPK;
												inPK.setValue(generatedPK);
												inRowPK.setValue(generatedPK);
											} else if ((originalPK instanceof DBNumber) && (rowPK instanceof DBInteger)) {
												final long generatedPK = rs.getLong(1);
												setPrimaryKeyGenerated(generatedPK);
												DBNumber inPK = (DBNumber) originalPK;
												inPK.setValue(rs.getBigDecimal(1));
												((DBInteger) rowPK).setValue(generatedPK);
											} else if ((originalPK instanceof DBString) && (rowPK instanceof DBString)) {
												DBString inPK = (DBString) originalPK;
												inPK.setValue(rs.getString(1));
												inPK = (DBString) rowPK;
												inPK.setValue(rs.getString(1));
											}
										}
									}
								}
							}
						}
						updateSequenceIfNecessary(defn, db, sql, table, statement);
					} catch (SQLException ex) {
						throw ex;
					}
				}
			}
		}
		DBInsertLargeObjects blobSave = new DBInsertLargeObjects(this.originalRow);
		actions.addAll(db.executeDBAction(blobSave));
		table.setDefined();
		return actions;
	}

	private void updateSequenceIfNecessary(final DBDefinition defn, DBDatabase db, String sql, DBRow table, final DBStatement statement) throws SQLException {
		if (primaryKeyWasGenerated && defn.requiresSequenceUpdateAfterManualInsert()) {
			final String sequenceUpdateSQL = defn.getSequenceUpdateSQL(table.getTableName(), table.getPrimaryKeyColumnNames().get(0), primaryKeyGenerated);
			statement.execute(sequenceUpdateSQL, QueryIntention.UPDATE_SEQUENCE);
		}
	}

	private synchronized void setPrimaryKeyOfStoredRows(final long pkValue, DBRow table, final DBInsert newInsert) {
		QueryableDatatype<?> pkQDT = this.originalRow.getPrimaryKeys().get(0);
		new InternalQueryableDatatypeProxy(pkQDT).setValueFromDatabase(pkValue);
		pkQDT = this.row.getPrimaryKeys().get(0);
		new InternalQueryableDatatypeProxy(pkQDT).setValueFromDatabase(pkValue);
		pkQDT = table.getPrimaryKeys().get(0);
		new InternalQueryableDatatypeProxy(pkQDT).setValueFromDatabase(pkValue);
		pkQDT = newInsert.row.getPrimaryKeys().get(0);
		new InternalQueryableDatatypeProxy(pkQDT).setValueFromDatabase(pkValue);
		pkQDT = newInsert.originalRow.getPrimaryKeys().get(0);
		new InternalQueryableDatatypeProxy(pkQDT).setValueFromDatabase(pkValue);
	}

	private InsertFields processAllFieldsForInsert(DBDatabase database, DBRow row) {
		InsertFields fields = new InsertFields();
		StringBuilder allColumns = fields.getAllColumns();
		StringBuilder allValues = fields.getAllValues();
		StringBuilder allChangedColumns = fields.getAllChangedColumns();
		StringBuilder allSetValues = fields.getAllSetValues();
		DBDefinition defn = database.getDefinition();
		List<PropertyWrapper> props = row.getColumnPropertyWrappers();
		String allColumnSeparator = "";
		String columnSeparator = "";
		String valuesSeparator = defn.beginValueClause();
		String allValuesSeparator = defn.beginValueClause();
		for (PropertyWrapper prop : props) {
			if (prop.isColumn() && !prop.hasColumnExpression()) {
				final QueryableDatatype<?> qdt = prop.getQueryableDatatype();
				if (qdt != null) {
					// BLOBS are not inserted normally so don't include them
					if (!(qdt instanceof DBLargeObject)) {
						//support for inserting empty rows in a table with an autoincrementing pk
						if (!prop.isAutoIncrement()) {
							allColumns
									.append(allColumnSeparator)
									.append(" ")
									.append(defn.formatColumnName(prop.columnName()));
							allColumnSeparator = defn.getValuesClauseColumnSeparator();
							// add the value
							allValues.append(allValuesSeparator);
							if (!qdt.hasBeenSet() && qdt.hasDefaultInsertValue()) {
								allValues.append(
										qdt.getDefaultInsertValueSQLString(database.getDefinition())
								);
							} else {
								allValues.append(
										qdt.toSQLString(database.getDefinition())
								);
							}
							allValuesSeparator = defn.getValuesClauseValueSeparator();
						}
						if (qdt.hasBeenSet() || qdt.hasDefaultInsertValue()) {
							// nice normal columns
							// Add the column
							allChangedColumns
									.append(columnSeparator)
									.append(" ")
									.append(defn.formatColumnName(prop.columnName()));
							columnSeparator = defn.getValuesClauseColumnSeparator();
							allSetValues.append(valuesSeparator);
							// add the value
							if (qdt.hasBeenSet()) {
								allSetValues.append(
										qdt.toSQLString(database.getDefinition())
								);
							} else if (qdt.hasDefaultInsertValue()) {
								allSetValues.append(
										qdt.getDefaultInsertValueSQLString(database.getDefinition())
								);
							}
							valuesSeparator = defn.getValuesClauseValueSeparator();
						}
					}
				}
			}
		}
		allValues.append(defn.endValueClause());
		allSetValues.append(defn.endValueClause());
		return fields;
	}

	@Override
	protected DBActionList getRevertDBActionList() {
		DBActionList reverts = new DBActionList();
		DBRow table = this.getRow();
		if (table.getPrimaryKeys() == null) {
			reverts.add(new DBDeleteUsingAllColumns(table));
		} else {
			reverts.add(new DBDeleteByPrimaryKey(table));
		}
		return reverts;
	}

	/**
	 * Creates a DBActionList of inserts actions for the rows.
	 *
	 * <p>
	 * The actions created can be applied on a particular database using
	 * {@link DBActionList#execute(nz.co.gregs.dbvolution.databases.DBDatabase)}
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

			sqlString.append(separator)
					.append(defn.formatColumnName(wrapper.columnName()))
					.append(defn.getEqualsComparator())
					.append(pkValue);
			separator = defn.beginAndLine();
		}
		return sqlString.toString();
	}

	private void setPrimaryKeyGenerated(long pkValue) {
		this.primaryKeyWasGenerated = true;
		if (this.primaryKeyGenerated == null) {
			primaryKeyGenerated = pkValue;
		}
	}

	@Override
	public boolean requiresRunOnIndividualDatabaseBeforeCluster() {
		return true;
	}

	@Override
	public boolean runOnDatabaseDuringCluster(DBDatabase initialDatabase, DBDatabase next) {
		return initialDatabase != next;
	}

	protected static class InsertFields {

		private final StringBuilder allColumns = new StringBuilder();
		private final StringBuilder allValues = new StringBuilder();
		private final StringBuilder allChangedColumns = new StringBuilder();
		private final StringBuilder allSetValues = new StringBuilder();

		public InsertFields() {
		}

		StringBuilder getAllColumns() {
			return this.allColumns;
		}

		StringBuilder getAllValues() {
			return this.allValues;
		}

		StringBuilder getAllChangedColumns() {
			return this.allChangedColumns;
		}

		StringBuilder getAllSetValues() {
			return this.allSetValues;
		}
	}
}
