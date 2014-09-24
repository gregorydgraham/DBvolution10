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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Provides support for the abstract concept of updating rows with BLOB columns.
 *
 * <p>
 * The best way to use this is by using {@link DBUpdate#getUpdates(nz.co.gregs.dbvolution.DBRow...)
 * } to automatically use this action.
 *
 * @author Gregory Graham
 */
public class DBUpdateLargeObjects extends DBUpdate {

	private static final Log log = LogFactory.getLog(DBUpdateLargeObjects.class);

	/**
	 * Creates a DBUpdateLargeObjects action for the supplied row.
	 *
	 * @param row
	 */
	protected DBUpdateLargeObjects(DBRow row) {
		super(row);
	}

	@Override
	protected DBActionList execute(DBDatabase db) throws SQLException {
		DBRow row = getRow();
		DBActionList actions = new DBActionList();
		DBDefinition defn = db.getDefinition();
		DBStatement statement = db.getDBStatement();
		try {
			actions = new DBActionList();
			for (PropertyWrapper prop : getInterestingLargeObjects(row)) {
				final String col = prop.columnName();
				final DBLargeObject largeObject = (DBLargeObject) prop.getQueryableDatatype();

				if (defn.prefersLargeObjectsSetAsCharacterStream()) {
					setUsingCharacterStream(defn, row, col, largeObject, db, statement);
				} else if (defn.prefersLargeObjectsSetAsBase64String()) {
					setUsingBase64String(defn, row, col, largeObject, db, statement);
				} else {
					setUsingBinaryStream(defn, row, col, largeObject, db, statement);
				}
				DBUpdateLargeObjects update = new DBUpdateLargeObjects(row);
				actions.add(update);

				largeObject.setUnchanged();
			}
		} catch (IOException ex) {
			Logger.getLogger(DBUpdateLargeObjects.class.getName()).log(Level.SEVERE, null, ex);
			throw new DBRuntimeException("Can't Set LargeObject: IOError", ex);
		} finally {
			statement.close();
		}
		return actions;
	}

	private void setUsingStringValue(DBDefinition defn, DBRow row, final String col, final DBLargeObject largeObject, DBDatabase db, DBStatement statement) throws SQLException {
		String sqlString = defn.beginUpdateLine()
				+ defn.formatTableName(row)
				+ defn.beginSetClause()
				+ defn.formatColumnName(col)
				+ defn.getEqualsComparator()
				+ "'" + largeObject.stringValue() + "'"
				+ defn.beginWhereClause()
				+ defn.formatColumnName(row.getPrimaryKeyColumnName())
				+ defn.getEqualsComparator()
				+ row.getPrimaryKey().toSQLString(db)
				+ defn.endSQLStatement();
//					db.printSQLIfRequested(sqlString);
		log.info(sqlString);
		statement.execute(sqlString);
	}

	private void setUsingBinaryStream(DBDefinition defn, DBRow row, final String col, final DBLargeObject largeObject, DBDatabase db, DBStatement statement) throws SQLException {
		String sqlString = defn.beginUpdateLine()
				+ defn.formatTableName(row)
				+ defn.beginSetClause()
				+ defn.formatColumnName(col)
				+ defn.getEqualsComparator()
				+ defn.getPreparedVariableSymbol()
				+ defn.beginWhereClause()
				+ defn.formatColumnName(row.getPrimaryKeyColumnName())
				+ defn.getEqualsComparator()
				+ row.getPrimaryKey().toSQLString(db)
				+ defn.endSQLStatement();
//					db.printSQLIfRequested(sqlString);
		log.info(sqlString);
		PreparedStatement prep = statement.getConnection().prepareStatement(sqlString);
		prep.setBinaryStream(1, largeObject.getInputStream(), largeObject.getSize());
		prep.execute();
	}

	private void setUsingBase64String(DBDefinition defn, DBRow row, final String col, final DBLargeObject largeObject, DBDatabase db, DBStatement statement) throws SQLException, IOException {
		String sqlString = defn.beginUpdateLine()
				+ defn.formatTableName(row)
				+ defn.beginSetClause()
				+ defn.formatColumnName(col)
				+ defn.getEqualsComparator()
				+ defn.getPreparedVariableSymbol()
				+ defn.beginWhereClause()
				+ defn.formatColumnName(row.getPrimaryKeyColumnName())
				+ defn.getEqualsComparator()
				+ row.getPrimaryKey().toSQLString(db)
				+ defn.endSQLStatement();
//					db.printSQLIfRequested(sqlString);
		log.info(sqlString);
		PreparedStatement prep = statement.getConnection().prepareStatement(sqlString);
		InputStream inputStream = largeObject.getInputStream();

		InputStream input = new BufferedInputStream(inputStream);
		List<byte[]> byteArrays = new ArrayList<byte[]>();

		int totalBytesRead = 0;
		byte[] resultSetBytes;
		resultSetBytes = new byte[100000];
		int bytesRead = input.read(resultSetBytes);
		while (bytesRead > 0) {
			totalBytesRead += bytesRead;
			byteArrays.add(resultSetBytes);
			resultSetBytes = new byte[100000];
			bytesRead = input.read(resultSetBytes);
		}
		byte[] bytes = new byte[totalBytesRead];
		int bytesAdded = 0;
		for (byte[] someBytes : byteArrays) {
			System.arraycopy(someBytes, 0, bytes, bytesAdded, Math.min(someBytes.length, bytes.length - bytesAdded));
			bytesAdded += someBytes.length;
		}
		String b64encoded = Base64.encodeBase64String(bytes);
		//System.out.println("BYTES TO WRITE: " + Arrays.toString(bytes));
		prep.setString(1, b64encoded);
		prep.execute();
	}

	private void setUsingCharacterStream(DBDefinition defn, DBRow row, final String col, final DBLargeObject largeObject, DBDatabase db, DBStatement statement) throws SQLException {
		String sqlString = defn.beginUpdateLine()
				+ defn.formatTableName(row)
				+ defn.beginSetClause()
				+ defn.formatColumnName(col)
				+ defn.getEqualsComparator()
				+ defn.getPreparedVariableSymbol()
				+ defn.beginWhereClause()
				+ defn.formatColumnName(row.getPrimaryKeyColumnName())
				+ defn.getEqualsComparator()
				+ row.getPrimaryKey().toSQLString(db)
				+ defn.endSQLStatement();
//					db.printSQLIfRequested(sqlString);
		log.info(sqlString);
		PreparedStatement prep = statement.getConnection().prepareStatement(sqlString);
		prep.setCharacterStream(1, new InputStreamReader(largeObject.getInputStream()));
		prep.execute();
	}

	@Override
	public List<String> getSQLStatements(DBDatabase db) {
		DBRow row = getRow();
		List<String> strs = new ArrayList<String>();
		strs.add(db.getDefinition().startMultilineComment() + " SAVE BINARY DATA" + db.getDefinition().endMultilineComment());
		return strs;
	}

	/**
	 * Finds all the DBLargeObject fields that this action will need to update.
	 *
	 * @param row
	 * @return a list of the interesting DBLargeObjects.
	 */
	protected List<PropertyWrapper> getInterestingLargeObjects(DBRow row) {
		return getChangedLargeObjects(row);
	}

	@Override
	protected DBActionList getRevertDBActionList() {
		return new DBActionList();
	}

	@Override
	protected DBActionList getActions() {
		return new DBActionList(new DBUpdateLargeObjects(getRow()));
	}

	private List<PropertyWrapper> getChangedLargeObjects(DBRow row) {
		List<PropertyWrapper> changed = new ArrayList<PropertyWrapper>();
		if (row.hasLargeObjects()) {
			for (QueryableDatatype qdt : row.getLargeObjects()) {
				if (qdt instanceof DBLargeObject) {
					DBLargeObject large = (DBLargeObject) qdt;
					if (large.hasChanged()) {
						changed.add(row.getPropertyWrapperOf(qdt));
					}
				}
			}
		}
		return changed;
	}
}
