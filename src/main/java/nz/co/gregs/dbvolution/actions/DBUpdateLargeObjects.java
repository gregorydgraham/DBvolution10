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
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
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

	private static final Log LOG = LogFactory.getLog(DBUpdateLargeObjects.class);

	/**
	 * Creates a DBUpdateLargeObjects action for the supplied row.
	 *
	 * @param row the row to be updated
	 */
	protected DBUpdateLargeObjects(DBRow row) {
		super(row);
	}

	@Override
	protected DBActionList execute(DBDatabase db) throws SQLException {
		DBRow row = getRow();
		DBActionList actions;
		DBDefinition defn = db.getDefinition();
		DBStatement statement = db.getDBStatement();
		try {
			actions = new DBActionList();
			for (PropertyWrapper prop : getInterestingLargeObjects(row)) {
				final String col = prop.columnName();
				final DBLargeObject<?> largeObject = (DBLargeObject<?>) prop.getQueryableDatatype();

				if (largeObject.isNull()) {
					setToNullUsingStringValue(defn, row, col, largeObject, db, statement);
				} else {
					DBDefinition.LargeObjectHandler handler = defn.preferredLargeObjectWriter(largeObject);
					switch (handler){
						case BLOB:
							setUsingBLOB(defn, row, col, largeObject, db, statement);
							break;
						case BASE64:
							setUsingBase64String(defn, row, col, largeObject, db, statement);
							break;
						case BINARYSTREAM:
							setUsingBinaryStream(defn, row, col, largeObject, db, statement);
							break;
						case CHARSTREAM:
							setUsingCharacterStream(defn, row, col, largeObject, db, statement);
							break;
						case CLOB:
							setUsingCLOB(defn, row, col, largeObject, db, statement);
							break;
						case STRING:
							setUsingStringValue(defn, row, col, largeObject, db, statement);
							break;
						case JAVAOBJECT:
							setUsingJavaObject(defn, row, col, largeObject, db, statement);
							break;
						case BYTE:
							setUsingByteArray(defn, row, col, largeObject, db, statement);
							break;
//						case UNICODESTREAM:
//							setUsingUnicodeStream(defn, row, col, largeObject, db, statement);
//							break;
					}
//					if (largeObject instanceof DBLargeText) {
//						try {
//							setUsingCLOB(defn, row, col, largeObject, db, statement);
//						} catch (Throwable expa) {
//							try {
//								Logger.getLogger(DBUpdateLargeObjects.class.getName()).log(Level.WARNING, "Database rejected CLOB method", expa);
//								setUsingCharacterStream(defn, row, col, largeObject, db, statement);
//							} catch (Throwable exp1) {
//								try {
//									Logger.getLogger(DBUpdateLargeObjects.class.getName()).log(Level.WARNING, "Database rejected Character Stream method", exp1);
//									setUsingBase64String(defn, row, col, largeObject, db, statement);
//								} catch (Throwable exp2) {
//									try {
//										Logger.getLogger(DBUpdateLargeObjects.class.getName()).log(Level.WARNING, "Database rejected Base64String method", exp2);
//										setUsingStringValue(defn, row, col, largeObject, db, statement);
//									} catch (Throwable exp0) {
//										try {
//											Logger.getLogger(DBUpdateLargeObjects.class.getName()).log(Level.WARNING, "Database rejected String method", exp0);
//											setUsingBLOB(defn, row, col, largeObject, db, statement);
//										} catch (Throwable exp3) {
//											try {
//												Logger.getLogger(DBUpdateLargeObjects.class.getName()).log(Level.WARNING, "Database rejected BLOB method", exp3);
//												setUsingBinaryStream(defn, row, col, largeObject, db, statement);
//											} catch (Throwable exp4) {
//												Logger.getLogger(DBUpdateLargeObjects.class.getName()).log(Level.WARNING, "Database rejected Binary Stream method", exp3);
//												Logger.getLogger(DBUpdateLargeObjects.class.getName()).log(Level.SEVERE, "Database rejected all implemented methods", exp4);
//												throw exp1;
//											}
//										}
//									}
//								}
//							}
//						}
//					} else {
//						try {
//							setUsingBinaryStream(defn, row, col, largeObject, db, statement);
//						} catch (Throwable exp1) {
//							try {
//								Logger.getLogger(DBUpdateLargeObjects.class.getName()).log(Level.WARNING, "Database rejected Binary Stream method", exp1);
//								setUsingBLOB(defn, row, col, largeObject, db, statement);
//							} catch (Throwable exp2) {
//								try {
//									Logger.getLogger(DBUpdateLargeObjects.class.getName()).log(Level.WARNING, "Database rejected BLOB method", exp2);
//									setUsingBase64String(defn, row, col, largeObject, db, statement);
//								} catch (Throwable exp3) {
//									try {
//										Logger.getLogger(DBUpdateLargeObjects.class.getName()).log(Level.WARNING, "Database rejected Base64 method", exp3);
//										setUsingCharacterStream(defn, row, col, largeObject, db, statement);
//									} catch (Throwable exp4) {
//										Logger.getLogger(DBUpdateLargeObjects.class.getName()).log(Level.SEVERE, "Database rejected all implemented methods", exp4);
//										throw exp1;
//									}
//								}
//							}
//						}
//					}
				}
				DBUpdateLargeObjects update = new DBUpdateLargeObjects(row);
				actions.add(update);
				largeObject.setUnchanged();
			}
		} catch (Exception ex) {
			Logger.getLogger(DBUpdateLargeObjects.class.getName()).log(Level.SEVERE, null, ex);
			throw new DBRuntimeException("Can't Set LargeObject: IOError", ex);
		} finally {
			statement.close();
		}
		return actions;
	}

	private void setUsingStringValue(DBDefinition defn, DBRow row, final String col, final DBLargeObject<?> largeObject, DBDatabase db, DBStatement statement) throws SQLException {

		String sqlString = defn.beginUpdateLine()
				+ defn.formatTableName(row)
				+ defn.beginSetClause()
				+ defn.formatColumnName(col)
				+ defn.getEqualsComparator()
				//+ "'" + largeObject.stringValue() + "'"
				+" ? "
				+ defn.beginWhereClause()
				+ getPrimaryKeySQL(db, row)
				+ defn.endSQLStatement();
//					db.printSQLIfRequested(sqlString);
		LOG.debug(sqlString);
		PreparedStatement prep = statement.getConnection().prepareStatement(sqlString);
		try {
			prep.setString(1, largeObject.stringValue());
			prep.execute();
		} finally {
			prep.close();
		}

		statement.execute(sqlString);
	}

	private void setToNullUsingStringValue(DBDefinition defn, DBRow row, final String col, final DBLargeObject<?> largeObject, DBDatabase db, DBStatement statement) throws SQLException {
		String sqlString = defn.beginUpdateLine()
				+ defn.formatTableName(row)
				+ defn.beginSetClause()
				+ defn.formatColumnName(col)
				+ defn.getEqualsComparator()
				+ defn.getNull()
				+ defn.beginWhereClause()
				+ getPrimaryKeySQL(db, row)
				+ defn.endSQLStatement();
//					db.printSQLIfRequested(sqlString);
		LOG.debug(sqlString);
		statement.execute(sqlString);
	}

	private void setUsingBinaryStream(DBDefinition defn, DBRow row, final String col, final DBLargeObject<?> largeObject, DBDatabase db, DBStatement statement) throws SQLException {
		String sqlString = defn.beginUpdateLine()
				+ defn.formatTableName(row)
				+ defn.beginSetClause()
				+ defn.formatColumnName(col)
				+ defn.getEqualsComparator()
				+ defn.getPreparedVariableSymbol()
				+ defn.beginWhereClause()
				+ getPrimaryKeySQL(db, row)
				+ defn.endSQLStatement();
		db.printSQLIfRequested(sqlString);
		LOG.debug(sqlString);
		PreparedStatement prep = statement.getConnection().prepareStatement(sqlString);
		try {
			try {
				prep.setBinaryStream(1, largeObject.getInputStream());
			} catch (SQLException exp) {
				try {
					prep.setBinaryStream(1, largeObject.getInputStream(), largeObject.getSize());
				} catch (SQLException exp2) {
					throw new DBRuntimeException(exp);
				}
			}
			prep.execute();
		} finally {
			prep.close();
		}
	}

	private void setUsingBLOB(DBDefinition defn, DBRow row, String col, DBLargeObject<?> largeObject, DBDatabase db, DBStatement statement) throws SQLException {
		String sqlString = defn.beginUpdateLine()
				+ defn.formatTableName(row)
				+ defn.beginSetClause()
				+ defn.formatColumnName(col)
				+ defn.getEqualsComparator()
				+ defn.getPreparedVariableSymbol()
				+ defn.beginWhereClause()
				+ getPrimaryKeySQL(db, row)
				+ defn.endSQLStatement();
//					db.printSQLIfRequested(sqlString);
		LOG.debug(sqlString);
		PreparedStatement prep = statement.getConnection().prepareStatement(sqlString);
		try {
			prep.setBlob(1, largeObject.getInputStream(), largeObject.getSize());
			prep.execute();
		} finally {
			prep.close();
		}
	}

	private void setUsingCLOB(DBDefinition defn, DBRow row, String col, DBLargeObject<?> largeObject, DBDatabase db, DBStatement statement) throws SQLException {
		String sqlString = defn.beginUpdateLine()
				+ defn.formatTableName(row)
				+ defn.beginSetClause()
				+ defn.formatColumnName(col)
				+ defn.getEqualsComparator()
				+ defn.getPreparedVariableSymbol()
				+ defn.beginWhereClause()
				+ getPrimaryKeySQL(db, row)
				+ defn.endSQLStatement();
//					db.printSQLIfRequested(sqlString);
		LOG.debug(sqlString);
		PreparedStatement prep = statement.getConnection().prepareStatement(sqlString);
		try {
			prep.setClob(1, new InputStreamReader(largeObject.getInputStream(), Charset.forName("UTF-8")), largeObject.getSize());
			prep.execute();
		} finally {
			prep.close();
		}
	}

	private void setUsingBase64String(DBDefinition defn, DBRow row, final String col, final DBLargeObject<?> largeObject, DBDatabase db, DBStatement statement) throws SQLException, IOException {
		String sqlString = defn.beginUpdateLine()
				+ defn.formatTableName(row)
				+ defn.beginSetClause()
				+ defn.formatColumnName(col)
				+ defn.getEqualsComparator()
				+ defn.getPreparedVariableSymbol()
				+ defn.beginWhereClause()
				+ getPrimaryKeySQL(db, row)
				+ defn.endSQLStatement();
//					db.printSQLIfRequested(sqlString);
		LOG.debug(sqlString);
		PreparedStatement prep = statement.getConnection().prepareStatement(sqlString);
		try {
			InputStream inputStream = largeObject.getInputStream();

			InputStream input = new BufferedInputStream(inputStream);
			try {
				List<byte[]> byteArrays = new ArrayList<>();

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
			} finally {
				input.close();
			}
		} finally {
			prep.close();
		}
	}

	private void setUsingCharacterStream(DBDefinition defn, DBRow row, final String col, final DBLargeObject<?> largeObject, DBDatabase db, DBStatement statement) throws SQLException, IOError, IOException {
		String sqlString = defn.beginUpdateLine()
				+ defn.formatTableName(row)
				+ defn.beginSetClause()
				+ defn.formatColumnName(col)
				+ defn.getEqualsComparator()
				+ defn.getPreparedVariableSymbol()
				+ defn.beginWhereClause()
				+ getPrimaryKeySQL(db, row)
				+ defn.endSQLStatement();
		db.printSQLIfRequested(sqlString);
		LOG.debug(sqlString);
		PreparedStatement prep = statement.getConnection().prepareStatement(sqlString);
		try {
			InputStream inputStream = largeObject.getInputStream();

			InputStreamReader input = new InputStreamReader(inputStream, "UTF-8");
			try {
				List<char[]> byteArrays = new ArrayList<>();

				int totalBytesRead = 0;
				char[] resultSetBytes;
				resultSetBytes = new char[100000];
				int bytesRead = input.read(resultSetBytes);
				while (bytesRead > 0) {
					totalBytesRead += bytesRead;
					byteArrays.add(resultSetBytes);
					resultSetBytes = new char[100000];
					bytesRead = input.read(resultSetBytes);
				}
				char[] bytes = new char[totalBytesRead];
				int bytesAdded = 0;
				for (char[] someBytes : byteArrays) {
					System.arraycopy(someBytes, 0, bytes, bytesAdded, Math.min(someBytes.length, bytes.length - bytesAdded));
					bytesAdded += someBytes.length;
				}
				prep.setCharacterStream(1, new BufferedReader(new CharArrayReader(bytes)), bytes.length);
				prep.execute();
			} finally {
				input.close();
			}
		} finally {
			prep.close();
		}
	}

	@Override
	public List<String> getSQLStatements(DBDatabase db) {
		List<String> strs = new ArrayList<>();
		strs.add(db.getDefinition().startMultilineComment() + " SAVE BINARY DATA" + db.getDefinition().endMultilineComment());
		return strs;
	}

	/**
	 * Finds all the DBLargeObject fields that this action will need to update.
	 *
	 * @param row the row to be updated
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
		List<PropertyWrapper> changed = new ArrayList<>();
		if (row.hasLargeObjects()) {
			for (QueryableDatatype<?> qdt : row.getLargeObjects()) {
				if (qdt instanceof DBLargeObject) {
					DBLargeObject<?> large = (DBLargeObject<?>) qdt;
					if (large.hasChanged()) {
						changed.add(row.getPropertyWrapperOf(qdt));
					}
				}
			}
		}
		return changed;
	}

	private void setUsingJavaObject(DBDefinition defn, DBRow row, String col, DBLargeObject<?> largeObject, DBDatabase db, DBStatement statement) throws SQLException {
		String sqlString = defn.beginUpdateLine()
				+ defn.formatTableName(row)
				+ defn.beginSetClause()
				+ defn.formatColumnName(col)
				+ defn.getEqualsComparator()
				+ defn.getPreparedVariableSymbol()
				+ defn.beginWhereClause()
				+ getPrimaryKeySQL(db, row)
				+ defn.endSQLStatement();
//					db.printSQLIfRequested(sqlString);
		LOG.debug(sqlString);
		try (PreparedStatement prep = statement.getConnection().prepareStatement(sqlString)) {
			prep.setObject(1, largeObject.getValue());
			prep.execute();
		}
	}

	private void setUsingByteArray(DBDefinition defn, DBRow row, String col, DBLargeObject<?> largeObject, DBDatabase db, DBStatement statement) throws SQLException, IOException {
		String sqlString = defn.beginUpdateLine()
				+ defn.formatTableName(row)
				+ defn.beginSetClause()
				+ defn.formatColumnName(col)
				+ defn.getEqualsComparator()
				+ defn.getPreparedVariableSymbol()
				+ defn.beginWhereClause()
				+ getPrimaryKeySQL(db, row)
				+ defn.endSQLStatement();
//					db.printSQLIfRequested(sqlString);
		LOG.debug(sqlString);
		PreparedStatement prep = statement.getConnection().prepareStatement(sqlString);
		try {
			InputStream inputStream = largeObject.getInputStream();

			InputStream input = new BufferedInputStream(inputStream);
			try {
				List<byte[]> byteArrays = new ArrayList<>();

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
//				String b64encoded = Base64.encodeBase64String(bytes);
				//System.out.println("BYTES TO WRITE: " + Arrays.toString(bytes));
				prep.setBytes(1, bytes);
				prep.execute();
			} finally {
				input.close();
			}
		} finally {
			prep.close();
		}
	}

//	private void setUsingUnicodeStream(DBDefinition defn, DBRow row, String col, DBLargeObject<?> largeObject, DBDatabase db, DBStatement statement) throws SQLException, IOException {
//		String sqlString = defn.beginUpdateLine()
//				+ defn.formatTableName(row)
//				+ defn.beginSetClause()
//				+ defn.formatColumnName(col)
//				+ defn.getEqualsComparator()
//				+ defn.getPreparedVariableSymbol()
//				+ defn.beginWhereClause()
//				+ getPrimaryKeySQL(db, row)
//				+ defn.endSQLStatement();
////					db.printSQLIfRequested(sqlString);
//		LOG.debug(sqlString);
//		PreparedStatement prep = statement.getConnection().prepareStatement(sqlString);
//		try {
//			InputStream inputStream = largeObject.getInputStream();
//
//			InputStream input = new BufferedInputStream(inputStream);
//			try {
//				List<byte[]> byteArrays = new ArrayList<>();
//
//				int totalBytesRead = 0;
//				byte[] resultSetBytes;
//				resultSetBytes = new byte[100000];
//				int bytesRead = input.read(resultSetBytes);
//				while (bytesRead > 0) {
//					totalBytesRead += bytesRead;
//					byteArrays.add(resultSetBytes);
//					resultSetBytes = new byte[100000];
//					bytesRead = input.read(resultSetBytes);
//				}
//				byte[] bytes = new byte[totalBytesRead];
//				int bytesAdded = 0;
//				for (byte[] someBytes : byteArrays) {
//					System.arraycopy(someBytes, 0, bytes, bytesAdded, Math.min(someBytes.length, bytes.length - bytesAdded));
//					bytesAdded += someBytes.length;
//				}
////				String b64encoded = Base64.encodeBase64String(bytes);
//				//System.out.println("BYTES TO WRITE: " + Arrays.toString(bytes));
//				prep.setUnicodeStream(1, new ByteArrayInputStream(bytes), bytes.length);
//				prep.execute();
//			} finally {
//				input.close();
//			}
//		} finally {
//			prep.close();
//		}
//	}

}
