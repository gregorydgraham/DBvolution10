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
package nz.co.gregs.dbvolution.datatypes;

import java.io.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import java.sql.*;
import java.util.*;
import java.util.logging.*;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.columns.LargeObjectColumn;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.internal.query.LargeObjectHandlerType;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
import nz.co.gregs.dbvolution.query.RowDefinition;
import org.apache.commons.codec.binary.Base64;

/**
 *
 * Implements the abstractions required for handling Java Objects stored in the
 * database
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 * @param <O> the specific type of the objects to be stored.
 */
public class DBJavaObject<O> extends DBLargeObject<O> {

	private static final long serialVersionUID = 1;
	private transient InputStream byteStream = null;
	private O literalObject;
	private boolean internalValueHasBeenSet = false;

	@Override
	public String getSQLDatatype() {
		return "JAVA_OBJECT";
	}

	@SuppressWarnings("unchecked")
	private void setInternalValue(O newLiteralValue) {
		if (!internalValueHasBeenSet) {
			if (newLiteralValue instanceof DBJavaObject) {
				final DBJavaObject<O> valBytes = (DBJavaObject<O>) newLiteralValue;
				setValue(valBytes.getValue());
			} else {
				try {
					literalObject = newLiteralValue;
					ByteArrayOutputStream tempByteStream = new ByteArrayOutputStream();
					ObjectOutputStream oStream = new ObjectOutputStream(tempByteStream);
					oStream.writeObject(literalObject);
					setLiteralValue(literalObject);
				} catch (IOException ex) {
					throw new RuntimeException(ex);
				}
				internalValueHasBeenSet = true;
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public O getValue() {
		setInternalValue(getLiteralValue());
		return literalObject;
	}

	@Override
	public String toString() {
		setInternalValue(getLiteralValue());
		if (literalObject == null) {
			return "NULL";
		} else {
			return literalObject.toString();
		}
	}

	@SuppressWarnings("unchecked")
	private O getFromBinaryStream(ResultSet resultSet, String fullColumnName) throws SQLException {
		O returnValue = null;
		InputStream inputStream;
		inputStream = resultSet.getBinaryStream(fullColumnName);
		if (resultSet.wasNull()) {
			inputStream = null;
		}
		if (inputStream == null) {
			this.setToNull();
		} else {
			final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
			try {
				try (ObjectInputStream input = new ObjectInputStream(bufferedInputStream)) {
					returnValue = (O) input.readObject();
				}
			} catch (IOException | ClassNotFoundException ex) {
				Logger.getLogger(DBJavaObject.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
		return returnValue;
	}

	@SuppressWarnings("unchecked")
	private O getFromBLOB(ResultSet resultSet, String fullColumnName) throws SQLException {
		O returnValue = null;
		Blob blob = resultSet.getBlob(fullColumnName);
		if (resultSet.wasNull()) {
			blob = null;
		}
		if (blob == null) {
			this.setToNull();
		} else {
			InputStream inputStream = blob.getBinaryStream();
			if (inputStream == null) {
				this.setToNull();
			} else {
				final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
				try {
					try (ObjectInputStream input = new ObjectInputStream(bufferedInputStream)) {
						returnValue = (O) input.readObject();
					}
				} catch (IOException | ClassNotFoundException ex) {
					Logger.getLogger(DBJavaObject.class.getName()).log(Level.SEVERE, null, ex);
				}
			}
		}
		return returnValue;
	}

	@SuppressWarnings("unchecked")
	private O getFromGetBytes(ResultSet resultSet, String fullColumnName) throws SQLException {
		try {
			byte[] bytes = resultSet.getBytes(fullColumnName);
			try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
				return (O) input.readObject();
			}
		} catch (IOException | ClassNotFoundException ex) {
			Logger.getLogger(DBJavaObject.class.getName()).log(Level.SEVERE, null, ex);
		}
		return null;
	}

	public static byte[] concatAllByteArrays(List<byte[]> bytes) {
		byte[] first = bytes.get(0);
		bytes.remove(0);
		byte[][] rest = bytes.toArray(new byte[][]{});
		int totalLength = first.length;
		for (byte[] array : rest) {
			totalLength += array.length;
		}
		byte[] result = Arrays.copyOf(first, totalLength);
		int offset = first.length;
		for (byte[] array : rest) {
			System.arraycopy(array, 0, result, offset, array.length);
			offset += array.length;
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	private O getFromCharacterReader(ResultSet resultSet, String fullColumnName) throws SQLException, IOException {
		O obj = null;
		Reader inputReader = null;
		try {
			inputReader = resultSet.getCharacterStream(fullColumnName);
		} catch (NullPointerException nullEx) {
			;// NullPointerException is thrown by a SQLite-JDBC bug sometimes.
		}
		if (inputReader != null) {
			if (resultSet.wasNull()) {
				this.setToNull();
			} else {
				try (BufferedReader input = new BufferedReader(inputReader)) {
					List<byte[]> byteArrays = new ArrayList<>();
					try {
						char[] resultSetBytes;
						final int byteArrayDefaultSize = 100000;
						resultSetBytes = new char[byteArrayDefaultSize];
						int bytesRead = input.read(resultSetBytes);
						while (bytesRead > 0) {
							if (bytesRead == byteArrayDefaultSize) {
								byteArrays.add(String.valueOf(resultSetBytes).getBytes(UTF_8));
							} else {
								char[] shortBytes = new char[bytesRead];
								System.arraycopy(resultSetBytes, 0, shortBytes, 0, bytesRead);
								byteArrays.add(String.valueOf(shortBytes).getBytes(UTF_8));
							}
							resultSetBytes = new char[byteArrayDefaultSize];
							bytesRead = input.read(resultSetBytes);
						}
					} catch (IOException ex) {
						Logger.getLogger(DBLargeBinary.class.getName()).log(Level.SEVERE, null, ex);
						throw new DBRuntimeException(ex);
					}
					byte[] bytes = concatAllByteArrays(byteArrays);
					byte[] decodeBuffer = Base64.decodeBase64(bytes);

					ObjectInputStream decodedInput = new ObjectInputStream(new ByteArrayInputStream(decodeBuffer));
					try {
						obj = (O) decodedInput.readObject();
					} catch (ClassNotFoundException ex) {
						Logger.getLogger(DBJavaObject.class.getName()).log(Level.SEVERE, null, ex);
					}
				}
			}
		}
		return obj;
	}

	@SuppressWarnings("unchecked")
	private O getFromCLOB(ResultSet resultSet, String fullColumnName) throws SQLException {
		O returnValue = null;
		Clob clob = resultSet.getClob(fullColumnName);
		if (resultSet.wasNull() || clob == null) {
			this.setToNull();
		} else {
			try {
				List<byte[]> byteArrays = new ArrayList<>();
				try (BufferedReader input = new BufferedReader(clob.getCharacterStream())) {

					try {

						char[] resultSetBytes;
						final int byteArrayDefaultSize = 100000;
						resultSetBytes = new char[byteArrayDefaultSize];
						int bytesRead = input.read(resultSetBytes);
						while (bytesRead > 0) {
							if (bytesRead == byteArrayDefaultSize) {
								byteArrays.add(String.valueOf(resultSetBytes).getBytes(UTF_8));
							} else {
								char[] shortBytes = new char[bytesRead];
								System.arraycopy(resultSetBytes, 0, shortBytes, 0, bytesRead);
								byteArrays.add(String.valueOf(shortBytes).getBytes(UTF_8));
							}
							resultSetBytes = new char[byteArrayDefaultSize];
							bytesRead = input.read(resultSetBytes);
						}
					} catch (IOException ex) {
						Logger.getLogger(DBLargeBinary.class.getName()).log(Level.SEVERE, null, ex);
						throw new DBRuntimeException(ex);
					}
					byte[] bytes = concatAllByteArrays(byteArrays);
					ObjectInputStream objectInput = new ObjectInputStream(new ByteArrayInputStream(bytes));
					returnValue = (O) objectInput.readObject();
				}
			} catch (IOException | ClassNotFoundException ex) {
				Logger.getLogger(DBJavaObject.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
		return returnValue;
	}

	@Override
	public String formatValueForSQLStatement(DBDefinition db) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public DBJavaObject<O> getQueryableDatatypeForExpressionValue() {
		return new DBJavaObject<>();
	}

	@Override
	public boolean isAggregator() {
		return false;
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		return new HashSet<>();
	}

	/**
	 * Returns the internal InputStream.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return an InputStream to read the bytes.
	 */
	@Override
	public InputStream getInputStream() {
		if (byteStream == null) {
			try {
				byteStream = new ByteArrayInputStream(getBytes());
			} catch (IOException ex) {
				throw new RuntimeException(ex);
			}
		}
		return byteStream;
	}

	/**
	 * Returns the byte[] used internally to store the value of this DBJavaObject.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the byte[] value of this DBJavaObject.
	 * @throws java.io.IOException java.io.IOException
	 *
	 */
	public byte[] getBytes() throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ObjectOutputStream os = new ObjectOutputStream(out);
		os.writeObject(getLiteralValue());
		return out.toByteArray();
	}

	@Override
	public String stringValue() {
		if (this.isNull()) {
			return super.stringValue();
		} else {
			O value = this.getValue();
			return "" + value;
		}
	}

	@Override
	public int getSize() {
		try {
			return getBytes().length;
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}

	@Override
	protected O getFromResultSet(DBDefinition defn, ResultSet resultSet, String fullColumnName) throws SQLException {
		O obj = null;
		LargeObjectHandlerType handler = defn.preferredLargeObjectReader(this);
		switch (handler) {
			case BLOB:
				obj = getFromBLOB(resultSet, fullColumnName);
				break;
			case BASE64:
				obj = getFromBase64(resultSet, fullColumnName);
				break;
			case BINARYSTREAM:
				obj = getFromBinaryStream(resultSet, fullColumnName);
				break;
			case CHARSTREAM:
				try {
					obj = getFromCharacterReader(resultSet, fullColumnName);
				} catch (IOException exp) {
					throw new DBRuntimeException(exp);
				}
				break;
			case CLOB:
				obj = getFromCLOB(resultSet, fullColumnName);
				break;
			case STRING:
				obj = getFromString(resultSet, fullColumnName);
				break;
			case JAVAOBJECT:
				obj = getFromJavaObject(resultSet, fullColumnName);
				break;
			case BYTE:
				obj = getFromGetBytes(resultSet, fullColumnName);
				break;
		}
		return obj;
	}

	@Override
	public boolean getIncludesNull() {
		return false;
	}

	@Override
	protected void setValueFromStandardStringEncoding(String encodedValue) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	private O getFromString(ResultSet resultSet, String fullColumnName) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@SuppressWarnings("unchecked")
	private O getFromJavaObject(ResultSet resultSet, String fullColumnName) throws SQLException {
		O returnValue = null;
		Object blob = resultSet.getObject(fullColumnName);
		if (resultSet.wasNull()) {
			blob = null;
		}
		if (blob == null) {
			this.setToNull();
		} else {
			returnValue = (O) blob;
		}
		return returnValue;
	}

	private O getFromBase64(ResultSet resultSet, String fullColumnName) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public LargeObjectColumn getColumn(RowDefinition row) throws IncorrectRowProviderInstanceSuppliedException {
		return new LargeObjectColumn(row, this);
	}

}
