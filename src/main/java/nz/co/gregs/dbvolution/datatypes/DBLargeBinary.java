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
import nz.co.gregs.dbvolution.*;
import nz.co.gregs.dbvolution.columns.LargeObjectColumn;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.internal.query.LargeObjectHandlerType;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
import nz.co.gregs.dbvolution.expressions.LargeObjectExpression;
import nz.co.gregs.dbvolution.query.RowDefinition;
import org.apache.commons.codec.binary.Base64;

/**
 *
 * Implements the abstractions necessary to handle arbitrary byte streams and
 * files stored in the database.
 *
 * <p>
 * Use DBLargeBinary for files and streams. Store exceptionally long text in
 * {@link DBLargeText} and Java instances/objects as {@link DBJavaObject} for
 * greater convenience.
 *
 * <p>
 * Generally DBLargeBinary is declared inside your DBRow sub-class as:
 * {@code @DBColumn public DBLargeBinary myBinaryColumn = new DBLargeBinary();}
 *
 * <p>
 * DBLargeBinary is the standard type of {@link DBLargeObject BLOB columns}.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBLargeBinary extends DBLargeObject<byte[]> {

	private static final long serialVersionUID = 1;

	/**
	 * The Default constructor for a DBBinaryObject.
	 *
	 */
	public DBLargeBinary() {
		super();
	}

	/**
	 * Creates a column expression with a large object result from the expression
	 * provided.
	 *
	 * <p>
	 * Used in {@link DBReport}, and some {@link DBRow}, sub-classes to derive
	 * data from the database prior to retrieval.
	 *
	 * @param aThis an expression that will result in a large object value
	 */
	public DBLargeBinary(LargeObjectExpression aThis) {
		super(aThis);
	}

	public DBLargeBinary(byte[] value) {
		super();
		setByteArray(value);
	}

	/**
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the standard SQL datatype that corresponds to this QDT as a String
	 */
	@Override
	public String getSQLDatatype() {
		return "BLOB";
	}

	/**
	 * Sets the value of this DBLargeBinary to the byte array supplied.
	 *
	 * @param byteArray	byteArray
	 */
	@Override
	public void setValue(byte[] byteArray) {
		setByteArray(byteArray);
	}

	private void setByteArray(byte[] byteArray) {
		super.setLiteralValue(byteArray);
	}

	/**
	 * Sets the value of this DBLargeBinary to the InputStream supplied.
	 *
	 * <p>
	 * The input stream will not be read until the containing DBRow is
	 * saved/inserted.
	 *
	 * @param inputViaStream	inputViaStream
	 */
	public void setValue(InputStream inputViaStream) {
		setValue(getBytesFromInputStream(inputViaStream));
	}

	/**
	 * Sets the value of this DBLargeBinary to the file supplied.
	 *
	 * <p>
	 * Unlike {@link #setValue(java.io.InputStream) setting an InputStream}, the
	 * file is read immediately and stored internally. If you would prefer to
	 * delay the reading of the file, wrap the file in a {@link FileInputStream}.
	 *
	 * @param fileToRead fileToRead
	 * @throws java.io.IOException java.io.IOException
	 */
	public void setValue(File fileToRead) throws IOException {
		setFromFileSystem(fileToRead);
	}

	/**
	 * Set the value of the DBLargeBinary to the String provided.
	 *
	 * @param string	string
	 */
	public void setValue(String string) {
		setValue(string.getBytes(UTF_8));
	}

	void setValue(DBLargeBinary newLiteralValue) {
		setValue(newLiteralValue.getValue());
	}

	private byte[] getFromBinaryStream(ResultSet resultSet, String fullColumnName) throws SQLException {
		byte[] bytes = new byte[]{};
		InputStream inputStream;
		inputStream = resultSet.getBinaryStream(fullColumnName);
		if (resultSet.wasNull()) {
			inputStream = null;
		}
		if (inputStream == null) {
			this.setToNull();
		} else {
			bytes = getBytesFromInputStream(inputStream);
		}
		return bytes;
	}

	private byte[] getFromBLOB(ResultSet resultSet, String fullColumnName) throws SQLException {
		byte[] bytes = new byte[]{};
		Blob blob = resultSet.getBlob(fullColumnName);
		if (resultSet.wasNull()) {
			blob = null;
		}
		if (blob == null) {
			this.setToNull();
		} else {
			InputStream inputStream = blob.getBinaryStream();
			bytes = getBytesFromInputStream(inputStream);
		}
		return bytes;
	}

	private byte[] getBytesFromInputStream(InputStream inputStream) {
		InputStream input = new BufferedInputStream(inputStream);
		List<byte[]> byteArrays = new ArrayList<>();
		try {
			byte[] resultSetBytes;
			final int byteArrayDefaultSize = 100000;
			resultSetBytes = new byte[byteArrayDefaultSize];
			int bytesRead = input.read(resultSetBytes);
			while (bytesRead > 0) {
				if (bytesRead == byteArrayDefaultSize) {
					byteArrays.add(resultSetBytes);
				} else {
					byte[] shortBytes = new byte[bytesRead];
					System.arraycopy(resultSetBytes, 0, shortBytes, 0, bytesRead);
					byteArrays.add(shortBytes);
				}
				resultSetBytes = new byte[byteArrayDefaultSize];
				bytesRead = input.read(resultSetBytes);
			}
		} catch (IOException ex) {
			Logger.getLogger(DBLargeBinary.class.getName()).log(Level.SEVERE, null, ex);
			throw new DBRuntimeException(ex);
		} finally {
			try {
				input.close();
			} catch (IOException ex) {
				Logger.getLogger(DBLargeBinary.class.getName()).log(Level.SEVERE, null, ex);
				throw new DBRuntimeException(ex);
			}
		}
		return concatAllByteArrays(byteArrays);
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

	private byte[] getFromGetBytes(ResultSet resultSet, String fullColumnName) throws SQLException {
		byte[] bytes = resultSet.getBytes(fullColumnName);
		return bytes;
	}

	private byte[] getFromCharacterReader(ResultSet resultSet, String fullColumnName) throws SQLException {
		byte[] decodeBuffer = new byte[]{};
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
				BufferedReader input = new BufferedReader(inputReader);
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
				} finally {
					try {
						input.close();
					} catch (IOException ex) {
						Logger.getLogger(DBLargeBinary.class.getName()).log(Level.SEVERE, null, ex);
						throw new DBRuntimeException(ex);
					}
				}
				byte[] bytes = concatAllByteArrays(byteArrays);
				decodeBuffer = Base64.decodeBase64(bytes);
			}
		}
		return decodeBuffer;
	}

	private byte[] getFromCLOB(ResultSet resultSet, String fullColumnName) throws SQLException {
		byte[] bytes = new byte[]{};
		Clob clob = resultSet.getClob(fullColumnName);
		if (resultSet.wasNull() || clob == null) {
			this.setToNull();
		} else {
			final Reader characterStream = clob.getCharacterStream();
			try {
				BufferedReader input = new BufferedReader(characterStream);
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
				} finally {
					try {
						input.close();
					} catch (IOException ex) {
						Logger.getLogger(DBLargeBinary.class.getName()).log(Level.SEVERE, null, ex);
					}
				}
				bytes = concatAllByteArrays(byteArrays);
			} finally {
				try {
					characterStream.close();
				} catch (IOException ex) {
					Logger.getLogger(DBLargeBinary.class.getName()).log(Level.SEVERE, null, ex);
					throw new DBRuntimeException(ex);
				}
			}
		}
		return bytes;
	}

	@Override
	public String formatValueForSQLStatement(DBDefinition db) {
		throw new UnsupportedOperationException("Binary datatypes like " + this.getClass().getSimpleName() + " do not have a simple SQL representation. Do not call getSQLValue(), use the getInputStream() method instead.");
	}

	/**
	 * Tries to set the DBDyteArray to the contents of the supplied file.
	 *
	 * <p>
	 * Convenience method for {@link #setFromFileSystem(java.io.File) }.
	 *
	 * @param originalFile	originalFile
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @throws java.io.FileNotFoundException java.io.FileNotFoundException
	 * @throws java.io.IOException java.io.IOException
	 *
	 *
	 */
	public void setFromFileSystem(String originalFile) throws FileNotFoundException, IOException {
		File file = new File(originalFile);
		setFromFileSystem(file);
	}

	/**
	 * Tries to set the DBDyteArray to the contents of the supplied file.
	 *
	 * <p>
	 * Convenience method for {@link #setFromFileSystem(java.io.File) }.
	 *
	 * @param originalFile	originalFile
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @throws java.io.FileNotFoundException java.io.FileNotFoundException
	 * @throws java.io.IOException java.io.IOException
	 *
	 *
	 */
	public void setFromFileSystem(DBString originalFile) throws FileNotFoundException, IOException {
		File file = new File(originalFile.stringValue());
		setFromFileSystem(file);
	}

	/**
	 * Tries to set the DBDyteArray to the contents of the supplied file.
	 *
	 * @param originalFile	originalFile
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @throws java.io.FileNotFoundException java.io.FileNotFoundException
	 * @throws java.io.IOException java.io.IOException
	 *
	 *
	 */
	private void setFromFileSystem(File originalFile) throws FileNotFoundException, IOException {
		byte[] bytes = new byte[(int) originalFile.length()];
		InputStream input = null;
		try {
			int totalBytesRead = 0;
			input = new BufferedInputStream(new FileInputStream(originalFile));
			while (totalBytesRead < bytes.length) {
				int bytesRemaining = bytes.length - totalBytesRead;
				int bytesRead = input.read(bytes, totalBytesRead, bytesRemaining);
				if (bytesRead > 0) {
					totalBytesRead += bytesRead;
				}
			}
			/*
			 the above style is a bit tricky: it places bytes into the 'result' array;
			 'result' is an output parameter;
			 the while loop usually has a single iteration only.
			 */
		} finally {
			if (input != null) {
				input.close();
			}
		}
		setValue(bytes);
	}

	/**
	 * Tries to write the contents of this DBLargeBinary to the file supplied.
	 *
	 * <p>
	 * Convenience method for {@link #writeToFileSystem(java.io.File) }.
	 *
	 * @param originalFile originalFile
	 * @throws java.io.FileNotFoundException java.io.FileNotFoundException
	 * @throws java.io.IOException java.io.IOException
	 */
	public void writeToFileSystem(String originalFile) throws FileNotFoundException, IOException {
		File file = new File(originalFile);
		writeToFileSystem(file);
	}

	/**
	 * Tries to write the contents of this DBLargeBinary to the file supplied.
	 *
	 * <p>
	 * Convenience method for {@link #writeToFileSystem(java.io.File) }.
	 *
	 * @param originalFile originalFile
	 * @throws java.io.FileNotFoundException java.io.FileNotFoundException
	 * @throws java.io.IOException java.io.IOException
	 */
	public void writeToFileSystem(DBString originalFile) throws FileNotFoundException, IOException {
		writeToFileSystem(originalFile.toString());
	}

	/**
	 * Tries to write the contents of this DBLargeBinary to the file supplied.
	 *
	 * <p>
	 * Convenience method for {@link #writeToFileSystem(java.io.File) }.
	 *
	 * @param originalFile originalFile
	 * @throws java.io.FileNotFoundException java.io.FileNotFoundException
	 * @throws java.io.IOException java.io.IOException
	 */
	public void writeToFileSystem(File originalFile) throws FileNotFoundException, IOException {
		if (getLiteralValue() != null && originalFile != null) {
			if (!originalFile.exists()) {
				boolean createNewFile = originalFile.createNewFile();
				if (!createNewFile) {
					boolean delete = originalFile.delete();
					if (!delete) {
						throw new IOException("Unable to delete file: " + originalFile.getPath() + " could not be deleted, check the permissions of the file, directory, drive, and current user.");
					}
					createNewFile = originalFile.createNewFile();
					if (!createNewFile) {
						throw new IOException("Unable to create file: " + originalFile.getPath() + " could not be created, check the permissions of the file, directory, drive, and current user.");
					}
				}
			}
			if (originalFile.exists()) {
				OutputStream output = null;
				try {
					output = new BufferedOutputStream(new FileOutputStream(originalFile));
					output.write(getBytes());
					output.flush();
					output.close();
					output = null;
				} finally {
					if (output != null) {
						output.close();
					}
				}
			} else {
				throw new FileNotFoundException("Unable Create File: the file \"" + originalFile.getAbsolutePath() + " could not be found or created.");
			}
		}
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
		return new BufferedInputStream(new ByteArrayInputStream(getBytes()));
	}

	/**
	 * Returns the byte[] used internally to store the value of this
	 * DBLargeBinary.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the byte[] value of this DBLargeBinary.
	 */
	public byte[] getBytes() {
		return this.getLiteralValue();
	}

	@Override
	public String stringValue() {
		byte[] value = this.getValue();
		if (this.isNull()) {
			return super.stringValue();
		} else {
			return new String(value, UTF_8);
		}
	}

	@Override
	public int getSize() throws IOException {
		final byte[] bytes = getBytes();
		if (bytes != null) {
			return bytes.length;
		} else {
			return 0;
		}
	}

	@Override
	public byte[] getValue() {
		return getBytes();
	}

	@Override
	public DBLargeBinary getQueryableDatatypeForExpressionValue() {
		return new DBLargeBinary();
	}

	@Override
	public boolean isAggregator() {
		return false;
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		return new HashSet<>();
	}

	@Override
	protected byte[] getFromResultSet(DBDefinition defn, ResultSet resultSet, String fullColumnName) throws SQLException {
		byte[] bytes = new byte[]{};
		LargeObjectHandlerType handler = defn.preferredLargeObjectReader(this);
		switch (handler) {
			case BLOB:
				bytes = getFromBLOB(resultSet, fullColumnName);
				break;
			case BASE64:
				bytes = getFromBase64(resultSet, fullColumnName);
				break;
			case BINARYSTREAM:
				bytes = getFromBinaryStream(resultSet, fullColumnName);
				break;
			case CHARSTREAM:
				bytes = getFromCharacterReader(resultSet, fullColumnName);
				break;
			case CLOB:
				bytes = getFromCLOB(resultSet, fullColumnName);
				break;
			case STRING:
				bytes = getFromString(resultSet, fullColumnName);
				break;
			case JAVAOBJECT:
				bytes = getFromJavaObject(resultSet, fullColumnName);
				break;
			case BYTE:
				bytes = getFromGetBytes(resultSet, fullColumnName);
				break;
		}
		return bytes;
	}

	@Override
	public boolean getIncludesNull() {
		return false;
	}

	@Override
	protected void setValueFromStandardStringEncoding(String encodedValue) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	private byte[] getFromBase64(ResultSet resultSet, String fullColumnName) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	private byte[] getFromString(ResultSet resultSet, String fullColumnName) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	private byte[] getFromJavaObject(ResultSet resultSet, String fullColumnName) {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public LargeObjectColumn getColumn(RowDefinition row) throws IncorrectRowProviderInstanceSuppliedException {
		return new LargeObjectColumn(row, this);
	}

	/**
	 * Indicates whether object is NULL within the database
	 *
	 * <p>
	 * Databases and Java both use the term NULL but for slightly different
	 * meanings.
	 *
	 * <p>
	 * This method indicates whether the field represented by this object is NULL
	 * in the database sense.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if this object represents a NULL database value, otherwise
	 * FALSE
	 */
	@Override
	public boolean isNull() {
		return super.isNull() && getLiteralValue() == null;//&& this.byteStream == null;
	}

	@Override
	public synchronized DBLargeBinary copy() {
		DBLargeBinary result = (DBLargeBinary) super.copy();
		return result;
	}
}
