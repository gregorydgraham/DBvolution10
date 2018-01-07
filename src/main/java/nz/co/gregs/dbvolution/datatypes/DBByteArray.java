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
 * Use DBByteArray for files, streams, and exceptionally long text. Store Java
 * instances/objects as {@link DBJavaObject} for greater convenience.
 *
 * <p>
 * Generally DBBoolean is declared inside your DBRow sub-class as:
 * {@code @DBColumn public DBByteArray myBoolColumn = new DBByteArray();}
 *
 * <p>
 * DBByteArray is the standard type of {@link DBLargeObject BLOB columns}.
 *
 * @deprecated DBByteArray does not correctly differentiate between CLOB and
 * BLOB producing inconsistencies between databases. Move to
 * {@link DBLargeBinary} for binaries like images or {@link DBLargeText} for
 * XML, HTML, or other large text values.
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 * @author Gregory Graham
 */
@Deprecated()
public class DBByteArray extends DBLargeObject<byte[]> {

	private static final long serialVersionUID = 1;
	transient InputStream byteStream = null;

	/**
	 * The Default constructor for a DBByteArray.
	 *
	 */
	public DBByteArray() {
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
	public DBByteArray(LargeObjectExpression aThis) {
		super(aThis);
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
	 * Sets the value of this DBByteArray to the byte array supplied.
	 *
	 * @param byteArray	byteArray
	 */
	@Override
	public void setValue(byte[] byteArray) {
		super.setLiteralValue(byteArray);
		if (byteArray == null) {
			byteStream = new BufferedInputStream(new ByteArrayInputStream(new byte[]{}));
		} else {
			byteStream = new BufferedInputStream(new ByteArrayInputStream(byteArray));
		}
	}

	/**
	 * Sets the value of this DBByteArray to the InputStream supplied.
	 *
	 * <p>
	 * The input stream will not be read until the containing DBRow is
	 * saved/inserted.
	 *
	 * @param inputViaStream	inputViaStream
	 */
	public void setValue(InputStream inputViaStream) {
		super.setLiteralValue(null);
		byteStream = new BufferedInputStream(inputViaStream);
	}

	/**
	 * Sets the value of this DBByteArray to the file supplied.
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
		setValue(setFromFileSystem(fileToRead));
	}

	/**
	 * Set the value of the DBByteArray to the String provided.
	 *
	 * @param string	string
	 */
	public void setValue(String string) {
		setValue(string.getBytes(UTF_8));
	}

	void setValue(DBByteArray newLiteralValue) {
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
		byte[] bytes;
		InputStream input = new BufferedInputStream(inputStream);
		List<byte[]> byteArrays = new ArrayList<>();
		int totalBytesRead = 0;
		try {
			byte[] resultSetBytes;
			resultSetBytes = new byte[100000];
			int bytesRead = input.read(resultSetBytes);
			while (bytesRead > 0) {
				totalBytesRead += bytesRead;
				byteArrays.add(resultSetBytes);
				resultSetBytes = new byte[100000];
				bytesRead = input.read(resultSetBytes);
			}
		} catch (IOException ex) {
			Logger.getLogger(DBByteArray.class.getName()).log(Level.SEVERE, null, ex);
		}
		bytes = new byte[totalBytesRead];
		int bytesAdded = 0;
		for (byte[] someBytes : byteArrays) {
			System.arraycopy(someBytes, 0, bytes, bytesAdded, Math.min(someBytes.length, bytes.length - bytesAdded));
			bytesAdded += someBytes.length;
		}
//			this.setValue(bytes);
		return bytes;
	}

	private byte[] getFromGetBytes(ResultSet resultSet, String fullColumnName) throws SQLException {
		byte[] bytes = resultSet.getBytes(fullColumnName);
//		this.setValue(bytes);
		return bytes;
	}

	private byte[] getFromCharacterReader(ResultSet resultSet, String fullColumnName) throws SQLException, IOException {
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

				int totalBytesRead = 0;
				try {
					char[] resultSetBytes;
					resultSetBytes = new char[100000];
					int bytesRead = input.read(resultSetBytes);
					while (bytesRead > 0) {
						totalBytesRead += bytesRead;
						byteArrays.add(String.valueOf(resultSetBytes).getBytes(UTF_8));
						resultSetBytes = new char[100000];
						bytesRead = input.read(resultSetBytes);
					}
				} catch (IOException ex) {
					Logger.getLogger(DBByteArray.class.getName()).log(Level.SEVERE, null, ex);
				} finally {
					input.close();
				}
				byte[] bytes = new byte[totalBytesRead];
				int bytesAdded = 0;
				for (byte[] someBytes : byteArrays) {
					System.arraycopy(someBytes, 0, bytes, bytesAdded, Math.min(someBytes.length, bytes.length - bytesAdded));
					bytesAdded += someBytes.length;
				}
				decodeBuffer = Base64.decodeBase64(bytes);
//				this.setValue(decodeBuffer);
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

				int totalBytesRead = 0;
				try {
					char[] resultSetBytes;
					resultSetBytes = new char[100000];
					try {
						int bytesRead = input.read(resultSetBytes);
						while (bytesRead > 0) {
							totalBytesRead += bytesRead;
							byteArrays.add(String.valueOf(resultSetBytes).getBytes(UTF_8));
							resultSetBytes = new char[100000];
							bytesRead = input.read(resultSetBytes);
						}
					} finally {
						input.close();
					}
				} catch (IOException ex) {
					Logger.getLogger(DBByteArray.class.getName()).log(Level.SEVERE, null, ex);
				}
				bytes = new byte[totalBytesRead];
				int bytesAdded = 0;
				for (byte[] someBytes : byteArrays) {
					System.arraycopy(someBytes, 0, bytes, bytesAdded, Math.min(someBytes.length, bytes.length - bytesAdded));
					bytesAdded += someBytes.length;
				}
			} finally {
				try {
					characterStream.close();
				} catch (IOException ex) {
					Logger.getLogger(DBByteArray.class.getName()).log(Level.SEVERE, null, ex);
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
	 * @return the byte[] of the contents of the file.
	 * @throws java.io.FileNotFoundException java.io.FileNotFoundException
	 * @throws java.io.IOException java.io.IOException
	 *
	 *
	 */
	public byte[] setFromFileSystem(String originalFile) throws FileNotFoundException, IOException {
		File file = new File(originalFile);
		return setFromFileSystem(file);
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
	 * @return the byte[] of the contents of the file.
	 * @throws java.io.FileNotFoundException java.io.FileNotFoundException
	 * @throws java.io.IOException java.io.IOException
	 *
	 *
	 */
	public byte[] setFromFileSystem(DBString originalFile) throws FileNotFoundException, IOException {
		File file = new File(originalFile.stringValue());
		return setFromFileSystem(file);
	}

	/**
	 * Tries to set the DBDyteArray to the contents of the supplied file.
	 *
	 * @param originalFile	originalFile
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the byte[] of the contents of the file.
	 * @throws java.io.FileNotFoundException java.io.FileNotFoundException
	 * @throws java.io.IOException java.io.IOException
	 *
	 *
	 */
	public byte[] setFromFileSystem(File originalFile) throws FileNotFoundException, IOException {
		byte[] bytes = new byte[(int) originalFile.length()];
		InputStream input = null;
		try {
			int totalBytesRead = 0;
			input = new BufferedInputStream(new FileInputStream(originalFile));
			while (totalBytesRead < bytes.length) {
				int bytesRemaining = bytes.length - totalBytesRead;
				//input.read() returns -1, 0, or more :
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
		return bytes;
	}

	/**
	 * Tries to write the contents of this DBByteArray to the file supplied.
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
	 * Tries to write the contents of this DBByteArray to the file supplied.
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
	 * Tries to write the contents of this DBByteArray to the file supplied.
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
					boolean deleteResult = originalFile.delete();
					if (!deleteResult) {
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
		if (byteStream == null) {
			this.setValue(getBytes());
		}
		return byteStream;
	}

	/**
	 * Returns the byte[] used internally to store the value of this DBByteArray.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the byte[] value of this DBByteArray.
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
	public int getSize() {
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
	public DBByteArray getQueryableDatatypeForExpressionValue() {
		return new DBByteArray();
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
		if (defn.prefersLargeObjectsReadAsBase64CharacterStream(this)) {
			try {
				bytes = getFromCharacterReader(resultSet, fullColumnName);
			} catch (IOException ex) {
				throw new DBRuntimeException("Unable To Set Value: " + ex.getMessage(), ex);
			}
		} else if (defn.prefersLargeObjectsReadAsBytes(this)) {
			bytes = getFromGetBytes(resultSet, fullColumnName);
		} else if (defn.prefersLargeObjectsReadAsCLOB(this)) {
			bytes = getFromCLOB(resultSet, fullColumnName);
		} else if (defn.prefersLargeObjectsReadAsBLOB(this)) {
			bytes = getFromBLOB(resultSet, fullColumnName);
		} else {
			bytes = getFromBinaryStream(resultSet, fullColumnName);
		}
		return bytes;
	}

	@Override
	public boolean getIncludesNull() {
		return false;
	}

	@Override
	protected void setValueFromStandardStringEncoding(String encodedValue) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public LargeObjectColumn getColumn(RowDefinition row) throws IncorrectRowProviderInstanceSuppliedException{
		return new LargeObjectColumn(row, this);
	}
}
