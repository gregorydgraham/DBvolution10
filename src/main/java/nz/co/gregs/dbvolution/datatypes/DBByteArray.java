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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.DBDatabase;

/**
 *
 * Implements the abstractions necessary to handle arbitrary byte streams and
 * files stored in the database
 *
 * @author Gregory Graham
 */
public class DBByteArray extends DBLargeObject {

    public static final long serialVersionUID = 1;
    InputStream byteStream = null;

    public DBByteArray() {
        super();
    }

    /**
     *
     * @return the standard SQL datatype that corresponds to this QDT as a
     * String
     */
    @Override
    public String getSQLDatatype() {
        return "BLOB";
    }

    @Override
    public void setValue(Object newLiteralValue) {
        if (newLiteralValue instanceof byte[]) {
            setValue((byte[]) newLiteralValue);
        } else if (newLiteralValue instanceof DBByteArray) {
            setValue(((DBByteArray) newLiteralValue).getValue());
        } else {
            throw new ClassCastException(this.getClass().getSimpleName() + ".setValue() Called With A Non-Byte[]: Use only byte arrays with this class");
        }
    }

    public void setValue(byte[] byteArray) {
        super.setLiteralValue(byteArray);
        byteStream = new BufferedInputStream(new ByteArrayInputStream(byteArray));
    }

    public void setValue(InputStream inputViaStream) {
        super.setLiteralValue(inputViaStream);
        byteStream = new BufferedInputStream(inputViaStream);
    }

    public void setValue(File fileToRead) throws IOException {
        setValue(setFromFileSystem(fileToRead));
    }
    
    @Override
    public void setFromResultSet(ResultSet resultSet, String fullColumnName) {
        InputStream dbValue;
        if (resultSet == null || fullColumnName == null) {
            this.setToNull();
        } else {

            try {
                dbValue = resultSet.getBinaryStream(fullColumnName);
                if (resultSet.wasNull()) {
                    dbValue = null;
                }
            } catch (SQLException ex) {
                dbValue = null;
            }
            if (dbValue == null) {
                this.setToNull();
            } else {
                InputStream input = new BufferedInputStream(dbValue);
                List<byte[]> byteArrays = new ArrayList<byte[]>();

                int totalBytesRead = 0;
                try {
                    byte[] resultSetBytes;
                    resultSetBytes = new byte[100000];
                    int bytesRead = input.read(resultSetBytes);
                    while (bytesRead > 0) {
                        totalBytesRead = totalBytesRead + bytesRead;
                        byteArrays.add(resultSetBytes);
                        resultSetBytes = new byte[100000];
                        bytesRead = input.read(resultSetBytes);
                    }
                } catch (IOException ex) {
                    Logger.getLogger(DBByteArray.class.getName()).log(Level.SEVERE, null, ex);
                }
                byte[] bytes = new byte[totalBytesRead];
                int bytesAdded = 0;
                for (byte[] someBytes : byteArrays) {
                    System.arraycopy(someBytes, 0, bytes, bytesAdded, Math.min(someBytes.length, bytes.length - bytesAdded));
                    bytesAdded += someBytes.length;
                }
                this.setValue(bytes);
            }
        }
    }

    @Override
    public String formatValueForSQLStatement(DBDatabase db) {
        throw new UnsupportedOperationException("Binary datatypes like " + this.getClass().getSimpleName() + " do not have a simple SQL representation. Do not call getSQLValue(), use the getInputStream() method instead.");
    }

    public byte[] setFromFileSystem(String originalFile) throws FileNotFoundException, IOException {
        File file = new File(originalFile);
        return setFromFileSystem(file);
    }

    public byte[] setFromFileSystem(DBString originalFile) throws FileNotFoundException, IOException {
        File file = new File(originalFile.stringValue());
        return setFromFileSystem(file);
    }

    public byte[] setFromFileSystem(File originalFile) throws FileNotFoundException, IOException {
        System.out.println("FILE: " + originalFile.getAbsolutePath());
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
                    totalBytesRead = totalBytesRead + bytesRead;
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

    public void writeToFileSystem(String originalFile) throws FileNotFoundException, IOException {
        File file = new File(originalFile);
        writeToFileSystem(file);
    }

    public void writeToFileSystem(DBString originalFile) throws FileNotFoundException, IOException {
        writeToFileSystem(originalFile.toString());
    }

    public void writeToFileSystem(File originalFile) throws FileNotFoundException, IOException {
        if (literalValue != null && originalFile != null) {
            System.out.println("FILE: " + originalFile.getAbsolutePath());
            if (!originalFile.exists()) {
                originalFile.createNewFile();
            }
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
        }
    }

    @Override
    public InputStream getInputStream() {
        if (byteStream == null) {
            this.setValue(getBytes());
        }
        return byteStream;
    }

    public byte[] getBytes() {
        return (byte[]) this.literalValue;
    }

    @Override
    public int getSize() {
        return getBytes().length;
    }

    @Override
    public byte[] getValue() {
        return getBytes();
    }

    @Override
    public DBByteArray getQueryableDatatypeForExpressionValue() {
        return new DBByteArray();
    }

}
