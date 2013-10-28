/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
 * @author gregory.graham
 */
public class DBByteArray extends DBLargeObject {

    public static final long serialVersionUID = 1;
    byte[] bytes;
    InputStream byteStream = null;

    public DBByteArray(Object object) {
        super(object);
    }

    public DBByteArray() {
        super();
    }

    /**
     *
     * @return 
     */
    @Override
    public String getSQLDatatype() {
        return "BLOB";
    }

    public void setValue(byte[] byteArray) {
        super.setValue(byteArray);
        bytes = byteArray;
        byteStream = new BufferedInputStream(new ByteArrayInputStream(bytes));
    }

    public void setValue(InputStream inputViaStream) {
        super.setValue(inputViaStream);
        bytes = null;
        byteStream = new BufferedInputStream(inputViaStream);
    }

    public void setValue(File fileToRead) throws IOException{
        setValue(readFromFileSystem(fileToRead));
    }

    @Override
    public void setFromResultSet(ResultSet resultSet, String fullColumnName) {
        InputStream dbValue;
        if (resultSet == null || fullColumnName == null) {
            this.useNullOperator();
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
                this.useNullOperator();
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

                bytes = new byte[totalBytesRead];
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

    public byte[] readFromFileSystem(String originalFile) throws FileNotFoundException, IOException {
        File file = new File(originalFile);
        return readFromFileSystem(file);
    }

    public byte[] readFromFileSystem(DBString originalFile) throws FileNotFoundException, IOException {
        File file = new File(originalFile.stringValue());
        return readFromFileSystem(file);
    }

    public byte[] readFromFileSystem(File originalFile) throws FileNotFoundException, IOException {
        System.out.println("FILE: " + originalFile.getAbsolutePath());
        bytes = new byte[(int) originalFile.length()];
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
        if (bytes != null && originalFile != null) {
            System.out.println("FILE: " + originalFile.getAbsolutePath());
            if (!originalFile.exists()) {
                originalFile.createNewFile();
            }
            OutputStream output = null;
            try {
                output = new BufferedOutputStream(new FileOutputStream(originalFile));
                output.write(this.bytes);
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
            this.setValue(bytes);
        }
        return byteStream;
    }

    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public int getSize() {
        return bytes.length;
    }
}
