/*
 * Copyright 2018 Gregory Graham.
 *
 * Commercial licenses are available, please contact info@gregs.co.nz for details.
 * 
 * This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. 
 * To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/4.0/ 
 * or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.
 * 
 * You are free to:
 *     Share - copy and redistribute the material in any medium or format
 *     Adapt - remix, transform, and build upon the material
 * 
 *     The licensor cannot revoke these freedoms as long as you follow the license terms.               
 *     Under the following terms:
 *                 
 *         Attribution - 
 *             You must give appropriate credit, provide a link to the license, and indicate if changes were made. 
 *             You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.
 *         NonCommercial - 
 *             You may not use the material for commercial purposes.
 *         ShareAlike - 
 *             If you remix, transform, or build upon the material, 
 *             you must distribute your contributions under the same license as the original.
 *         No additional restrictions - 
 *             You may not apply legal terms or technological measures that legally restrict others from doing anything the 
 *             license permits.
 * 
 * Check the Creative Commons website for any details, legalese, and updates.
 */
package nz.co.gregs.dbvolution.datatypes;

import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.exceptions.CannotEncryptInputException;
import nz.co.gregs.dbvolution.exceptions.IncorrectPasswordException;
import nz.co.gregs.dbvolution.exceptions.UnableToDecryptInput;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import nz.co.gregs.dbvolution.utility.encryption.Encrypted;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Test;

public class DBEncryptedTextTest extends AbstractTest {

	public DBEncryptedTextTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testDBEncryptedString() throws SQLException, IncorrectPasswordException, CannotEncryptInputException, UnableToDecryptInput {

		EncryptedTextTestTable insertRow = new EncryptedTextTestTable();
		String passphrase = "very secret phraseAAA!!!{}|!@#$%^&*()_+-=';:/?.,<>\"";
		String correctSecret = "correct secretAAA!!!{}|!@#$%^&*()_+-=';:/?.,<>\"";

		insertRow.encryptedString.setValue(Encrypted.encrypt(passphrase, correctSecret));
		final Encrypted encryptedValue = insertRow.encryptedString.getEncryptedValue();
		assertThat(insertRow.encryptedString.getEncryptedValue().toString(), startsWith("BASE64_AES/GCM/NoPadding|"));
		assertThat(insertRow.encryptedString.decryptWith(passphrase), is(correctSecret));

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(insertRow);
		database.createTableNoExceptions(insertRow);
		database.insert(insertRow);
		DBTable<EncryptedTextTestTable> table = database.getDBTable(new EncryptedTextTestTable());
		table.setBlankQueryAllowed(true);

		List<EncryptedTextTestTable> allRows = table.getAllRows();
		for (EncryptedTextTestTable row : allRows) {
			assertThat(row.encryptedString.getEncryptedValue(), is(encryptedValue));
			assertThat(row.encryptedString.getEncryptedValue(), not(correctSecret));
			assertThat(row.encryptedString.decryptWith(passphrase), is(correctSecret));
		}
	}

	@Test
	public void testDBEncryptedStringEasyAPI() throws SQLException, IncorrectPasswordException, CannotEncryptInputException, UnableToDecryptInput {

		EncryptedTextTestTable insertRow = new EncryptedTextTestTable();
		String passphrase = "very secret phraseAAA!!!{}|!@#$%^&*()_+-=';:/?.,<>\"";
		String correctSecret = "correct secretAAA!!!{}|!@#$%^&*()_+-=';:/?.,<>\"insertRow.encryptedString.setValue(passphrase, correctSecret);\n"
				+ "		final EncryptedString encryptedValue = insertRow.encryptedString.getEncryptedValue();\n"
				+ "		assertThat(insertRow.encryptedString.getValue().toString(), startsWith(\"BASE64_AES_CBC_PKCS5Padding|\"));\n"
				+ "		assertThat(insertRow.encryptedString.getValue().decrypt(passphrase), is(correctSecret));\n"
				+ "\n"
				+ "		database.preventDroppingOfTables(false);\n"
				+ "		database.dropTableNoExceptions(insertRow);\n"
				+ "		database.createTableNoExceptions(insertRow);\n"
				+ "		database.insert(insertRow);\n"
				+ "		DBTable<EncryptedTestTable> table = database.getDBTable(new EncryptedTestTable());\n"
				+ "		table.setBlankQueryAllowed(true);\n"
				+ "\n"
				+ "		List<EncryptedTestTable> allRows = table.getAllRows();\n"
				+ "		for (EncryptedTestTable row : allRows) {\n"
				+ "			assertThat(row.encryptedString.getEncryptedValue(), is(encryptedValue));\n"
				+ "			assertThat(row.encryptedString.getEncryptedValue(), not(correctSecret));\n"
				+ "			assertThat(row.encryptedString.getDecryptedValue(passphrase), is(correctSecret));"				+ "		final EncryptedString encryptedValue = insertRow.encryptedString.getEncryptedValue();\n"
				+ "		assertThat(insertRow.encryptedString.getValue().toString(), startsWith(\"BASE64_AES_CBC_PKCS5Padding|\"));\n"
				+ "		assertThat(insertRow.encryptedString.getValue().decrypt(passphrase), is(correctSecret));\n"
				+ "\n"
				+ "		database.preventDroppingOfTables(false);\n"
				+ "		database.dropTableNoExceptions(insertRow);\n"
				+ "		database.createTableNoExceptions(insertRow);\n"
				+ "		database.insert(insertRow);\n"
				+ "		DBTable<EncryptedTestTable> table = database.getDBTable(new EncryptedTestTable());\n"
				+ "		table.setBlankQueryAllowed(true);\n"
				+ "\n"
				+ "		List<EncryptedTestTable> allRows = table.getAllRows();\n"
				+ "		for (EncryptedTestTable row : allRows) {\n"
				+ "			assertThat(row.encryptedString.getEncryptedValue(), is(encryptedValue));\n"
				+ "			assertThat(row.encryptedString.getEncryptedValue(), not(correctSecret));\n"
				+ "			assertThat(row.encryptedString.getDecryptedValue(passphrase), is(correctSecret));"				+ "		final EncryptedString encryptedValue = insertRow.encryptedString.getEncryptedValue();\n"
				+ "		assertThat(insertRow.encryptedString.getValue().toString(), startsWith(\"BASE64_AES_CBC_PKCS5Padding|\"));\n"
				+ "		assertThat(insertRow.encryptedString.getValue().decrypt(passphrase), is(correctSecret));\n"
				+ "\n"
				+ "		database.preventDroppingOfTables(false);\n"
				+ "		database.dropTableNoExceptions(insertRow);\n"
				+ "		database.createTableNoExceptions(insertRow);\n"
				+ "		database.insert(insertRow);\n"
				+ "		DBTable<EncryptedTestTable> table = database.getDBTable(new EncryptedTestTable());\n"
				+ "		table.setBlankQueryAllowed(true);\n"
				+ "\n"
				+ "		List<EncryptedTestTable> allRows = table.getAllRows();\n"
				+ "		for (EncryptedTestTable row : allRows) {\n"
				+ "			assertThat(row.encryptedString.getEncryptedValue(), is(encryptedValue));\n"
				+ "			assertThat(row.encryptedString.getEncryptedValue(), not(correctSecret));\n"
				+ "			assertThat(row.encryptedString.getDecryptedValue(passphrase), is(correctSecret));"
				+ "		}";

		insertRow.encryptedString.setValue(passphrase, correctSecret);
		final Encrypted encryptedValue = insertRow.encryptedString.getEncryptedValue();
		assertThat(insertRow.encryptedString.getEncryptedValue().toString(), startsWith("BASE64_AES/GCM/NoPadding|"));
		assertThat(insertRow.encryptedString.getDecryptedValue(passphrase), is(correctSecret));

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(insertRow);
		database.createTableNoExceptions(insertRow);
		database.insert(insertRow);
		DBTable<EncryptedTextTestTable> table = database.getDBTable(new EncryptedTextTestTable());
		table.setBlankQueryAllowed(true);

		List<EncryptedTextTestTable> allRows = table.getAllRows();
		for (EncryptedTextTestTable row : allRows) {
			assertThat(row.encryptedString.getEncryptedValue(), is(encryptedValue));
			assertThat(row.encryptedString.getEncryptedValue(), not(correctSecret));
			assertThat(row.encryptedString.getDecryptedValue(passphrase), is(correctSecret));
		}
	}

	public static class EncryptedTextTestTable extends DBRow {

		private final static long serialVersionUID = 1l;

		@DBAutoIncrement
		@DBPrimaryKey
		@DBColumn("pkid")
		public DBInteger pkid = new DBInteger();

		@DBColumn("encryptedcol")
		public DBEncryptedText encryptedString = new DBEncryptedText();

		@DBColumn()
		public DBInteger dummy = new DBInteger(1);

	}

}
