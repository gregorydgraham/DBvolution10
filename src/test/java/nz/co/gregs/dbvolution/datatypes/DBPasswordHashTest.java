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
import nz.co.gregs.dbvolution.exceptions.IncorrectPasswordException;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

public class DBPasswordHashTest extends AbstractTest {

	public DBPasswordHashTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testDBPasswordHash() throws SQLException, IncorrectPasswordException {
		PasswordTestTable insertRow = new PasswordTestTable();
		String correctPassword = "correct secretAAA!!!{}|!@#$%^&*()_+-=';:/?.,<>\"";
		String wrongPassword = "wrong passwordAAA!!!{}|!@#$%^&*()_+-=';:/?.,<>\"";

		insertRow.password.setValue(correctPassword);
		insertRow.passwordHash.setValue(correctPassword);
		insertRow.wrongpassword.setValue(wrongPassword);
		Assert.assertThat(insertRow.password.getValue(), not(insertRow.passwordHash.getValue()));

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(insertRow);
		database.createTableNoExceptions(insertRow);
		database.insert(insertRow);
		DBTable<PasswordTestTable> table = database.getDBTable(new PasswordTestTable());
		table.setBlankQueryAllowed(true);

		List<PasswordTestTable> allRows = table.getAllRows();
		for (PasswordTestTable row : allRows) {
			Assert.assertThat(row.password.getValue(), not(row.passwordHash.getValue()));
			Assert.assertThat(row.passwordHash.checkPassword(correctPassword), is(true));
			Assert.assertThat(row.passwordHash.checkPassword(wrongPassword), is(false));
			Assert.assertThat(row.passwordHash.checkPasswordWithException(correctPassword), is(true));
			try {
				Assert.assertThat(row.passwordHash.checkPasswordWithException(wrongPassword), is(false));
				Assert.fail("row.passwordHash.checkPasswordWithException(wrongPassword) should have thrown an exception");
			} catch (IncorrectPasswordException exp) {
				// all good, we were hoping for an exception :)
			}

			String hash1 = row.passwordHash.getValue();
			boolean loggedin = row.passwordHash.checkPasswordAndUpdateHash(correctPassword);
			Assert.assertTrue(loggedin);
//			System.out.println(hash1 + " not equals " + row.passwordHash.getValue());
			Assert.assertThat(hash1, not(row.passwordHash.getValue()));
			
			Assert.assertThat(row.passwordHash.checkPassword(correctPassword), is(true));
			Assert.assertThat(row.passwordHash.checkPassword(wrongPassword), is(false));
		}
	}

	public static class PasswordTestTable extends DBRow {

		private final static long serialVersionUID = 1l;

		@DBAutoIncrement
		@DBPrimaryKey
		@DBColumn("pkid")
		public DBInteger pkid = new DBInteger();

		@DBColumn("hash_col")
		public DBPasswordHash passwordHash = new DBPasswordHash();

		@DBColumn("password")
		public DBString password = new DBString();

		@DBColumn("wrongpassword")
		public DBString wrongpassword = new DBString();

	}

}
