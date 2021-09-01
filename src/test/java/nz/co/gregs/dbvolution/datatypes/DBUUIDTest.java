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

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Assert;
import org.junit.Test;

public class DBUUIDTest extends AbstractTest {

	public DBUUIDTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testDBUUID() throws SQLException {

		final UUID correctUUID = UUID.randomUUID();
		final String correctString = correctUUID.toString();

		UUIDTestTable insertRow = new UUIDTestTable();
		insertRow.uuidValue.setValue(correctUUID);
		assertThat(insertRow.uuidValue.getValue().toString(), is(correctString));

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(insertRow);
		database.createTableNoExceptions(insertRow);
		database.insert(insertRow);
		DBTable<UUIDTestTable> table = database.getDBTable(new UUIDTestTable());
		table.setBlankQueryAllowed(true);

		List<UUIDTestTable> allRows = table.getAllRows();
		for (UUIDTestTable row : allRows) {
			assertThat(row.uuidValue.stringValue(), is(correctString));
			assertThat(row.uuidValue.getValue().toString(), is(correctString));
			assertThat(row.uuidValue.getValue(), is(correctUUID));
		}
	}

	@Test
	public void testSetToRandomUUID() throws SQLException {

		UUIDTestTable insertRow = new UUIDTestTable();
		insertRow.uuidValue.setValueToNewRandomUUID();

		final UUID correctUUID = insertRow.uuidValue.getValue();
		final String correctString = correctUUID.toString();

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(insertRow);
		database.createTableNoExceptions(insertRow);
		database.insert(insertRow);
		DBTable<UUIDTestTable> table = database.getDBTable(new UUIDTestTable());
		table.setBlankQueryAllowed(true);

		List<UUIDTestTable> allRows = table.getAllRows();
		for (UUIDTestTable row : allRows) {
			assertThat(row.uuidValue.stringValue(), is(correctString));
			assertThat(row.uuidValue.getValue().toString(), is(correctString));
			assertThat(row.uuidValue.getValue(), is(correctUUID));
		}
	}

	@Test
	public void testSetToCustomUUID() throws SQLException {
		SecureRandom random = new SecureRandom();

		UUIDTestTable insertRow = new UUIDTestTable();
		insertRow.uuidValue.setValueToNewUUID(random.nextLong(), random.nextLong());

		final UUID correctUUID = insertRow.uuidValue.getValue();
		final String correctString = correctUUID.toString();

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(insertRow);
		database.createTableNoExceptions(insertRow);
		database.insert(insertRow);
		DBTable<UUIDTestTable> table = database.getDBTable(new UUIDTestTable());
		table.setBlankQueryAllowed(true);

		List<UUIDTestTable> allRows = table.getAllRows();
		for (UUIDTestTable row : allRows) {
			assertThat(row.uuidValue.stringValue(), is(correctString));
			assertThat(row.uuidValue.getValue().toString(), is(correctString));
			assertThat(row.uuidValue.getValue(), is(correctUUID));
		}
	}

	@Test
	public void testSetToNamedUUID() throws SQLException {
		
		String source = "dbvolution.gregs.co.nz" + "DBUUID";
		byte[] bytes = source.getBytes(StandardCharsets.UTF_8);
		
		final UUID correctUUID = UUID.nameUUIDFromBytes(bytes);
		final String correctString = correctUUID.toString();

		UUIDTestTable insertRow = new UUIDTestTable();
		insertRow.uuidValue.setValue(correctUUID);
		assertThat(insertRow.uuidValue.getValue().toString(), is(correctString));

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(insertRow);
		database.createTableNoExceptions(insertRow);
		database.insert(insertRow);
		DBTable<UUIDTestTable> table = database.getDBTable(new UUIDTestTable());
		table.setBlankQueryAllowed(true);

		List<UUIDTestTable> allRows = table.getAllRows();
		for (UUIDTestTable row : allRows) {
			assertThat(row.uuidValue.stringValue(), is(correctString));
			assertThat(row.uuidValue.getValue().toString(), is(correctString));
			assertThat(row.uuidValue.getValue(), is(correctUUID));
		}
	}

	@Test
	public void testSetToNamedUUIDGeneratesSameUUID() throws SQLException {
		
		String source = "dbvolution.gregs.co.nz" + "DBUUID";
		byte[] bytes = source.getBytes(StandardCharsets.UTF_8);
		
		final UUID correctUUID = UUID.nameUUIDFromBytes(bytes);
		final String correctString = correctUUID.toString();

		UUIDTestTable insertRow = new UUIDTestTable();
		insertRow.uuidValue.setValueToNamedUUIDFromBytes(bytes);
		assertThat(insertRow.uuidValue.getValue().toString(), is(correctString));

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(insertRow);
		database.createTableNoExceptions(insertRow);
		database.insert(insertRow);
		DBTable<UUIDTestTable> table = database.getDBTable(new UUIDTestTable());
		table.setBlankQueryAllowed(true);

		List<UUIDTestTable> allRows = table.getAllRows();
		for (UUIDTestTable row : allRows) {
			assertThat(row.uuidValue.stringValue(), is(correctString));
			assertThat(row.uuidValue.getValue().toString(), is(correctString));
			assertThat(row.uuidValue.getValue(), is(correctUUID));
		}
	}

	@Test
	public void testDefaultToRandomUUID() throws SQLException {
		
		UUIDTestTable insertRow = new UUIDTestTable();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(insertRow);
		database.createTableNoExceptions(insertRow);
		
		database.insert(new UUIDTestTable(), new UUIDTestTable());
		
		DBTable<UUIDTestTable> table = database.getDBTable(new UUIDTestTable());
		table.setBlankQueryAllowed(true);
		List<UUIDTestTable> allRows = table.getAllRows();
		
		assertThat(allRows.size(), is(2));
		
		UUIDTestTable firstRow = allRows.get(0);
		UUIDTestTable secondRow = allRows.get(1);
		
		Assert.assertNull(firstRow.uuidValue.getValue());
		Assert.assertNull(secondRow.uuidValue.getValue());
		
		Assert.assertNotNull(firstRow.defaultUUIDValueByName.getValue());
		Assert.assertNotNull(secondRow.defaultUUIDValueByName.getValue());
		
		Assert.assertNotNull(firstRow.defaultUUIDValueRandomly.getValue());
		Assert.assertNotNull(secondRow.defaultUUIDValueRandomly.getValue());
		
		assertThat(firstRow.defaultUUIDValueByName.getValue(), is(secondRow.defaultUUIDValueByName.getValue()));
		assertThat(firstRow.defaultUUIDValueRandomly.getValue().toString(), is(not(secondRow.defaultUUIDValueRandomly.getValue())));
	}

	public static class UUIDTestTable extends DBRow {

		private final static long serialVersionUID = 1l;

		@DBAutoIncrement
		@DBPrimaryKey
		@DBColumn("pkid")
		public DBInteger pkid = new DBInteger();

		@DBColumn("uuidColumn")
		public DBUUID uuidValue = new DBUUID();

		@DBColumn("defaulting_uuid")
		public DBUUID defaultUUIDValueByName = new DBUUID().setDefaultInsertValueByName("dbvolution.gregs.co.nz" + "UUIDTestTable");

		@DBColumn("random_uuid")
		public DBUUID defaultUUIDValueRandomly = new DBUUID().setDefaultInsertValueRandomly();

	}

}
