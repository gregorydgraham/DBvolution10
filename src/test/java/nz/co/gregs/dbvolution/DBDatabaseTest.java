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
package nz.co.gregs.dbvolution;

import java.sql.SQLException;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.exceptions.AccidentalDroppingOfDatabaseException;
import nz.co.gregs.dbvolution.exceptions.AccidentalDroppingOfTableException;
import nz.co.gregs.dbvolution.exceptions.AutoCommitActionDuringTransactionException;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBDatabaseTest extends AbstractTest {

	public DBDatabaseTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Before
	@SuppressWarnings("empty-statement")
	@Override
	public void setUp() throws Exception {
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new CreateTableWithForeignKeyTestClass());
		super.setUp();
	}

	@After
	@Override
	public void tearDown() throws Exception {
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new DropTable2TestClass());
		super.tearDown();
	}

	@Test
	public void testCreateTable() throws SQLException {

		try {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(new CreateTableTestClass());
		} catch (AutoCommitActionDuringTransactionException ex) {
//			SETUP: CreateTableTestClass table not dropped, probably doesn't exist
		}

		final CreateTableTestClass createTableTestClass = new CreateTableTestClass();
		database.createTable(createTableTestClass);
		Assert.assertThat(database.getDBTable(createTableTestClass).setBlankQueryAllowed(true).getAllRows().size(), is(0));

		try {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(new CreateTableTestClass());
		} catch (AutoCommitActionDuringTransactionException ex) {
		}
	}

	@Test
	public void testTableExists() throws SQLException {
		final CreateTableTestClass createTableTestClass = new CreateTableTestClass();
		Assert.assertThat(database.tableExists(createTableTestClass), is(false));
		try {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(createTableTestClass);
		} catch (AutoCommitActionDuringTransactionException ex) {
//			SETUP: CreateTableTestClass table not dropped, probably doesn't exist
		}
		Assert.assertThat(database.tableExists(createTableTestClass), is(false));

		database.createTable(createTableTestClass);
		Assert.assertThat(database.tableExists(createTableTestClass), is(true));
		Assert.assertThat(database.getDBTable(createTableTestClass).setBlankQueryAllowed(true).getAllRows().size(), is(0));

		try {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(new CreateTableTestClass());
		} catch (AutoCommitActionDuringTransactionException ex) {
		}
		Assert.assertThat(database.tableExists(createTableTestClass), is(false));

	}

	@Test
	public void testCreateTableWithForeignKeys() throws SQLException {

		try {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(new CreateTableWithForeignKeyTestClass());
		} catch (AutoCommitActionDuringTransactionException ex) {
			//SETUP: CreateTableWithForeignKeyTestClass table not dropped, probably doesn't exist
		}
		try {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(new CreateTableTestClass());
		} catch (AccidentalDroppingOfTableException | AutoCommitActionDuringTransactionException ex) {
		}

		final CreateTableTestClass createTableClass = new CreateTableTestClass();
		database.createTableWithForeignKeys(createTableClass);
		// This will throw an error if the table doesn't exist
		Assert.assertThat(database.getDBTable(createTableClass).setBlankQueryAllowed(true).getAllRows().size(), is(0));

		final CreateTableWithForeignKeyTestClass createTableTestClass = new CreateTableWithForeignKeyTestClass();
		database.createTableWithForeignKeys(createTableTestClass);
		// This will throw an error if the table doesn't exist
		Assert.assertThat(database.getDBTable(createTableTestClass).setBlankQueryAllowed(true).getAllRows().size(), is(0));
		database.createIndexesOnAllFields(createTableTestClass);

		try {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(new CreateTableWithForeignKeyTestClass());
		} catch (AutoCommitActionDuringTransactionException ex) {
		}
	}

	@Test
	public void testCreateTableAndAddForeignKeys() throws SQLException {

		try {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(new CreateTableWithForeignKeyTestClass2());
		} catch (AccidentalDroppingOfTableException | AutoCommitActionDuringTransactionException ex) {
			//SETUP: CreateTableWithForeignKeyTestClass table not dropped, probably doesn't exist
		}
		try {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(new CreateTableTestClass2());
		} catch (AccidentalDroppingOfTableException | AutoCommitActionDuringTransactionException ex) {
		}

		final CreateTableTestClass2 createTableClass = new CreateTableTestClass2();
		database.createTable(createTableClass);
		Assert.assertThat(database.getDBTable(createTableClass).setBlankQueryAllowed(true).getAllRows().size(), is(0));

		final CreateTableWithForeignKeyTestClass2 createTableTestClass = new CreateTableWithForeignKeyTestClass2();
		database.createTable(createTableTestClass);
		Assert.assertThat(database.getDBTable(createTableTestClass).setBlankQueryAllowed(true).getAllRows().size(), is(0));

		database.createForeignKeyConstraints(createTableTestClass);

		database.createIndexesOnAllFields(createTableTestClass);

		try {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(new CreateTableWithForeignKeyTestClass2());
		} catch (AutoCommitActionDuringTransactionException ex) {
		}
		try {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(new CreateTableWithForeignKeyTestClass());
		} catch (AutoCommitActionDuringTransactionException ex) {
		}
	}

	@Test(expected = AccidentalDroppingOfTableException.class)
	public void testDropTableException() throws SQLException {
		database.preventDroppingOfTables(true);
		try {
			database.createTable(new DropTable2TestClass());
		} catch (SQLException | AutoCommitActionDuringTransactionException ex) {
			//SETUP: DropTable2TestClass table not created, probably already exists
		}
		//SETUP: DropTable2TestClass table not created, because you are in a transaction
		database.dropTable(new DropTable2TestClass());
	}

	@Test(expected = AccidentalDroppingOfTableException.class)
	public void testDropTableAllowedOnlyOnceException() throws SQLException {
		database.preventDroppingOfTables(true);
		try {
			database.createTable(new DropTable2TestClass());
		} catch (SQLException | AutoCommitActionDuringTransactionException ex) {
		}
		try {
			database.preventDroppingOfTables(false);
			database.dropTable(new DropTable2TestClass());
		} catch (AccidentalDroppingOfTableException oops) {
		}
		database.createTable(new DropTable2TestClass());
		database.dropTable(new DropTable2TestClass());
	}

	@Test(expected = RuntimeException.class)
	public void testDropTable() throws SQLException {
		try {
			database.createTable(new DropTableTestClass());
		} catch (SQLException | AutoCommitActionDuringTransactionException ex) {
			//SETUP: DropTableTestClass table not created, probably already exists
		}
		//SETUP: DropTableTestClass table not created, probably already exists
		//Prove that the table exists
		Assert.assertThat(database.getDBTable(new DropTableTestClass()).setBlankQueryAllowed(true).getAllRows().size(), is(0));
		database.preventDroppingOfTables(false);
		database.dropTable(new DropTableTestClass());
		try {
			Assert.assertThat(database.getDBTable(new DropTableTestClass()).setBlankQueryAllowed(true).getAllRows().size(), is(0));
		} catch (SQLException exp) {
			throw new DBRuntimeException();
		}
	}

	@Test(expected = AccidentalDroppingOfDatabaseException.class)
	public void testDropDatabaseException() throws SQLException, Exception {
		database.preventDroppingOfTables(false);
		database.preventDroppingOfDatabases(true);
		database.dropDatabase(false);
	}

	public static class CreateTableTestClass extends DBRow {

		public static final long serialVersionUID = 1L;

		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		DBInteger id = new DBInteger();

		@DBColumn
		DBString name = new DBString();
	}

	public static class CreateTableWithForeignKeyTestClass extends DBRow {

		public static final long serialVersionUID = 1L;

		@DBColumn
		DBString name = new DBString();

		@DBColumn
		@DBForeignKey(CreateTableTestClass.class)
		DBInteger marqueForeignKey = new DBInteger();
	}

	public static class CreateTableTestClass2 extends DBRow {

		public static final long serialVersionUID = 1L;

		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		DBInteger id = new DBInteger();

		@DBColumn
		DBString name = new DBString();
	}

	public static class CreateTableWithForeignKeyTestClass2 extends DBRow {

		public static final long serialVersionUID = 1L;

		@DBColumn
		DBString name = new DBString();

		@DBColumn
		@DBForeignKey(CreateTableTestClass2.class)
		DBInteger marqueForeignKey = new DBInteger();
	}

	public static class DropTableTestClass extends DBRow {

		public static final long serialVersionUID = 1L;

		@DBColumn
		DBString name = new DBString();
	}

	public static class DropTable2TestClass extends DBRow {

		public static final long serialVersionUID = 1L;

		@DBColumn
		DBString name = new DBString();
	}
}
