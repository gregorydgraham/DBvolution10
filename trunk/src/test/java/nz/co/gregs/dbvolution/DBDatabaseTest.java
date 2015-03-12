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
import nz.co.gregs.dbvolution.databases.SQLiteDB;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.exceptions.AccidentalDroppingOfDatabaseException;
import nz.co.gregs.dbvolution.exceptions.AccidentalDroppingOfTableException;
import nz.co.gregs.dbvolution.exceptions.AutoCommitActionDuringTransactionException;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
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
			System.out.println("SETUP: CreateTableTestClass table not dropped, probably doesn't exist: " + ex.getMessage());
		}

		final CreateTableTestClass createTableTestClass = new CreateTableTestClass();
		database.createTable(createTableTestClass);
		System.out.println("CreateTableTestClass table created successfully");

		try {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(new CreateTableTestClass());
		} catch (AutoCommitActionDuringTransactionException ex) {
			System.out.println("SETUP: CreateTableTestClass table not dropped, probably doesn't exist: " + ex.getMessage());
		}
	}

	@Test
	public void testCreateTableWithForeignKeys() throws SQLException {

		try {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(new CreateTableWithForeignKeyTestClass());
		} catch (AutoCommitActionDuringTransactionException ex) {
			System.out.println("SETUP: CreateTableWithForeignKeyTestClass table not dropped, probably doesn't exist: " + ex.getMessage());
		}
		try {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(new CreateTableTestClass());
		} catch (Exception ex) {
		}

		final CreateTableTestClass createTableClass = new CreateTableTestClass();
		database.createTableWithForeignKeys(createTableClass);

		final CreateTableWithForeignKeyTestClass createTableTestClass = new CreateTableWithForeignKeyTestClass();
		database.createTableWithForeignKeys(createTableTestClass);
		System.out.println("CreateTableWithForeignKeyTestClass table created successfully");
		database.createIndexesOnAllFields(createTableTestClass);
		System.out.println("CreateTableWithForeignKeyTestClass indexes created successfully");

		try {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(new CreateTableWithForeignKeyTestClass());
		} catch (AutoCommitActionDuringTransactionException ex) {
			System.out.println("SETUP: CreateTableWithForeignKeyTestClass table not dropped, probably doesn't exist: " + ex.getMessage());
		}
	}

	@Test
	public void testCreateTableAndAddForeignKeys() throws SQLException {

		try {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(new CreateTableWithForeignKeyTestClass2());
		} catch (Exception ex) {
			System.out.println("SETUP: CreateTableWithForeignKeyTestClass table not dropped, probably doesn't exist: " + ex.getMessage());
		}
		try {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(new CreateTableTestClass2());
		} catch (Exception ex) {
		}

		final CreateTableTestClass2 createTableClass = new CreateTableTestClass2();
		database.createTable(createTableClass);

		final CreateTableWithForeignKeyTestClass2 createTableTestClass = new CreateTableWithForeignKeyTestClass2();
		database.createTable(createTableTestClass);
		if (!(database instanceof SQLiteDB)) {
			database.createForeignKeyConstraints(createTableTestClass);
			System.out.println("CreateTableWithForeignKeyTestClass foreign keys created successfully");
		}
		database.createIndexesOnAllFields(createTableTestClass);
		System.out.println("CreateTableWithForeignKeyTestClass indexes created successfully");

		try {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(new CreateTableWithForeignKeyTestClass2());
		} catch (AutoCommitActionDuringTransactionException ex) {
			System.out.println("SETUP: CreateTableWithForeignKeyTestClass2 table not dropped, probably doesn't exist: " + ex.getMessage());
		}
		try {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(new CreateTableWithForeignKeyTestClass());
		} catch (AutoCommitActionDuringTransactionException ex) {
			System.out.println("SETUP: CreateTableWithForeignKeyTestClass table not dropped, probably doesn't exist: " + ex.getMessage());
		}
	}

	@Test
	public void testDropTableException() throws SQLException {
		database.preventDroppingOfTables(true);
		try {
			database.createTable(new DropTable2TestClass());
		} catch (SQLException ex) {
			System.out.println("SETUP: DropTable2TestClass table not created, probably already exists" + ex.getMessage());
		} catch (AutoCommitActionDuringTransactionException ex) {
			System.out.println("SETUP: DropTable2TestClass table not created, because you are in a transaction.");
		}
		try {
			database.dropTable(new DropTable2TestClass());
			throw new DBRuntimeException("Drop Table Method failed to throw a AccidentalDroppingOfTableException exception.");
		} catch (AccidentalDroppingOfTableException oops) {
			System.out.println("AccidentalDroppingOfTableException successfully thrown");
		}
		try {
			database.preventDroppingOfTables(false);
			database.dropTable(new DropTable2TestClass());
			database.createTable(new DropTable2TestClass());
			database.dropTable(new DropTable2TestClass());
			throw new DBRuntimeException("Drop Table Method failed to throw a AccidentalDroppingOfTableException exception.");
		} catch (AccidentalDroppingOfTableException oops) {
			System.out.println("AccidentalDroppingOfTableException successfully thrown");
		}
		database.preventDroppingOfTables(false);
		database.dropTable(new DropTable2TestClass());
	}

	@Test
	public void testDropTable() throws SQLException {
		try {
			database.createTable(new DropTableTestClass());
		} catch (SQLException ex) {
			System.out.println("SETUP: DropTableTestClass table not created, probably already exists" + ex.getMessage());
		} catch (AutoCommitActionDuringTransactionException ex) {
			System.out.println("SETUP: DropTableTestClass table not created, probably already exists" + ex.getMessage());
		}
		database.preventDroppingOfTables(false);
		database.dropTable(new DropTableTestClass());
		System.out.println("DropTableTestClass table dropped successfully");
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
