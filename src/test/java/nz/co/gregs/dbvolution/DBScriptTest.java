/*
 * Copyright 2014 Gregory Graham.
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

import nz.co.gregs.dbvolution.databases.DBDatabase;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.*;
import org.junit.Assert;
import static org.hamcrest.Matchers.*;

import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.exceptions.ExceptionThrownDuringTransaction;
import org.junit.rules.ExpectedException;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBScriptTest extends AbstractTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	public DBScriptTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	/**
	 * Test of implement method, of class DBScript.
	 *
	 * @throws java.lang.Exception
	 */
	@Test
	public void testImplement() throws Exception {
		List<Marque> allMarques = database.getDBTable(new Marque()).setBlankQueryAllowed(true).getAllRows();
		DBScript script = new ScriptThatAdds2Marques();
		DBActionList result = script.implement(database);
		List<Marque> allMarques2 = database.getDBTable(new Marque()).setBlankQueryAllowed(true).getAllRows();
		Assert.assertThat(
				allMarques2.size(),
				is(allMarques.size() + 2));
	}

	/**
	 * Test of implement method, of class DBScript.
	 *
	 * @throws java.lang.Exception
	 */
	@Test
	public void testImplementOfDBDatabase() throws Exception {
		List<Marque> allMarques = database.getDBTable(new Marque()).setBlankQueryAllowed(true).getAllRows();
		DBScript script = new ScriptThatAdds2Marques();
		DBActionList result = database.implement(script);
		List<Marque> allMarques2 = database.getDBTable(new Marque()).setBlankQueryAllowed(true).getAllRows();
		Assert.assertThat(
				allMarques2.size(),
				is(allMarques.size() + 2));
	}

	/**
	 * Test of test method, of class DBScript.
	 *
	 * @throws java.lang.Exception
	 */
	@Test
	public void testTest() throws Exception {
		List<Marque> allMarques = database.getDBTable(new Marque()).setBlankQueryAllowed(true).getAllRows();
		DBScript script = new ScriptThatAdds2Marques();
		DBActionList result = script.test(database);
		List<Marque> allMarques2 = database.getDBTable(new Marque()).setBlankQueryAllowed(true).getAllRows();
		Assert.assertThat(
				allMarques2.size(),
				is(allMarques.size()));
	}

	/**
	 * Test of test method, of class DBScript.
	 *
	 * @throws java.lang.Exception
	 */
	@Test
	public void testTestOfDBDatabase() throws Exception {
		List<Marque> allMarques = database.getDBTable(new Marque()).setBlankQueryAllowed(true).getAllRows();
		DBScript script = new ScriptThatAdds2Marques();
		DBActionList result = database.test(script);
		List<Marque> allMarques2 = database.getDBTable(new Marque()).setBlankQueryAllowed(true).getAllRows();
		Assert.assertThat(
				allMarques2.size(),
				is(allMarques.size()));
	}

	@Test
	public void testExceptionThrowing() throws Exception {
		List<Marque> allMarques = database.getDBTable(new Marque()).setBlankQueryAllowed(true).getAllRows();

		thrown.expect(ExceptionThrownDuringTransaction.class);
		DBScript script = new ScriptThatThrowsAnException();
		DBActionList result = script.test(database);
		List<Marque> allMarques2 = database.getDBTable(new Marque()).setBlankQueryAllowed(true).getAllRows();
		Assert.assertThat(
				allMarques2.size(),
				is(allMarques.size()));
	}

	/**
	 * Test of test method, of class DBScript.
	 *
	 * @throws java.lang.Exception
	 */
	@Test
	public void testTestTransactionsAreIsolated() throws Exception {
		final ScriptTestTable scriptTestTable = new ScriptTestTable();
		final DBTable<ScriptTestTable> table = database.getDBTable(scriptTestTable);
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(scriptTestTable);
		database.createTable(scriptTestTable);
		List<ScriptTestTable> origRows = table.setBlankQueryAllowed(true).getAllRows();

		ExecutorService threadpool = Executors.newFixedThreadPool(10);
		ArrayList<Callable<DBActionList>> taskGroup = new ArrayList<Callable<DBActionList>>();
		taskGroup.add(new CallableTestScript<>(database));
		taskGroup.add(new CallableTestScript<>(database));
		taskGroup.add(new CallableTestScript<>(database));
		taskGroup.add(new CallableTestScript<>(database));
		taskGroup.add(new CallableTestScript<>(database));
		taskGroup.add(new CallableTestScript<>(database));
		taskGroup.add(new CallableTestScript<>(database));
		threadpool.invokeAll(taskGroup);

		List<ScriptTestTable> allRows = table.setBlankQueryAllowed(true).getAllRows();
		Assert.assertThat(allRows.size(), is(origRows.size()));
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(scriptTestTable);
	}

	@Test
	public void testImplementTransactionsAreIsolated() throws Exception {
		final ScriptTestTable scriptTestTable = new ScriptTestTable();
		final DBTable<ScriptTestTable> table = database.getDBTable(scriptTestTable);
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(scriptTestTable);
		database.createTable(scriptTestTable);
		List<ScriptTestTable> origRows = table.setBlankQueryAllowed(true).getAllRows();

		ExecutorService threadpool = Executors.newFixedThreadPool(10);
		ArrayList<Callable<DBActionList>> taskGroup = new ArrayList<>();
		taskGroup.add(new CallableImplementScript<>(database));
		taskGroup.add(new CallableImplementScript<>(database));
		taskGroup.add(new CallableImplementScript<>(database));
		taskGroup.add(new CallableImplementScript<>(database));
		taskGroup.add(new CallableImplementScript<>(database));
		taskGroup.add(new CallableImplementScript<>(database));
		taskGroup.add(new CallableImplementScript<>(database));
		threadpool.invokeAll(taskGroup);

		List<ScriptTestTable> allRows = table.setBlankQueryAllowed(true).getAllRows();
		Assert.assertThat(allRows.size(), is(origRows.size()));
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(scriptTestTable);
	}

	public class ScriptThatThrowsAnException extends DBScript {

		public ScriptThatThrowsAnException() {
		}

		@Override
		public DBActionList script(DBDatabase db) throws Exception {
			final IndexOutOfBoundsException indexOutOfBoundsException = new IndexOutOfBoundsException("Correct Exception");
			StackTraceElement[] stackTrace = indexOutOfBoundsException.getStackTrace();
			indexOutOfBoundsException.setStackTrace(new StackTraceElement[]{stackTrace[0]});

			throw indexOutOfBoundsException;
		}
	}

	public class ScriptThatAdds2Marques extends DBScript {

		@Override
		public DBActionList script(DBDatabase db) throws Exception {
			DBActionList actions = new DBActionList();
			Marque myTableRow = new Marque();
			DBTable<Marque> marques = DBTable.getInstance(db, myTableRow);
			myTableRow.getUidMarque().setValue(999);
			myTableRow.getName().setValue("TOYOTA");
			myTableRow.getNumericCode().setValue(10);
			actions.addAll(marques.insert(myTableRow));
			marques.setBlankQueryAllowed(true).getAllRows();

			List<Marque> myTableRows = new ArrayList<Marque>();
			myTableRows.add(new Marque(3, "False", 1246974, "", 3, "UV", "TVR", "", "Y", new Date(), 4, null));

			actions.addAll(marques.insert(myTableRows));

			marques.getAllRows();
			return actions;
		}
	}

	public class ScriptThatAddsAndRemoves2Rows extends DBScript {

		@Override
		public synchronized DBActionList script(DBDatabase db) throws Exception {
			DBActionList actions = new DBActionList();
			ArrayList<ScriptTestTable> myTableRows = new ArrayList<ScriptTestTable>();

			ScriptTestTable myTableRow = new ScriptTestTable();
			myTableRows.add(myTableRow);
			DBTable<ScriptTestTable> table = DBTable.getInstance(db, myTableRow);

			List<ScriptTestTable> origRows = table.setBlankQueryAllowed(true).getAllRows();

			myTableRow.name.setValue("TOYOTA");
			actions.addAll(table.insert(myTableRow));

			List<ScriptTestTable> allRows = table.setBlankQueryAllowed(true).getAllRows();

			Assert.assertThat(allRows.size(), is(origRows.size() + 1));
			final ScriptTestTable newRow = new ScriptTestTable("False");
			myTableRows.add(newRow);

			actions.addAll(table.insert(newRow));

			allRows = table.setBlankQueryAllowed(true).getAllRows();

			Assert.assertThat(allRows.size(), is(origRows.size() + 2));

			table.getAllRows();

			table.delete(myTableRows);
			allRows = table.setBlankQueryAllowed(true).getAllRows();

			Assert.assertThat(allRows.size(), is(origRows.size()));

			return actions;
		}
	}

	public class CallableTestScript<A> implements Callable<DBActionList> {

		private final DBScript script;
		private final DBDatabase database;

		public CallableTestScript(DBDatabase database) {
			this.script = new ScriptThatAddsAndRemoves2Rows();
			this.database = database;
		}

		@Override
		public DBActionList call() throws Exception {
			return script.test(database);
		}

	}

	public class CallableImplementScript<A> implements Callable<DBActionList> {

		private final DBScript script;
		private final DBDatabase database;

		public CallableImplementScript(DBDatabase database) {
			this.script = new ScriptThatAddsAndRemoves2Rows();
			this.database = database;
		}

		@Override
		public DBActionList call() throws Exception {
			return script.implement(database);
		}

	}

	public static class ScriptTestTable extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBPrimaryKey
		@DBColumn
		@DBAutoIncrement
		DBInteger uid = new DBInteger();

		@DBColumn
		DBString name = new DBString();

		public ScriptTestTable(String name) {
			super();
			this.name.setValue(name);
		}

		public ScriptTestTable() {
			super();
		}

	}
}
