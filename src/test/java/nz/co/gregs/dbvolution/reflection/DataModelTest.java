/*
 * Copyright 2015 gregorygraham.
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
package nz.co.gregs.dbvolution.reflection;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author gregorygraham
 */
public class DataModelTest extends AbstractTest {

	public DataModelTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testGetDatabases() {
		System.out.println("getDatabases");
		Set<Class<? extends DBDatabase>> result = DataModel.getUseableDBDatabaseClasses();
		for (Class<? extends DBDatabase> result1 : result) {
			System.out.println("DBDatabase: " + result1.getName());
		}
		Assert.assertThat(result.size(), is(9));
	}

	@Test
	public void testGetDBDatabaseConstructors() {
		System.out.println("getDBDatabaseConstructors");
		Set<Constructor<DBDatabase>> result = DataModel.getDBDatabaseConstructors();
		for (Constructor<DBDatabase> result1 : result) {
			System.out.println("Constructor: " + result1.getName());
		}
		Assert.assertThat(result.size(), is(9));
	}

	@Test
	public void testGetDBDatabaseConstructorsPublicWithoutParameters() {
		System.out.println("getDBDatabaseConstructorsPublicWithoutParameters");
		Set<Constructor<DBDatabase>> result = DataModel.getDBDatabaseConstructorsPublicWithoutParameters();
		for (Constructor<DBDatabase> constr : result) {
			try {
				constr.setAccessible(true);
				System.out.println("DBDatabase Constructor: " + constr.getName());
				DBDatabase newInstance = constr.newInstance();
				Assert.assertThat(newInstance, instanceOf(DBDatabase.class));
				System.out.println("DBDatabase created: " + newInstance.getClass().getCanonicalName());
			} catch (InstantiationException ex) {
				Logger.getLogger(DataModelTest.class.getName()).log(Level.SEVERE, null, ex);
			} catch (IllegalAccessException ex) {
				Logger.getLogger(DataModelTest.class.getName()).log(Level.SEVERE, null, ex);
			} catch (IllegalArgumentException ex) {
				Logger.getLogger(DataModelTest.class.getName()).log(Level.SEVERE, null, ex);
			} catch (InvocationTargetException ex) {
				Logger.getLogger(DataModelTest.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
		Assert.assertThat(result.size(), is(8));
	}

	@Test
	public void testGetDBRowClasses() {
		System.out.println("getDBRowClasses");
		Set<Class<? extends DBRow>> result = DataModel.getDBRowClasses();
		Assert.assertThat(result.size(), is(184));
	}

	@Test
	public void testGetDBDatabaseCreationMethodsStaticWithoutParameters() {
		System.out.println("getDBDatabaseCreationMethodsWithoutParameters");
		List<Method> dbDatabaseCreationMethods = DataModel.getDBDatabaseCreationMethodsStaticWithoutParameters();
		for (Method creator : dbDatabaseCreationMethods) {
			System.out.println("Creator: " + creator.getDeclaringClass().getCanonicalName() + "." + creator.getName() + "()");
			creator.setAccessible(true);
			try {
				DBDatabase db = (DBDatabase) creator.invoke(null);
				System.out.println("DBDatabase Created: " + db);
			} catch (IllegalAccessException ex) {
				Assert.fail("Unable to invoke " + creator.getDeclaringClass().getCanonicalName() + "." + creator.getName() + "()");
			} catch (IllegalArgumentException ex) {
				Assert.fail("Unable to invoke " + creator.getDeclaringClass().getCanonicalName() + "." + creator.getName() + "()");
			} catch (InvocationTargetException ex) {
				Assert.fail("Unable to invoke " + creator.getDeclaringClass().getCanonicalName() + "." + creator.getName() + "()");
			}
		}
		Assert.assertThat(dbDatabaseCreationMethods.size(), is(2));
	}

	@Test
	public void testCreateDBQueryFromEncodedTablePropertiesAndValues() throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
		DBQuery query = DataModel.createDBQueryFromEncodedTablesPropertiesAndValues(
				database,
				"nz.co.gregs.dbvolution.example.CarCompany-name=TOYOTA&nz.co.gregs.dbvolution.example.Marque",
				new DefaultEncodingInterpreter()
		);
		List<DBQueryRow> allRows = query.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(2));
		Assert.assertThat(allRows.get(0).get(new CarCompany()).name.stringValue(), is("TOYOTA"));
		Assert.assertThat(allRows.get(0).get(new Marque()).name.stringValue(), isOneOf("TOYOTA", "HYUNDAI"));
		Assert.assertThat(allRows.get(1).get(new Marque()).name.stringValue(), isOneOf("TOYOTA", "HYUNDAI"));

	}

	@Test
	public void testCreateDBQueryFromEncodedThrowsBlankException() throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
		DBQuery query = DataModel.createDBQueryFromEncodedTablesPropertiesAndValues(
				database,
				"nz.co.gregs.dbvolution.example.CarCompany&nz.co.gregs.dbvolution.example.Marque",
				new DefaultEncodingInterpreter()
		);
		try {
			database.print(query.getAllRows());
			throw new DBRuntimeException("Failed To Create AccidentalBlankQueryException!");
		} catch (AccidentalBlankQueryException blank) {
		}
		query.setBlankQueryAllowed(true);
		List<DBQueryRow> allRows = query.getAllRows();
		database.print(allRows);

		Assert.assertThat(allRows.size(), is(22));

	}

	@Test
	public void testCreateDBQueryFromEncodedAcceptsSimpleIntegerValue() throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
		DBQuery query = DataModel.createDBQueryFromEncodedTablesPropertiesAndValues(
				database,
				"nz.co.gregs.dbvolution.example.CarCompany-uidCarCompany=1&nz.co.gregs.dbvolution.example.Marque",
				new DefaultEncodingInterpreter()
		);
		List<DBQueryRow> allRows = query.getAllRows();
		database.print(allRows);

		Assert.assertThat(allRows.size(), is(2));

	}

	@Test
	public void testCreateDBQueryFromEncodedAcceptsDoubleEndedIntegerRange() throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
		DBQuery query = DataModel.createDBQueryFromEncodedTablesPropertiesAndValues(
				database,
				"nz.co.gregs.dbvolution.example.CarCompany-uidCarCompany=3...4&nz.co.gregs.dbvolution.example.Marque",
				new DefaultEncodingInterpreter()
		);
		database.setPrintSQLBeforeExecuting(true);
		List<DBQueryRow> allRows = query.getAllRows();
		database.print(allRows);

		Assert.assertThat(allRows.size(), is(19));

	}

	@Test
	public void testCreateDBQueryFromEncodedAcceptsDownwardOpenEndedIntegerRange() throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
		DBQuery query = DataModel.createDBQueryFromEncodedTablesPropertiesAndValues(
				database,
				"nz.co.gregs.dbvolution.example.CarCompany-uidCarCompany=...4&nz.co.gregs.dbvolution.example.Marque",
				new DefaultEncodingInterpreter()
		);
		database.setPrintSQLBeforeExecuting(true);
		List<DBQueryRow> allRows = query.getAllRows();
		database.print(allRows);

		Assert.assertThat(allRows.size(), is(22));

	}

	@Test
	public void testCreateDBQueryFromEncodedAcceptsUpwardOpeEnded() throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
		DBQuery query = DataModel.createDBQueryFromEncodedTablesPropertiesAndValues(
				database,
				"nz.co.gregs.dbvolution.example.CarCompany-uidCarCompany=3...&nz.co.gregs.dbvolution.example.Marque",
				new DefaultEncodingInterpreter()
		);
		database.setPrintSQLBeforeExecuting(true);
		List<DBQueryRow> allRows = query.getAllRows();
		database.print(allRows);

		Assert.assertThat(allRows.size(), is(19));

	}

	@Test
	@SuppressWarnings("deprecation")
	public void testCreateDBQueryFromEncodedAcceptsSimpleDateValue() throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
		DBQuery query = DataModel.createDBQueryFromEncodedTablesPropertiesAndValues(
				database,
				"nz.co.gregs.dbvolution.example.CarCompany&nz.co.gregs.dbvolution.example.Marque-creationDate=23 Mar 2013 12:34:56 +1300",
				new DefaultEncodingInterpreter()
		);
		database.setPrintSQLBeforeExecuting(true);
		List<DBQueryRow> allRows = query.getAllRows();
		database.print(allRows);

		Assert.assertThat(allRows.size(), is(18));
		Assert.assertThat(allRows.get(0).get(new Marque()).creationDate.dateValue(), is(new Date("23 Mar 2013 12:34:56 +1300")));
	}

	@Test
	@SuppressWarnings("deprecation")
	public void testCreateDBQueryFromEncodedAcceptsDateRangeValue() throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
		DBQuery query = DataModel.createDBQueryFromEncodedTablesPropertiesAndValues(
				database,
				"nz.co.gregs.dbvolution.example.CarCompany&nz.co.gregs.dbvolution.example.Marque-creationDate=22 Mar 2013 12:34:56 +1300...24 Mar 2013 12:34:56 +1300",
				new DefaultEncodingInterpreter()
		);
		database.setPrintSQLBeforeExecuting(true);
		List<DBQueryRow> allRows = query.getAllRows();
		database.print(allRows);

		Assert.assertThat(allRows.size(), is(18));
		Assert.assertThat(allRows.get(0).get(new Marque()).creationDate.dateValue(), is(new Date("23 Mar 2013 12:34:56 +1300")));
	}

	@Test
	public void testCreateDBQueryFromEncodedAcceptsSimpleNumberValue() throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
		DBQuery query = DataModel.createDBQueryFromEncodedTablesPropertiesAndValues(
				database,
				"nz.co.gregs.dbvolution.example.CarCompany&nz.co.gregs.dbvolution.example.Marque-statusClassID=1246974",
				new DefaultEncodingInterpreter()
		);
		database.setPrintSQLBeforeExecuting(true);
		List<DBQueryRow> allRows = query.getAllRows();
		database.print(allRows);

		Assert.assertThat(allRows.size(), is(21));
		Assert.assertThat(allRows.get(0).get(new Marque()).statusClassID.intValue(), is(1246974));
	}

	@Test
	public void testCreateDBQueryFromEncodedAcceptsSimpleNumberRange() throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
		DBQuery query = DataModel.createDBQueryFromEncodedTablesPropertiesAndValues(
				database,
				"nz.co.gregs.dbvolution.example.CarCompany&nz.co.gregs.dbvolution.example.Marque-statusClassID=1246972...1246974",
				new DefaultEncodingInterpreter()
		);
		database.setPrintSQLBeforeExecuting(true);
		List<DBQueryRow> allRows = query.getAllRows();
		database.print(allRows);

		Assert.assertThat(allRows.size(), is(22));
		Assert.assertThat(allRows.get(0).get(new Marque()).statusClassID.intValue(), isOneOf(1246974,1246972));
	}
}
