/*
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
package nz.co.gregs.dbvolution.expressions;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.columns.JavaObjectColumn;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.exceptions.AutoCommitActionDuringTransactionException;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class JavaObjectExpressionTest extends AbstractTest {

	public JavaObjectExpressionTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@After
	public void after() throws AutoCommitActionDuringTransactionException, SQLException{
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new JavaObjectExpressionTable());
	}
	
	@Before
	public void before() throws AutoCommitActionDuringTransactionException, SQLException{
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new JavaObjectExpressionTable());
		database.createTable(new JavaObjectExpressionTable());
	}

	@Test
	public void testCopy() {
		var companyLogo = new JavaObjectExpressionTable();
		var instance = new JavaObjectExpression<>(companyLogo.column(companyLogo.someRandomClass));
		var result = instance.copy();
		final DBDefinition definition = database.getDefinition();
		assertEquals(instance.toSQLString(definition), result.toSQLString(definition));
	}

	@Test
	public void testGetQueryableDatatypeForExpressionValue() {
		var companyLogo = new JavaObjectExpressionTable();
		var instance = companyLogo.column(companyLogo.someRandomClass);
		DBJavaObject<SomeClass> expResult = new DBJavaObject<SomeClass>();
		DBJavaObject<SomeClass> result = instance.getQueryableDatatypeForExpressionValue();
		assertEquals(expResult.getClass(), result.getClass());
	}

	@Test
	public void testIsAggregator() {
		JavaObjectExpression<SomeClass> instance = new JavaObjectExpression<>();
		boolean expResult = false;
		boolean result = instance.isAggregator();
		assertEquals(expResult, result);
	}

	@Test
	public void testGetTablesInvolved() {
		var companyLogo = new JavaObjectExpressionTable();
		var instance = new JavaObjectExpression<SomeClass>(companyLogo.column(companyLogo.someRandomClass));
		Set<DBRow> result = instance.getTablesInvolved();
		DBRow[] resultArray = result.toArray(new DBRow[]{});
		Assert.assertThat(result.size(), is(1));
		Assert.assertThat(resultArray[0].getClass().getSimpleName(), is(companyLogo.getClass().getSimpleName()));
	}

	@Test
	public void testIsNotNull() throws SQLException, IOException {
		var row = new JavaObjectExpressionTable();

		DBQuery dbQuery = database.getDBQuery(row);
		JavaObjectColumn<SomeClass> someRandomClassColumn = row.column(row.someRandomClass);
		dbQuery.addCondition(someRandomClassColumn.isNotNull());
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		Assert.assertThat(allRows.size(), is(0));

		row = new JavaObjectExpressionTable();
		row.colInt.setValue(1);
		row.javaInteger.setValue(1);//Toyota
		row.javaString.setValue("toyota_logo.jpg");
		row.someRandomClass.setValue(new SomeClass(4, "Testing is not null"));
		database.insert(row);

		row = new JavaObjectExpressionTable();
		row.colInt.setValue(2);
		database.insert(row);

		dbQuery = database.getDBQuery(new JavaObjectExpressionTable()).setBlankQueryAllowed(true);
		dbQuery.addCondition(someRandomClassColumn.isNotNull());
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).get(row).colInt.intValue(), is(1));
	}

	@Test
	public void testIsNull() throws SQLException, IOException {
		var joTable = new JavaObjectExpressionTable();

		DBQuery dbQuery = database.getDBQuery(joTable);
		JavaObjectColumn<SomeClass> randomClassColumn = joTable.column(joTable.someRandomClass);
		dbQuery.addCondition(randomClassColumn.isNotNull());
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		Assert.assertThat(allRows.size(), is(0));

		joTable = new JavaObjectExpressionTable();
		joTable.colInt.setValue(1);
		joTable.javaInteger.setValue(1);//Toyota
		joTable.javaString.setValue("toyota_logo.jpg");
		joTable.someRandomClass.setValue(new SomeClass(0, "Very testy"));
		database.insert(joTable);

		joTable = new JavaObjectExpressionTable();
		joTable.colInt.setValue(2);
		database.insert(joTable);

		dbQuery = database.getDBQuery(new JavaObjectExpressionTable()).setBlankQueryAllowed(true);
		dbQuery.addCondition(randomClassColumn.isNull());
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).get(joTable).colInt.intValue(), is(2));
	}

	@Test
	public void testGetIncludesNull() {
		JavaObjectExpression<SomeClass> instance = new JavaObjectExpression<SomeClass>();
		boolean expResult = false;
		boolean result = instance.getIncludesNull();
		assertEquals(expResult, result);

		instance = new JavaObjectExpression<SomeClass>(null);
		expResult = true;
		result = instance.getIncludesNull();
		assertEquals(expResult, result);
	}

	@Test
	public void testIsPurelyFunctional() {
		JavaObjectExpression<SomeClass> instance = new JavaObjectExpression<SomeClass>();
		boolean result = instance.isPurelyFunctional();
		assertEquals(true, instance.isPurelyFunctional());

		var joTable = new JavaObjectExpressionTable();
		JavaObjectColumn<SomeClass> imageBytesColumn = joTable.column(joTable.someRandomClass);
		assertEquals(false, imageBytesColumn.isPurelyFunctional());
	}

	public static class JavaObjectExpressionTable extends DBRow {

		private static final long serialVersionUID = 1L;
		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		DBInteger pkcolumn = new DBInteger();

		@DBColumn
		DBInteger colInt = new DBInteger();

		@DBColumn
		DBJavaObject<Integer> javaInteger = new DBJavaObject<Integer>();

		@DBColumn
		DBJavaObject<String> javaString = new DBJavaObject<String>();

		@DBColumn
		DBJavaObject<SomeClass> someRandomClass = new DBJavaObject<SomeClass>();
	}

	public static class SomeClass implements Serializable {

		private static final long serialVersionUID = 1L;
		public String str;
		public int integer;

		public SomeClass(int integer, String str) {
			this.str = str;
			this.integer = integer;
		}
	}
}
