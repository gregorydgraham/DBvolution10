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
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.*;
import nz.co.gregs.dbvolution.columns.LargeTextColumn;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBLargeText;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.exceptions.AutoCommitActionDuringTransactionException;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.junit.Before;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class LargeTextExpressionTest extends AbstractTest {

	public LargeTextExpressionTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}
	
	@After
	public void dropTable() throws AutoCommitActionDuringTransactionException, SQLException{
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new ExampleTableForLargeText());
	}
	
	@Before
	public void createTable() throws AutoCommitActionDuringTransactionException, SQLException{
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new ExampleTableForLargeText());
		database.createTable(new ExampleTableForLargeText());
	}

	@Test
	public void testCopy() {
		ExampleTableForLargeText companyLogo = new ExampleTableForLargeText();
		LargeTextExpression instance = new LargeTextExpression(companyLogo.column(companyLogo.essayText));
		LargeTextExpression result = instance.copy();
		final DBDefinition definition = database.getDefinition();
		assertEquals(instance.toSQLString(definition), result.toSQLString(definition));
	}

	@Test
	public void testGetQueryableDatatypeForExpressionValue() {
		ExampleTableForLargeText example = new ExampleTableForLargeText();
		LargeTextColumn instance = example.column(example.essayText);
		DBLargeText expResult = new DBLargeText();
		var result = instance.getQueryableDatatypeForExpressionValue();
		assertEquals(expResult.getClass(), result.getClass());
	}

	@Test
	public void testIsAggregator() {
		LargeTextExpression instance = new LargeTextExpression();
		boolean expResult = false;
		boolean result = instance.isAggregator();
		assertEquals(expResult, result);
	}

	@Test
	public void testGetTablesInvolved() {
		ExampleTableForLargeText textTable = new ExampleTableForLargeText();
		LargeTextExpression instance = new LargeTextExpression(textTable.column(textTable.essayText));
		Set<DBRow> result = instance.getTablesInvolved();
		DBRow[] resultArray = result.toArray(new DBRow[]{});
		assertThat(result.size(), is(1));
		assertThat(resultArray[0].getClass().getSimpleName(), is(textTable.getClass().getSimpleName()));
	}

	@Test
	public void testIsNotNull() throws SQLException, IOException {
		ExampleTableForLargeText textTable = new ExampleTableForLargeText();

		DBQuery dbQuery = database.getDBQuery(textTable);
		LargeTextColumn imageBytesColumn = textTable.column(textTable.essayText);
		dbQuery.addCondition(imageBytesColumn.isNotNull());
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		assertThat(allRows.size(), is(0));

		textTable = new ExampleTableForLargeText();
		textTable.logoID.setValue(1);
		textTable.essayTitle.setValue("toyota_logo.jpg");
		textTable.essayText.setFromFileSystem("carols.txt");
		database.insert(textTable);

		textTable = new ExampleTableForLargeText();
		textTable.logoID.setValue(2);
		database.insert(textTable);

		dbQuery = database.getDBQuery(new ExampleTableForLargeText()).setBlankQueryAllowed(true);
		dbQuery.addCondition(imageBytesColumn.isNotNull());
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		assertThat(allRows.get(0).get(textTable).logoID.intValue(), is(1));
	}

	@Test
	public void testIsNull() throws SQLException, IOException {
		ExampleTableForLargeText textTable = new ExampleTableForLargeText();

		DBQuery dbQuery = database.getDBQuery(textTable);
		LargeTextColumn imageBytesColumn = textTable.column(textTable.essayText);
		dbQuery.addCondition(imageBytesColumn.isNotNull());
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		assertThat(allRows.size(), is(0));

		textTable = new ExampleTableForLargeText();
		textTable.logoID.setValue(1);
		textTable.essayTitle.setValue("toyota_logo.jpg");
		textTable.essayText.setFromFileSystem("carols.txt");
		database.insert(textTable);

		textTable = new ExampleTableForLargeText();
		textTable.logoID.setValue(2);
		database.insert(textTable);

		dbQuery = database.getDBQuery(new ExampleTableForLargeText()).setBlankQueryAllowed(true);
		dbQuery.addCondition(imageBytesColumn.isNull());
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		assertThat(allRows.get(0).get(textTable).logoID.intValue(), is(2));
	}

	@Test
	public void testGetIncludesNull() {
		LargeTextExpression instance = new LargeTextExpression();
		boolean expResult = false;
		boolean result = instance.getIncludesNull();
		assertEquals(expResult, result);

		instance = new LargeTextExpression(null);
		expResult = true;
		result = instance.getIncludesNull();
		assertEquals(expResult, result);
	}

	@Test
	public void testIsPurelyFunctional() {
		LargeTextExpression instance = new LargeTextExpression();
		boolean result = instance.isPurelyFunctional();
		assertEquals(true, instance.isPurelyFunctional());

		ExampleTableForLargeText companyLogo = new ExampleTableForLargeText();
		LargeTextColumn imageBytesColumn = companyLogo.column(companyLogo.essayText);
		assertEquals(false, imageBytesColumn.isPurelyFunctional());
	}

	public static class ExampleTableForLargeText extends DBRow {

		private static final long serialVersionUID = 1L;

		/**
		 * A DBInteger field representing the "logo_id" column in the database.
		 *
		 * <p>
		 * &#64;DBPrimaryKey both indicates that the field is the primary key of the
		 * table and should be used to connect other related tables to this table.
		 *
		 * <p>
		 * &#64;DBColumn both indicates that the field is part of the database table
		 * and protects the actual database column name from any refactoring.
		 *
		 * <p>
		 * DBInteger indicates that the field is INTEGER or NUMBER field that
		 * naturally provides Integer values in Java. It has an instance as that
		 * just makes everyone's life easier.
		 *
		 */
		@DBPrimaryKey
		@DBColumn("logo_id")
		public DBInteger logoID = new DBInteger();

		/**
		 * A DBLargeBinary field representing the "image_file" column in the
		 * database.
		 *
		 * <p>
		 * &#64;DBColumn both indicates that the field is part of the database table
		 * and protects the actual database column name from any refactoring.
		 *
		 * <p>
		 * DBLargeBinary indicates that the field is BLOB or BITS field that
		 * naturally provides a byte[] value in Java. It has an instance as that
		 * just makes everyone's life easier.
		 *
		 */
		@DBColumn("image_file")
		public DBLargeText essayText = new DBLargeText();

		/**
		 * A DBString field representing the "image_name" column in the database.
		 *
		 * <p>
		 * &#64;DBColumn both indicates that the field is part of the database table
		 * and protects the actual database column name from any refactoring.
		 *
		 * <p>
		 * DBString indicates that the field is CHAR or VARCHAR field that naturally
		 * provides String values in Java. It has an instance as that just makes
		 * everyone's life easier.
		 *
		 */
		@DBColumn("image_name")
		public DBString essayTitle = new DBString();
	}
}
