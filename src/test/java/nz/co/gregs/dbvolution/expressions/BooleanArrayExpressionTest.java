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

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.datatypes.DBBooleanArray;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class BooleanArrayExpressionTest extends AbstractTest {

	public BooleanArrayExpressionTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testGetQueryableDatatypeForExpressionValue() {
		BooleanArrayExpression instance = new BooleanArrayExpression();
		QueryableDatatype<?> expResult = new DBBooleanArray();
		QueryableDatatype<?> result = instance.getQueryableDatatypeForExpressionValue();
		assertEquals(expResult.getClass(), result.getClass());
	}

	@Test
	public void testIsAggregator() {
		BooleanArrayExpression instance = new BooleanArrayExpression();
		boolean expResult = false;
		boolean result = instance.isAggregator();
		assertEquals(expResult, result);
	}

	@Test
	public void testGetTablesInvolved() {
		BooleanArrayExpression instance = new BooleanArrayExpression();
		Set<DBRow> expResult = null;
		Set<DBRow> result = instance.getTablesInvolved();
		assertEquals(new HashSet<DBRow>(), result);
	}

	@Test
	public void testGetIncludesNull() {
		BooleanArrayExpression instance = new BooleanArrayExpression();
		boolean expResult = false;
		boolean result = instance.getIncludesNull();
		assertEquals(expResult, result);
	}

	@Test
	public void testIsPurelyFunctional() {
		BooleanArrayExpression instance = new BooleanArrayExpression();
		boolean expResult = true;
		boolean result = instance.isPurelyFunctional();
		assertEquals(expResult, result);
	}

	@Test
	public void testAsExpressionColumn() {
		BooleanArrayExpression instance = new BooleanArrayExpression();
		DBBooleanArray result = instance.asExpressionColumn();
		Assert.assertThat(result, isA(DBBooleanArray.class));
	}

	@Test
	public void testIsExpression() throws SQLException {
		final BooleanArrayExpressionTable tab = new BooleanArrayExpressionTable();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(tab);
		database.createTable(tab);
		tab.boolArrayColumn.setValue(new Boolean[]{true, false, true, true});
		database.insert(tab);

		final BooleanArrayExpressionTable example = new BooleanArrayExpressionTable();
		DBQuery dbQuery = database.getDBQuery(example);
		dbQuery.addCondition(example.column(example.boolArrayColumn).is(new Boolean[]{true, false, true, true}));
		List<BooleanArrayExpressionTable> allRows = dbQuery.getAllInstancesOf(example);
		Assert.assertThat(allRows.size(), is(1));

		dbQuery = database.getDBQuery(example);
		dbQuery.addCondition(example.column(example.boolArrayColumn).is(new Boolean[]{false, false, true, true}));
		allRows = dbQuery.getAllInstancesOf(example);
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testIsNotExpression() throws SQLException {
		final BooleanArrayExpressionTable tab = new BooleanArrayExpressionTable();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(tab);
		database.createTable(tab);
		tab.boolArrayColumn.setValue(new Boolean[]{true, false, true, true});
		database.insert(tab);

		final BooleanArrayExpressionTable example = new BooleanArrayExpressionTable();
		DBQuery dbQuery = database.getDBQuery(example);
		dbQuery.addCondition(example.column(example.boolArrayColumn).isNot(new Boolean[]{true, false, true, true}));
		List<BooleanArrayExpressionTable> allRows = dbQuery.getAllInstancesOf(example);
		Assert.assertThat(allRows.size(), is(0));

		dbQuery = database.getDBQuery(example);
		dbQuery.addCondition(example.column(example.boolArrayColumn).isNot(new Boolean[]{false, false, true, true}));
		allRows = dbQuery.getAllInstancesOf(example);
		Assert.assertThat(allRows.size(), is(1));
	}

	@Test
	public void testIsExpressionWithNull() throws SQLException {
		BooleanArrayExpressionTable tab = new BooleanArrayExpressionTable();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(tab);
		database.createTable(tab);
		tab.boolArrayColumn.setValue(new Boolean[]{true, false, true, true});
		database.insert(tab);
		tab = new BooleanArrayExpressionTable();
		tab.boolArrayColumn.setValue(null);
		database.insert(tab);

		final BooleanArrayExpressionTable example = new BooleanArrayExpressionTable();
		DBQuery dbQuery = database.getDBQuery(example);
		dbQuery.addCondition(example.column(example.boolArrayColumn).is((DBBooleanArray) null));
		List<BooleanArrayExpressionTable> allRows = dbQuery.getAllInstancesOf(example);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).pk.intValue(), is(2));

		dbQuery = database.getDBQuery(example);
		dbQuery.addCondition(example.column(example.boolArrayColumn).is((DBBooleanArray) null).not());
		allRows = dbQuery.getAllInstancesOf(example);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).pk.intValue(), is(1));
	}

	@Test
	public void testIsExpressionWithBooleanArrayResult() throws SQLException {
		final BooleanArrayExpressionTable tab = new BooleanArrayExpressionTable();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(tab);
		database.createTable(tab);
		tab.boolArrayColumn.setValue(new Boolean[]{true, false, true, true});
		database.insert(tab);

		final BooleanArrayExpressionTable example = new BooleanArrayExpressionTable();
		DBQuery dbQuery = database.getDBQuery(example);
		dbQuery.addCondition(example.column(example.boolArrayColumn).is(new DBBooleanArray(new Boolean[]{true, false, true, true})));
		List<BooleanArrayExpressionTable> allRows = dbQuery.getAllInstancesOf(example);
		Assert.assertThat(allRows.size(), is(1));

		dbQuery = database.getDBQuery(example);
		dbQuery.addCondition(example.column(example.boolArrayColumn).is(new DBBooleanArray(new Boolean[]{false, false, true, true})));
		allRows = dbQuery.getAllInstancesOf(example);
		Assert.assertThat(allRows.size(), is(0));
	}

	public static class BooleanArrayExpressionTable extends DBRow {

		private static final long serialVersionUID = 1L;
		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		DBInteger pk = new DBInteger();

		@DBColumn
		DBBooleanArray boolArrayColumn = new DBBooleanArray();
	}
}
