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
package nz.co.gregs.dbvolution.expressions;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import nz.co.gregs.dbvolution.results.NumberResult;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

public class NumberExpressionTest extends AbstractTest {

	public NumberExpressionTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testSimpleEquation() throws SQLException {

		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.uidMarque).numberResult().mod(2).is(0));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(11));
		for (Marque marque : dbQuery.getAllInstancesOf(marq)) {
			assertThat(marque.uidMarque.getValue().intValue() % 2, is(0));
		}

	}

	@Test
	public void testSimpleEquationWithValue() throws SQLException {

		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.uidMarque).numberResult().mod(2).is(0));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(11));
		for (Marque marque : dbQuery.getAllInstancesOf(marq)) {
			assertThat(marque.uidMarque.getValue().intValue() % 2, is(0));
		}

	}

	@Test
	public void testIfThenElse() throws SQLException {

		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.uidMarque).numberResult().mod(2).is(0).ifThenElse(2, 1).is(2));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(11));
		for (Marque marque : dbQuery.getAllInstancesOf(marq)) {
			assertThat(marque.uidMarque.getValue().intValue() % 2, is(0));
		}

	}

	@Test
	public void testIsStringExpressionAggregator() throws SQLException {
		Marque marq = new Marque();
		DBQuery q = database.getDBQuery(marq);
		q.addCondition(marq.column(marq.uidMarque).numberResult().mod(2).is(0).ifThenElse(2, 1).min().is(2));
		List<Marque> rowsByExample = q.getAllInstancesOf(marq);

		Assert.assertEquals(11, rowsByExample.size());
	}

	@Test
	public void testIsStringExpressionAggregators() throws SQLException {
		Marque marq = new Marque();
		DBQuery q = database.getDBQuery(marq);
		q.addCondition(marq.column(marq.uidMarque).numberResult().mod(2).is(0).ifThenElse(2, 1).min().is(2));
		q.addCondition(marq.column(marq.uidMarque).numberResult().mod(2).is(0).ifThenElse(2, 1).max().is(2));
		List<Marque> rowsByExample = q.getAllInstancesOf(marq);

		Assert.assertEquals(11, rowsByExample.size());
	}

	@Test
	public void testBooleanConvertToInteger() throws SQLException {

		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.updateCount).numberResult().mod(2).is(0).integerValue().is(1));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(14));
		for (Marque marque : dbQuery.getAllInstancesOf(marq)) {
			assertThat(marque.updateCount.getValue().intValue() % 2, is(0));
		}

	}

	@Test
	public void testBooleanConvertToNumber() throws SQLException {

		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.uidMarque).numberResult().mod(2).is(0).numberValue().is(1));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(11));
		for (Marque marque : dbQuery.getAllInstancesOf(marq)) {
			assertThat(marque.uidMarque.getValue().intValue() % 2, is(0));
		}

	}

	@Test
	public void testAllArithmetic() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult()
						.plus(2)
						.minus(4)
						.times(6)
						.dividedBy(3)
						.mod(5)
						.is(0));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(2));
		for (DBQueryRow dBQueryRow : allRows) {
			Marque marque = dBQueryRow.get(marq);
			assertThat(
					marque.uidMarque.getValue().intValue(),
					Matchers.anyOf(
							is(1),
							is(4893101)
					)
			);
		}
	}

	@Test
	public void testBrackets() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult()
						.plus(2)
						.minus(4)
						.bracket()
						.times(6)
						.bracket()
						.dividedBy(3)
						.is(-2));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(1));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult()
						.plus(2)
						.minus(4)
						.bracket()
						.times(6)
						.bracket()
						.dividedBy(3)
						.is(-2));
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testBracketsInCondition() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult().plus(2).minus(4).bracket().times(6).bracket().dividedBy(3).is(-2));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(1));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult()
						.plus(2)
						.minus(4)
						.bracket()
						.times(6)
						.bracket()
						.dividedBy(3)
						.is(-2));
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testCubed() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult().plus(2).minus(4).bracket().times(6).bracket().dividedBy(3).is(-2));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(1));

		marq.uidMarque.permittedRange(0, 10000);

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult()
						.cubed()
						.is(8));
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(2));
	}

	@Test
	public void testRound() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult().plus(2).minus(4).bracket().times(6).bracket().dividedBy(3).is(-2));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(1));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult().dividedBy(335)
						.round(5L)
						.is(23074.69254));
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(7730022));
	}

	@Test
	public void testRoundWithNegativeDP() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult().plus(2).minus(4).bracket().times(6).bracket().dividedBy(3).is(-2));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(1));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult()
						.round(-5)
						.round()// Postgres is a little funny about the results of this maths
						.is(7700000.0));
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(3));
		for (DBQueryRow allRow : allRows) {
			marque = allRow.get(marq);
			assertThat(marque.uidMarque.getValue(), isOneOf(7659280l, 7681544l, 7730022l));
		}
	}

	@Test
	public void testRoundNumberExpression() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult().plus(2).minus(4).bracket().times(6).bracket().dividedBy(3).is(-2));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(1));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult().dividedBy(335)
						.round(NumberExpression.value(5))
						.is(23074.69254));
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(7730022));
	}

	@Test
	public void testRoundNumberResult() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult().plus(2).minus(4).bracket().times(6).bracket().dividedBy(3).is(-2));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(1));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult().dividedBy(335)
						.round(new DBNumber(5))
						.is(23074.69254));
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(7730022));
	}

	@Test
	public void testRoundDown() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult().plus(2).minus(4).bracket().times(6).bracket().dividedBy(3).is(-2));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(1));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult().dividedBy(335)
						.roundDown()
						.is(23074));
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(7730022));
	}

	@Test
	public void testRoundUp() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult().plus(2).minus(4).bracket().times(6).bracket().dividedBy(3).is(-2));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(1));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult().dividedBy(333)
						.roundUp()
						.is(23214));
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(7730022));
	}

	@Test
	public void testSignPlusMinus() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult().plus(2).minus(4).bracket().times(6).bracket().dividedBy(3).is(-2));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(1));
		marq.uidMarque.permittedValues(7730022);

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult().dividedBy(333)
						.signPlusMinus()
						.is("+"));
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(7730022));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult().dividedBy(-333)
						.signPlusMinus()
						.is("-"));
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(7730022));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult().minus(7730022)
						.signPlusMinus()
						.is("+"));
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(7730022));
	}

	@Test
	public void testSign() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult().plus(2).minus(4).bracket().times(6).bracket().dividedBy(3).is(-2));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(1));
		marq.uidMarque.permittedValues(7730022);

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult().dividedBy(333)
						.sign()
						.is(1));
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(7730022));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult().dividedBy(-333)
						.sign()
						.is(-1));
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(7730022));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult().minus(7730022)
						.sign()
						.is(0));
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(7730022));
	}

	@Test
	public void testACOS() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult()
						.isBetween(-1, 1)
		);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult()
						.arccos()
						.is(0)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testASIN() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult()
						.isBetween(-1, 10)
		);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult()
						.times(0.3)
						.arcsin()
						.isBetween(0.3, 0.31)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testATAN() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult()
						.isBetween(-1, 10)
		);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult()
						.arctan()
						.isBetween(0.78, 0.79)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testATAN2() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult()
						.isBetween(-1, 10)
		);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult()
						.arctan2(NumberExpression.value(2))
						.isBetween(0.46, 0.47)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testCotangent() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult()
						.isBetween(-1, 10)
		);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult()
						.cotangent()
						.isBetween(0.64, 0.65)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testCOSH() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult()
						.isBetween(-1, 10)
		);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult()
						.cosh()
						.isBetween(1.54, 1.55)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testTANH() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult()
						.isBetween(-1, 10)
		);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult()
						.tanh()
						.isLessThan(1)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(2));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testGreatestOf() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				NumberExpression.greatestOf(marq.column(marq.uidMarque).numberResult(),
						NumberExpression.value(900000), NumberExpression.value(800000)
				).is(marq.column(marq.uidMarque).numberResult())
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(20));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(4893059));
	}

	@Test
	public void testGreatestOfCollection() throws SQLException {
		if(database instanceof  DBDatabaseCluster){
			DBDatabaseCluster cluster = (DBDatabaseCluster)database;
			cluster.waitUntilSynchronised();
		}
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		ArrayList<NumberResult> vals = new ArrayList<NumberResult>();
		vals.add(marq.column(marq.uidMarque).numberResult());
		vals.add(NumberExpression.value(9000000.0));
		vals.add(NumberExpression.value(800000.0));
		dbQuery.addCondition(
				NumberExpression.greatestOf(vals)
						.is(marq.column(marq.uidMarque).numberResult())
		);
		List<DBQueryRow> allRows = dbQuery
				.setQueryLabel("testGreatestOfCollection")
				.getAllRows();

		assertThat(allRows.size(), is(2));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(9971178));
	}

	@Test
	public void testGreatestOfNumberArray() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				NumberExpression.greatestOf(800000, 900000, 4893059)
						.is(marq.column(marq.uidMarque).numberResult())
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(4893059));
	}

	@Test
	public void testLeastOf() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		ArrayList<NumberExpression> arrayList = new ArrayList<NumberExpression>();
		arrayList.add(marq.column(marq.uidMarque).numberResult());
		arrayList.add(NumberExpression.value(900000.0));
		arrayList.add(NumberExpression.value(800000.0));
		dbQuery.addCondition(
				NumberExpression.leastOf(arrayList)
						.is(marq.column(marq.uidMarque).numberResult())
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(2));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testNumberOfDigits() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult().numberOfDigits().is(1)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(2));
	}

	@Test
	public void testIn() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isIn(1, 2, 3, 4, 5)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(0));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isIn(1, 2, 3, 4, 5, 1246974)
		);
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(21));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.statusClassID.getValue().intValue(), is(1246974));
	}

	@Test
	public void testInWithIntegers() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isIn(
						AnyExpression.value(1),
						IntegerExpression.value(2),
						AnyExpression.value(3),
						AnyExpression.value(4),
						AnyExpression.value(5)
				)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(0));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isIn(
						new ArrayList<Integer>() {
					private static final long serialVersionUID = 1l;

					{
						this.add(1);
						this.add(2);
						this.add(3);
						this.add(4);
						this.add(5);
					}
				}
				)
		);
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(0));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isIn(
						AnyExpression.value(1),
						IntegerExpression.value(2),
						AnyExpression.value(3),
						AnyExpression.value(4),
						AnyExpression.value(5),
						AnyExpression.value(1246974)
				)
		);
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(21));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.statusClassID.getValue().intValue(), is(1246974));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isIn(
						new ArrayList<Integer>() {
					{
						this.add(1);
						this.add(2);
						this.add(3);
						this.add(4);
						this.add(5);
						this.add(1246974);
					}
					private static final long serialVersionUID = 1l;
				}
				)
		);
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(21));
		marque = allRows.get(0).get(marq);
		assertThat(marque.statusClassID.getValue().intValue(), is(1246974));
	}

	@Test
	public void testIsBetween() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isBetween(1, 5)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(0));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isBetween(1246972, 1246974)
		);
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(21));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.statusClassID.getValue().intValue(), is(1246974));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isBetween(NumberExpression.value(1246972), 1246974)
		);
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(21));
		marque = allRows.get(0).get(marq);
		assertThat(marque.statusClassID.getValue().intValue(), is(1246974));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isBetween(1246972, NumberExpression.value(1246974))
		);
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(21));
		marque = allRows.get(0).get(marq);
		assertThat(marque.statusClassID.getValue().intValue(), is(1246974));
	}

	@Test
	public void testIsBetweenExclusive() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isBetweenExclusive(1246972, 1246974)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(0));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isBetweenExclusive(1246972, 1246975)
		);
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(21));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.statusClassID.getValue().intValue(), is(1246974));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isBetweenExclusive(NumberExpression.value(1246972), 1246975)
		);
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(21));
		marque = allRows.get(0).get(marq);
		assertThat(marque.statusClassID.getValue().intValue(), is(1246974));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isBetweenExclusive(1246972, NumberExpression.value(1246975))
		);
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(21));
		marque = allRows.get(0).get(marq);
		assertThat(marque.statusClassID.getValue().intValue(), is(1246974));
	}

	@Test
	public void testIsBetweenInclusive() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isBetweenInclusive(1246972, 1246974)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(22));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isBetweenInclusive(1246973, 1246975)
		);
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(21));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.statusClassID.getValue().intValue(), is(1246974));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isBetweenInclusive(NumberExpression.value(1246973), 1246975)
		);
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(21));
		marque = allRows.get(0).get(marq);
		assertThat(marque.statusClassID.getValue().intValue(), is(1246974));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isBetweenInclusive(1246973, NumberExpression.value(1246975))
		);
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(21));
		marque = allRows.get(0).get(marq);
		assertThat(marque.statusClassID.getValue().intValue(), is(1246974));
	}

	@Test
	public void testAppend() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult()
						.append("$")
						.isLike("%2$")
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(3));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult()
						.append(StringExpression.value("$"))
						.isLike("%2$")
		);
		allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(3));
	}

	@Test
	public void testIsEven() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult()
						.isEven()
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(11));
	}

	@Test
	public void testAbs() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult()
						.times(-2).abs().is(4)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
	}

	@Test
	public void testAbsoluteValue() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult()
						.times(-2).absoluteValue().is(4)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
	}

	@Test
	public void testDecimalPart() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult()
						.dividedBy(2).bracket().decimalPart().is(0.5)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(11));
	}

	@Test
	public void testIntegerPart() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult()
						.dividedBy(2).integerPart().is(1)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
	}

	@Test
	public void testIsOdd() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult()
						.isOdd()
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(11));
	}

	@Test
	public void testIsNotNull() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.updateCount)
						.isNotNull()
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(20));
	}

	@Test
	public void testIsNull() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.updateCount)
						.isNull()
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(2));
	}

	@Test
	public void testLeastOfNumberArray() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				NumberExpression.leastOf(1, 2, 3, 4, 5)
						.is(marq.column(marq.uidMarque).numberResult())
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testCOS() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult().minus(1).cos().is(1));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testSIN() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult().sine().round(4).is(0.9093));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(2));
	}

	@Test
	public void testSINH() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult().sinh().round(5).isBetween(3.62685, 3.62686));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(2));
	}

	@Test
	public void testLOG() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult().logN().round(6).is(0.693147));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(2));
	}

	@Test
	public void testLOG10() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).numberResult().logBase10().round(5).is(0.30103));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		assertThat(marque.uidMarque.getValue().intValue(), is(2));
	}

	@Test
	public void testEXP() throws SQLException {
		CarCompany carCo = new CarCompany();
		DBQuery dbQuery = database.getDBQuery(carCo);
		dbQuery.addCondition(
				carCo.column(carCo.uidCarCompany).exp().times(1000).trunc().is(7389));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		CarCompany carCompany = allRows.get(0).get(carCo);
		assertThat(carCompany.uidCarCompany.getValue().intValue(), is(2));
	}

	public static class degreeRow extends CarCompany {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBNumber degrees = new DBNumber(this.column(this.uidCarCompany).degrees());
		@DBColumn
		DBNumber radians = new DBNumber(this.column(this.uidCarCompany).degrees().radians());
		@DBColumn
		DBNumber tangent = new DBNumber(this.column(this.uidCarCompany).degrees().tan());
	}

	@Test
	public void testDegrees() throws SQLException {
		degreeRow carCo = new degreeRow();
		DBQuery dbQuery = database.getDBQuery(carCo).setBlankQueryAllowed(true);

		dbQuery.addCondition(
				carCo.column(carCo.uidCarCompany).degrees().tan().isGreaterThan(0));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(2));
		for (CarCompany carCompany : dbQuery.getAllInstancesOf(carCo)) {
			assertThat(Math.tan(Math.toDegrees(carCompany.uidCarCompany.getValue().doubleValue())) > 0,
					is(true));
		}
	}

	public static class RandomRow extends Marque {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBNumber randomNumber = new DBNumber(NumberExpression.random());
	}

	@Test
	public void testRandom() throws SQLException {
		RandomRow randRow = new RandomRow();
		DBQuery dbQuery = database.getDBQuery(randRow).setBlankQueryAllowed(true);

		List<DBQueryRow> allRows = dbQuery.getDistinctCombinationsOfColumnValues(randRow.randomNumber);

		HashSet<Double> hashSet = new HashSet<Double>();
		Double maxRand = 0d;
		Double minRand = 1d;
		for (DBQueryRow row : allRows) {
			final Double doubleRand = row.get(randRow).randomNumber.doubleValue();
			hashSet.add(doubleRand);
			if (doubleRand > maxRand) {
				maxRand = doubleRand;
			} else if (doubleRand < minRand) {
				minRand = doubleRand;
			}
		}
		assertThat(hashSet.size(), is(22));
		assertThat(maxRand, lessThanOrEqualTo(1d));
		assertThat(minRand, greaterThanOrEqualTo(0d));
	}

	public static class CountIfRow extends Marque {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBString bigger = new DBString(this.column(uidMarque).isGreaterThan(10).ifThenElse("Bigger", "Smaller"));

		@DBColumn
		DBNumber countif = new DBNumber(NumberExpression.countIf(this.column(uidMarque).isGreaterThan(10)));

		@DBColumn
		DBNumber count = new DBNumber(NumberExpression.countAll());

		{
			this.setReturnFields(bigger, countif, count);
		}
	}

	@Test
	public void testCountIf() throws SQLException {
		CountIfRow randRow = new CountIfRow();
		DBQuery dbQuery = database.getDBQuery(randRow).setBlankQueryAllowed(true);
		dbQuery.setSortOrder(randRow.column(randRow.countif));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(2));
		assertThat(allRows.get(0).get(randRow).bigger.stringValue(), is("Smaller"));
		assertThat(allRows.get(0).get(randRow).countif.intValue(), is(0));
		assertThat(allRows.get(0).get(randRow).count.intValue(), is(2));
		assertThat(allRows.get(1).get(randRow).bigger.stringValue(), is("Bigger"));
		assertThat(allRows.get(1).get(randRow).countif.intValue(), is(20));
		assertThat(allRows.get(1).get(randRow).count.intValue(), is(20));
	}

	@Test
	public void testRadians() throws SQLException {
		CarCompany carCo = new CarCompany();
		DBQuery dbQuery = database.getDBQuery(carCo);
		dbQuery.addCondition(carCo.column(carCo.uidCarCompany).degrees().radians().degrees().tan().isGreaterThan(0));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(2));
		for (CarCompany carCompany : dbQuery.getAllInstancesOf(carCo)) {
			assertThat(Math.tan(Math.toDegrees(carCompany.uidCarCompany.getValue().doubleValue())) > 0,
					is(true));
		}
	}

	@Test
	public void testLength() throws SQLException {
		CarCompany carCo = new CarCompany();
		carCo.uidCarCompany.permittedValues(carCo.column(carCo.name).length().minus(1));
		DBQuery dbQuery = database.getDBQuery(carCo);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		for (CarCompany carCompany : dbQuery.getAllInstancesOf(carCo)) {
			assertThat(carCompany.uidCarCompany.getValue().intValue(),
					is(4));
		}
	}

	@Test
	public void testLocationOf() throws SQLException {
		CarCompany carCo = new CarCompany();
		carCo.uidCarCompany.permittedValues(carCo.column(carCo.name).locationOf("ord"));
		DBQuery dbQuery = database.getDBQuery(carCo);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		for (CarCompany carCompany : dbQuery.getAllInstancesOf(carCo)) {
			assertThat(carCompany.uidCarCompany.getValue().intValue(),
					is(2));
		}
	}

	@Test
	public void testLocationOfAsDBRowField() throws SQLException {
		ExtendedCarCompany carCo = new ExtendedCarCompany();

		DBQuery dbQuery = database.getDBQuery(carCo);
		dbQuery.setBlankQueryAllowed(true);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(4));
		for (ExtendedCarCompany carCompany : dbQuery.getAllInstancesOf(carCo)) {
			assertThat(carCompany.locationOfORD.getValue().intValue(),
					is(Matchers.isOneOf(0, carCompany.name.stringValue().indexOf("ord") + 1)));
		}
	}

	public static class ExtendedCarCompany extends CarCompany {

		public static final long serialVersionUID = 1L;

		@DBColumn
		DBNumber locationOfORD = new DBNumber(this.column(this.name).locationOf("ord"));

		public ExtendedCarCompany() {
			super();
		}

	}

	@Test
	public void testChoose() throws SQLException {
		CarCompanyWithChoose carCo = new CarCompanyWithChoose();
		DBQuery dbQuery = database.getDBQuery(carCo);
		dbQuery.setBlankQueryAllowed(true);

		for (CarCompanyWithChoose carCompany : dbQuery.getAllInstancesOf(carCo)) {
			if (carCompany.uidCarCompany.intValue() <= 0) {
				assertThat(carCompany.chooseOnID.getValue(), is("too low"));
			} else if (carCompany.uidCarCompany.intValue() == 1) {
				assertThat(carCompany.chooseOnID.getValue(), is("ok"));
			} else if (carCompany.uidCarCompany.intValue() == 2) {
				assertThat(carCompany.chooseOnID.getValue(), is("high"));
			} else if (carCompany.uidCarCompany.intValue() == 3) {
				assertThat(carCompany.chooseOnID.getValue(), is("too high"));
			} else {
				assertThat(carCompany.chooseOnID.getValue(), isEmptyOrNullString());
			}
		}
	}

	public static class CarCompanyWithChoose extends CarCompany {

		public static final long serialVersionUID = 1L;

		@DBColumn
		DBString chooseOnID = new DBString(this.column(this.uidCarCompany).numberResult().choose("too low", "ok", "high", "too high"));

		public CarCompanyWithChoose() {
			super();
		}
	}

	@Test
	public void testChooseWithDefault() throws SQLException {
		CarCompanyWithChooseWithDefault carCo = new CarCompanyWithChooseWithDefault();
		DBQuery dbQuery = database.getDBQuery(carCo);
		dbQuery.setBlankQueryAllowed(true);

		for (CarCompanyWithChooseWithDefault carCompany : dbQuery.getAllInstancesOf(carCo)) {
			if (carCompany.uidCarCompany.intValue() <= 0) {
				assertThat(carCompany.chooseOnID.getValue(), is("too low"));
			} else if (carCompany.uidCarCompany.intValue() == 1) {
				assertThat(carCompany.chooseOnID.getValue(), is("ok"));
			} else if (carCompany.uidCarCompany.intValue() == 2) {
				assertThat(carCompany.chooseOnID.getValue(), is("high"));
			} else {
				assertThat(carCompany.chooseOnID.getValue(), is("too high"));
			}
		}
	}

	public static class CarCompanyWithChooseWithDefault extends CarCompany {

		public static final long serialVersionUID = 1L;

		@DBColumn
		DBString chooseOnID = new DBString(this.column(this.uidCarCompany).numberResult().chooseWithDefault("too low", "ok", "high", "too high"));

		public CarCompanyWithChooseWithDefault() {
			super();
		}
	}

	@Test
	public void testLagAndLeadFunctions() throws SQLException {
		MarqueWithLagAndLeadFunctions marq = new MarqueWithLagAndLeadFunctions();

		DBQuery query = database.getDBQuery(marq)
				.setBlankQueryAllowed(true)
				.setSortOrder(
						marq.column(marq.carCompany).ascending(),
						marq.column(marq.uidMarque).ascending()
				);

		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(22));

		MarqueWithLagAndLeadFunctions got;// = allRows.get(0).get(marq);
		ArrayList<Object[]> expectedValues = new ArrayList<>();

		expectedValues.add(new Object[]{20, 1, 2, 2, null, 1.0});
		expectedValues.add(new Object[]{20, 2, 2, 2, 0.0, 0.5});
		expectedValues.add(new Object[]{20, 3, 1, 1, 1.0, (0.0)});
		expectedValues.add(new Object[]{20, 4, 3, 3, 0.5, (1.0)});
		expectedValues.add(new Object[]{20, 5, 3, 3, 0.0, (0.5)});
		expectedValues.add(new Object[]{20, 6, 3, 3, 1.0, 1.5});
		expectedValues.add(new Object[]{20, 7, 14, 14, 0.5, (1.0)});
		expectedValues.add(new Object[]{20, 8, 14, 14, 1.5, 1.5});
		expectedValues.add(new Object[]{20, 9, 14, 14, 1.0, (1.0)});
		expectedValues.add(new Object[]{20, 10, 14, 14, (1.5), (2.0)});
		expectedValues.add(new Object[]{20, 11, 14, 14, (1.0), (1.0)});
		expectedValues.add(new Object[]{20, 12, 14, 14, (2.0), (1.0)});
		expectedValues.add(new Object[]{20, 13, 14, 14, (1.0), (1.0)});
		expectedValues.add(new Object[]{20, 14, 14, 14, 1.0, (0.0)});
		expectedValues.add(new Object[]{20, 15, 14, 14, (1.0), (1.5)});
		expectedValues.add(new Object[]{20, 16, 14, 14, (0.0), (1.0)});
		expectedValues.add(new Object[]{20, 17, 14, 14, (1.5), (1.0)});
		expectedValues.add(new Object[]{20, 18, 14, 14, (1.0), (null)});
		expectedValues.add(new Object[]{20, 19, 14, 14, (1.0), (null)});
		expectedValues.add(new Object[]{20, 20, 14, 14, (null), (0.5)});
		expectedValues.add(new Object[]{20, 21, 14, 14, (null), (0.0)});
		expectedValues.add(new Object[]{20, 22, 14, 14, (0.5), (null)});

		for (int i = 0; i < allRows.size(); i++) {
			got = allRows.get(i).get(marq);
//			System.out.println("" + got.toString());
			Object[] expect = expectedValues.get(i);
			assertThat(got.countOfAllRows.intValue(), is((Integer) expect[0]));
			assertThat(got.rowNumber.intValue(), is((Integer) expect[1]));
			assertThat(got.countOfEnabled.intValue(), is((Integer) expect[2]));
			assertThat(got.rowWithinCarCo.intValue(), is((Integer) expect[3]));
			assertThat(got.lag.doubleValue(), is((Double) expect[4]));
			assertThat(got.lead.doubleValue(), is((Double) expect[5]));
		}
	}

	public static class MarqueWithLagAndLeadFunctions extends Marque {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBNumber countOfAllRows = new DBNumber(this.column(this.updateCount).numberResult().dividedBy(2)
				.count()
				.over()
				.allRows());
		@DBColumn
		DBNumber rowNumber = new DBNumber(this.column(this.uidMarque)
				.count()
				.over()
				.AllRowsAndOrderBy(this.column(this.carCompany).ascending(), this.column(this.uidMarque).ascending()));
		@DBColumn
		DBNumber countOfEnabled = new DBNumber(this.column(this.updateCount).numberResult().dividedBy(2)
				.count()
				.over()
				.partition(this.column(this.carCompany)).unordered());
		@DBColumn
		DBNumber rowWithinCarCo = new DBNumber(this.column(this.updateCount).numberResult().dividedBy(2)
				.count()
				.over()
				.partition(this.column(this.carCompany))
				.orderBy(this.column(this.carCompany).ascending())
				.defaultFrame());
		@DBColumn
		DBNumber lag
				= this.column(this.updateCount).numberResult().dividedBy(2)
						.lag()
						.allRows()
						.orderBy(this.column(this.carCompany).ascending(), this.column(this.uidMarque).ascending())
						.asExpressionColumn();
		@DBColumn
		DBNumber lead = new DBNumber(
				this.column(this.updateCount).numberResult().dividedBy(2)
				.nextRowValue()
				.AllRowsAndOrderBy(
						this.column(this.carCompany).ascending(),
						this.column(this.uidMarque).ascending()
				)
		);
	}

}
