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
import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import nz.co.gregs.dbvolution.results.IntegerResult;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.Matchers.*;

public class IntegerExpressionTest extends AbstractTest {

	public IntegerExpressionTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testSimpleEquation() throws SQLException {

		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.uidMarque).mod(2).is(0));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(11));
		for (Marque marque : dbQuery.getAllInstancesOf(marq)) {
			Assert.assertThat(marque.uidMarque.getValue().intValue() % 2, is(0));
		}

	}

	@Test
	public void testSimpleEquationWithValue() throws SQLException {

		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.uidMarque).mod(2).is(0));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(11));
		for (Marque marque : dbQuery.getAllInstancesOf(marq)) {
			Assert.assertThat(marque.uidMarque.getValue().intValue() % 2, is(0));
		}

	}

	@Test
	public void testIfThenElse() throws SQLException {

		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.uidMarque).mod(2).is(0).ifThenElse(2, 1).is(2));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(11));
		for (Marque marque : dbQuery.getAllInstancesOf(marq)) {
			Assert.assertThat(marque.uidMarque.getValue().intValue() % 2, is(0));
		}

	}

	@Test
	public void testIsStringExpressionAggregator() throws SQLException {
		Marque marq = new Marque();
		DBQuery q = database.getDBQuery(marq);
		q.addCondition(marq.column(marq.uidMarque).mod(2).is(0).ifThenElse(2, 1).min().is(2));
		List<Marque> rowsByExample = q.getAllInstancesOf(marq);

		Assert.assertEquals(11, rowsByExample.size());
	}

	@Test
	public void testIsStringExpressionAggregators() throws SQLException {
		Marque marq = new Marque();
		DBQuery q = database.getDBQuery(marq);
		q.addCondition(marq.column(marq.uidMarque).mod(2).is(0).ifThenElse(2, 1).min().is(2));
		q.addCondition(marq.column(marq.uidMarque).mod(2).is(0).ifThenElse(2, 1).max().is(2));
		List<Marque> rowsByExample = q.getAllInstancesOf(marq);

		Assert.assertEquals(11, rowsByExample.size());
	}

	@Test
	public void testBooleanConvertToInteger() throws SQLException {

		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.uidMarque).mod(2).is(0).integerValue().is(1));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(11));
		for (Marque marque : dbQuery.getAllInstancesOf(marq)) {
			Assert.assertThat(marque.uidMarque.getValue().intValue() % 2, is(0));
		}

	}

	@Test
	public void testAllArithmetic() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq).setBlankQueryAllowed(true);		
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
						.plus(2)
						.minus(4)
						.times(6)
						.dividedBy(3)
						.mod(5)
						.is(0));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(2));
		for (DBQueryRow dBQueryRow : allRows) {
			Marque marque = dBQueryRow.get(marq);
			Assert.assertThat(
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
				marq.column(marq.uidMarque)
						.plus(2)
						.minus(4)
						.bracket()
						.times(6)
						.bracket()
						.dividedBy(3)
						.is(-2));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
						.plus(2)
						.minus(4)
						.bracket()
						.times(6)
						.bracket()
						.dividedBy(3)
						.is(-2));
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testBracketsInCondition() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).plus(2).minus(4).bracket().times(6).bracket().dividedBy(3).is(-2));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
						.plus(2)
						.minus(4)
						.bracket()
						.times(6)
						.bracket()
						.dividedBy(3)
						.is(-2));
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testCubed() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).plus(2).minus(4).bracket().times(6).bracket().dividedBy(3).is(-2));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));

		marq.uidMarque.permittedRange(0, 10000);

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
						.cubed()
						.is(8));
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(2));
	}

	@Test
	public void testRound() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).plus(2).minus(4).bracket().times(6).bracket().dividedBy(3).is(-2));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).dividedBy(335)
						.round(5L)
						.is(23074.69254));
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(7730022));
	}

	@Test
	public void testRoundNumberExpression() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).plus(2).minus(4).bracket().times(6).bracket().dividedBy(3).is(-2));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).dividedBy(335)
						.round(NumberExpression.value(5))
						.is(23074.69254));
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(7730022));
	}

	@Test
	public void testRoundNumberResult() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).plus(2).minus(4).bracket().times(6).bracket().dividedBy(3).is(-2));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).dividedBy(335)
						.round(-1)
						.is(23070));
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(7730022));
	}

	@Test
	public void testRoundDown() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).plus(2).minus(4).bracket().times(6).bracket().dividedBy(3).is(-2));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).dividedBy(335)
						.roundDown()
						.is(23074));
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(7730022));
	}

	@Test
	public void testRoundUp() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).plus(2).minus(4).bracket().times(6).bracket().dividedBy(3).is(-2));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).dividedBy(333)
						.roundUp()
						.is(23214));
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(7730022));
	}

	@Test
	public void testSignPlusMinus() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).plus(2).minus(4).bracket().times(6).bracket().dividedBy(3).is(-2));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));
		marq.uidMarque.permittedValues(7730022);

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).dividedBy(333)
						.signPlusMinus()
						.is("+"));
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(7730022));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).dividedBy(-333)
						.signPlusMinus()
						.is("-"));
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(7730022));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).minus(7730022)
						.signPlusMinus()
						.is("+"));
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(7730022));
	}

	@Test
	public void testSign() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).plus(2).minus(4).bracket().times(6).bracket().dividedBy(3).is(-2));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));
		marq.uidMarque.permittedValues(7730022);

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).dividedBy(333)
						.sign()
						.is(1));
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(7730022));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).dividedBy(-333)
						.sign()
						.is(-1));
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(7730022));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).minus(7730022)
						.sign()
						.is(0));
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(7730022));
	}

	@Test
	public void testACOS() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
						.isBetween(-1, 1)
		);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
						.arccos()
						.is(0)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testASIN() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
						.isBetween(-1, 10)
		);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
						.times(0.3)
						.arcsin()
						.isBetween(0.3, 0.31)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testATAN() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
						.isBetween(-1, 10)
		);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
						.arctan()
						.isBetween(0.78, 0.79)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testATAN2() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
						.isBetween(-1, 10)
		);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
						.arctan2(2)
						.isBetween(0.46, 0.47)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testCotangent() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
						.isBetween(-1, 10)
		);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
						.cotangent()
						.isBetween(0.64, 0.65)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testCOSH() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
						.isBetween(-1, 10)
		);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
						.cosh()
						.isBetween(1.54, 1.55)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testTANH() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
						.isBetween(-1, 10)
		);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
						.tanh()
						.isLessThan(1)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(2));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testGreatestOf() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				NumberExpression.greatestOf(marq.column(marq.uidMarque), 
						IntegerExpression.value(900000), IntegerExpression.value(800000)
				).is(marq.column(marq.uidMarque))
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(20));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(4893059));
	}

	@Test
	public void testGreatestOfCollection() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		ArrayList<IntegerResult> vals = new ArrayList<>();
		vals.add(marq.column(marq.uidMarque));
		vals.add(IntegerExpression.value(900000));
		vals.add(IntegerExpression.value(800000));
		dbQuery.addCondition(
				IntegerExpression.greatestOf(vals)
						.is(marq.column(marq.uidMarque))
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(20));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(4893059));
	}

	@Test
	public void testGreatestOfNumberArray() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				NumberExpression.greatestOf(800000, 900000, 4893059)
						.is(marq.column(marq.uidMarque))
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(4893059));
	}

	@Test
	public void testLeastOf() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		ArrayList<IntegerExpression> arrayList = new ArrayList<>();
		arrayList.add(marq.column(marq.uidMarque));
		arrayList.add(IntegerExpression.value(900000));
		arrayList.add(IntegerExpression.value(800000));
		dbQuery.addCondition(
				IntegerExpression.leastOf(arrayList)
						.is(marq.column(marq.uidMarque))
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(2));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testIn() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isIn(1, 2, 3, 4, 5)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(0));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isIn(1, 2, 3, 4, 5, 1246974)
		);
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(21));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.statusClassID.getValue().intValue(), is(1246974));
	}

	@Test
	public void testIsBetween() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isBetween(1, 5)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(0));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isBetween(1246972, 1246974)
		);
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(21));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.statusClassID.getValue().intValue(), is(1246974));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isBetween(NumberExpression.value(1246972), 1246974)
		);
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(21));
		marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.statusClassID.getValue().intValue(), is(1246974));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isBetween(1246972, NumberExpression.value(1246974))
		);
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(21));
		marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.statusClassID.getValue().intValue(), is(1246974));
	}

	@Test
	public void testIsBetweenExclusive() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isBetweenExclusive(1246972, 1246974)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(0));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isBetweenExclusive(1246972, 1246975)
		);
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(21));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.statusClassID.getValue().intValue(), is(1246974));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isBetweenExclusive(NumberExpression.value(1246972), 1246975)
		);
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(21));
		marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.statusClassID.getValue().intValue(), is(1246974));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isBetweenExclusive(1246972, NumberExpression.value(1246975))
		);
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(21));
		marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.statusClassID.getValue().intValue(), is(1246974));
	}

	@Test
	public void testIsBetweenInclusive() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isBetweenInclusive(1246972, 1246974)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(22));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isBetweenInclusive(1246973, 1246975)
		);
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(21));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.statusClassID.getValue().intValue(), is(1246974));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isBetweenInclusive(NumberExpression.value(1246973), 1246975)
		);
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(21));
		marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.statusClassID.getValue().intValue(), is(1246974));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.statusClassID).isBetweenInclusive(1246973, NumberExpression.value(1246975))
		);
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(21));
		marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.statusClassID.getValue().intValue(), is(1246974));
	}

	@Test
	public void testAppend() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
						.append("$")
						.isLike("%2$")
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(3));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
						.append(StringExpression.value("$"))
						.isLike("%2$")
		);
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testIsEven() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
						.isEven()
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(11));
	}

	@Test
	public void testAbs() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
						.times(-2).abs().is(4)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
	}

	@Test
	public void testAbsoluteValue() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
						.times(-2).absoluteValue().is(4)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
	}

	@Test
	public void testDecimalPart() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
						.dividedBy(2).bracket().decimalPart().is(0.5)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(11));
	}

	@Test
	public void testIntegerPart() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
						.dividedBy(2).integerPart().is(1)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
	}

	@Test
	public void testIsOdd() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
						.isOdd()
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(11));
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

		Assert.assertThat(allRows.size(), is(20));
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

		Assert.assertThat(allRows.size(), is(2));
	}

	@Test
	public void testLeastOfNumberArray() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				NumberExpression.leastOf(1, 2, 3, 4, 5)
						.is(marq.column(marq.uidMarque))
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testCOS() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).minus(1).cos().is(1));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testSIN() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).sine().round(4).is(0.9093));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(2));
	}

	@Test
	public void testSINH() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).sinh().round(5).isBetween(3.62685, 3.62686));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(2));
	}

	@Test
	public void testLOG() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).logN().round(6).is(0.693147));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(2));
	}

	@Test
	public void testLOG10() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).logBase10().round(5).is(0.30103));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(2));
	}

	@Test
	public void testEXP() throws SQLException {
		CarCompany carCo = new CarCompany();
		DBQuery dbQuery = database.getDBQuery(carCo);
		dbQuery.addCondition(
				carCo.column(carCo.uidCarCompany).exp().times(1000).trunc().is(7389));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		CarCompany carCompany = allRows.get(0).get(carCo);
		Assert.assertThat(carCompany.uidCarCompany.getValue().intValue(), is(2));
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

		Assert.assertThat(allRows.size(), is(2));
		for (CarCompany carCompany : dbQuery.getAllInstancesOf(carCo)) {
			Assert.assertThat(Math.tan(Math.toDegrees(carCompany.uidCarCompany.getValue().doubleValue())) > 0,
					is(true));
		}
	}

	public static class CountIfRow extends Marque {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBString bigger = new DBString(this.column(uidMarque).isGreaterThan(10).ifThenElse("Bigger", "Smaller"));

		@DBColumn
		DBInteger countif = new DBInteger(IntegerExpression.countIf(this.column(uidMarque).isGreaterThan(10)));

		@DBColumn
		DBInteger count = new DBInteger(NumberExpression.countAll());

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

		Assert.assertThat(allRows.size(), is(2));
		Assert.assertThat(allRows.get(0).get(randRow).bigger.stringValue(), is("Smaller"));
		Assert.assertThat(allRows.get(0).get(randRow).countif.intValue(), is(0));
		Assert.assertThat(allRows.get(0).get(randRow).count.intValue(), is(2));
		Assert.assertThat(allRows.get(1).get(randRow).bigger.stringValue(), is("Bigger"));
		Assert.assertThat(allRows.get(1).get(randRow).countif.intValue(), is(20));
		Assert.assertThat(allRows.get(1).get(randRow).count.intValue(), is(20));
	}

	@Test
	public void testRadians() throws SQLException {
		CarCompany carCo = new CarCompany();
		DBQuery dbQuery = database.getDBQuery(carCo);
		dbQuery.addCondition(carCo.column(carCo.uidCarCompany).degrees().radians().degrees().tan().isGreaterThan(0));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(2));
		for (CarCompany carCompany : dbQuery.getAllInstancesOf(carCo)) {
			Assert.assertThat(Math.tan(Math.toDegrees(carCompany.uidCarCompany.getValue().doubleValue())) > 0,
					is(true));
		}
	}

	@Test
	public void testLength() throws SQLException {
		CarCompany carCo = new CarCompany();
		carCo.uidCarCompany.permittedValues(carCo.column(carCo.name).length().minus(1));
		DBQuery dbQuery = database.getDBQuery(carCo);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		for (CarCompany carCompany : dbQuery.getAllInstancesOf(carCo)) {
			Assert.assertThat(carCompany.uidCarCompany.getValue().intValue(),
					is(4));
		}
	}

	@Test
	public void testLocationOf() throws SQLException {
		CarCompany carCo = new CarCompany();
		carCo.uidCarCompany.permittedValues(carCo.column(carCo.name).locationOf("ord"));
		DBQuery dbQuery = database.getDBQuery(carCo);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
		for (CarCompany carCompany : dbQuery.getAllInstancesOf(carCo)) {
			Assert.assertThat(carCompany.uidCarCompany.getValue().intValue(),
					is(2));
		}
	}

	@Test
	public void testLocationOfAsDBRowField() throws SQLException {
		ExtendedCarCompany carCo = new ExtendedCarCompany();
//		carCo.uidCarCompany.permittedValues(carCo.column(carCo.name).locationOf("ord"));
		DBQuery dbQuery = database.getDBQuery(carCo);
		dbQuery.setBlankQueryAllowed(true);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(4));
		for (ExtendedCarCompany carCompany : dbQuery.getAllInstancesOf(carCo)) {
			Assert.assertThat(carCompany.locationOfORD.getValue().intValue(),
					is(Matchers.isOneOf(0, carCompany.name.stringValue().indexOf("ord") + 1)));
		}
	}

	public static class ExtendedCarCompany extends CarCompany {

		public static final long serialVersionUID = 1L;

		@DBColumn
		DBInteger locationOfORD = new DBInteger(this.column(this.name).locationOf("ord"));

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
				Assert.assertThat(carCompany.chooseOnID.getValue(), is("too low"));
			} else if (carCompany.uidCarCompany.intValue() == 1) {
				Assert.assertThat(carCompany.chooseOnID.getValue(), is("ok"));
			} else if (carCompany.uidCarCompany.intValue() == 2) {
				Assert.assertThat(carCompany.chooseOnID.getValue(), is("high"));
			} else {
				Assert.assertThat(carCompany.chooseOnID.getValue(), is("too high"));
			}
		}
	}

	public static class CarCompanyWithChoose extends CarCompany {

		public static final long serialVersionUID = 1L;

		@DBColumn
		DBString chooseOnID = new DBString(this.column(this.uidCarCompany).choose("too low", "ok", "high", "too high"));

		public CarCompanyWithChoose() {
			super();
		}
	}

}