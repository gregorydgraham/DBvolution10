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
import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.hamcrest.Matchers;
import static org.hamcrest.Matchers.is;
import org.junit.Assert;
import org.junit.Test;

public class NumberExpressionTest extends AbstractTest {

	public NumberExpressionTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testSimpleEquation() throws SQLException {
//        database.setPrintSQLBeforeExecuting(true);

		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.uidMarque).mod(2).is(0));
		List<DBQueryRow> allRows = dbQuery.getAllRows();
//        database.print(allRows);
		Assert.assertThat(allRows.size(), is(11));
		for (Marque marque : dbQuery.getAllInstancesOf(marq)) {
			Assert.assertThat(marque.uidMarque.getValue().intValue() % 2, is(0));
		}

	}

	@Test
	public void testSimpleEquationWithValue() throws SQLException {

		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(NumberExpression.value(0).is(marq.column(marq.uidMarque).mod(2)));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(11));
		for (Marque marque : dbQuery.getAllInstancesOf(marq)) {
			Assert.assertThat(marque.uidMarque.getValue().intValue() % 2, is(0));
		}

	}

	@Test
	public void testAllArithmetic() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
				.plus(2)
				.minus(4)
				.times(6)
				.dividedBy(3)
				.mod(5)
				.is(0));
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
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
//        database.print(allRows);
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
//        database.print(allRows);
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
//        database.print(allRows);
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
//        database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));
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
//        database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testGreatestOf() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				NumberExpression.greatestOf(marq.column(marq.uidMarque), NumberExpression.value(900000))
				.is(marq.column(marq.uidMarque))
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
//        database.print(allRows);
		Assert.assertThat(allRows.size(), is(20));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(4893059));
	}

	@Test
	public void testLeastOf() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				NumberExpression.leastOf(marq.column(marq.uidMarque), NumberExpression.value(900000))
				.is(marq.column(marq.uidMarque))
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
//        database.print(allRows);
		Assert.assertThat(allRows.size(), is(2));
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
//        database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testEXP() throws SQLException {
		CarCompany carCo = new CarCompany();
		DBQuery dbQuery = database.getDBQuery(carCo);
		dbQuery.addCondition(
				carCo.column(carCo.uidCarCompany).exp().times(1000).trunc().is(7389));
		List<DBQueryRow> allRows = dbQuery.getAllRows();
//        database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
		CarCompany carCompany = allRows.get(0).get(carCo);
		Assert.assertThat(carCompany.uidCarCompany.getValue().intValue(), is(2));
	}

	@Test
	public void testDegrees() throws SQLException {
		CarCompany carCo = new CarCompany();
		DBQuery dbQuery = database.getDBQuery(carCo);
		dbQuery.addCondition(
				carCo.column(carCo.uidCarCompany).degrees().tan().isGreaterThan(0));
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(2));
		for (CarCompany carCompany : dbQuery.getAllInstancesOf(carCo)) {
			Assert.assertThat(Math.tan(Math.toDegrees(carCompany.uidCarCompany.getValue().doubleValue())) > 0,
					is(true));
		}
	}

	@Test
	public void testRadians() throws SQLException {
		CarCompany carCo = new CarCompany();
		DBQuery dbQuery = database.getDBQuery(carCo);
		dbQuery.addCondition(carCo.column(carCo.uidCarCompany).degrees().radians().degrees().tan().isGreaterThan(0));
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
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
		database.print(allRows);
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
		database.print(allRows);
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
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(4));
		for (ExtendedCarCompany carCompany : dbQuery.getAllInstancesOf(carCo)) {
			System.out.println("LOCATION OF 'ORD': " + carCompany.locationOfORD.getValue().intValue() + " == " + carCompany.name.stringValue().indexOf("ord"));
			Assert.assertThat(carCompany.locationOfORD.getValue().intValue(),
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
}
