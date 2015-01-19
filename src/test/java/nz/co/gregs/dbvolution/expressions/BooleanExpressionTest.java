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
package nz.co.gregs.dbvolution.expressions;

import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Gregory Graham
 */
public class BooleanExpressionTest extends AbstractTest {

	public BooleanExpressionTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testStringLike() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);
		final BooleanExpression like = marque.column(marque.name).isLike("TOY%");
		Assert.assertThat(like.isAggregator(), is(false));
		
		dbQuery.addCondition(like);

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
	}

	@Test
	public void testStringLikeIgnoreCase() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.name).isLikeIgnoreCase("%o%")
				.xor(marque.column(marque.name).isLikeIgnoreCase("%a%"))
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(10));
	}

	@Test
	public void testXOR() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.name).isLikeIgnoreCase("Toy%"));

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
	}

	@Test
	public void testStringIs() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.name).is("TOYOTA"));

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
	}

	@Test
	public void testStringIsIgnoreCase() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.name).isIgnoreCase("TOYOTA"));

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));

		dbQuery = database.getDBQuery(marque);
		dbQuery.addCondition(marque.column(marque.name).isIgnoreCase("Toyota"));

		allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
	}

	@Test
	public void testStringIsIgnoreCaseFord() throws SQLException {
		CarCompany carco = new CarCompany();
		DBQuery dbQuery = database.getDBQuery(carco);

		dbQuery.addCondition(carco.column(carco.name).is("FORD"));

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(0));

		dbQuery.clearConditions();
		dbQuery.addCondition(carco.column(carco.name).isIgnoreCase("FORD"));

		allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
	}

	@Test
	public void testStringIsLessThan() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.name).isLessThan("FORD"));

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(4));
	}

	@Test
	public void testStringIsLessThanOrEqual() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.name).isLessThanOrEqual("FORD"));

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(5));
	}

	@Test
	public void testStringIsGreaterThan() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.name).isGreaterThan("FORD"));

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(17));
	}

	@Test
	public void testStringIsGreaterThanOrEqual() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.name).isGreaterThanOrEqual("FORD"));

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(18));
	}

	@Test
	public void testStringIsIn() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.name).isIn("TOYOTA", "FORD"));

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(2));
	}

	@Test
	public void testStringIsInList() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);
		List<String> strs = new ArrayList<String>();
		strs.add("TOYOTA");
		strs.add("FORD");

		dbQuery.addCondition(marque.column(marque.name).isIn(strs));

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(2));
	}

	@Test
	public void testStringIsInListWithNull() throws SQLException {
		CarCompany carCo = new CarCompany();
		List<String> strs = new ArrayList<String>();
		strs.add("TOYOTA");
		strs.add("Ford");
		strs.add(null);

		DBQuery dbQuery = database.getDBQuery(carCo);
		dbQuery.addCondition(carCo.column(carCo.name).isIn(strs));

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(2));

		CarCompany newCarCo = new CarCompany(null, 17);
		database.insert(newCarCo);

		dbQuery = database.getDBQuery(carCo);
		dbQuery.addCondition(carCo.column(carCo.name).isIn(strs));

		allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testNumberIs() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.uidMarque).is(1));

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
	}

	@Test
	public void testBooleanIs() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.uidMarque).is(1));
		dbQuery.addCondition(BooleanExpression.value(Boolean.TRUE).is(Boolean.TRUE));

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
	}

	@Test
	public void testBooleanIsnt() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.uidMarque).is(1));
		dbQuery.addCondition(BooleanExpression.value(Boolean.TRUE).is(Boolean.FALSE));

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(0));

		dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.uidMarque).is(1));
		dbQuery.addCondition(BooleanExpression.value(Boolean.TRUE).is(Boolean.TRUE).not());

		allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testNumberIsLessThan() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.uidMarque).isLessThan(2));

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
	}

	@Test
	public void testNumberIsLessThanOrEqual() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.uidMarque).isLessThanOrEqual(2));

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(2));
	}

	@Test
	public void testNumberIsGreaterThan() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.uidMarque).isGreaterThan(2));

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(20));
	}

	@Test
	public void testNumberIsGreaterThanOrEqual() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.uidMarque).isGreaterThanOrEqual(2));

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(21));
	}

	@Test
	public void testNumberIsIn() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.uidMarque).isIn(1, 2));

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(2));
	}

	@Test
	public void testNumberIsInList() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);
		List<Long> longs = new ArrayList<Long>();
		longs.add(new Long(1));
		longs.add(new Long(2));

		dbQuery.addCondition(marque.column(marque.uidMarque).isIn(longs));

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(2));
	}

	@Test
	public void testNumberIsAnyOf() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);
		List<Long> longs = new ArrayList<Long>();
		longs.add(new Long(1));
		longs.add(new Long(2));

		dbQuery.addCondition(
				BooleanExpression.anyOf(
						marque.column(marque.uidMarque).is(1),
						marque.column(marque.uidMarque).is(2)
				));

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(2));
	}

	@Test
	public void testNumberIsNoneOf() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);
		List<Long> longs = new ArrayList<Long>();
		longs.add(new Long(1));
		longs.add(new Long(2));

		dbQuery.addCondition(
				BooleanExpression.noneOf(
						marque.column(marque.uidMarque).is(1),
						marque.column(marque.uidMarque).is(2)
				));

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(20));
	}

	/*
	 query.addCondition(BooleanExpression.anyOf(
	 row.column(row.date1).is((Date)null),
	 row.column(row.date1).is(new Date())
	 ).not());
	 */
	@Test
	public void testIsAnyOfWithNulls() throws SQLException, ParseException {
		CarCompany carCo = new CarCompany();
		DBQuery dbQuery = database.getDBQuery(carCo);

		dbQuery.addCondition(
				carCo.column(carCo.name).isIn(null, "TOYOTA", "Ford")
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(2));

		CarCompany newCarCo = new CarCompany(null, 17);
		database.insert(newCarCo);

		dbQuery = database.getDBQuery(carCo);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();
		database.print(allRows);

		dbQuery.addCondition(BooleanExpression.anyOf(
				carCo.column(carCo.name).is((String) null),
				carCo.column(carCo.name).isIn("TOYOTA", "Ford")
		));

		allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testIsStringInWithNulls() throws SQLException, ParseException {
		CarCompany carCo = new CarCompany();
		DBQuery dbQuery = database.getDBQuery(carCo);

		dbQuery.addCondition(
				carCo.column(carCo.name).isIn(null, "TOYOTA", "Ford")
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(2));

		CarCompany newCarCo = new CarCompany(null, 17);
		database.insert(newCarCo);

		dbQuery = database.getDBQuery(carCo);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();
		database.print(allRows);

		dbQuery.addCondition(
				carCo.column(carCo.name).isIn(null, "TOYOTA", "Ford")
		);

		allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testIsAnyOfWithNullsWorksWithNot() throws SQLException, ParseException {
		CarCompany carCo = new CarCompany();
		DBQuery dbQuery = database.getDBQuery(carCo);

		dbQuery.addCondition(
				carCo.column(carCo.name).isIn(null, "TOYOTA", "Ford").not()
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(2));

		CarCompany newCarCo = new CarCompany(null, 17);
		database.insert(newCarCo);

		dbQuery = database.getDBQuery(carCo);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();
		database.print(allRows);

		dbQuery.addCondition(BooleanExpression.anyOf(
				carCo.column(carCo.name).is((String) null),
				carCo.column(carCo.name).isIn("TOYOTA", "Ford")
		).not());

		allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(2));
	}

	@Test
	public void testIsStringWithNulls() throws SQLException, ParseException {
		CarCompany carCo = new CarCompany();
		DBQuery dbQuery = database.getDBQuery(carCo);

		dbQuery.addCondition(
				carCo.column(carCo.name).is((String) null)
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(0));

		CarCompany newCarCo = new CarCompany(null, 17);
		database.insert(newCarCo);

		dbQuery = database.getDBQuery(carCo);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();
		database.print(allRows);

		dbQuery.addCondition(
				carCo.column(carCo.name).is((String) null)
		);

		allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
	}

	@Test
	public void testIsNumberInWithNulls() throws SQLException, ParseException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.updateCount).isIn(null, 0, 1)
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(9));

		Marque newMarque = new Marque(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", datetimeFormat.parse(firstDateStr), 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();
		database.print(allRows);

		dbQuery.addCondition(
				marque.column(marque.updateCount).isIn(null, 0, 1)
		);

		allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(10));
	}

	@Test
	public void testNumberIsWithNulls() throws SQLException, ParseException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.updateCount).is((Number) null)
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(2));

		Marque newMarque = new Marque(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", datetimeFormat.parse(firstDateStr), 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();
		database.print(allRows);

		dbQuery.addCondition(
				marque.column(marque.updateCount).is((Number) null)
		);

		allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testNumberIsNull() throws SQLException, ParseException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				marque.column(marque.updateCount).isNull()
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(2));

		Marque newMarque = new Marque(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", datetimeFormat.parse(firstDateStr), 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();
		database.print(allRows);

		dbQuery.addCondition(
				marque.column(marque.updateCount).isNull()
		);

		allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testIsNullString() throws SQLException, ParseException {
		CarCompany carCo = new CarCompany();
		DBQuery dbQuery = database.getDBQuery(carCo);

		dbQuery.addCondition(
				carCo.column(carCo.name).isNull()
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(0));

		CarCompany newCarCo = new CarCompany(null, 17);
		database.insert(newCarCo);

		dbQuery = database.getDBQuery(carCo);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();
		database.print(allRows);

		dbQuery.addCondition(
				carCo.column(carCo.name).isNull()
		);

		allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
	}

	@Test
	public void testIsNotNullString() throws SQLException, ParseException {
		CarCompany carCo = new CarCompany();
		DBQuery dbQuery = database.getDBQuery(carCo);

		dbQuery.addCondition(
				carCo.column(carCo.name).isNotNull()
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(4));

		CarCompany newCarCo = new CarCompany("LADA", 17);
		database.insert(newCarCo);

		newCarCo = new CarCompany(null, 18);
		database.insert(newCarCo);

		dbQuery = database.getDBQuery(carCo);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();
		database.print(allRows);

		dbQuery.addCondition(
				carCo.column(carCo.name).isNotNull()
		);

		allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(5));
	}
}
