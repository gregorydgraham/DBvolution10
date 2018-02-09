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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.exceptions.AccidentalCartesianJoinException;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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

		Assert.assertThat(allRows.size(), is(10));
	}

	@Test
	public void testXOR() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.name).isLikeIgnoreCase("Toy%"));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
	}

	@Test
	public void testNullExpression() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(BooleanExpression.nullBoolean().isNull());
		
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(22));

		dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(BooleanExpression.nullBoolean().isNotNull());

		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testStringIfThenElse() throws SQLException {
		MarqueWithIfThenElse marque = new MarqueWithIfThenElse();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.toyotaMarque).is("TOYOTA"));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(2));
		for (DBQueryRow row : allRows) {
			Assert.assertThat(row.get(marque).toyotaMarque.getValue(), is("TOYOTA"));
		}

		dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.toyotaMarque).isNot("TOYOTA"));

		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(20));
		for (DBQueryRow row : allRows) {
			Assert.assertThat(row.get(marque).toyotaMarque.getValue(), is("NON-TOYOTA"));
		}

		dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.numberMarque).is(1));

		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(2));
		for (DBQueryRow row : allRows) {
			Assert.assertThat(row.get(marque).numberMarque.intValue(), is(1));
		}

		dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.dateMarque).is(MarqueWithIfThenElse.THEN_DATE));

		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(18));
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMDD");
		for (DBQueryRow row : allRows) {
			Assert.assertThat(format.format(row.get(marque).dateMarque.getValue()), is(format.format(MarqueWithIfThenElse.THEN_DATE)));
		}

		dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.dateMarque).is(MarqueWithIfThenElse.ELSE_DATE));

		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(4)); // NULL is included in the ELSE collection
		for (DBQueryRow row : allRows) {
			Assert.assertThat(format.format(row.get(marque).dateMarque.getValue()), is(format.format(MarqueWithIfThenElse.ELSE_DATE)));
		}
	}

	public static class MarqueWithIfThenElse extends Marque {

		private static final long serialVersionUID = 1L;

		@DBColumn
		public DBString toyotaMarque = new DBString(this.column(this.name).isIn("TOYOTA", "HYUNDAI").ifThenElse("TOYOTA", "NON-TOYOTA"));

		@DBColumn
		public DBNumber numberMarque = new DBNumber(this.column(this.carCompany).isIn(1, 4896300).ifThenElse(1, 2));

		public static final Date THEN_DATE = (new GregorianCalendar(2015, 1, 3, 11, 22, 33)).getTime();
		public static final Date ELSE_DATE = (new GregorianCalendar(2010, 11, 30, 23, 45, 56)).getTime();
		@DBColumn
		public DBDate dateMarque = new DBDate(this.column(this.creationDate).is(march23rd2013).ifThenElse(THEN_DATE, ELSE_DATE));
	}

	@Test
	public void testStringIs() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.name).is("TOYOTA"));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
	}

	@Test
	public void testStringIsIgnoreCase() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.name).isIgnoreCase("TOYOTA"));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));

		dbQuery = database.getDBQuery(marque);
		dbQuery.addCondition(marque.column(marque.name).isIgnoreCase("Toyota"));

		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
	}

	@Test
	public void testStringIsIgnoreCaseFord() throws SQLException {
		CarCompany carco = new CarCompany();
		DBQuery dbQuery = database.getDBQuery(carco);

		dbQuery.addCondition(carco.column(carco.name).is("FORD"));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(0));

		dbQuery.clearConditions();
		dbQuery.addCondition(carco.column(carco.name).isIgnoreCase("FORD"));

		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
	}

	@Test
	public void testStringIsLessThan() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.name).isLessThan("FORD"));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(4));
	}

	@Test
	public void testStringIsLessThanOrEqual() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.name).isLessThanOrEqual("FORD"));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(5));
	}

	@Test
	public void testStringIsLessThanOrBooleanExpressionl() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.name).isLessThan("FORD", BooleanExpression.trueExpression()));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(5));
	}

	@Test
	public void testStringIsGreaterThan() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.name).isGreaterThan("FORD"));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(17));
	}

	@Test
	public void testStringIsGreaterThanOrBooleanExpression() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.name).isGreaterThan("FORD", BooleanExpression.trueExpression()));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(18));
	}

	@Test
	public void testStringIsGreaterThanOrEqual() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.name).isGreaterThanOrEqual("FORD"));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(18));
	}

	@Test
	public void testStringIsIn() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.name).isIn("TOYOTA", "FORD"));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

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

		Assert.assertThat(allRows.size(), is(2));

		CarCompany newCarCo = new CarCompany(null, 17);
		database.insert(newCarCo);

		dbQuery = database.getDBQuery(carCo);
		dbQuery.addCondition(carCo.column(carCo.name).isIn(strs));
		
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testNumberIs() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.uidMarque).is(1));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
	}

	@Test
	public void testBooleanIs() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.uidMarque).is(1));
		dbQuery.addCondition(BooleanExpression.value(Boolean.TRUE).is(Boolean.TRUE));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
	}

	@Test
	public void testBooleanIsWithNull() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.uidMarque).is(1));
		Boolean nullBool = null;
		dbQuery.addCondition(BooleanExpression.value(nullBool).is(nullBool));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
	}

	@Test
	public void testBooleanIsNot() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.uidMarque).is(1));
		dbQuery.addCondition(BooleanExpression.value(Boolean.TRUE).isNot(Boolean.TRUE));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(0));

		dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.uidMarque).is(1));
		dbQuery.addCondition(BooleanExpression.value(Boolean.TRUE).is(Boolean.TRUE).not());

		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void testNumberIsLessThan() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.uidMarque).isLessThan(2));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(1));
	}

	@Test
	public void testNumberIsLessThanOrBooleanExpression() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.uidMarque).isLessThan(2, BooleanExpression.trueExpression()));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(2));
	}

	@Test
	public void testNumberIsLessThanOrEqual() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.uidMarque).isLessThanOrEqual(2));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(2));
	}

	@Test
	public void testNumberIsGreaterThan() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.uidMarque).isGreaterThan(2));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(20));
	}

	@Test
	public void testNumberIsGreaterThanOrEqual() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.uidMarque).isGreaterThanOrEqual(2));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(21));
	}

	@Test
	public void testNumberIsGreaterThanOrBooleanExpression() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.uidMarque).isGreaterThan(2, BooleanExpression.trueExpression()));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(21));
	}

	@Test
	public void testNumberIsIn() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(marque.column(marque.uidMarque).isIn(1, 2));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

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

		Assert.assertThat(allRows.size(), is(2));
	}

	@Test
	public void testNumberIsAnyOf() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				BooleanExpression.anyOf(
						marque.column(marque.uidMarque).is(1),
						marque.column(marque.uidMarque).is(2)
				));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(2));
	}

	@Test
	public void testNumberIsNoneOf() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				BooleanExpression.noneOf(
						marque.column(marque.uidMarque).is(1),
						marque.column(marque.uidMarque).is(2)
				));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(20));
	}

	@Test
	public void testNotAllOf() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				BooleanExpression.notAllOf(
						marque.column(marque.name).is("TOYOTA"),
						marque.column(marque.carCompany).is(4)
				));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(22));
	}

	@Test
	public void testSomeButNotAllOf() throws SQLException {
		Marque marque = new Marque();
		DBQuery dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				BooleanExpression.someButNotAllOf(
						marque.column(marque.name).is("TOYOTA"),
						marque.column(marque.carCompany).is(4)
				));

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(17));
	}

	@Test
	public void testIsAnyOfWithNulls() throws SQLException, ParseException {
		CarCompany carCo = new CarCompany();
		DBQuery dbQuery = database.getDBQuery(carCo);

		dbQuery.addCondition(
				carCo.column(carCo.name).isIn(null, "TOYOTA", "Ford")
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(2));

		CarCompany newCarCo = new CarCompany(null, 17);
		database.insert(newCarCo);

		dbQuery = database.getDBQuery(carCo);
		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(BooleanExpression.anyOf(
				carCo.column(carCo.name).is((String) null),
				carCo.column(carCo.name).isIn("TOYOTA", "Ford")
		));

		allRows = dbQuery.getAllRows();
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

		Assert.assertThat(allRows.size(), is(2));

		CarCompany newCarCo = new CarCompany(null, 17);
		database.insert(newCarCo);

		dbQuery = database.getDBQuery(carCo);
		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(
				carCo.column(carCo.name).isIn(null, "TOYOTA", "Ford")
		);

		allRows = dbQuery.getAllRows();

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

		Assert.assertThat(allRows.size(), is(2));

		CarCompany newCarCo = new CarCompany(null, 17);
		database.insert(newCarCo);

		dbQuery = database.getDBQuery(carCo);
		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(BooleanExpression.anyOf(
				carCo.column(carCo.name).is((String) null),
				carCo.column(carCo.name).isIn("TOYOTA", "Ford")
		).not());

		allRows = dbQuery.getAllRows();

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

		Assert.assertThat(allRows.size(), is(0));

		CarCompany newCarCo = new CarCompany(null, 17);
		database.insert(newCarCo);

		dbQuery = database.getDBQuery(carCo);
		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(
				carCo.column(carCo.name).is((String) null)
		);

		allRows = dbQuery.getAllRows();

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

		Assert.assertThat(allRows.size(), is(9));

		Marque newMarque = new Marque(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", DATETIME_FORMAT.parse(firstDateStr), 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(
				marque.column(marque.updateCount).isIn(null, 0, 1)
		);

		allRows = dbQuery.getAllRows();

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

		Assert.assertThat(allRows.size(), is(2));

		Marque newMarque = new Marque(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", DATETIME_FORMAT.parse(firstDateStr), 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(
				marque.column(marque.updateCount).is((Number) null)
		);

		allRows = dbQuery.getAllRows();

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

		Assert.assertThat(allRows.size(), is(2));

		dbQuery = database.getDBQuery(marque);

		dbQuery.addCondition(
				BooleanExpression.isNull((ColumnProvider) marque.column(marque.updateCount))
		);

		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(2));

		Marque newMarque = new Marque(178, "False", 1246974, "", null, "UV", "HULME", "", "Y", DATETIME_FORMAT.parse(firstDateStr), 4, null);
		database.insert(newMarque);

		dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(
				marque.column(marque.updateCount).isNull()
		);

		allRows = dbQuery.getAllRows();
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
		Assert.assertThat(allRows.size(), is(0));

		CarCompany newCarCo = new CarCompany(null, 17);
		database.insert(newCarCo);

		dbQuery = database.getDBQuery(carCo);
		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(
				carCo.column(carCo.name).isNull()
		);

		allRows = dbQuery.getAllRows();
		Assert.assertThat(allRows.size(), is(1));
	}

	public static class MarqueReportWithBooleanExpressionCount extends DBReport {

		private static final long serialVersionUID = 1L;

		public Marque marque = new Marque();
		@DBColumn
		public DBBoolean greaterThan3 = new DBBoolean(marque.column(marque.carCompany).isGreaterThan(3));
		@DBColumn
		public DBNumber counted = new DBNumber(marque.column(marque.carCompany).isGreaterThan(3).count());

		{
			this.setSortOrder(greaterThan3, counted);
		}

	}

	@Test
	public void testCount() throws SQLException, ParseException {
		MarqueReportWithBooleanExpressionCount marque = new MarqueReportWithBooleanExpressionCount();

		List<MarqueReportWithBooleanExpressionCount> allRows = database.getAllRows(marque);

		Assert.assertThat(allRows.size(), is(2));
		Assert.assertThat(allRows.get(0).greaterThan3.booleanValue(), is(false));
		Assert.assertThat(allRows.get(1).greaterThan3.booleanValue(), is(true));
		Assert.assertThat(allRows.get(0).counted.intValue(), is(6));
		Assert.assertThat(allRows.get(1).counted.intValue(), is(16));
	}

	public static class MarqueWithBooleanExpressionCount extends Marque {

		private static final long serialVersionUID = 1L;

		@DBColumn
		public DBBoolean greaterThan3 = new DBBoolean(this.column(this.carCompany).isGreaterThan(3));
		@DBColumn
		public DBNumber counted = new DBNumber(this.column(this.carCompany).isGreaterThan(3).count());

		{
			this.setReturnFields(greaterThan3, counted);
		}
	}

	@Test
	public void testCountUsingDBRow() throws SQLException, ParseException {
		MarqueWithBooleanExpressionCount marque = new MarqueWithBooleanExpressionCount();
		final DBQuery dbQuery = database.getDBQuery(marque);
		dbQuery.setBlankQueryAllowed(true);
		dbQuery.setSortOrder(marque.column(marque.greaterThan3));
		
		List<MarqueWithBooleanExpressionCount> allRows = dbQuery.getAllInstancesOf(marque);

		Assert.assertThat(allRows.size(), is(2));
		Assert.assertThat(allRows.get(0).greaterThan3.booleanValue(), is(false));
		Assert.assertThat(allRows.get(0).counted.intValue(), is(6));
		Assert.assertThat(allRows.get(1).greaterThan3.booleanValue(), is(true));
		Assert.assertThat(allRows.get(1).counted.intValue(), is(16));
	}

	@Test
	public void testIsNotNullString() throws SQLException, ParseException {
		CarCompany carCo = new CarCompany();
		DBQuery dbQuery = database.getDBQuery(carCo);

		dbQuery.addCondition(
				carCo.column(carCo.name).isNotNull()
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(4));

		dbQuery = database.getDBQuery(carCo);

		dbQuery.addCondition(
				BooleanExpression.isNotNull((ColumnProvider) carCo.column(carCo.name))
		);

		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(4));

		CarCompany newCarCo = new CarCompany("LADA", 17);
		database.insert(newCarCo);

		newCarCo = new CarCompany(null, 18);
		database.insert(newCarCo);

		dbQuery = database.getDBQuery(carCo);
		dbQuery.setBlankQueryAllowed(true);

		dbQuery.addCondition(
				carCo.column(carCo.name).isNotNull()
		);

		allRows = dbQuery.getAllRows();
		Assert.assertThat(allRows.size(), is(5));
	}

	@Test
	public void testSeekLessThanStringNumberDate() throws AccidentalBlankQueryException, AccidentalCartesianJoinException, SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);

		dbQuery.addCondition(
				BooleanExpression.seekLessThan(
						marq.column(marq.name), StringExpression.value("BMW"),
						marq.column(marq.uidMarque), IntegerExpression.value(6664478),
						marq.column(marq.creationDate), DateExpression.value(april2nd2011)
				)
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		Assert.assertThat(allRows.size(), is(1));

		dbQuery = database.getDBQuery(marq);

		dbQuery.addCondition(
				BooleanExpression.seekLessThan(
						marq.column(marq.name), StringExpression.value("BMW"),
						marq.column(marq.uidMarque), IntegerExpression.value(6664478),
						marq.column(marq.creationDate), DateExpression.value(april2nd2011)
				)
		);

		allRows = dbQuery.getAllRows();
		Assert.assertThat(allRows.size(), is(1));
	}

	@Test
	public void testSeekGreaterThanStringNumberDate() throws AccidentalBlankQueryException, AccidentalCartesianJoinException, SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);

		dbQuery.addCondition(
				BooleanExpression.seekGreaterThan(
						marq.column(marq.name), StringExpression.value("BMW"),
						marq.column(marq.uidMarque), IntegerExpression.value(6664478),
						marq.column(marq.creationDate), DateExpression.value(march23rd2013)
				)
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		Assert.assertThat(allRows.size(), is(21));
	}

	@Test
	public void testSeekGreaterThan() throws AccidentalBlankQueryException, AccidentalCartesianJoinException, SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);

		dbQuery.addCondition(
				BooleanExpression.seekGreaterThan(
						marq.column(marq.name), StringExpression.value("FORD"), marq.column(marq.auto_created).isNull()
				)
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		Assert.assertThat(allRows.size(), is(17));
	}

	@Test
	public void testSeekLessThan() throws AccidentalBlankQueryException, AccidentalCartesianJoinException, SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);

		dbQuery.addCondition(
				BooleanExpression.seekLessThan(
						marq.column(marq.name), StringExpression.value("FORD"), marq.column(marq.auto_created).isNotNull()
				)
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		Assert.assertThat(allRows.size(), is(5));
	}

	@Test
	public void testOr() throws AccidentalBlankQueryException, AccidentalCartesianJoinException, SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);

		dbQuery.addCondition(
				marq.column(marq.name).isLessThan("FORD")
						.or(marq.column(marq.updateCount).isGreaterThan(2))
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		Assert.assertThat(allRows.size(), is(7));

		dbQuery = database.getDBQuery(marq);

		dbQuery.addCondition(
				marq.column(marq.name).isLessThan("FORD")
						.or(marq.column(marq.creationDate).isLessThan(march23rd2013))
		);

		allRows = dbQuery.getAllRows();
		Assert.assertThat(allRows.size(), is(6));
	}
}
