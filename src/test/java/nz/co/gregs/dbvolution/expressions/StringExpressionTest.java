/*
 * Copyright 2013 greg.
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
import java.util.Date;
import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.columns.StringColumn;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import nz.co.gregs.dbvolution.results.NumberResult;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class StringExpressionTest extends AbstractTest {

	public StringExpressionTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testUserFunctions() throws SQLException {
		Marque marq = new Marque();
		marq.name.permittedValues(StringExpression.currentUser());
		DBQuery query = database.getDBQuery(marq);
		List<Marque> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(0));
	}

	@Test
	public void testQDTOfExpression() throws SQLException {
		final StringExpression currentUser = StringExpression.currentUser();
		DBString qdt = currentUser.getQueryableDatatypeForExpressionValue();
		assertThat(qdt, isA(DBString.class));
	}

	@Test
	public void testIsNotNullAndEmptyString() throws SQLException, ParseException {
		CarCompany carCo = new CarCompany();
		DBQuery dbQuery = database.getDBQuery(carCo);

		dbQuery.addCondition(
				carCo.column(carCo.name).isNotNullAndNotEmpty()
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		assertThat(allRows.size(), is(4));

		CarCompany newCarCo = new CarCompany(null, 18);
		database.insert(newCarCo);

		allRows = dbQuery.getAllRows();
		assertThat(allRows.size(), is(4));
	}

	@Test
	public void testIsNullOrEmptyString() throws SQLException, ParseException {
		CarCompany carCo = new CarCompany();
		DBQuery dbQuery = database.getDBQuery(carCo);

		dbQuery.addCondition(
				carCo.column(carCo.name).isNullOrEmpty()
		);

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		assertThat(allRows.size(), is(0));

		CarCompany newCarCo = new CarCompany(null, 18);
		database.insert(newCarCo);

		allRows = dbQuery.getAllRows();
		assertThat(allRows.size(), is(1));
	}

	@Test
	public void testIsNotLikeStringExpressionUsingDBString() throws SQLException {
		Marque likeQuery = new Marque();
		final DBString str = new DBString("%e%");
		likeQuery.name.excludedPattern(new StringExpression(str).uppercase());
		List<Marque> rowsByExample = marquesTable.getRowsByExample(likeQuery);

		assertEquals(14, rowsByExample.size());
	}

	@Test
	public void testIsNotLikeStringExpressionUsingNullDBString() throws SQLException {
		Marque likeQuery = new Marque();
		final DBString str = new DBString("%e%");
		likeQuery.name.excludedPattern(new StringExpression((DBString) null).uppercase());
		List<Marque> rowsByExample = marquesTable.getRowsByExample(likeQuery);

		assertEquals(22, rowsByExample.size());
	}

	@Test
	public void testIsNotLikeStringExpressionUsingNumber() throws SQLException {
		Marque likeQuery = new Marque();
		likeQuery.name.excludedPattern(new StringExpression(5).uppercase());
		List<Marque> rowsByExample = marquesTable.getRowsByExample(likeQuery);

		assertEquals(22, rowsByExample.size());
	}

	@Test
	public void testIsNotLikeStringExpressionUsingNumberResult() throws SQLException {
		Marque likeQuery = new Marque();
		final NumberExpression num = new NumberExpression(5);
		likeQuery.name.excludedPattern(new StringExpression(num).uppercase());

		List<Marque> rowsByExample = marquesTable.getRowsByExample(likeQuery);

		assertEquals(22, rowsByExample.size());
	}

	@Test
	public void testIsNotLikeStringExpressionUsingNullNumberResult() throws SQLException {
		Marque likeQuery = new Marque();
		likeQuery.name.excludedPattern(new StringExpression((NumberResult) null).uppercase());
		List<Marque> rowsByExample = marquesTable.getRowsByExample(likeQuery);
		assertEquals(22, rowsByExample.size());
	}

	@Test
	public void testIsNotStringExpressionNull() throws SQLException {
		Marque likeQuery = new Marque();
		final StringExpression nullExpr = StringExpression.nullString();
		likeQuery.individualAllocationsAllowed.excludedValues(nullExpr);

		List<Marque> rowsByExample = marquesTable.getRowsByExample(likeQuery);

		if (database.supportsDifferenceBetweenNullAndEmptyString()) {
			assertEquals(20, rowsByExample.size());
		} else {
			assertEquals(1, rowsByExample.size());
		}
	}

	@Test
	public void testNumberResult() throws SQLException {
		Marque marq = new Marque();
		DBQuery q = database.getDBQuery(marq);
		q.addCondition(marq.column(marq.uidMarque).is(StringExpression.value("6664478").numberResult()));
		List<Marque> rowsByExample = q.getAllInstancesOf(marq);

		assertEquals(1, rowsByExample.size());
		assertThat(rowsByExample.get(0).name.getValue(), is("BMW"));
	}

	@Test
	public void testIsNumber() throws SQLException {
		Marque marq = new Marque();
		DBQuery q = database.getDBQuery(marq);
		q.addCondition(marq.column(marq.uidMarque).stringResult().is(6664478));
		List<Marque> rowsByExample = q.getAllInstancesOf(marq);

		assertEquals(1, rowsByExample.size());
		assertThat(rowsByExample.get(0).name.getValue(), is("BMW"));
	}

	@Test
	public void testIsNumberIgnoreCase() throws SQLException {
		Marque marq = new Marque();
		DBQuery q = database.getDBQuery(marq);
		q.addCondition(marq.column(marq.uidMarque).stringResult().isIgnoreCase(6664478));
		List<Marque> rowsByExample = q.getAllInstancesOf(marq);

		assertEquals(1, rowsByExample.size());
		assertThat(rowsByExample.get(0).name.getValue(), is("BMW"));
	}

	@Test
	public void testIsNotNumber() throws SQLException {
		Marque marq = new Marque();
		DBQuery q = database.getDBQuery(marq);
		q.addCondition(marq.column(marq.uidMarque).stringResult().isNot(6664478));
		List<Marque> rowsByExample = q.getAllInstancesOf(marq);

		assertEquals(21, rowsByExample.size());
	}

	@Test
	public void testIsNotNumberResult() throws SQLException {
		Marque marq = new Marque();
		DBQuery q = database.getDBQuery(marq);
		q.addCondition(marq.column(marq.uidMarque).stringResult().isNot(NumberExpression.value(6664478)));
		List<Marque> rowsByExample = q.getAllInstancesOf(marq);

		assertEquals(21, rowsByExample.size());
	}

	@Test
	public void testIsStringExpressionAggregator() throws SQLException {
		Marque marq = new Marque();
		DBQuery q = database.getDBQuery(marq);
		q.addCondition(marq.column(marq.name).min().is("FORD"));
		List<Marque> rowsByExample = q.getAllInstancesOf(marq);

		assertEquals(1, rowsByExample.size());
	}

	@Test
	public void testIsStringExpressionAggregators() throws SQLException {
		Marque marq = new Marque();
		DBQuery q = database.getDBQuery(marq);
		q.addCondition(marq.column(marq.name).min().is("FORD"));
		q.addCondition(marq.column(marq.name).max().is("FORD"));
		List<Marque> rowsByExample = q.getAllInstancesOf(marq);

		assertEquals(1, rowsByExample.size());
	}

	@Test
	public void testTrimTransform() throws SQLException {
		database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));
		Marque marq = new Marque();
		marq.name.permittedValuesIgnoreCase("HUMMER");
		DBQuery dbQuery = database.getDBQuery(marq);
		List<Marque> got = dbQuery.getAllInstancesOf(marq);
		assertThat(got.size(), is(1));

		marq = new Marque();
		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.name).trim().is("HUMMER"));
		got = dbQuery.getAllInstancesOf(marq);
		assertThat(got.size(), is(2));
	}

	public static class trimmedMarque extends Marque {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBString leftTrim = new DBString(this.column(this.name).leftTrim());
		@DBColumn
		DBString rightTrim = new DBString(this.column(this.name).rightTrim());
	}

	@Test
	public void testLeftAndRightTrimTransforms() throws SQLException {
		database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));
		trimmedMarque marq = new trimmedMarque();
		marq.name.permittedValuesIgnoreCase("HUMMER");
		DBQuery dbQuery = database.getDBQuery(marq);
		List<trimmedMarque> got = dbQuery.getAllInstancesOf(marq);
		assertThat(got.size(), is(1));

		marq.name.clear();

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.name).leftTrim().is("HUMMER")
		);
		got = dbQuery.getAllInstancesOf(marq);
		assertThat(got.size(), is(1));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.name).rightTrim().is("HUMMER"));
		got = dbQuery.getAllInstancesOf(marq);
		assertThat(got.size(), is(1));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.name).rightTrim().leftTrim().is("HUMMER"));
		got = dbQuery.getAllInstancesOf(marq);
		assertThat(got.size(), is(2));
	}

	@Test
	public void testUpperAndLowercaseTransforms() throws SQLException {
		Marque marq = new Marque();

		marq.name.permittedValues("HUMMER");
		DBQuery dbQuery = database.getDBQuery(marq);
		List<Marque> got = dbQuery.getAllInstancesOf(marq);
		assertThat(got.size(), is(1));

		marq = new Marque();

		String hummerLowerCase = "hummer";
		final String hummerUpperCase = "HUMMER";

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.name).lowercase().is(hummerUpperCase));
		got = dbQuery.getAllInstancesOf(marq);
		assertThat(got.size(), is(0));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.name).lowercase().is(hummerLowerCase));
		got = dbQuery.getAllInstancesOf(marq);
		assertThat(got.size(), is(1));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.name).uppercase().is(hummerLowerCase));
		got = dbQuery.getAllInstancesOf(marq);
		assertThat(got.size(), is(0));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.name).lowercase().uppercase().is(hummerUpperCase));
		got = dbQuery.getAllInstancesOf(marq);
		assertThat(got.size(), is(1));
	}

	@Test
	public void testSubstringTransform() throws SQLException {
		Marque marq = new Marque();
		marq.name.permittedValues("HUMMER".substring(0, 3));
		DBQuery dbQuery = database.getDBQuery(marq);
		List<Marque> got = dbQuery.getAllInstancesOf(marq);
		assertThat(got.size(), is(0));

		marq.name.clear();

		String last3LettersOfHUMMER = "HUMMER".substring(3, 6);
		String first3LettersOfHUMMER = "HUMMER".substring(0, 3);

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.name).substring(0, 3).is(first3LettersOfHUMMER));
		got = dbQuery.getAllInstancesOf(marq);
		assertThat(got.size(), is(1));
		assertThat((got.get(0)).name.stringValue(), is("HUMMER"));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.name).substring(NumberExpression.value(3), 6).is(first3LettersOfHUMMER));
		got = dbQuery.getAllInstancesOf(marq);
		assertThat(got.size(), is(0));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.name).substring(3, NumberExpression.value(6)).is(last3LettersOfHUMMER));
		got = dbQuery.getAllInstancesOf(marq);

		assertThat(got.size(), is(1));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.name).substring(3, 6).lowercase().is(last3LettersOfHUMMER));
		got = dbQuery.getAllInstancesOf(marq);

		assertThat(got.size(), is(0));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.name).substring(3, 6).lowercase().uppercase().is(last3LettersOfHUMMER));
		got = dbQuery.getAllInstancesOf(marq);

		assertThat(got.size(), is(1));

		database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.name).trim().uppercase().substring(3, 6).is(last3LettersOfHUMMER));
		got = dbQuery.getAllInstancesOf(marq);

		assertThat(got.size(), is(2));
	}

	@Test
	public void testStringLengthTransform() throws SQLException {

		Marque marq = new Marque();
		marq.name.clear();
		DBQuery query = database.getDBQuery(marq);

		query.addCondition(marq.column(marq.name).length().isBetweenInclusive(1, 3));
		query.setSortOrder(marq.column(marq.name));
		List<Marque> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(2));
		assertThat((got.get(0)).name.stringValue(), is("BMW"));
		assertThat((got.get(1)).name.stringValue(), is("VW"));
	}

	@Test
	public void testStringBefore() throws SQLException {

		Marque marq = new Marque();
		marq.name.clear();
		DBQuery query = database.getDBQuery(marq);

		query.addCondition(marq.column(marq.name).substringBefore("Y").is("TO"));
		query.setSortOrder(marq.column(marq.name));
		List<Marque> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(1));
		assertThat((got.get(0)).name.stringValue(), is("TOYOTA"));
	}

	@Test
	public void testStringAfter() throws SQLException {

		Marque marq = new Marque();
		marq.name.clear();
		DBQuery query = database.getDBQuery(marq);

		query.addCondition(marq.column(marq.name).substringAfter("Y").is("OTA"));
		query.setSortOrder(marq.column(marq.name));
		List<Marque> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(1));
		assertThat((got.get(0)).name.stringValue(), is("TOYOTA"));
	}

	@Test
	public void testStringBeforeAndAfter() throws SQLException {
		Marque marq = new Marque();
		marq.name.clear();
		DBQuery query = database.getDBQuery(marq);

		query.addCondition(marq.column(marq.name).substringBefore("Y").substringAfter("T").is("O"));
		query.setSortOrder(marq.column(marq.name));
		List<Marque> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(1));
		assertThat((got.get(0)).name.stringValue(), is("TOYOTA"));
	}

	@Test
	public void testStringBetween() throws SQLException {
		Marque marq = new Marque();
		marq.name.clear();
		DBQuery query = database.getDBQuery(marq);

		query.addCondition(marq.column(marq.name).lowercase().isBetween("toy", "tup"));
		query.setSortOrder(marq.column(marq.name));
		List<Marque> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(1));
		assertThat((got.get(0)).name.stringValue(), is("TOYOTA"));
	}

	@Test
	public void testStringBetweenResultLeft() throws SQLException {

		Marque marq = new Marque();
		marq.name.clear();
		DBQuery query = database.getDBQuery(marq);

		query.addCondition(
				marq.column(marq.name).lowercase()
						.isBetween(StringExpression.value("toy"), "tup"));
		query.setSortOrder(marq.column(marq.name));
		List<Marque> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(1));
		assertThat((got.get(0)).name.stringValue(), is("TOYOTA"));
	}

	@Test
	public void testStringBetweenResultRight() throws SQLException {

		Marque marq = new Marque();
		marq.name.clear();
		DBQuery query = database.getDBQuery(marq);

		query.addCondition(
				marq.column(marq.name).lowercase()
						.isBetween("toy", StringExpression.value("tup")));
		query.setSortOrder(marq.column(marq.name));
		List<Marque> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(1));
		assertThat((got.get(0)).name.stringValue(), is("TOYOTA"));
	}

	@Test
	public void testStringBetweenExclusive() throws SQLException {

		Marque marq = new Marque();
		marq.name.clear();
		DBQuery query = database.getDBQuery(marq);

		query.addCondition(marq.column(marq.name).lowercase().isBetweenExclusive("toy", "volvo"));
		query.setSortOrder(marq.column(marq.name));
		List<Marque> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(1));
		assertThat((got.get(0)).name.stringValue(), is("TOYOTA"));
	}

	@Test
	public void testStringBetweenExclusiveResultLeft() throws SQLException {

		Marque marq = new Marque();
		marq.name.clear();
		DBQuery query = database.getDBQuery(marq);

		query.addCondition(
				marq.column(marq.name).lowercase()
						.isBetweenExclusive(StringExpression.value("toy"), "volvo"));
		query.setSortOrder(marq.column(marq.name));
		List<Marque> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(1));
		assertThat((got.get(0)).name.stringValue(), is("TOYOTA"));
	}

	@Test
	public void testStringBetweenExclusiveResultRight() throws SQLException {

		Marque marq = new Marque();
		marq.name.clear();
		DBQuery query = database.getDBQuery(marq);

		query.addCondition(
				marq.column(marq.name).lowercase()
						.isBetweenExclusive("toy", StringExpression.value("volvo")));
		query.setSortOrder(marq.column(marq.name));
		List<Marque> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(1));
		assertThat((got.get(0)).name.stringValue(), is("TOYOTA"));
	}

	@Test
	public void testStringBetweenInclusive() throws SQLException {

		Marque marq = new Marque();
		marq.name.clear();
		DBQuery query = database.getDBQuery(marq);

		query.addCondition(marq.column(marq.name).lowercase().isBetweenInclusive("toy", "volvo"));
		query.setSortOrder(marq.column(marq.name));
		List<Marque> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(2));
		assertThat((got.get(0)).name.stringValue(), is("TOYOTA"));
		assertThat((got.get(1)).name.stringValue(), is("VOLVO"));
	}

	@Test
	public void testStringContains() throws SQLException {

		Marque marq = new Marque();
		marq.name.clear();
		DBQuery query = database.getDBQuery(marq);

		query.addCondition(
				BooleanExpression.anyOf(
						marq.column(marq.name).lowercase().contains("oyo"),
						marq.column(marq.name).lowercase().contains("lv")
				)
		);
		query.setSortOrder(marq.column(marq.name));
		List<Marque> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(2));
		assertThat((got.get(0)).name.stringValue(), is("TOYOTA"));
		assertThat((got.get(1)).name.stringValue(), is("VOLVO"));
	}

	@Test
	public void testStringContainsIgnoreCase() throws SQLException {

		Marque marq = new Marque();
		marq.name.clear();
		DBQuery query = database.getDBQuery(marq);
		StringColumn column = marq.column(marq.name);

		query.addCondition(BooleanExpression.anyOf(
				column.containsIgnoreCase("oyo"),
				column.containsIgnoreCase("lv")
		)
		);
		query.setSortOrder(marq.column(marq.name));
		List<Marque> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(2));
		assertThat((got.get(0)).name.stringValue(), is("TOYOTA"));
		assertThat((got.get(1)).name.stringValue(), is("VOLVO"));
	}

	@Test
	public void testStringBetweenInclusiveResultLeft() throws SQLException {

		Marque marq = new Marque();
		marq.name.clear();
		DBQuery query = database.getDBQuery(marq);

		query.addCondition(
				marq.column(marq.name).lowercase()
						.isBetweenInclusive(StringExpression.value("toy"), "volvo"));
		query.setSortOrder(marq.column(marq.name));
		List<Marque> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(2));
		assertThat((got.get(0)).name.stringValue(), is("TOYOTA"));
		assertThat((got.get(1)).name.stringValue(), is("VOLVO"));
	}

	@Test
	public void testStringStartsWith() throws SQLException {

		Marque marq = new Marque();
		marq.name.clear();
		DBQuery query = database.getDBQuery(marq);
		StringExpression lowercase = marq.column(marq.name).lowercase();

		query.addCondition(
				BooleanExpression.anyOf(
						lowercase.startsWith("toyo"),
						lowercase.startsWith("vo")
				)
		);
		query.setSortOrder(marq.column(marq.name));
		List<Marque> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(2));
		assertThat((got.get(0)).name.stringValue(), is("TOYOTA"));
		assertThat((got.get(1)).name.stringValue(), is("VOLVO"));
	}

	@Test
	public void testStringEndsWith() throws SQLException {

		Marque marq = new Marque();
		marq.name.clear();
		DBQuery query = database.getDBQuery(marq);
		StringExpression lowercase = marq.column(marq.name).lowercase();

		query.addCondition(
				BooleanExpression.anyOf(
						lowercase.endsWith("ota"),
						lowercase.endsWith("lvo")
				)
		);
		query.setSortOrder(marq.column(marq.name));
		List<Marque> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(2));
		assertThat((got.get(0)).name.stringValue(), is("TOYOTA"));
		assertThat((got.get(1)).name.stringValue(), is("VOLVO"));
	}

	@Test
	public void testStringBetweenInclusiveResultRight() throws SQLException {

		Marque marq = new Marque();
		marq.name.clear();
		DBQuery query = database.getDBQuery(marq);

		query.addCondition(
				marq.column(marq.name).lowercase()
						.isBetweenInclusive("toy", StringExpression.value("volvo")));
		query.setSortOrder(marq.column(marq.name));
		List<Marque> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(2));
		assertThat((got.get(0)).name.stringValue(), is("TOYOTA"));
		assertThat((got.get(1)).name.stringValue(), is("VOLVO"));
	}

	@Test
	public void testStringAccumulate() throws SQLException {
		Marque marq = new Marque();
		marq.name.clear();
		marq.setReturnFieldsToNone();
		DBQuery query = database.getDBQuery(marq);

		query.addCondition(
				marq.column(marq.name).lowercase()
						.isBetweenInclusive("toy", StringExpression.value("volvo")));
		query.addExpressionColumn(this, marq.column(marq.name).aggregate(", ").asExpressionColumn());

		List<DBQueryRow> got = query.getAllRows();

		assertThat(got.size(), is(1));
		assertThat(
				got.get(0).getExpressionColumnValue(this).stringValue(),
				isOneOf("TOYOTA, VOLVO", "VOLVO, TOYOTA"));
	}

	@Test
	public void testStringAccumulateWithOrderByDescending() throws SQLException {
		Marque marq = new Marque();
		marq.name.clear();
		marq.setReturnFieldsToNone();
		DBQuery query = database.getDBQuery(marq);

		query.addCondition(
				marq.column(marq.name).lowercase()
						.isBetweenInclusive("toy", StringExpression.value("volvo")));
		query.addExpressionColumn(
				this,
				marq.column(marq.name)
						.aggregate(", ", marq.column(marq.name).descending())
						.asExpressionColumn()
		);
		List<DBQueryRow> got = query.getAllRows();

		assertThat(got.size(), is(1));
		assertThat(got.get(0).getExpressionColumnValue(this).stringValue(), is("VOLVO, TOYOTA"));
	}

	@Test
	public void testStringAccumulateWithOrderByAscending() throws SQLException {
		Marque marq = new Marque();
		marq.name.clear();
		marq.setReturnFieldsToNone();
		DBQuery query = database.getDBQuery(marq);

		query.addCondition(
				marq.column(marq.name).lowercase()
						.isBetweenInclusive("toy", StringExpression.value("volvo")));
		query.addExpressionColumn(
				this,
				marq.column(marq.name)
						.aggregate(", ", marq.column(marq.name).ascending())
						.asExpressionColumn()
		);
		List<DBQueryRow> got = query.getAllRows();

		assertThat(got.size(), is(1));
		assertThat(got.get(0).getExpressionColumnValue(this).stringValue(), is("TOYOTA, VOLVO"));
	}

	@Test
	public void testReplaceTransform() throws SQLException {

		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		final StringColumn nameColumn = marq.column(marq.name);
		final StringExpression nameValue = nameColumn;

		query.addCondition(nameValue.is("TOY"));
		List<Marque> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(0));

		query = database.getDBQuery(marq);
		query.addCondition(nameValue.replace("OTA", "").is("TOY"));
		query.setSortOrder(nameColumn);
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(1));
		assertThat((got.get(0)).name.stringValue(), is("TOYOTA"));

		query = database.getDBQuery(marq);
		query.addCondition(nameValue.replace(StringExpression.value("BM"), "V").is("VW"));
		query.setSortOrder(nameColumn);
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(2));
		assertThat((got.get(0)).name.stringValue(), is("BMW"));
		assertThat((got.get(1)).name.stringValue(), is("VW"));

		query = database.getDBQuery(marq);
		query.addCondition(nameValue.replace("BM", StringExpression.value("V")).is("VW"));
		query.setSortOrder(nameColumn);
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(2));
		assertThat((got.get(0)).name.stringValue(), is("BMW"));
		assertThat((got.get(1)).name.stringValue(), is("VW"));

		// A rather complicated way to find out how many marques start with V
		query = database.getDBQuery(marq);
		query.addCondition(nameValue.replace(nameValue.substring(1), StringExpression.value("")).is("V"));
		query.setSortOrder(nameColumn);
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(2));
		assertThat((got.get(0)).name.stringValue(), is("VOLVO"));
		assertThat((got.get(1)).name.stringValue(), is("VW"));
	}

	@Test
	public void testConcatTransform() throws SQLException {

		Marque marq = new Marque();
		DBQuery query;
		List<Marque> got;
		final StringColumn nameColumn = marq.column(marq.name);
		final StringExpression nameValue = nameColumn;

		query = database.getDBQuery(marq);
		// Find VW and BMW by appending V and W around the replaced brands
		query.addCondition(nameValue.replace("M", "O").is("BOW"));
		query.setSortOrder(nameColumn);
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(1));
		assertThat((got.get(0)).name.stringValue(), is("BMW"));

		query = database.getDBQuery(marq);
		// Find VW and BMW by appending V and W around the replaced brands
		query.addCondition(StringExpression.value("V").append(nameValue.replace("BMW", "-").replace("VW", "-")).replace("-", "W").is("VW"));
		query.setSortOrder(nameColumn);
		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(2));
		assertThat(got.get(0).name.stringValue(), is("BMW"));
		assertThat(got.get(1).name.stringValue(), is("VW"));

		query = database.getDBQuery(marq);
		query.addCondition(nameValue.length().is(6));
		query.addCondition(nameValue.substring(3, 6).append(nameValue.substring(0, 3)).is("OTATOY"));
		query.setSortOrder(nameColumn);

		got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(1));
		assertThat(got.get(0).name.stringValue(), is("TOYOTA"));
	}

	@Test
	public void testStringCount() throws SQLException {
		Marque marque = new Marque();
		marque.setReturnFields(marque.carCompany);
		DBQuery query = database.getDBQuery(marque)
				.addCondition(
						marque.column(marque.carCompany)
								.stringResult()
								.count()
								.isGreaterThan(1));
		query.setSortOrder(marque.column(marque.carCompany));
		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(3));
		assertThat(allRows.get(0).get(marque).carCompany.getValue(), is(1l));
		assertThat(allRows.get(1).get(marque).carCompany.getValue(), is(3l));
		assertThat(allRows.get(2).get(marque).carCompany.getValue(), is(4l));

	}

	@Test
	public void testFindFirstNumber() throws SQLException {
		FindFirstNumberTable tab = new FindFirstNumberTable();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(tab);
		database.createTable(tab);
		database.insert(new FindFirstNumberTable[]{
			new FindFirstNumberTable("aaa -09.90 yabber", "-09.90", -9.9),
			new FindFirstNumberTable("aab 09.90 y", "09.90", 9.9),
			new FindFirstNumberTable("aac 900.90 y", "900.90", 900.9),
			new FindFirstNumberTable("aad 900.90.90 y", "900.90", 900.9),
			new FindFirstNumberTable("aae 900.90 -0.90 y", "900.90", 900.9),
			new FindFirstNumberTable("aaf -900.90.90 0.90 y", "-900.90", -900.9),
			new FindFirstNumberTable("aag 900. -0.90 y", "900", 900),
			new FindFirstNumberTable("aah 900.", "900", 900),
			new FindFirstNumberTable("aai - 900 0.90 y", "900", 900),
			new FindFirstNumberTable("900.90.90 y", "900.90", 900.9),
			new FindFirstNumberTable("c 9.90 c", "9.90", 9.9),
			new FindFirstNumberTable("d 9 d", "9", 9),
			new FindFirstNumberTable("e -9 e", "-9", -9),
			new FindFirstNumberTable("f A e", null, null)
		});
		final DBTable<FindFirstNumberTable> query = database.getDBTable(tab)
				.setBlankQueryAllowed(true)
				.setSortOrder(tab.column(tab.sample));

		List<FindFirstNumberTable> allRows = query.getAllRows();

		if (allRows.size() != 14) {
			System.out.println(query.getSQLForQuery());
			database.print(allRows);
		}

		assertThat(allRows.size(), is(14));
		for (FindFirstNumberTable fab : allRows) {
			assertThat(fab.actualString.getValue(), is(fab.expectString.getValue()));
			assertThat(fab.actualNumber.doubleValue(), is(fab.expectNumber.doubleValue()));
		}
	}

	@Test
	public void testFindFirstInteger() throws SQLException {
		FindFirstIntegerTable tab = new FindFirstIntegerTable();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(tab);
		database.createTable(tab);
		database.insert(new FindFirstIntegerTable[]{
			new FindFirstIntegerTable("aaa -09.90 yabber", "-09", -9),
			new FindFirstIntegerTable("aab 09.90 y", "09", 9),
			new FindFirstIntegerTable("aac 900.90 y", "900", 900),
			new FindFirstIntegerTable("aad 900.90.90 y", "900", 900),
			new FindFirstIntegerTable("aae 900.90 -0.90 y", "900", 900),
			new FindFirstIntegerTable("aaf -900.90.90 0.90 y", "-900", -900),
			new FindFirstIntegerTable("aag 900. -0.90 y", "900", 900),
			new FindFirstIntegerTable("aah 900.", "900", 900),
			new FindFirstIntegerTable("aai - 900 0.90 y", "900", 900),
			new FindFirstIntegerTable("900.90.90 y", "900", 900),
			new FindFirstIntegerTable("c 9.90 c", "9", 9),
			new FindFirstIntegerTable("d 9 d", "9", 9),
			new FindFirstIntegerTable("e -9 e", "-9", -9),
			new FindFirstIntegerTable("f A e", null, null)
		});
		final DBTable<FindFirstIntegerTable> query = database.getDBTable(tab)
				.setBlankQueryAllowed(true)
				.setSortOrder(tab.column(tab.sample));

		List<FindFirstIntegerTable> allRows = query.getAllRows();

		if (allRows.size() != 14) {
			System.out.println(query.getSQLForQuery());
			database.print(allRows);
			System.out.println("---------");
			database.print(database.getDBTable(tab)
					.setBlankQueryAllowed(true)
					.setSortOrder(tab.column(tab.sample))
					.getAllRows());
		}

		assertThat(allRows.size(), is(14));
		for (FindFirstIntegerTable fab : allRows) {
			assertThat(fab.actualIntegerString.getValue(), is(fab.expectString.getValue()));
			assertThat(fab.actualInteger.longValue(), is(fab.expectNumber.longValue()));
		}
	}

	public static class FindFirstNumberTable extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBAutoIncrement
		@DBPrimaryKey
		@DBColumn
		public DBInteger pkid = new DBInteger();

		@DBColumn
		public DBString sample = new DBString();

		@DBColumn
		public DBString expectString = new DBString();

		@DBColumn
		public DBString actualString = new DBString(this.column(sample).getFirstNumberAsSubstring());

		@DBColumn
		public DBNumber expectNumber = new DBNumber();

		@DBColumn
		public DBNumber actualNumber = new DBNumber(this.column(sample).getFirstNumber());

		@DBColumn
		public DBString actualIntegerString = new DBString(this.column(sample).getFirstIntegerAsSubstring());

		@DBColumn
		public DBInteger actualInteger = new DBInteger(this.column(sample).getFirstInteger());

		public FindFirstNumberTable() {
			super();
		}

		public FindFirstNumberTable(String sample, String expect, Number expectNumber) {
			this.sample.setValue(sample);
			this.expectString.setValue(expect);
			this.expectNumber.setValue(expectNumber);
		}
	}

	public static class FindFirstIntegerTable extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBAutoIncrement
		@DBPrimaryKey
		@DBColumn
		public DBInteger pkid = new DBInteger();

		@DBColumn
		public DBString sample = new DBString();

		@DBColumn
		public DBString expectString = new DBString();

		@DBColumn
		public DBString actualString = new DBString(this.column(sample).getFirstNumberAsSubstring());

		@DBColumn
		public DBNumber expectNumber = new DBNumber();

		@DBColumn
		public DBNumber actualNumber = new DBNumber(this.column(sample).getFirstNumber());

		@DBColumn
		public DBString actualIntegerString = new DBString(this.column(sample).getFirstIntegerAsSubstring());

		@DBColumn
		public DBInteger actualInteger = new DBInteger(this.column(sample).getFirstInteger());

		public FindFirstIntegerTable() {
			super();
		}

		public FindFirstIntegerTable(String sample, String expect, Number expectNumber) {
			this.sample.setValue(sample);
			this.expectString.setValue(expect);
			this.expectNumber.setValue(expectNumber);
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

		expectedValues.add(new Object[]{22, 1, 2, 2, null, "HY"});
		expectedValues.add(new Object[]{22, 2, 2, 2, "TO", "FO"});
		expectedValues.add(new Object[]{22, 3, 1, 1, "HY", "HU"});
		expectedValues.add(new Object[]{22, 4, 3, 3, "FO", "HO"});
		expectedValues.add(new Object[]{22, 5, 3, 3, "HU", "JE"});
		expectedValues.add(new Object[]{22, 6, 3, 3, "HO", "PE"});
		expectedValues.add(new Object[]{22, 7, 16, 16, "JE", "MI"});
		expectedValues.add(new Object[]{22, 8, 16, 16, "PE", "SU"});
		expectedValues.add(new Object[]{22, 9, 16, 16, "MI", "HO"});
		expectedValues.add(new Object[]{22, 10, 16, 16, "SU", "NI"});
		expectedValues.add(new Object[]{22, 11, 16, 16, "HO", "SU"});
		expectedValues.add(new Object[]{22, 12, 16, 16, "NI", "MA"});
		expectedValues.add(new Object[]{22, 13, 16, 16, "SU", "RO"});
		expectedValues.add(new Object[]{22, 14, 16, 16, "MA", "BM"});
		expectedValues.add(new Object[]{22, 15, 16, 16, "RO", "DA"});
		expectedValues.add(new Object[]{22, 16, 16, 16, "BM", "LA"});
		expectedValues.add(new Object[]{22, 17, 16, 16, "DA", "VO"});
		expectedValues.add(new Object[]{22, 18, 16, 16, "LA", "IS"});
		expectedValues.add(new Object[]{22, 19, 16, 16, "VO", "DA"});
		expectedValues.add(new Object[]{22, 20, 16, 16, "IS", "CH"});
		expectedValues.add(new Object[]{22, 21, 16, 16, "DA", "VW"});
		expectedValues.add(new Object[]{22, 22, 16, 16, "CH", null});

		for (int i = 0; i < allRows.size(); i++) {
			got = allRows.get(i).get(marq);
//			System.out.println("" + got.toString());
			Object[] expect = expectedValues.get(i);
			assertThat(got.countOfAllRows.intValue(), is((Integer) expect[0]));
			assertThat(got.rowNumber.intValue(), is((Integer) expect[1]));
			assertThat(got.countOfEnabled.intValue(), is((Integer) expect[2]));
			assertThat(got.rowWithinCarCo.intValue(), is((Integer) expect[3]));
			final String lagTest = oracleSafeStrings((String) expect[4]);
			assertThat(got.lag.getValue(), is(lagTest));
			final String leadTest = oracleSafeStrings((String) expect[5]);
			assertThat(got.lead.getValue(), is(leadTest));
		}
	}

	public static class MarqueWithLagAndLeadFunctions extends Marque {

		private static final long serialVersionUID = 1L;

		StringExpression stringTest = this.column(this.name).substring(0, 2);
		@DBColumn
		DBInteger countOfAllRows = new DBInteger(
				stringTest
						.count()
						.over()
						.allRows());
		@DBColumn
		DBNumber rowNumber = new DBNumber(
				stringTest
						.count()
						.over()
						.AllRowsAndOrderBy(this.column(this.carCompany).ascending(), this.column(this.uidMarque).ascending())
		);
		@DBColumn
		DBNumber countOfEnabled = new DBNumber(
				stringTest
						.count()
						.over()
						.partition(this.column(this.carCompany)).unordered());
		@DBColumn
		DBNumber rowWithinCarCo = new DBNumber(
				stringTest
						.count()
						.over()
						.partition(this.column(this.carCompany))
						.orderBy(this.column(this.carCompany).ascending())
						.defaultFrame());
		@DBColumn
		DBString lag
				= stringTest
						.lag()
						.allRows()
						.orderBy(this.column(this.carCompany).ascending(), this.column(this.uidMarque).ascending())
						.asExpressionColumn();
		@DBColumn
		DBString lead = new DBString(
				stringTest
						.nextRowValue()
						.AllRowsAndOrderBy(
								this.column(this.carCompany).ascending(),
								this.column(this.uidMarque).ascending()
						)
		);
	}
}
