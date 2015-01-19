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
import java.util.Date;
import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.columns.StringColumn;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
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
		database.print(got);
		System.out.println(query.getSQLForQuery());
		Assert.assertThat(got.size(), is(0));
	}

	@Test
	public void testTrimTransform() throws SQLException {
		database.setPrintSQLBeforeExecuting(true);
		database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));
		Marque marq = new Marque();
		marq.name.permittedValuesIgnoreCase("HUMMER");
		DBQuery dbQuery = database.getDBQuery(marq);
		List<Marque> got = dbQuery.getAllInstancesOf(marq);
		Assert.assertThat(got.size(), is(1));

		marq = new Marque();
		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.name).trim().is("HUMMER"));
		got = dbQuery.getAllInstancesOf(marq);
		Assert.assertThat(got.size(), is(2));
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
		database.setPrintSQLBeforeExecuting(true);
		database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));
		trimmedMarque marq = new trimmedMarque();
		marq.name.permittedValuesIgnoreCase("HUMMER");
		DBQuery dbQuery = database.getDBQuery(marq);
		List<trimmedMarque> got = dbQuery.getAllInstancesOf(marq);
		Assert.assertThat(got.size(), is(1));

		marq.name.clear();

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.name).leftTrim().is("HUMMER")
		);
		got = dbQuery.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(1));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.name).rightTrim().is("HUMMER"));
		got = dbQuery.getAllInstancesOf(marq);
		Assert.assertThat(got.size(), is(1));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.name).rightTrim().leftTrim().is("HUMMER"));
		got = dbQuery.getAllInstancesOf(marq);
		Assert.assertThat(got.size(), is(2));
	}

	@Test
	public void testUpperAndLowercaseTransforms() throws SQLException {
		database.setPrintSQLBeforeExecuting(true);
		Marque marq = new Marque();

		marq.name.permittedValues("HUMMER");
		DBQuery dbQuery = database.getDBQuery(marq);
		List<Marque> got = dbQuery.getAllInstancesOf(marq);
		Assert.assertThat(got.size(), is(1));

		marq = new Marque();

		String hummerLowerCase = "hummer";
		final String hummerUpperCase = "HUMMER";

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.name).lowercase().is(hummerUpperCase));
		got = dbQuery.getAllInstancesOf(marq);
		Assert.assertThat(got.size(), is(0));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.name).lowercase().is(hummerLowerCase));
		got = dbQuery.getAllInstancesOf(marq);
		Assert.assertThat(got.size(), is(1));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.name).uppercase().is(hummerLowerCase));
		got = dbQuery.getAllInstancesOf(marq);
		Assert.assertThat(got.size(), is(0));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.name).lowercase().uppercase().is(hummerUpperCase));
		got = dbQuery.getAllInstancesOf(marq);
		Assert.assertThat(got.size(), is(1));
	}

	@Test
	public void testSubstringTransform() throws SQLException {
		database.setPrintSQLBeforeExecuting(true);
		Marque marq = new Marque();
		marq.name.permittedValues("HUMMER".substring(0, 3));
		DBQuery dbQuery = database.getDBQuery(marq);
		List<Marque> got = dbQuery.getAllInstancesOf(marq);
		Assert.assertThat(got.size(), is(0));

		marq.name.clear();

		String last3LettersOfHUMMER = "HUMMER".substring(3, 6);
		String first3LettersOfHUMMER = "HUMMER".substring(0, 3);

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.name).substring(0, 3).is(first3LettersOfHUMMER));
		got = dbQuery.getAllInstancesOf(marq);
		Assert.assertThat(got.size(), is(1));
		Assert.assertThat(got.get(0).name.stringValue(), is("HUMMER"));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.name).substring(3, 6).is(first3LettersOfHUMMER));
		got = dbQuery.getAllInstancesOf(marq);
		Assert.assertThat(got.size(), is(0));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.name).substring(3, 6).is(last3LettersOfHUMMER));
		got = dbQuery.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(1));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.name).substring(3, 6).lowercase().is(last3LettersOfHUMMER));
		got = dbQuery.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(0));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.name).substring(3, 6).lowercase().uppercase().is(last3LettersOfHUMMER));
		got = dbQuery.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(1));

		database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.name).trim().uppercase().substring(3, 6).is(last3LettersOfHUMMER));
		got = dbQuery.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(2));
	}

	@Test
	public void testStringLengthTransform() throws SQLException {
		database.setPrintSQLBeforeExecuting(true);
		Marque marq = new Marque();
		marq.name.clear();
		DBQuery query = database.getDBQuery(marq);

		query.addCondition(marq.column(marq.name).length().isBetweenInclusive(1, 3));
		query.setSortOrder(marq.column(marq.name));
		List<Marque> got = query.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(2));
		Assert.assertThat(got.get(0).name.stringValue(), is("BMW"));
		Assert.assertThat(got.get(1).name.stringValue(), is("VW"));
	}

	@Test
	public void testReplaceTransform() throws SQLException {
		database.setPrintSQLBeforeExecuting(true);
		Marque marq = new Marque();
		DBQuery query = database.getDBQuery(marq);
		final StringColumn nameColumn = marq.column(marq.name);
		final StringExpression nameValue = nameColumn;

		query.addCondition(nameValue.is("TOY"));
		List<Marque> got = query.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(0));

		query = database.getDBQuery(marq);
		query.addCondition(nameValue.replace("OTA", "").is("TOY"));
		query.setSortOrder(nameColumn);
		got = query.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(1));
		Assert.assertThat(got.get(0).name.stringValue(), is("TOYOTA"));

		query = database.getDBQuery(marq);
		query.addCondition(nameValue.replace("BM", "V").is("VW"));
		query.setSortOrder(nameColumn);
		got = query.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(2));
		Assert.assertThat(got.get(0).name.stringValue(), is("BMW"));
		Assert.assertThat(got.get(1).name.stringValue(), is("VW"));

		// A rather complicated way to find out how many marques start with V
		query = database.getDBQuery(marq);
		query.addCondition(nameValue.replace(nameValue.substring(1), StringExpression.value("")).is("V"));
		query.setSortOrder(nameColumn);
		got = query.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(2));
		Assert.assertThat(got.get(0).name.stringValue(), is("VOLVO"));
		Assert.assertThat(got.get(1).name.stringValue(), is("VW"));
	}

	@Test
	public void testConcatTransform() throws SQLException {
		database.setPrintSQLBeforeExecuting(true);
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
		database.print(got);
		Assert.assertThat(got.size(), is(1));
		Assert.assertThat(got.get(0).name.stringValue(), is("BMW"));

		query = database.getDBQuery(marq);
		// Find VW and BMW by appending V and W around the replaced brands
		query.addCondition(StringExpression.value("V").append(nameValue.replace("BMW", "-").replace("VW", "-")).replace("-", "W").is("VW"));
		query.setSortOrder(nameColumn);
		got = query.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(2));
		Assert.assertThat(got.get(0).name.stringValue(), is("BMW"));
		Assert.assertThat(got.get(1).name.stringValue(), is("VW"));

		query = database.getDBQuery(marq);
		query.addCondition(nameValue.length().is(6));
		query.addCondition(nameValue.substring(3, 6).append(nameValue.substring(0, 3)).is("OTATOY"));
		query.setSortOrder(nameColumn);
		System.out.println(query.getSQLForQuery());
		got = query.getAllInstancesOf(marq);
		database.print(got);
		Assert.assertThat(got.size(), is(1));
		Assert.assertThat(got.get(0).name.stringValue(), is("TOYOTA"));
	}
}
