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
package nz.co.gregs.dbvolution.actions;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.GregorianCalendar;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.*;
import nz.co.gregs.dbvolution.columns.InstantColumn;
import nz.co.gregs.dbvolution.columns.LocalDateTimeColumn;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.exceptions.AutoIncrementFieldClassAndDatatypeMismatch;
import nz.co.gregs.dbvolution.expressions.DateExpression;
import nz.co.gregs.dbvolution.expressions.InstantExpression;
import nz.co.gregs.dbvolution.expressions.LocalDateTimeExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Test;
import org.junit.Assert;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBInsertTest extends AbstractTest {

	public DBInsertTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	/**
	 * Test of save method, of class DBInsert.
	 *
	 * @throws java.lang.Exception anything could happen
	 */
	@Test
	public void testSave() throws Exception {
		DBRow row = new CarCompany("TATA", 123);
		DBActionList result = DBInsert.save(database, row);
		Assert.assertThat(result.size(), is(1));
	}

	@Test
	public void testSaveWithAutoIncrementValues() throws Exception {
		if (database.getDefinition().supportsGeneratedKeys() || database.getDefinition().supportsRetrievingLastInsertedRowViaSQL()) {
			TestDefaultValueRetrieval row = new TestDefaultValueRetrieval();
			TestDefaultValueRetrieval row2 = new TestDefaultValueRetrieval();

			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(row);

			database.createTable(row);

			row.name.setValue("First Row");
			row2.name.setValue("Second Row");
			database.insert(row);
			Assert.assertThat(row.pk_uid.getValue(), is(1L));
			database.insert(row2);
			Assert.assertThat(row2.pk_uid.getValue(), is(2L));
			final Long pkValue = row2.pk_uid.getValue();
			TestDefaultValueRetrieval gotRow2 = database.getDBTable(row2).getRowsByPrimaryKey(pkValue).get(0);
			Assert.assertThat(gotRow2.pk_uid.getValue(), is(2L));

			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(row);
		}
	}

	@Test
	public void testSaveWithoutValues() throws Exception {
		if (database.getDefinition().supportsGeneratedKeys() || database.getDefinition().supportsRetrievingLastInsertedRowViaSQL()) {
			TestDefaultValueRetrieval row = new TestDefaultValueRetrieval();
			TestDefaultValueRetrieval row2 = new TestDefaultValueRetrieval();

			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(row);

			database.createTable(row);

			database.insert(row);
			Assert.assertThat(row.pk_uid.getValue(), is(1L));
			database.insert(row2);
			Assert.assertThat(row2.pk_uid.getValue(), is(2L));
			final Long pkValue = row2.pk_uid.getValue();
			TestDefaultValueRetrieval gotRow2 = database.getDBTable(row2).getRowsByPrimaryKey(pkValue).get(0);
			Assert.assertThat(gotRow2.pk_uid.getValue(), is(2L));

			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(row);
		}
	}

	@Test
	public void testSaveWithExpressionColumn() throws Exception {
		if (database.getDefinition().supportsGeneratedKeys() || database.getDefinition().supportsRetrievingLastInsertedRowViaSQL()) {
			TestInsertDoesNotUpdateExpressionColumns row = new TestInsertDoesNotUpdateExpressionColumns();
			TestInsertDoesNotUpdateExpressionColumns row2 = new TestInsertDoesNotUpdateExpressionColumns();

			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(row);

			database.createTable(row);

			database.insert(row);
			Assert.assertThat(row.pk_uid.getValue(), is(1L));
			database.insert(row2);
			Assert.assertThat(row2.pk_uid.getValue(), is(2L));
			final Long pkValue = row2.pk_uid.getValue();
			TestInsertDoesNotUpdateExpressionColumns gotRow2 = database.getDBTable(row2).getRowsByPrimaryKey(pkValue).get(0);
			Assert.assertThat(gotRow2.pk_uid.getValue(), is(2L));

			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(row);
		}
	}

	@Test(expected = AutoIncrementFieldClassAndDatatypeMismatch.class)
	public void testSaveWithDefaultValuesAndIncorrectDatatype() throws Exception {
		TestDefaultValueIncorrectDatatype row = new TestDefaultValueIncorrectDatatype();

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(row);
		try {
			// avoid printing an exception as we're expecting one
			database.setQuietExceptionsPreference(true);
			database.createTable(row);
		} finally {
			// make sure that subsequent exceptions are reported
			database.setQuietExceptionsPreference(false);
		}
		try {
			row.name.setValue("First Row");
			database.insert(row);
		} finally {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(row);
		}
	}

	@Test
	public void testSaveWithDefaultValues() throws Exception {
		GregorianCalendar cal = new GregorianCalendar();
		cal.add(GregorianCalendar.MINUTE, -1);
		Date startTime = cal.getTime();

		TestDefaultInsertValue row = new TestDefaultInsertValue();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(row);
		database.createTable(row);

		/* Check that row can be inserted successfully*/
		database.insert(row);
		Assert.assertThat(row.pk_uid.getValue(), is(1L));

		TestDefaultInsertValue gotRow = database.getDBTable(row).getRowsByPrimaryKey(row.pk_uid.getValue()).get(0);
//		System.out.println(""+gotRow);

		Assert.assertThat(gotRow.pk_uid.getValue(), is(1L));
		Assert.assertThat(gotRow.name.getValue(), is("def"));
		Assert.assertThat(gotRow.defaultExpression.getValue(), is("def"));
		Assert.assertThat(gotRow.creationDate.getValue(), greaterThanOrEqualTo(startTime));
		Assert.assertThat(gotRow.creationDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
		Assert.assertThat(gotRow.updateDate.getValue(), nullValue());
		Assert.assertThat(gotRow.creationOrUpdateDate.getValue(), greaterThanOrEqualTo(startTime));
		Assert.assertThat(gotRow.creationOrUpdateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));

		/* Check that default insert values can be overridden */
		TestDefaultInsertValue row2 = new TestDefaultInsertValue();
		row2.name.setValue("notdefault");
		row2.defaultExpression.setValue("notdefaulteither");
		Assert.assertThat(row2.creationDate.hasDefaultInsertValue(), is(true));
		database.insert(row2);
		Assert.assertThat(row2.pk_uid.getValue(), is(2L));

		/* Retreive the default values and check they're correct */
		final Long pkValue = row2.pk_uid.getValue();
		gotRow = database.getDBTable(row2).getRowsByPrimaryKey(pkValue).get(0);
		Assert.assertThat(gotRow.pk_uid.getValue(), is(2L));
		Assert.assertThat(gotRow.name.getValue(), is("notdefault"));
		Assert.assertThat(gotRow.defaultExpression.getValue(), is("notdefaulteither"));
		Assert.assertThat(gotRow.creationDate.getValue(), greaterThanOrEqualTo(startTime));
		Assert.assertThat(gotRow.creationDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
		Assert.assertThat(gotRow.updateDate.getValue(), nullValue());
		Assert.assertThat(gotRow.creationOrUpdateDate.getValue(), greaterThanOrEqualTo(startTime));
		Assert.assertThat(gotRow.creationOrUpdateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));

		Thread.sleep(1000);

		gotRow.name.setValue("blarg");
		database.update(gotRow);
		gotRow = database.getDBTable(row2).getRowsByPrimaryKey(pkValue).get(0);
		Assert.assertThat(gotRow.pk_uid.getValue(), is(2L));
		Assert.assertThat(gotRow.name.getValue(), is("blarg"));
		Assert.assertThat(gotRow.defaultExpression.getValue(), is("notdefaulteither"));
		Assert.assertThat(gotRow.creationDate.getValue(), greaterThan(startTime));
		Assert.assertThat(gotRow.creationDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
		Assert.assertThat(gotRow.updateDate.getValue(), greaterThanOrEqualTo(gotRow.creationDate.getValue()));
		Assert.assertThat(gotRow.updateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
		Assert.assertThat(gotRow.creationOrUpdateDate.getValue(), greaterThanOrEqualTo(gotRow.creationDate.getValue()));
		Assert.assertThat(gotRow.creationOrUpdateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
		Date formerUpdateDate = gotRow.updateDate.getValue();

		Thread.sleep(1000);

		gotRow.name.setValue("blarg");
		gotRow.creationDate.setValue(april2nd2011);
		database.update(gotRow);
		gotRow = database.getDBTable(row2).getRowsByPrimaryKey(pkValue).get(0);
		Assert.assertThat(gotRow.pk_uid.getValue(), is(2L));
		Assert.assertThat(gotRow.name.getValue(), is("blarg"));
		Assert.assertThat(gotRow.defaultExpression.getValue(), is("notdefaulteither"));
		Assert.assertThat(gotRow.creationDate.getValue(), is(april2nd2011));
		Assert.assertThat(gotRow.updateDate.getValue(), greaterThanOrEqualTo(formerUpdateDate));
		Assert.assertThat(gotRow.updateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
		Assert.assertThat(gotRow.creationOrUpdateDate.getValue(), greaterThanOrEqualTo(formerUpdateDate));
		Assert.assertThat(gotRow.creationOrUpdateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(row);
	}

	public static class TestDefaultValueRetrieval extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBPrimaryKey
		@DBColumn
		@DBAutoIncrement
		public DBInteger pk_uid = new DBInteger();

		@DBColumn
		public DBString name = new DBString();

	}

	public static class TestDefaultInsertValue extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBPrimaryKey
		@DBColumn
		@DBAutoIncrement
		public DBInteger pk_uid = new DBInteger();

		@DBColumn
		public DBString name = new DBString().setDefaultInsertValue("def");

		@DBColumn
		public DBString defaultExpression = new DBString()
				.setDefaultInsertValue(StringExpression.value("default").substring(0, 3));

		@DBColumn
		public DBDate javaDate = new DBDate()
				.setDefaultInsertValue(new Date());

		@DBColumn
		public DBDate creationDate = new DBDate()
				.setDefaultInsertValue(DateExpression.currentDate());

		@DBColumn
		public DBDate updateDate = new DBDate()
				.setDefaultUpdateValue(DateExpression.currentDate());

		@DBColumn
		public DBDate creationOrUpdateDate = new DBDate()
				.setDefaultInsertValue(DateExpression.currentDate())
				.setDefaultUpdateValue(DateExpression.currentDate());

		@DBColumn
		public DBDate currentDate = new DBDate(DateExpression.currentDate());

	}

	@Test
	public void testSaveWithDefaultWithLocalDateTimeValue() throws Exception {
		GregorianCalendar cal = new GregorianCalendar();
		cal.add(GregorianCalendar.MINUTE, -1);
		LocalDateTime startTime = cal.toZonedDateTime().toLocalDateTime();

		TestDefaultInsertWithLocalDateTimeValue row = new TestDefaultInsertWithLocalDateTimeValue();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(row);
		database.createTable(row);

		/* Check that row can be inserted successfully*/
		database.insert(row);
		Assert.assertThat(row.pk_uid.getValue(), is(1L));

		TestDefaultInsertWithLocalDateTimeValue gotRow = database.getDBTable(row).getRowsByPrimaryKey(row.pk_uid.getValue()).get(0);
//		System.out.println(""+gotRow);

		Assert.assertThat(gotRow.pk_uid.getValue(), is(1L));
		Assert.assertThat(gotRow.name.getValue(), is("def"));
		Assert.assertThat(gotRow.defaultExpression.getValue(), is("def"));
		Assert.assertThat(gotRow.creationDate.getValue(), greaterThanOrEqualTo(startTime));
		Assert.assertThat(gotRow.creationDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
		Assert.assertThat(gotRow.updateDate.getValue(), nullValue());
		Assert.assertThat(gotRow.creationOrUpdateDate.getValue(), greaterThanOrEqualTo(startTime));
		Assert.assertThat(gotRow.creationOrUpdateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));

		/* Check that default insert values can be overridden */
		TestDefaultInsertWithLocalDateTimeValue row2 = new TestDefaultInsertWithLocalDateTimeValue();
		row2.name.setValue("notdefault");
		row2.defaultExpression.setValue("notdefaulteither");
		Assert.assertThat(row2.creationDate.hasDefaultInsertValue(), is(true));
		database.insert(row2);
		Assert.assertThat(row2.pk_uid.getValue(), is(2L));

		/* Retreive the default values and check they're correct */
		final Long pkValue = row2.pk_uid.getValue();
		gotRow = database.getDBTable(row2).getRowsByPrimaryKey(pkValue).get(0);
		Assert.assertThat(gotRow.pk_uid.getValue(), is(2L));
		Assert.assertThat(gotRow.name.getValue(), is("notdefault"));
		Assert.assertThat(gotRow.defaultExpression.getValue(), is("notdefaulteither"));
		Assert.assertThat(gotRow.creationDate.getValue(), greaterThanOrEqualTo(startTime));
		Assert.assertThat(gotRow.creationDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
		Assert.assertThat(gotRow.updateDate.getValue(), nullValue());
		Assert.assertThat(gotRow.creationOrUpdateDate.getValue(), greaterThanOrEqualTo(startTime));
		Assert.assertThat(gotRow.creationOrUpdateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));

		Thread.sleep(1000);

		gotRow.name.setValue("blarg");
		database.update(gotRow);
		gotRow = database.getDBTable(row2).getRowsByPrimaryKey(pkValue).get(0);
		Assert.assertThat(gotRow.pk_uid.getValue(), is(2L));
		Assert.assertThat(gotRow.name.getValue(), is("blarg"));
		Assert.assertThat(gotRow.defaultExpression.getValue(), is("notdefaulteither"));
		Assert.assertThat(gotRow.creationDate.getValue(), greaterThan(startTime));
		Assert.assertThat(gotRow.creationDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
		Assert.assertThat(gotRow.updateDate.getValue(), greaterThanOrEqualTo(gotRow.creationDate.getValue()));
		Assert.assertThat(gotRow.updateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
		Assert.assertThat(gotRow.creationOrUpdateDate.getValue(), greaterThanOrEqualTo(gotRow.creationDate.getValue()));
		Assert.assertThat(gotRow.creationOrUpdateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
		LocalDateTime formerUpdateDate = gotRow.updateDate.getValue();

		Thread.sleep(1000);

		gotRow.name.setValue("blarg");
		gotRow.creationDate.setValue(april2nd2011);
		database.update(gotRow);
		gotRow = database.getDBTable(row2).getRowsByPrimaryKey(pkValue).get(0);
		Assert.assertThat(gotRow.pk_uid.getValue(), is(2L));
		Assert.assertThat(gotRow.name.getValue(), is("blarg"));
		Assert.assertThat(gotRow.defaultExpression.getValue(), is("notdefaulteither"));
		final LocalDateTime april2nd2011Instant = LocalDateTime.ofInstant(april2nd2011.toInstant(), ZoneId.systemDefault());
		Assert.assertThat(gotRow.creationDate.getValue(), is(april2nd2011Instant));
		Assert.assertThat(gotRow.updateDate.getValue(), greaterThanOrEqualTo(formerUpdateDate));
		Assert.assertThat(gotRow.updateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
		Assert.assertThat(gotRow.creationOrUpdateDate.getValue(), greaterThanOrEqualTo(formerUpdateDate));
		Assert.assertThat(gotRow.creationOrUpdateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(row);
	}

	public static class TestDefaultInsertWithLocalDateTimeValue extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBPrimaryKey
		@DBColumn
		@DBAutoIncrement
		public DBInteger pk_uid = new DBInteger();

		@DBColumn
		public DBString name = new DBString().setDefaultInsertValue("def");

		@DBColumn
		public DBString defaultExpression = new DBString()
				.setDefaultInsertValue(StringExpression.value("default").substring(0, 3));

		@DBColumn
		public DBLocalDateTime javaDate = new DBLocalDateTime()
				.setDefaultInsertValue(LocalDateTimeColumn.now());

		@DBColumn
		public DBLocalDateTime creationDate = new DBLocalDateTime()
				.setDefaultInsertValue(LocalDateTimeExpression.currentLocalDateTime());

		@DBColumn
		public DBLocalDateTime updateDate = new DBLocalDateTime()
				.setDefaultUpdateValue(LocalDateTimeExpression.currentLocalDateTime());

		@DBColumn
		public DBLocalDateTime creationOrUpdateDate = new DBLocalDateTime()
				.setDefaultInsertValue(LocalDateTimeExpression.currentLocalDateTime())
				.setDefaultUpdateValue(LocalDateTimeExpression.currentLocalDateTime());

		@DBColumn
		public DBLocalDateTime currentDate = new DBLocalDateTime(LocalDateTimeExpression.currentLocalDateTime());

	}

	@Test
	public void testSaveWithDefaultWithInstantValue() throws Exception {
		GregorianCalendar cal = new GregorianCalendar();
		cal.add(GregorianCalendar.MINUTE, -1);
		Instant startTime = cal.toInstant();

		TestDefaultInsertWithInstantValue row = new TestDefaultInsertWithInstantValue();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(row);
		database.createTable(row);

		/* Check that row can be inserted successfully*/
		database.insert(row);
		Assert.assertThat(row.pk_uid.getValue(), is(1L));

		TestDefaultInsertWithInstantValue gotRow = database.getDBTable(row).getRowsByPrimaryKey(row.pk_uid.getValue()).get(0);
//		System.out.println(""+gotRow);

		Assert.assertThat(gotRow.pk_uid.getValue(), is(1L));
		Assert.assertThat(gotRow.name.getValue(), is("def"));
		Assert.assertThat(gotRow.defaultExpression.getValue(), is("def"));
		Assert.assertThat(gotRow.creationDate.getValue(), greaterThanOrEqualTo(startTime));
		Assert.assertThat(gotRow.creationDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
		Assert.assertThat(gotRow.updateDate.getValue(), nullValue());
		Assert.assertThat(gotRow.creationOrUpdateDate.getValue(), greaterThanOrEqualTo(startTime));
		Assert.assertThat(gotRow.creationOrUpdateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));

		/* Check that default insert values can be overridden */
		TestDefaultInsertWithInstantValue row2 = new TestDefaultInsertWithInstantValue();
		row2.name.setValue("notdefault");
		row2.defaultExpression.setValue("notdefaulteither");
		Assert.assertThat(row2.creationDate.hasDefaultInsertValue(), is(true));
		database.insert(row2);
		Assert.assertThat(row2.pk_uid.getValue(), is(2L));

		/* Retreive the default values and check they're correct */
		final Long pkValue = row2.pk_uid.getValue();
		gotRow = database.getDBTable(row2).getRowsByPrimaryKey(pkValue).get(0);
		Assert.assertThat(gotRow.pk_uid.getValue(), is(2L));
		Assert.assertThat(gotRow.name.getValue(), is("notdefault"));
		Assert.assertThat(gotRow.defaultExpression.getValue(), is("notdefaulteither"));
		Assert.assertThat(gotRow.creationDate.getValue(), greaterThanOrEqualTo(startTime));
		Assert.assertThat(gotRow.creationDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
		Assert.assertThat(gotRow.updateDate.getValue(), nullValue());
		Assert.assertThat(gotRow.creationOrUpdateDate.getValue(), greaterThanOrEqualTo(startTime));
		Assert.assertThat(gotRow.creationOrUpdateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));

		Thread.sleep(1000);

		gotRow.name.setValue("blarg");
		database.update(gotRow);
		gotRow = database.getDBTable(row2).getRowsByPrimaryKey(pkValue).get(0);
		Assert.assertThat(gotRow.pk_uid.getValue(), is(2L));
		Assert.assertThat(gotRow.name.getValue(), is("blarg"));
		Assert.assertThat(gotRow.defaultExpression.getValue(), is("notdefaulteither"));
		Assert.assertThat(gotRow.creationDate.getValue(), greaterThan(startTime));
		Assert.assertThat(gotRow.creationDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
		Assert.assertThat(gotRow.updateDate.getValue(), greaterThanOrEqualTo(gotRow.creationDate.getValue()));
		Assert.assertThat(gotRow.updateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
		Assert.assertThat(gotRow.creationOrUpdateDate.getValue(), greaterThanOrEqualTo(gotRow.creationDate.getValue()));
		Assert.assertThat(gotRow.creationOrUpdateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
		Instant formerUpdateDate = gotRow.updateDate.getValue();

		Thread.sleep(1000);

		gotRow.name.setValue("blarg");
		final Instant april2nd2011Instant = april2nd2011.toInstant();
		gotRow.creationDate.setValue(april2nd2011Instant);
		database.update(gotRow);
		gotRow = database.getDBTable(row2).getRowsByPrimaryKey(pkValue).get(0);
		Assert.assertThat(gotRow.pk_uid.getValue(), is(2L));
		Assert.assertThat(gotRow.name.getValue(), is("blarg"));
		Assert.assertThat(gotRow.defaultExpression.getValue(), is("notdefaulteither"));
		Assert.assertThat(gotRow.creationDate.getValue(), is(april2nd2011Instant));
		Assert.assertThat(gotRow.updateDate.getValue(), greaterThanOrEqualTo(formerUpdateDate));
		Assert.assertThat(gotRow.updateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
		Assert.assertThat(gotRow.creationOrUpdateDate.getValue(), greaterThanOrEqualTo(formerUpdateDate));
		Assert.assertThat(gotRow.creationOrUpdateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(row);
	}

	public static class TestDefaultInsertWithInstantValue extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBPrimaryKey
		@DBColumn
		@DBAutoIncrement
		public DBInteger pk_uid = new DBInteger();

		@DBColumn
		public DBString name = new DBString().setDefaultInsertValue("def");

		@DBColumn
		public DBString defaultExpression = new DBString()
				.setDefaultInsertValue(StringExpression.value("default").substring(0, 3));

		@DBColumn
		public DBInstant javaDate = new DBInstant()
				.setDefaultInsertValue(InstantColumn.now());

		@DBColumn
		public DBInstant creationDate = new DBInstant()
				.setDefaultInsertValue(InstantExpression.currentInstant());

		@DBColumn
		public DBInstant updateDate = new DBInstant()
				.setDefaultUpdateValue(InstantExpression.currentInstant());

		@DBColumn
		public DBInstant creationOrUpdateDate = new DBInstant()
				.setDefaultInsertValue(InstantExpression.currentInstant())
				.setDefaultUpdateValue(InstantExpression.currentInstant());

		@DBColumn
		public DBInstant currentDate = new DBInstant(InstantExpression.currentInstant());

	}

	public static class TestDefaultValueIncorrectDatatype extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBPrimaryKey
		@DBColumn(value = "pkuid")
		@DBAutoIncrement
		public DBString pk_uid = new DBString();

		@DBColumn
		public DBString name = new DBString();

	}

	public static class TestValueRetrievalWith2PKs extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBPrimaryKey
		@DBColumn
		public DBInteger pk_uid = new DBInteger();

		@DBPrimaryKey
		@DBColumn
		public DBInteger pk_other_uid = new DBInteger();

		@DBColumn
		public DBString name = new DBString();

	}

	public static class TestInsertDoesNotUpdateExpressionColumns extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBPrimaryKey
		@DBColumn
		@DBAutoIncrement
		public DBInteger pk_uid = new DBInteger();

		@DBColumn
		public DBString name = new DBString();

		@DBColumn
		public DBBoolean hasValue = new DBBoolean(this.column(this.name).length().isGreaterThan(0));

	}
}
