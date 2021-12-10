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
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.SlowSynchingDatabase;
import nz.co.gregs.dbvolution.annotations.*;
import nz.co.gregs.dbvolution.columns.InstantColumn;
import nz.co.gregs.dbvolution.columns.LocalDateTimeColumn;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.exceptions.AutoIncrementFieldClassAndDatatypeMismatch;
import nz.co.gregs.dbvolution.expressions.DateExpression;
import nz.co.gregs.dbvolution.expressions.InstantExpression;
import nz.co.gregs.dbvolution.expressions.LocalDateTimeExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import nz.co.gregs.dbvolution.utility.Brake;
import org.hamcrest.Matcher;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Test;

/**
 *
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
		assertThat(result.size(), is(1));
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
			assertThat(row.pk_uid.getValue(), is(1L));
			database.insert(row2);
			assertThat(row2.pk_uid.getValue(), is(2L));
			final Long pkValue = row2.pk_uid.getValue();
			TestDefaultValueRetrieval gotRow2 = database.getDBTable(row2).getRowsByPrimaryKey(pkValue).get(0);
			assertThat(gotRow2.pk_uid.getValue(), is(2L));

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
			assertThat(row.pk_uid.getValue(), is(1L));
			database.insert(row2);
			assertThat(row2.pk_uid.getValue(), is(2L));
			final Long pkValue = row2.pk_uid.getValue();
			TestDefaultValueRetrieval gotRow2 = database.getDBTable(row2).getRowsByPrimaryKey(pkValue).get(0);
			assertThat(gotRow2.pk_uid.getValue(), is(2L));

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
			assertThat(row.pk_uid.getValue(), is(1L));
			database.insert(row2);
			assertThat(row2.pk_uid.getValue(), is(2L));
			final Long pkValue = row2.pk_uid.getValue();
			TestInsertDoesNotUpdateExpressionColumns gotRow2 = database.getDBTable(row2).getRowsByPrimaryKey(pkValue).get(0);
			assertThat(gotRow2.pk_uid.getValue(), is(2L));

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
		try {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(row);
			database.createTable(row);

			/* Check that row can be inserted successfully*/
			database.insert(row);
			assertThat(row.pk_uid.getValue(), is(1L));

			TestDefaultInsertValue gotRow = database.getDBTable(row).getRowsByPrimaryKey(row.pk_uid.getValue()).get(0);
			System.out.println("gotRow: " + gotRow.toString());

			assertThat(gotRow.pk_uid.getValue(), is(1L));
			assertThat(gotRow.name.getValue(), is("def"));
			assertThat(gotRow.defaultExpression.getValue(), is("def"));
			assertThat(gotRow.creationDate.getValue(), greaterThanOrEqualTo(startTime));
			assertThat(gotRow.creationDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
			assertThat(gotRow.updateDate.getValue(), nullValue());
			assertThat(gotRow.creationOrUpdateDate.getValue(), greaterThanOrEqualTo(startTime));
			assertThat(gotRow.creationOrUpdateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));

			/* Check that default insert values can be overridden */
			TestDefaultInsertValue row2 = new TestDefaultInsertValue();
			row2.name.setValue("notdefault");
			row2.defaultExpression.setValue("notdefaulteither");
			assertThat(row2.creationDate.hasDefaultInsertValue(), is(true));
			database.insert(row2);
			assertThat(row2.pk_uid.getValue(), is(2L));

			/* Retreive the default values and check they're correct */
			final Long pkValue = row2.pk_uid.getValue();
			gotRow = database.getDBTable(row2).getRowsByPrimaryKey(pkValue).get(0);
			System.out.println("gotRow: " + gotRow);

			assertThat(gotRow.pk_uid.getValue(), is(2L));
			assertThat(gotRow.name.getValue(), is("notdefault"));
			assertThat(gotRow.defaultExpression.getValue(), is("notdefaulteither"));
			assertThat(gotRow.creationDate.getValue(), greaterThanOrEqualTo(startTime));
			assertThat(gotRow.creationDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
			assertThat(gotRow.updateDate.getValue(), nullValue());
			assertThat(gotRow.creationOrUpdateDate.getValue(), greaterThanOrEqualTo(startTime));
			assertThat(gotRow.creationOrUpdateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));

			gotRow.name.setValue("blarg");
			database.update(gotRow);
			gotRow = database.getDBTable(row2).getRowsByPrimaryKey(pkValue).get(0);
			System.out.println("gotRow: " + gotRow);

			assertThat(gotRow.pk_uid.getValue(), is(2L));
			assertThat(gotRow.name.getValue(), is("blarg"));
			assertThat(gotRow.defaultExpression.getValue(), is("notdefaulteither"));
			assertThat(gotRow.creationDate.getValue(), greaterThan(startTime));
			assertThat(gotRow.creationDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
			assertThat(gotRow.updateDate.getValue(), greaterThanOrEqualTo(gotRow.creationDate.getValue()));
			assertThat(gotRow.updateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
			assertThat(gotRow.creationOrUpdateDate.getValue(), greaterThanOrEqualTo(gotRow.creationDate.getValue()));
			assertThat(gotRow.creationOrUpdateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
			Date formerUpdateDate = gotRow.updateDate.getValue();

			gotRow.name.setValue("blarg");
			gotRow.creationDate.setValue(april2nd2011);
			database.update(gotRow);
			gotRow = database.getDBTable(row2).getRowsByPrimaryKey(pkValue).get(0);
			System.out.println("gotRow: " + gotRow);

			assertThat(gotRow.pk_uid.getValue(), is(2L));
			assertThat(gotRow.name.getValue(), is("blarg"));
			assertThat(gotRow.defaultExpression.getValue(), is("notdefaulteither"));
			assertThat(gotRow.creationDate.getValue(), is(april2nd2011));
			assertThat(gotRow.updateDate.getValue(), greaterThanOrEqualTo(formerUpdateDate));
			assertThat(gotRow.updateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
			assertThat(gotRow.creationOrUpdateDate.getValue(), greaterThanOrEqualTo(formerUpdateDate));
			assertThat(gotRow.creationOrUpdateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
		} finally {
			database.setPrintSQLBeforeExecuting(true);
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(row);
		}
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
		TestDefaultInsertWithLocalDateTimeValue row = new TestDefaultInsertWithLocalDateTimeValue();
		try {
			if(database instanceof DBDatabaseCluster){
				// if the cluster members are on different servers, like we'd expect,
				// then we can expect their clocks to be slightly different.
				// That mean an update performed on a different member to the insert may 
				// produce an update date that is earlier than the creation date.
				// This is not a big deal operationally, but it will make these tests fail
				// So we set a preferred member to get a consistent system clock.
				DBDatabaseCluster cluster = (DBDatabaseCluster)database;
				final DBDatabase readyDatabase = cluster.getReadyDatabase();
				cluster.getDetails().setPreferredDatabase(readyDatabase);
			}
			database.setPrintSQLBeforeExecuting(true);
			GregorianCalendar cal = new GregorianCalendar();
			cal.add(GregorianCalendar.MINUTE, -1);
			LocalDateTime startTime = cal.toZonedDateTime().toLocalDateTime();

			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(row);
			database.createTable(row);

			/* Check that row can be inserted successfully*/
			database.insert(row);
			System.out.println("inserted row: " + row);

			assertThat(row.pk_uid.getValue(), is(1L));
			assertThat(row.name.getValue(), is("def"));
			assertThat(row.defaultExpression.getValue(), is("def"));
			assertThat(row.creationDate.getValue(), greaterThanOrEqualTo(startTime));
			assertThat(row.updateDate.getValue(), nullValue());
			assertThat(row.creationOrUpdateDate.getValue(), greaterThanOrEqualTo(startTime));

			LocalDateTime soon = row.currentDatePlus.getValue();
			assertThat(row.creationDate.getValue(), lessThanOrEqualTo(soon));
			assertThat(row.updateDate.getValue(), nullValue());
			assertThat(row.creationOrUpdateDate.getValue(), lessThanOrEqualTo(soon));
			assertThat(row.currentDate.getValue(), lessThanOrEqualTo(soon));

			TestDefaultInsertWithLocalDateTimeValue gotRow = database.getDBTable(row).setQueryLabel("AFTER INSERT").getRowsByPrimaryKey(row.pk_uid.getValue()).get(0);
			System.out.println("gotRow: " + gotRow);

			soon = gotRow.currentDatePlus.getValue();
			assertThat(gotRow.pk_uid.getValue(), is(1L));
			assertThat(gotRow.name.getValue(), is("def"));
			assertThat(gotRow.defaultExpression.getValue(), is("def"));
			assertThat(gotRow.creationDate.getValue(), greaterThanOrEqualTo(startTime));
			assertThat(gotRow.updateDate.getValue(), nullValue());
			assertThat(gotRow.creationOrUpdateDate.getValue(), greaterThanOrEqualTo(startTime));
			assertThat(gotRow.creationDate.getValue(), lessThanOrEqualTo(soon));
			assertThat(gotRow.updateDate.getValue(), nullValue());
			assertThat(gotRow.creationOrUpdateDate.getValue(), lessThanOrEqualTo(soon));
			assertThat(gotRow.currentDate.getValue(), lessThanOrEqualTo(soon));

			if (database instanceof DBDatabaseCluster) {
				DBDatabaseCluster cluster = (DBDatabaseCluster) database;
				cluster.waitUntilDatabaseIsSynchronised(database, 10000);
				DBDatabase[] databases = cluster.getDatabases();
				for (DBDatabase db : databases) {
					TestDefaultInsertWithLocalDateTimeValue rowFromMember = db.getDBTable(row).setQueryLabel("CHECK MEMBERS").getRowsByPrimaryKey(row.pk_uid.getValue()).get(0);
					System.out.println("rowFromMember: " + rowFromMember);
					assertThat(rowFromMember.pk_uid.getValue(), is(gotRow.pk_uid.getValue()));
					assertThat(rowFromMember.name.getValue(), is(gotRow.name.getValue()));
					assertThat(rowFromMember.defaultExpression.getValue(), is(gotRow.defaultExpression.getValue()));
					assertThat(rowFromMember.creationDate.getValue(), isApproximately(gotRow.creationDate.getValue()));
					assertThat(rowFromMember.updateDate.getValue(), isApproximately(gotRow.updateDate.getValue()));
					assertThat(rowFromMember.creationOrUpdateDate.getValue(), isApproximately(gotRow.creationOrUpdateDate.getValue()));
				}
			}

			/* Check that default insert values can be overridden */
			TestDefaultInsertWithLocalDateTimeValue row2 = new TestDefaultInsertWithLocalDateTimeValue();
			row2.name.setValue("notdefault");
			row2.defaultExpression.setValue("notdefaulteither");
			assertThat(row2.creationDate.hasDefaultInsertValue(), is(true));
			database.insert(row2);
			assertThat(row2.pk_uid.getValue(), is(2L));

			/* Retreive the default values and check they're correct */
			final Long pkValue = row2.pk_uid.getValue();
			gotRow = database.getDBTable(row2).setQueryLabel("CHECK DEFAULT VALUES 1").getRowsByPrimaryKey(pkValue).get(0);
			System.out.println("gotRow: " + gotRow);

			soon = gotRow.currentDatePlus.getValue();
			assertThat(gotRow.pk_uid.getValue(), is(2L));
			assertThat(gotRow.name.getValue(), is("notdefault"));
			assertThat(gotRow.defaultExpression.getValue(), is("notdefaulteither"));
			assertThat(gotRow.creationDate.getValue(), greaterThanOrEqualTo(startTime));
			assertThat(gotRow.creationDate.getValue(), lessThanOrEqualTo(soon));
			assertThat(gotRow.updateDate.getValue(), nullValue());
			assertThat(gotRow.creationOrUpdateDate.getValue(), greaterThanOrEqualTo(startTime));
			assertThat(gotRow.creationOrUpdateDate.getValue(), lessThanOrEqualTo(soon));

			gotRow.name.setValue("blarg");
			database.update(gotRow);
			gotRow = database.getDBTable(row2).setQueryLabel("CHECK DEFAULT VALUES 2").getRowsByPrimaryKey(pkValue).get(0);
			System.out.println("gotRow: " + gotRow);

			soon = gotRow.currentDatePlus.getValue();
			assertThat(gotRow.pk_uid.getValue(), is(2L));
			assertThat(gotRow.name.getValue(), is("blarg"));
			assertThat(gotRow.defaultExpression.getValue(), is("notdefaulteither"));
			assertThat(gotRow.creationDate.getValue(), greaterThan(startTime));
			assertThat(gotRow.creationDate.getValue(), lessThanOrEqualTo(soon));
			assertThat(gotRow.updateDate.getValue(), greaterThanOrEqualTo(gotRow.creationDate.getValue()));
			assertThat(gotRow.updateDate.getValue(), lessThanOrEqualTo(soon));
			assertThat(gotRow.creationOrUpdateDate.getValue(), greaterThanOrEqualTo(gotRow.creationDate.getValue()));
			assertThat(gotRow.creationOrUpdateDate.getValue(), lessThanOrEqualTo(soon));
			LocalDateTime formerUpdateDate = gotRow.updateDate.getValue();

			gotRow.name.setValue("blargest");
			gotRow.creationDate.setValue(april2nd2011);
			database.update(gotRow);
			gotRow = database.getDBTable(row2).setQueryLabel("CHECK DEFAULT VALUES 3").getRowsByPrimaryKey(pkValue).get(0);
			System.out.println("gotRow: " + gotRow);

			soon = gotRow.currentDatePlus.getValue();
			assertThat(gotRow.pk_uid.getValue(), is(2L));
			assertThat(gotRow.name.getValue(), is("blargest"));
			assertThat(gotRow.defaultExpression.getValue(), is("notdefaulteither"));
			final LocalDateTime april2nd2011Instant = LocalDateTime.ofInstant(april2nd2011.toInstant(), ZoneId.systemDefault());
			assertThat(gotRow.creationDate.getValue(), is(april2nd2011Instant));
			assertThat(gotRow.updateDate.getValue(), greaterThanOrEqualTo(formerUpdateDate));
			// the below errors occasionally when using a cluster
			assertThat(gotRow.updateDate.getValue(), lessThanOrEqualTo(soon));
			assertThat(gotRow.creationOrUpdateDate.getValue(), greaterThanOrEqualTo(formerUpdateDate));
			assertThat(gotRow.creationOrUpdateDate.getValue(), lessThanOrEqualTo(soon));

		} finally {
			database.setPrintSQLBeforeExecuting(true);
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(row);
		}
	}

	public static Matcher<LocalDateTime> isApproximately(LocalDateTime then) {
		if (then == null) {
			return is(then);
		} else {
			return isOneOf(
					then,
					then.truncatedTo(ChronoUnit.MILLIS),
					then.truncatedTo(ChronoUnit.MICROS)
			);
		}
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

		@DBColumn
		public DBLocalDateTime currentDatePlus = new DBLocalDateTime(LocalDateTimeExpression.currentLocalDateTime().addSeconds(10));
	}

//	public static class TestDefaultInsertWithLocalDateTimeValue extends DBRow {
//
//		private static final long serialVersionUID = 1L;
//
//		@DBPrimaryKey
//		@DBColumn
//		@DBAutoIncrement
//		public DBInteger pk_uid = new DBInteger();
//
//		@DBColumn
//		public DBString name = new DBString().setDefaultInsertValue("def");
//
//		@DBColumn
//		public DBString defaultExpression = new DBString()
//				.setDefaultInsertValue(StringExpression.value("default").substring(0, 3));
//
//		@DBColumn
//		public DBLocalDateTime javaDate = new DBLocalDateTime()
//				.setDefaultInsertValue(LocalDateTimeColumn.now());
//
//		@DBColumn
//		public DBLocalDateTime creationDate = new DBLocalDateTime()
//				.setDefaultInsertValue(LocalDateTimeExpression.currentLocalDateTime());
//
//		@DBColumn
//		public DBLocalDateTime updateDate = new DBLocalDateTime()
//				.setDefaultUpdateValue(LocalDateTimeExpression.currentLocalDateTime());
//
//		@DBColumn
//		public DBLocalDateTime creationOrUpdateDate = new DBLocalDateTime()
//				.setDefaultInsertValue(LocalDateTimeExpression.currentLocalDateTime())
//				.setDefaultUpdateValue(LocalDateTimeExpression.currentLocalDateTime());
//
//		@DBColumn
//		public DBLocalDateTime currentDate = new DBLocalDateTime(LocalDateTimeExpression.currentLocalDateTime());
//
//	}

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
		assertThat(row.pk_uid.getValue(), is(1L));

		TestDefaultInsertWithInstantValue gotRow = database.getDBTable(row).getRowsByPrimaryKey(row.pk_uid.getValue()).get(0);

		assertThat(gotRow.pk_uid.getValue(), is(1L));
		assertThat(gotRow.name.getValue(), is("def"));
		assertThat(gotRow.defaultExpression.getValue(), is("def"));
		assertThat(gotRow.creationDate.getValue(), greaterThanOrEqualTo(startTime));
		assertThat(gotRow.creationDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
		assertThat(gotRow.updateDate.getValue(), nullValue());
		assertThat(gotRow.creationOrUpdateDate.getValue(), greaterThanOrEqualTo(startTime));
		assertThat(gotRow.creationOrUpdateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));

		/* Check that default insert values can be overridden */
		TestDefaultInsertWithInstantValue row2 = new TestDefaultInsertWithInstantValue();
		row2.name.setValue("notdefault");
		row2.defaultExpression.setValue("notdefaulteither");
		assertThat(row2.creationDate.hasDefaultInsertValue(), is(true));
		database.insert(row2);
		assertThat(row2.pk_uid.getValue(), is(2L));

		/* Retreive the default values and check they're correct */
		final Long pkValue = row2.pk_uid.getValue();
		gotRow = database.getDBTable(row2).getRowsByPrimaryKey(pkValue).get(0);
		assertThat(gotRow.pk_uid.getValue(), is(2L));
		assertThat(gotRow.name.getValue(), is("notdefault"));
		assertThat(gotRow.defaultExpression.getValue(), is("notdefaulteither"));
		assertThat(gotRow.creationDate.getValue(), greaterThanOrEqualTo(startTime));
		assertThat(gotRow.creationDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
		assertThat(gotRow.updateDate.getValue(), nullValue());
		assertThat(gotRow.creationOrUpdateDate.getValue(), greaterThanOrEqualTo(startTime));
		assertThat(gotRow.creationOrUpdateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));

		gotRow.name.setValue("blarg");
		database.update(gotRow);
		gotRow = database.getDBTable(row2).getRowsByPrimaryKey(pkValue).get(0);
		assertThat(gotRow.pk_uid.getValue(), is(2L));
		assertThat(gotRow.name.getValue(), is("blarg"));
		assertThat(gotRow.defaultExpression.getValue(), is("notdefaulteither"));
		assertThat(gotRow.creationDate.getValue(), greaterThan(startTime));
		assertThat(gotRow.creationDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
		assertThat(gotRow.updateDate.getValue(), greaterThanOrEqualTo(gotRow.creationDate.getValue()));
		assertThat(gotRow.updateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
		assertThat(gotRow.creationOrUpdateDate.getValue(), greaterThanOrEqualTo(gotRow.creationDate.getValue()));
		assertThat(gotRow.creationOrUpdateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
		Instant formerUpdateDate = gotRow.updateDate.getValue();

		gotRow.name.setValue("blarg");
		final Instant april2nd2011Instant = april2nd2011.toInstant();
		gotRow.creationDate.setValue(april2nd2011Instant);
		database.update(gotRow);
		gotRow = database.getDBTable(row2).getRowsByPrimaryKey(pkValue).get(0);
		assertThat(gotRow.pk_uid.getValue(), is(2L));
		assertThat(gotRow.name.getValue(), is("blarg"));
		assertThat(gotRow.defaultExpression.getValue(), is("notdefaulteither"));
		assertThat(gotRow.creationDate.getValue(), is(april2nd2011Instant));
		assertThat(gotRow.updateDate.getValue(), greaterThanOrEqualTo(formerUpdateDate));
		assertThat(gotRow.updateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));
		assertThat(gotRow.creationOrUpdateDate.getValue(), greaterThanOrEqualTo(formerUpdateDate));
		assertThat(gotRow.creationOrUpdateDate.getValue(), lessThanOrEqualTo(gotRow.currentDate.getValue()));

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(row);
	}

	@Test
	public void testDefaultValuesAreConsistentInCluster1() throws Exception {
		for (int i = 0; i < 1; i++) {
			try ( DBDatabaseCluster cluster = new DBDatabaseCluster(
					"testDefaultValuesAreConsistentInCluster",
					DBDatabaseCluster.Configuration.autoStart(),
					database)) {

				SlowSynchingDatabase slowDatabase2 = SlowSynchingDatabase.createANewRandomDatabase("testDefaultValuesAreConsistentInCluster-", "-2");
				Brake brake = slowDatabase2.getBrake();
				brake.setTimeout(10000);
				brake.release();
				cluster.addDatabaseAndWait(slowDatabase2);

				cluster.getDetails().setPreferredDatabase(slowDatabase2);

				brake.apply();
				SlowSynchingDatabase slowDatabase3 = SlowSynchingDatabase.createANewRandomDatabase("testDefaultValuesAreConsistentInCluster-", "-3");
				brake = slowDatabase3.getBrake();
				brake.setTimeout(10000);
				brake.release();
				cluster.addDatabaseAndWait(slowDatabase3);
				brake.apply();

				TestDefaultInsertWithInstantValue row = new TestDefaultInsertWithInstantValue();
				cluster.preventDroppingOfTables(false);
				cluster.dropTableNoExceptions(row);
				cluster.createTable(row);

				/* Check that row can be inserted successfully*/
				cluster.insert(row);
				assertThat(row.pk_uid.getValue(), is(1L));

				cluster.waitUntilSynchronised();

				final List<TestDefaultInsertWithInstantValue> rows1 = database.getDBTable(row).getRowsByPrimaryKey(row.pk_uid.getValue());
				final List<TestDefaultInsertWithInstantValue> rows2 = slowDatabase2.getDBTable(row).getRowsByPrimaryKey(row.pk_uid.getValue());
				final List<TestDefaultInsertWithInstantValue> rows3 = slowDatabase3.getDBTable(row).getRowsByPrimaryKey(row.pk_uid.getValue());

				TestDefaultInsertWithInstantValue gotRow1 = rows1.get(0);
				TestDefaultInsertWithInstantValue gotRow2 = rows2.get(0);
				TestDefaultInsertWithInstantValue gotRow3 = rows3.get(0);

				ChronoUnit precision = ChronoUnit.MILLIS;
				if (database.supportsNanosecondPrecision() && slowDatabase2.supportsNanosecondPrecision() && slowDatabase3.supportsNanosecondPrecision()) {
					precision = ChronoUnit.NANOS;
				} else if (database.supportsMicrosecondPrecision() && slowDatabase2.supportsMicrosecondPrecision() && slowDatabase3.supportsMicrosecondPrecision()) {
					precision = ChronoUnit.MICROS;
				}

				final Instant db1CreationValue = gotRow1.creationDate.getValue().truncatedTo(precision);
				final Instant db1UpdateValue = gotRow1.updateDate.getValue() != null ? gotRow1.updateDate.getValue().truncatedTo(precision) : null;
				final Instant db1CreationOrUpdateValue = gotRow1.creationOrUpdateDate.getValue().truncatedTo(precision);

				final Instant db2CreationValue = gotRow2.creationDate.getValue().truncatedTo(precision);
				final Instant db2UpdateValue = gotRow2.updateDate.getValue() != null ? gotRow2.updateDate.getValue().truncatedTo(precision) : null;
				final Instant db2CreationOrUpdateValue = gotRow2.creationOrUpdateDate.getValue().truncatedTo(precision);

				final Instant db3CreationValue = gotRow3.creationDate.getValue().truncatedTo(precision);
				final Instant db3UpdateValue = gotRow3.updateDate.getValue() != null ? gotRow3.updateDate.getValue().truncatedTo(precision) : null;
				final Instant db3CreationOrUpdateValue = gotRow3.creationOrUpdateDate.getValue().truncatedTo(precision);

				assertThat(db2CreationValue, is(db1CreationValue));
				assertThat(db2UpdateValue, is(db1UpdateValue));
				assertThat(db2CreationOrUpdateValue, is(db1CreationOrUpdateValue));

				assertThat(db3CreationValue, is(db1CreationValue));
				assertThat(db3UpdateValue, is(db1UpdateValue));
				assertThat(db3CreationOrUpdateValue, is(db1CreationOrUpdateValue));
			}
		}
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
