/*
 * Copyright 2023 Gregory Graham.
 *
 * Commercial licenses are available, please contact info@gregs.co.nz for details.
 * 
 * This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. 
 * To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/4.0/ 
 * or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.
 * 
 * You are free to:
 *     Share - copy and redistribute the material in any medium or format
 *     Adapt - remix, transform, and build upon the material
 * 
 *     The licensor cannot revoke these freedoms as long as you follow the license terms.               
 *     Under the following terms:
 *                 
 *         Attribution - 
 *             You must give appropriate credit, provide a link to the license, and indicate if changes were made. 
 *             You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.
 *         NonCommercial - 
 *             You may not use the material for commercial purposes.
 *         ShareAlike - 
 *             If you remix, transform, or build upon the material, 
 *             you must distribute your contributions under the same license as the original.
 *         No additional restrictions - 
 *             You may not apply legal terms or technological measures that legally restrict others from doing anything the 
 *             license permits.
 * 
 * Check the Creative Commons website for any details, legalese, and updates.
 */
package nz.co.gregs.dbvolution.generic;

import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.TestingDatabase;
import nz.co.gregs.dbvolution.annotations.*;
import nz.co.gregs.dbvolution.columns.InstantColumn;
import nz.co.gregs.dbvolution.databases.*;
import nz.co.gregs.dbvolution.datatypes.DBInstant;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBLocalDateTime;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.exceptions.AutoCommitActionDuringTransactionException;
import nz.co.gregs.dbvolution.expressions.InstantExpression;
import nz.co.gregs.dbvolution.expressions.LocalDateTimeExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.utility.Brake;
import nz.co.gregs.regexi.Match;
import nz.co.gregs.regexi.Regex;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author gregorygraham
 */
public class TempTest extends AbstractTest {

	public TempTest(Object testIterationName, DBDatabase db) throws AutoCommitActionDuringTransactionException, SQLException {
		super(testIterationName, db);

	}

	LocalDateTime march23rd2013LocalDateTime = (new GregorianCalendar(2013, 2, 23, 12, 34, 56)).toZonedDateTime().toLocalDateTime();
	LocalDateTime april2nd2011LocalDateTime = (new GregorianCalendar(2011, 3, 2, 1, 2, 3)).toZonedDateTime().toLocalDateTime();

	@Before
	public void setupMarqueWithLocalDateTime() throws Exception {
		DBDatabase db = database;
//		db.setPrintSQLBeforeExecuting(true);
		db.preventDroppingOfTables(false);
		db.dropTableIfExists(new LocalDateTimeTestTable());
		db.createTable(new LocalDateTimeTestTable());

		List<LocalDateTimeTestTable> toInsert = new ArrayList<>();
		toInsert.add(new LocalDateTimeTestTable(4893059, "True", 1246974, null, 3, "UV", "PEUGEOT", null, "Y", null, 4, true));
		toInsert.add(new LocalDateTimeTestTable(4893090, "False", 1246974, "", 1, "UV", "FORD", "", "Y", march23rd2013LocalDateTime, 2, false));
		toInsert.add(new LocalDateTimeTestTable(4893101, "False", 1246974, "", 2, "UV", "HOLDEN", "", "Y", march23rd2013LocalDateTime, 3, null));
		toInsert.add(new LocalDateTimeTestTable(4893112, "False", 1246974, "", 2, "UV", "MITSUBISHI", "", "Y", march23rd2013LocalDateTime, 4, null));
		toInsert.add(new LocalDateTimeTestTable(4893150, "False", 1246974, "", 3, "UV", "SUZUKI", "", "Y", march23rd2013LocalDateTime, 4, null));
		toInsert.add(new LocalDateTimeTestTable(4893263, "False", 1246974, "", 2, "UV", "HONDA", "", "Y", march23rd2013LocalDateTime, 4, null));
		toInsert.add(new LocalDateTimeTestTable(4893353, "False", 1246974, "", 4, "UV", "NISSAN", "", "Y", march23rd2013LocalDateTime, 4, null));
		toInsert.add(new LocalDateTimeTestTable(4893557, "False", 1246974, "", 2, "UV", "SUBARU", "", "Y", march23rd2013LocalDateTime, 4, null));
		toInsert.add(new LocalDateTimeTestTable(4894018, "False", 1246974, "", 2, "UV", "MAZDA", "", "Y", march23rd2013LocalDateTime, 4, null));
		toInsert.add(new LocalDateTimeTestTable(4895203, "False", 1246974, "", 2, "UV", "ROVER", "", "Y", march23rd2013LocalDateTime, 4, null));
		toInsert.add(new LocalDateTimeTestTable(4896300, "False", 1246974, null, 2, "UV", "HYUNDAI", null, "Y", march23rd2013LocalDateTime, 1, null));
		toInsert.add(new LocalDateTimeTestTable(4899527, "False", 1246974, "", 1, "UV", "JEEP", "", "Y", march23rd2013LocalDateTime, 3, null));
		toInsert.add(new LocalDateTimeTestTable(7659280, "False", 1246972, "Y", 3, "", "DAIHATSU", "", "Y", march23rd2013LocalDateTime, 4, null));
		toInsert.add(new LocalDateTimeTestTable(7681544, "False", 1246974, "", 2, "UV", "LANDROVER", "", "Y", march23rd2013LocalDateTime, 4, null));
		toInsert.add(new LocalDateTimeTestTable(7730022, "False", 1246974, "", 2, "UV", "VOLVO", "", "Y", march23rd2013LocalDateTime, 4, null));
		toInsert.add(new LocalDateTimeTestTable(8376505, "False", 1246974, "", null, "", "ISUZU", "", "Y", march23rd2013LocalDateTime, 4, null));
		toInsert.add(new LocalDateTimeTestTable(8587147, "False", 1246974, "", null, "", "DAEWOO", "", "Y", march23rd2013LocalDateTime, 4, null));
		toInsert.add(new LocalDateTimeTestTable(9971178, "False", 1246974, "", 1, "", "CHRYSLER", "", "Y", march23rd2013LocalDateTime, 4, null));
		toInsert.add(new LocalDateTimeTestTable(13224369, "False", 1246974, "", 0, "", "VW", "", "Y", april2nd2011LocalDateTime, 4, null));
		toInsert.add(new LocalDateTimeTestTable(6664478, "False", 1246974, "", 0, "", "BMW", "", "Y", april2nd2011LocalDateTime, 4, null));
		toInsert.add(new LocalDateTimeTestTable(1, "False", 1246974, "", 0, "", "TOYOTA", "", "Y", march23rd2013LocalDateTime, 1, true));
		toInsert.add(new LocalDateTimeTestTable(2, "False", 1246974, "", 0, "", "HUMMER", "", "Y", april2nd2011LocalDateTime, 3, null));

		db.insert(toInsert);
	}

	@Test
	public void testIsNotDateExpression() throws SQLException {
		for (int i = 0; i < 10; i++) {

			try {
				setupMarqueWithLocalDateTime();
			} catch (Exception ex) {
				Logger.getLogger(TempTest.class.getName()).log(Level.SEVERE, null, ex);
			}
			LocalDateTimeTestTable marq = new LocalDateTimeTestTable();
			DBQuery query = database.getDBQuery(marq);
			final LocalDateTimeExpression fiveDaysPriorToCreation = marq.column(marq.creationLocalDateTime).addDays(-5);
			query.addCondition(
					LocalDateTimeExpression.leastOf(
							marq.column(marq.creationLocalDateTime),
							fiveDaysPriorToCreation,
							LocalDateTimeExpression.value(march23rd2013LocalDateTime).addWeeks(-5),
							LocalDateTimeExpression.value(march23rd2013LocalDateTime).addDays(-2))
							.isNot(fiveDaysPriorToCreation)
			);
			List<DBQueryRow> allRows = query.getAllRows();

			assertThat(allRows.size(), is(18));
		}
	}

	@Test
	public void testIsNotDate() throws SQLException {
		System.out.println("nz.co.gregs.dbvolution.expressions.LocalDateTimeExpressionTest.testIsNotDate()");
		LocalDateTimeTestTable marq = new LocalDateTimeTestTable();
		DBQuery query = database.getDBQuery(marq);

		query.addCondition(
				marq.column(marq.creationLocalDateTime).isNot(april2nd2011LocalDateTime)
		);
		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(18));
	}

	@DBTableName("temp_test_table")
	public static class LocalDateTimeTestTable extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn("uid_marque")
		@DBPrimaryKey
		public DBInteger uidMarque = new DBInteger();

		@DBColumn
		public DBString name = new DBString();

		@DBColumn("creation_localdatetime")
		public DBLocalDateTime creationLocalDateTime = new DBLocalDateTime();

		@DBForeignKey(CarCompany.class)
		@DBColumn("fk_carcompany")
		public DBInteger carCompany = new DBInteger();

		@DBColumn()
		public DBLocalDateTime insertTime = new DBLocalDateTime().setDefaultInsertValueToNow();

		@DBColumn()
		public DBLocalDateTime updateTime = new DBLocalDateTime().setDefaultUpdateValueToNow();

		/**
		 * Required Public No-Argument Constructor.
		 *
		 */
		public LocalDateTimeTestTable() {
		}

		/**
		 * Convenience Constructor.
		 *
		 * @param uidMarque uidMarque
		 * @param isUsedForTAFROs isUsedForTAFROs
		 * @param statusClass statusClass
		 * @param carCompany carCompany
		 * @param intIndividualAllocationsAllowed intIndividualAllocationsAllowed
		 * @param pricingCodePrefix pricingCodePrefix
		 * @param updateCount updateCount
		 * @param name name
		 * @param reservationsAllowed reservationsAllowed
		 * @param autoCreated autoCreated
		 * @param creationLocalDateTime creationLocalDateTime
		 * @param enabled enabled
		 */
		public LocalDateTimeTestTable(int uidMarque, String isUsedForTAFROs, int statusClass, String intIndividualAllocationsAllowed, Integer updateCount, String autoCreated, String name, String pricingCodePrefix, String reservationsAllowed, LocalDateTime creationLocalDateTime, int carCompany, Boolean enabled) {
			this.uidMarque.setValue(uidMarque);
			this.name.setValue(name);
			this.creationLocalDateTime.setValue(creationLocalDateTime);
			this.carCompany.setValue(carCompany);
		}

		public LocalDateTimeTestTable(int uidMarque, String name, LocalDateTime creationLocalDateTime) {
			this.uidMarque.setValue(uidMarque);
			this.name.setValue(name);
			this.creationLocalDateTime.setValue(creationLocalDateTime);
			this.carCompany.setValue(carCompany);
		}
	}

	@Test
	public void testDefaultValuesAreConsistentInCluster() throws Exception {
		for (int i = 0; i < 1; i++) {
			try (DBDatabaseCluster cluster = new DBDatabaseCluster(
					"testDefaultValuesAreConsistentInCluster",
					DBDatabaseCluster.Configuration.autoStart(),
					database)) {

				TestingDatabase slowDatabase2 = TestingDatabase.createANewRandomDatabase("testDefaultValuesAreConsistentInCluster-", "-SECOND");
				Brake brake2 = slowDatabase2.getBrake();
				brake2.setTimeout(10000);
				brake2.release();
				cluster.addDatabaseAndWait(slowDatabase2);

				cluster.getDetails().setPreferredDatabase(slowDatabase2);
				brake2.apply();
				
				TestingDatabase slowDatabase3 = TestingDatabase.createANewRandomDatabase("testDefaultValuesAreConsistentInCluster-", "-THIRD");
				Brake brake3 = slowDatabase3.getBrake();
				brake3.setTimeout(10000);
				brake3.release();
				cluster.addDatabaseAndWait(slowDatabase3);
				brake3.apply();

				testDefaultValuesAreConsistentInCluster row = new testDefaultValuesAreConsistentInCluster();
				cluster.preventDroppingOfTables(false);
				cluster.dropTableNoExceptions(row);
				cluster.createTable(row);

				/* Check that row can be inserted successfully*/
				cluster.insert(row);
				assertThat(row.pk_testDefaultValuesAreConsistentInCluster.getValue(), is(1L));

				cluster.waitUntilSynchronised();
				
				String databaseStatuses = cluster.getDatabaseStatuses();
				System.out.println("STATUSES: \n"+databaseStatuses);
				List<Match> allMatches = Regex.empty().literal("READY").getAllMatches(databaseStatuses);
				assertThat("ALL STATUSES SHOULD BE READY", allMatches.size()==3);

				final List<testDefaultValuesAreConsistentInCluster> rows1 = database.getDBTable(row).getRowsByPrimaryKey(row.pk_testDefaultValuesAreConsistentInCluster.getValue());
				final List<testDefaultValuesAreConsistentInCluster> rows2 = slowDatabase2.getDBTable(row).getRowsByPrimaryKey(row.pk_testDefaultValuesAreConsistentInCluster.getValue());
				final List<testDefaultValuesAreConsistentInCluster> rows3 = slowDatabase3.getDBTable(row).getRowsByPrimaryKey(row.pk_testDefaultValuesAreConsistentInCluster.getValue());
				
				assertThat(rows1.size(), is(1));
				assertThat(rows2.size(), is(1));
				assertThat(rows3.size(), is(1));

				testDefaultValuesAreConsistentInCluster gotRow1 = rows1.get(0);
				testDefaultValuesAreConsistentInCluster gotRow2 = rows2.get(0);
				testDefaultValuesAreConsistentInCluster gotRow3 = rows3.get(0);

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

	public static class testDefaultValuesAreConsistentInCluster extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBPrimaryKey
		@DBColumn
		@DBAutoIncrement
		public DBInteger pk_testDefaultValuesAreConsistentInCluster = new DBInteger();

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

}
