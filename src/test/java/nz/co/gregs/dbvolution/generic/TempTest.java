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
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.databases.*;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBLocalDateTime;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.exceptions.AutoCommitActionDuringTransactionException;
import nz.co.gregs.dbvolution.expressions.LocalDateTimeExpression;
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
}
