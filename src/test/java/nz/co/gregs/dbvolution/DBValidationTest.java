/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

import java.sql.SQLException;
import java.util.Map;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class DBValidationTest extends AbstractTest {

	public DBValidationTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Before
	public void setup() throws SQLException {
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new Villain());
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new Hero());

		database.createTable(new Villain());
		database.createTable(new Hero());

		database.insert(new Villain("Dr Nonono"), new Villain("Dr Karma"), new Villain("Dr Dark"));
		database.insert(new Hero("James Security"), new Hero("Straw Richards"), new Hero("Lightwing"));
	}

	@After
	public void teardown() throws SQLException {
//		database.preventDroppingOfTables(false);
//		database.dropTableNoExceptions(new Villain());
//		database.preventDroppingOfTables(false);
//		database.dropTableNoExceptions(new Hero());
	}

	public static class Villain extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn
		public DBString name = new DBString();

		public Villain() {
		}

		public Villain(String name) {
			this.name.setValue(name);
		}
	}

	public static class Hero extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn
		public DBString name = new DBString();

		public Hero(String name) {
			this.name.setValue(name);
		}

		public Hero() {
		}
	}

	public static class Fight extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn
		public DBString hero = new DBString();

		@DBColumn
		public DBString villain = new DBString();
	}

	public static class MigrateJamesAndAllVillainsToFight extends Fight {

		private static final long serialVersionUID = 1L;

		public Villain baddy = new Villain();
		public Hero goody = new Hero();

		{
			goody.name.permittedPattern("James%");
			hero = goody.column(goody.name).asExpressionColumn();
			villain = baddy.column(baddy.name).replace("Dr Nonono", "").asExpressionColumn();
		}
	}

	@Test
	public void testvalidating2TablesWithDBMigation() throws SQLException, UnexpectedNumberOfRowsException {
		DBQueryInsert<MigrateJamesAndAllVillainsToFight> migration = database.getDBQueryInsert(new MigrateJamesAndAllVillainsToFight());
		migration.setBlankQueryAllowed(Boolean.TRUE);
		migration.setCartesianJoinAllowed(Boolean.TRUE);

		if (database.getDefinition().supportsFullOuterJoin()) {
			DBValidation.Results validateAllRows = migration.validateAllRows();
			for (DBValidation.Result valid : validateAllRows) {
				final Hero hero = valid.getRow(new Hero());
				final Villain villain = valid.getRow(new Villain());

				if (valid.getWillBeProcessed()) {
					Assert.assertThat(hero.name.stringValue(), is("James Security"));
				} else {
					Assert.assertThat(hero.name.stringValue(), not("James Security"));
				}
				Map<String, String> map = valid.getMap();
				Assert.assertThat(map.size(), greaterThan(0));
				for (Map.Entry<String, String> entry : map.entrySet()) {
					String key = entry.getKey();
					String value = entry.getValue();

					Assert.assertThat(value, isOneOf("success", "NO DATA"));
					if (key.equals("villian")) {
						if (value.equals("success")) {
							Assert.assertThat(valid.getRow(new DBQueryInsertTest.Villain()).name.stringValue(), not("Dr Nonono"));
						} else {
							Assert.assertThat(valid.getRow(new DBQueryInsertTest.Villain()).name.stringValue(), is("Dr Nonono"));
						}
					}
				}
			}
			Assert.assertThat(validateAllRows.size(), is(9));
		}
	}

}
