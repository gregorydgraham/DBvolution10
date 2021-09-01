/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class DBQueryInsertTest extends AbstractTest {

	public DBQueryInsertTest(Object testIterationName, Object db) {
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

	public static class Professional extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn
		public DBString title = new DBString();

		@DBColumn
		public DBString surname = new DBString();
	}

	@Test
	public void testMapping1ColumnWithDBmigrationMap() throws SQLException {
		DBQueryInsert<MigrateVillainToProfessional> queryInsert = database.getDBQueryInsert(new MigrateVillainToProfessional());
		queryInsert.setBlankQueryAllowed(Boolean.TRUE);
		List<MigrateVillainToProfessional> rows = queryInsert.getAllRows();

		for (Professional prof : rows) {
			assertThat(prof.title.stringValue(), is("Dr"));
			assertThat(prof.surname.stringValue(), isOneOf("Nonono", "Karma", "Dark"));
		}

		database.preventDroppingOfTables(false);
		final Professional professional = new Professional();
		database.dropTableNoExceptions(professional);
		database.createTable(professional);

		queryInsert.insertAllRows();

		DBTable<Professional> table = database.getDBTable(professional);
		List<Professional> allRows = table.setBlankQueryAllowed(true).getAllRows();
		assertThat(allRows.size(), is(3));
		for (Professional prof : allRows) {
			assertThat(prof.title.stringValue(), is("Dr"));
			assertThat(prof.surname.stringValue(), isOneOf("Nonono", "Karma", "Dark"));
		}
	}

	public static class MigrateVillainToProfessional extends Professional {

		private static final long serialVersionUID = 1L;
		public Villain baddy = new Villain();

		{
			baddy.name.permittedPattern("Dr %");
			title = baddy.column(baddy.name).substringBefore(" ").asExpressionColumn();
			surname = baddy.column(baddy.name).substringAfter(" ").asExpressionColumn();
		}
	}

	public static class MigrateHeroAndVillianToFight extends Fight {

		private static final long serialVersionUID = 1L;

		public Villain baddy = new Villain();
		public Hero goody = new Hero();

		{
			baddy.name.permittedPattern("Dr%");
			hero = goody.column(goody.name).asExpressionColumn();
			villain = baddy.column(baddy.name).asExpressionColumn();
		}
	}

	@Test
	public void testJoining2TablesWithDBMigation() throws SQLException, UnexpectedNumberOfRowsException {

		DBQueryInsert<MigrateHeroAndVillianToFight> queryInsert = database.getDBQueryInsert(new MigrateHeroAndVillianToFight());
		queryInsert.setBlankQueryAllowed(Boolean.TRUE);
		queryInsert.setCartesianJoinAllowed(Boolean.TRUE);
		List<MigrateHeroAndVillianToFight> fights = queryInsert.getAllRows();

		assertThat(fights.size(), is(9));
		assertThat(fights.get(0).villain.stringValue(), isOneOf("Dr Nonono", "Dr Karma", "Dr Dark"));
		assertThat(fights.get(0).hero.stringValue(), isOneOf("James Security", "Straw Richards", "Lightwing"));

		for (Fight fight : fights) {
			assertThat(fight.villain.stringValue(), isOneOf("Dr Nonono", "Dr Karma", "Dr Dark"));
			assertThat(fight.hero.stringValue(), isOneOf("James Security", "Straw Richards", "Lightwing"));
		}

		database.preventDroppingOfTables(false);
		final Fight fight = new Fight();
		database.dropTableNoExceptions(fight);
		database.createTable(fight);

		queryInsert.insertAllRows();

		DBTable<Fight> query = database.getDBTable(fight);
		List<Fight> allRows = query.setBlankQueryAllowed(true).getAllRows();
		assertThat(allRows.size(), is(9));
		for (Fight newFight : allRows) {
			assertThat(newFight.villain.stringValue(), isOneOf("Dr Nonono", "Dr Karma", "Dr Dark"));
			assertThat(newFight.hero.stringValue(), isOneOf("James Security", "Straw Richards", "Lightwing"));
		}

	}

}
