/*
 * Copyright 2013 Gregory Graham.
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
package nz.co.gregs.dbvolution;

import java.sql.SQLException;
import java.util.List;

import java.util.SortedSet;
import static org.hamcrest.Matchers.*;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.CompanyLogo;
import nz.co.gregs.dbvolution.example.LinkCarCompanyAndLogo;
import nz.co.gregs.dbvolution.example.LinkCarCompanyAndLogoWithPreviousLink;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.example.MarqueSelectQuery;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Assert;
import org.junit.Test;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import nz.co.gregs.dbvolution.example.CompanyText;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBQueryTest extends AbstractTest {

	public DBQueryTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testQueryGeneration() throws SQLException {
		DBQuery dbQuery = database.getDBQuery();
		CarCompany carCompany = new CarCompany();
		carCompany.name.permittedValues("TOYOTA");
		dbQuery.add(new Marque());
		dbQuery.add(carCompany);
		// make sure it works
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		Assert.assertThat(allRows.size(), is(2));
	}

	@Test
	public void testQueryExecution() throws SQLException {
		Object[][] tests = new Object[][]{
			{"TOYOTA", 2, new long[]{1l, 4896300l}},
			{"TATA", 0, null},
			{"Ford", 1, 4893090}
		};
		for (Object[] test : tests) {
			DBQuery dbQuery = database.getDBQuery();
			CarCompany carCompany = new CarCompany();
			carCompany.name.permittedValues(test[0]);
			dbQuery.add(carCompany);
			dbQuery.add(new Marque());

			List<DBQueryRow> results = dbQuery.getAllRows();

			assertEquals(test[1], results.size());

			for (DBQueryRow queryRow : results) {

				CarCompany carCo = queryRow.get(carCompany);
				String carCoName = carCo.name.toString();

				Marque marque = queryRow.get(new Marque());
				Long marqueUID = marque.getUidMarque().getValue();

				assertTrue(carCoName.equals(test[0]));
				if (test[0] == "TOYOTA") {
					assertTrue(marqueUID == 1 || marqueUID == 4896300);
				}
			}
		}
	}

	@Test
	public void testQueryExecutionWithoutPrimaryKeyColumn() throws SQLException {
		DBQuery dbQuery = database.getDBQuery();
		CarCompany carCompany = new CarCompany();
		carCompany.name.permittedValues("TOYOTA");
		carCompany.setReturnFields(carCompany.name);
		dbQuery.add(carCompany);
		Marque marque = new Marque();
		marque.setReturnFields(marque.name);
		dbQuery.add(marque);

		List<DBQueryRow> results = dbQuery.getAllRows();

		assertEquals(2, results.size());
		boolean foundToyota = false;
		boolean foundHyundai = false;

		for (DBQueryRow queryRow : results) {

			CarCompany carCo = queryRow.get(carCompany);
			String carCoName = carCo.name.toString();

			marque = queryRow.get(new Marque());
			Long marqueUID = marque.getUidMarque().getValue();

			assertTrue(carCoName.equals("TOYOTA"));
			assertTrue(marque.name.stringValue().equals("TOYOTA") || marque.name.stringValue().equals("HYUNDAI"));
			if (marque.name.stringValue().equals("TOYOTA")) {
				foundToyota = true;
			}
			if (marque.name.stringValue().equals("HYUNDAI")) {
				foundHyundai = true;
			}
		}
		assertTrue("Did not find both marques expected.", foundToyota && foundHyundai);
	}

	@Test
	public void quickQueryCreation() throws SQLException {

		CarCompany carCompany = new CarCompany();
		carCompany.name.permittedValues("TOYOTA");
		DBQuery dbQuery = database.getDBQuery(carCompany, new Marque());

		List<DBQueryRow> results = dbQuery.getAllRows();

		assertEquals(2, results.size());

		for (DBQueryRow queryRow : results) {

			CarCompany carCo = queryRow.get(carCompany);
			String carCoName = carCo.name.toString();

			Marque marque = queryRow.get(new Marque());
			Long marqueUID = marque.getUidMarque().getValue();

			assertTrue(carCoName.equals("TOYOTA"));
			assertTrue(marqueUID == 1 || marqueUID == 4896300);
		}
	}

	@Test
	public void testDBTableRowReuse() throws SQLException {
		CarCompany carCompany = new CarCompany();
		carCompany.name.permittedValues("TOYOTA");
		DBQuery dbQuery = database.getDBQuery(carCompany, new Marque());

		List<DBQueryRow> results = dbQuery.getAllRows();

		assertEquals(2, results.size());

		DBQueryRow[] rows = new DBQueryRow[2];
		rows = results.toArray(rows);

		CarCompany firstCarCo = rows[0].get(carCompany);
		CarCompany secondCarCo = rows[1].get(carCompany);
		assertTrue(firstCarCo == secondCarCo);
	}

	@Test
	public void testGettingRelatedInstances() throws SQLException {
		CarCompany carCompany = new CarCompany();
		DBQuery dbQuery = database.getDBQuery(carCompany, new Marque());
		dbQuery.setBlankQueryAllowed(true);

		CarCompany toyota = null;
		for (CarCompany carco : dbQuery.getAllInstancesOf(carCompany)) {
			if (carco.name.stringValue().equalsIgnoreCase("toyota")) {
				toyota = carco;
			}
		}

		if (toyota != null) {
			List<Marque> relatedInstances = toyota.getRelatedInstancesFromQuery(dbQuery.getQueryDetails(), new Marque());

			Assert.assertThat(relatedInstances.size(), is(2));
		} else {
			throw new RuntimeException("Unable To Find Toyota From The Query Results");
		}
	}

	@Test
	public void thrownExceptionIfTheForeignKeyFieldToBeIgnoredIsNotInTheInstance() throws Exception {
		Marque wrongMarque = new Marque();
		Marque marqueQuery = new Marque();
		marqueQuery.uidMarque.permittedValues(wrongMarque.uidMarque.getValue());

		DBQuery query = database.getDBQuery(marqueQuery, new CarCompany());
		try {
			marqueQuery.ignoreForeignKey(wrongMarque.carCompany);
			throw new RuntimeException("IncorrectDBRowInstanceSuppliedException should have been thrown");
		} catch (IncorrectRowProviderInstanceSuppliedException wrongDBRowEx) {
		}
		marqueQuery.ignoreForeignKey(marqueQuery.carCompany);
		query.setCartesianJoinsAllowed(true);
		List<DBQueryRow> rows = query.getAllRows();
	}

	@Test
	public void thrownExceptionIfColumnIsNotInTheInstance() throws Exception {
		Marque wrongMarque = new Marque();
		Marque marqueQuery = new Marque();
		marqueQuery.uidMarque.permittedValues(wrongMarque.uidMarque.getValue());

		DBQuery query = database.getDBQuery(marqueQuery, new CarCompany());
		try {
			query.getDistinctCombinationsOfColumnValues(wrongMarque.carCompany);
			throw new RuntimeException("IncorrectDBRowInstanceSuppliedException should have been thrown");
		} catch (IncorrectRowProviderInstanceSuppliedException wrongDBRowEx) {
		}
		marqueQuery.ignoreForeignKey(marqueQuery.carCompany);
		query.setCartesianJoinsAllowed(true);
		List<DBQueryRow> rows = query.getAllRows();
	}

	@Test
	public void thrownExceptionIfTheForeignKeyFieldsToBeIgnoredIsNotInTheInstance() throws Exception {
		Marque wrongMarque = new Marque();
		Marque marqueQuery = new Marque();
		marqueQuery.uidMarque.permittedValues(wrongMarque.uidMarque.getValue());

		DBQuery query = database.getDBQuery(marqueQuery, new CarCompany());
		try {
			marqueQuery.ignoreForeignKeys(wrongMarque.carCompany, wrongMarque.carCompany);
			throw new RuntimeException("IncorrectDBRowInstanceSuppliedException should have been thrown");
		} catch (IncorrectRowProviderInstanceSuppliedException wrongDBRowEx) {
		}
		marqueQuery.ignoreForeignKey(marqueQuery.carCompany);
		query.setCartesianJoinsAllowed(true);
		List<DBQueryRow> rows = query.getAllRows();
	}

	@Test
	public void thrownExceptionIfTheForeignKeyColumnsToBeIgnoredIsNotInTheInstance() throws Exception {
		Marque wrongMarque = new Marque();
		Marque marqueQuery = new Marque();
		marqueQuery.uidMarque.permittedValues(wrongMarque.uidMarque.getValue());

		DBQuery query = database.getDBQuery(marqueQuery, new CarCompany());
		try {
			marqueQuery.ignoreForeignKeys(marqueQuery.column(wrongMarque.carCompany), marqueQuery.column(wrongMarque.carCompany));
			throw new RuntimeException("IncorrectDBRowInstanceSuppliedException should have been thrown");
		} catch (IncorrectRowProviderInstanceSuppliedException wrongDBRowEx) {
		}
		marqueQuery.ignoreForeignKeys(marqueQuery.column(marqueQuery.carCompany), marqueQuery.column(marqueQuery.carCompany));
		query.setCartesianJoinsAllowed(true);
		List<DBQueryRow> rows = query.getAllRows();
	}

	@Test
	public void thrownExceptionIfAllTheForeignKeyFieldsToBeIgnoredIsNotInTheInstance() throws Exception {
		Marque wrongMarque = new Marque();
		Marque marqueQuery = new Marque();
		marqueQuery.uidMarque.permittedValues(wrongMarque.uidMarque.getValue());

		DBQuery query = database.getDBQuery(marqueQuery, new CarCompany());
		try {
			marqueQuery.ignoreAllForeignKeysExcept(wrongMarque.carCompany);
			throw new RuntimeException("IncorrectDBRowInstanceSuppliedException should have been thrown");
		} catch (IncorrectRowProviderInstanceSuppliedException wrongDBRowEx) {
		}
		marqueQuery.ignoreAllForeignKeysExcept(marqueQuery.carCompany);
		query.setCartesianJoinsAllowed(true);
		List<DBQueryRow> rows = query.getAllRows();
	}

	@Test
	public void getRelatedTablesTest() {
		CarCompany carco = new CarCompany();
		SortedSet<DBRow> relatedTables = database.getDBQuery(carco).getRelatedTables();

		Assert.assertThat(relatedTables.size(), is(6));
		final DBRow[] rowArray = relatedTables.toArray(new DBRow[]{});
		Assert.assertEquals(CompanyLogo.class, rowArray[0].getClass());
		Assert.assertEquals(CompanyText.class, rowArray[1].getClass());
		Assert.assertEquals(LinkCarCompanyAndLogo.class, rowArray[2].getClass());
		Assert.assertEquals(LinkCarCompanyAndLogoWithPreviousLink.class, rowArray[3].getClass());
		Assert.assertEquals(Marque.class, rowArray[4].getClass());
		Assert.assertEquals(MarqueSelectQuery.class, rowArray[5].getClass());
	}

	@Test
	public void getReferencedTablesTest() {
		CarCompany carco = new CarCompany();
		SortedSet<DBRow> relatedTables = database.getDBQuery(carco).getReferencedTables();

		Assert.assertThat(relatedTables.size(), is(0));

		relatedTables = database.getDBQuery(new Marque(), new LinkCarCompanyAndLogo()).getReferencedTables();

		Assert.assertThat(relatedTables.size(), is(2));

		final DBRow[] rowArray = relatedTables.toArray(new DBRow[]{});
		Assert.assertEquals(CarCompany.class, rowArray[0].getClass());
		Assert.assertEquals(CompanyLogo.class, rowArray[1].getClass());
	}
}
