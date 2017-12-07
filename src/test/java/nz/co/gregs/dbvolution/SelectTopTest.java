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
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.Matchers.*;

public class SelectTopTest extends AbstractTest {

	public SelectTopTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void selectTop2CarCompanies() throws SQLException {
		CarCompany carCompany = new CarCompany();
		DBTable<CarCompany> carCoTable = database.getDBTable(carCompany);
		carCoTable.setRowLimit(2);
		List<CarCompany> allRows = carCoTable.setBlankQueryAllowed(true).getAllRows();

		Assert.assertThat(allRows.size(), is(2));
	}

	@Test
	public void selectTop3CarCompanies() throws SQLException {
		CarCompany carCompany = new CarCompany();
		DBTable<CarCompany> carCoTable = database.getDBTable(carCompany);
		carCoTable.setRowLimit(3);
		List<CarCompany> allRows = carCoTable.setBlankQueryAllowed(true).getAllRows();

		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testClearingTheRowLimit() throws SQLException {
		CarCompany carCompany = new CarCompany();
		DBTable<CarCompany> carCoTable = database.getDBTable(carCompany);
		carCoTable.setBlankQueryAllowed(true);

		List<CarCompany> allRows = carCoTable.setRowLimit(2).getAllRows();

		Assert.assertThat(allRows.size(), is(2));

		allRows = carCoTable.clearRowLimit().getAllRows();

		Assert.assertThat(allRows.size(), is(4));
	}

	@Test
	public void queryTopTest() throws SQLException {
		CarCompany carCompany = new CarCompany();
		DBQuery query = database.getDBQuery(carCompany, new Marque());
		query.setBlankQueryAllowed(true);
		query.setRowLimit(2);
		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(2));
		query.setRowLimit(3);
		allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(3));
		query.clearRowLimit();
		allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(22));
	}

	@Test
	public void queryPagingTest() throws SQLException {
		CarCompany carCompany = new CarCompany();
		final Marque marque = new Marque();
		DBQuery query = database.getDBQuery(carCompany, marque);
		query.setSortOrder(carCompany.column(carCompany.name), marque.column(marque.name));
		query.setBlankQueryAllowed(true);
		query.setRowLimit(5);

		List<DBQueryRow> firstPage = query.getAllRowsForPage(0);
		List<DBQueryRow> secondPage = query.getAllRowsForPage(1);
		List<DBQueryRow> thirdPage = query.getAllRowsForPage(2);

		Assert.assertThat(firstPage.size(), is(5));
		Assert.assertThat(secondPage.size(), is(5));
		Assert.assertThat(thirdPage.size(), is(5));

		Assert.assertThat(firstPage.get(0).get(carCompany).name.stringValue(), is("Ford"));
		Assert.assertThat(firstPage.get(1).get(carCompany).name.stringValue(), is("GENERAL MOTORS"));

		Assert.assertThat(secondPage.get(0).get(carCompany).name.stringValue(), is("OTHER"));
		Assert.assertThat(secondPage.get(1).get(carCompany).name.stringValue(), is("OTHER"));
		Assert.assertThat(secondPage.get(0).get(marque).name.stringValue(), is("CHRYSLER"));
		Assert.assertThat(secondPage.get(1).get(marque).name.stringValue(), is("DAEWOO"));
		Assert.assertThat(secondPage.get(0).get(marque).uidMarque.intValue(), is(9971178));
		Assert.assertThat(secondPage.get(4).get(marque).uidMarque.intValue(), is(8376505));

		Assert.assertThat(thirdPage.get(0).get(carCompany).name.stringValue(), is("OTHER"));
		Assert.assertThat(thirdPage.get(1).get(carCompany).name.stringValue(), is("OTHER"));
		Assert.assertThat(thirdPage.get(0).get(marque).name.stringValue(), is("LANDROVER"));
		Assert.assertThat(thirdPage.get(1).get(marque).name.stringValue(), is("MAZDA"));
	}
}
