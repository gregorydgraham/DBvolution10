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
import nz.co.gregs.dbvolution.columns.IntegerColumn;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class IgnoreForeignKeyTest extends AbstractTest {

	public IgnoreForeignKeyTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testIgnoreForeignKey() throws SQLException {
		Marque marque = new Marque();
		CarCompany carCompany = new CarCompany();
		DBQuery dbQuery = database.getDBQuery(carCompany, marque);
		dbQuery.setBlankQueryAllowed(true);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertTrue("Number of rows should be 22", allRows.size() == 22);

		marque.ignoreForeignKey(marque.getCarCompany());
		dbQuery = database.getDBQuery(carCompany, marque);
		dbQuery.setBlankQueryAllowed(true);
		dbQuery.setCartesianJoinsAllowed(true);
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(88));

		marque.useAllForeignKeys();
		dbQuery = database.getDBQuery(carCompany, marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();

		Assert.assertTrue("Number of rows should be 88", allRows.size() == 22);

		marque.ignoreAllForeignKeys();
		dbQuery = database.getDBQuery(carCompany, marque);
		dbQuery.setBlankQueryAllowed(true);
		dbQuery.setCartesianJoinsAllowed(true);
		allRows = dbQuery.getAllRows();

		Assert.assertTrue("Number of rows should be 88", allRows.size() == 88);

	}

	@Test
	public void testIgnoreForeignKeyUsingColumnProviders() throws SQLException {
		Marque marque = new Marque();
		CarCompany carCompany = new CarCompany();
		DBQuery dbQuery = database.getDBQuery(carCompany, marque);
		dbQuery.setBlankQueryAllowed(true);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertTrue("Number of rows should be 22", allRows.size() == 22);
		final IntegerColumn carCompanyColumn = marque.column(marque.carCompany);

		marque.ignoreForeignKey(carCompanyColumn);
		dbQuery = database.getDBQuery(carCompany, marque);
		dbQuery.setBlankQueryAllowed(true);
		dbQuery.setCartesianJoinsAllowed(true);
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(88));

		marque.useAllForeignKeys();
		dbQuery = database.getDBQuery(carCompany, marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();

		Assert.assertTrue("Number of rows should be 22", allRows.size() == 22);
	}

	@Test
	public void testIgnoreForeignKeyUsingColumnProvidersAndADifferentObject() throws SQLException {
		Marque marque = new Marque();
		CarCompany carCompany = new CarCompany();
		DBQuery dbQuery = database.getDBQuery(carCompany, marque);
		dbQuery.setBlankQueryAllowed(true);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertTrue("Number of rows should be 22", allRows.size() == 22);
		final IntegerColumn carCompanyColumn = marque.column(marque.carCompany);

		marque = new Marque();
		marque.ignoreForeignKey(carCompanyColumn);
		dbQuery = database.getDBQuery(carCompany, marque);
		dbQuery.setBlankQueryAllowed(true);
		dbQuery.setCartesianJoinsAllowed(true);
		allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(88));

		marque.useAllForeignKeys();
		dbQuery = database.getDBQuery(carCompany, marque);
		dbQuery.setBlankQueryAllowed(true);
		allRows = dbQuery.getAllRows();

		Assert.assertTrue("Number of rows should be 22", allRows.size() == 22);
	}
}
