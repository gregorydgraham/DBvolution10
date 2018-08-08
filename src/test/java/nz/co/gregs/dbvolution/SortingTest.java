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

public class SortingTest extends AbstractTest {

	public SortingTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void sortingATable() throws SQLException {
		final Marque marque = new Marque();
		DBTable<Marque> dbTable = database.getDBTable(marque);
		dbTable.setSortOrder(marque.column(marque.carCompany), marque.column(marque.name));
		dbTable.setBlankQueryAllowed(true);
		List<Marque> sortedMarques = dbTable.getAllRows();
		Assert.assertThat(sortedMarques.size(), is(22));
		Assert.assertThat(sortedMarques.get(0).name.toString(), is("HYUNDAI"));
		Assert.assertThat(sortedMarques.get(1).name.toString(), is("TOYOTA"));
		Assert.assertThat(sortedMarques.get(2).name.toString(), is("FORD"));
		Assert.assertThat(sortedMarques.get(3).name.toString(), is("HOLDEN"));
		dbTable.setSortOrder(marque.column(marque.name));

		sortedMarques = dbTable.getAllRows();
		Assert.assertThat(sortedMarques.size(), is(22));
		Assert.assertThat(sortedMarques.get(0).name.toString(), is("BMW"));
		Assert.assertThat(sortedMarques.get(1).name.toString(), is("CHRYSLER"));
		Assert.assertThat(sortedMarques.get(2).name.toString(), is("DAEWOO"));
		Assert.assertThat(sortedMarques.get(3).name.toString(), is("DAIHATSU"));
		marque.name.setSortOrderDescending();

		sortedMarques = dbTable.getAllRows();
		Assert.assertThat(sortedMarques.size(), is(22));
		Assert.assertThat(sortedMarques.get(0).name.toString(), is("VW"));
		Assert.assertThat(sortedMarques.get(1).name.toString(), is("VOLVO"));
		Assert.assertThat(sortedMarques.get(2).name.toString(), is("TOYOTA"));
		Assert.assertThat(sortedMarques.get(3).name.toString(), is("SUZUKI"));
	}

	@Test
	public void sortingDBQuery() throws SQLException {
		final Marque marque = new Marque();
		final CarCompany carCo = new CarCompany();
		DBQuery query = database.getDBQuery(marque, carCo);
		query.setSortOrder(marque.column(marque.carCompany), marque.column(marque.name));
		query.setBlankQueryAllowed(true);

		List<Marque> sortedMarques = query.getAllInstancesOf(marque);
		Assert.assertThat(sortedMarques.size(), is(22));
		Assert.assertThat(sortedMarques.get(0).name.toString(), is("HYUNDAI"));
		Assert.assertThat(sortedMarques.get(1).name.toString(), is("TOYOTA"));
		Assert.assertThat(sortedMarques.get(2).name.toString(), is("FORD"));
		Assert.assertThat(sortedMarques.get(3).name.toString(), is("HOLDEN"));
		query.setSortOrder(marque.column(marque.name));

		sortedMarques = query.getAllInstancesOf(marque);
		Assert.assertThat(sortedMarques.size(), is(22));
		Assert.assertThat(sortedMarques.get(0).name.toString(), is("BMW"));
		Assert.assertThat(sortedMarques.get(1).name.toString(), is("CHRYSLER"));
		Assert.assertThat(sortedMarques.get(2).name.toString(), is("DAEWOO"));
		Assert.assertThat(sortedMarques.get(3).name.toString(), is("DAIHATSU"));
		
		marque.name.setSortOrderDescending();
		sortedMarques = query.getAllInstancesOf(marque);
		Assert.assertThat(sortedMarques.size(), is(22));
		Assert.assertThat(sortedMarques.get(0).name.toString(), is("VW"));
		Assert.assertThat(sortedMarques.get(1).name.toString(), is("VOLVO"));
		Assert.assertThat(sortedMarques.get(2).name.toString(), is("TOYOTA"));
		Assert.assertThat(sortedMarques.get(3).name.toString(), is("SUZUKI"));
	}

	@Test
	public void sortingDBQueryUsingExpressions() throws SQLException {
		final Marque marque = new Marque();
		final CarCompany carCo = new CarCompany();
		DBQuery query = database.getDBQuery(marque, carCo);
		query.setBlankQueryAllowed(true);

		query.setSortOrder(marque.column(marque.carCompany).times(2).ascending(), marque.column(marque.name).substring(0, 3).ascending());
		List<Marque> sortedMarques = query.getAllInstancesOf(marque);
		Assert.assertThat(sortedMarques.size(), is(22));
		Assert.assertThat(sortedMarques.get(0).name.toString(), is("HYUNDAI"));
		Assert.assertThat(sortedMarques.get(1).name.toString(), is("TOYOTA"));
		Assert.assertThat(sortedMarques.get(2).name.toString(), is("FORD"));
		Assert.assertThat(sortedMarques.get(3).name.toString(), is("HOLDEN"));
		
		query.setSortOrder(marque.column(marque.name).substring(0, 3).ascending());
		sortedMarques = query.getAllInstancesOf(marque);
		Assert.assertThat(sortedMarques.size(), is(22));
		Assert.assertThat(sortedMarques.get(0).name.toString(), is("BMW"));
		Assert.assertThat(sortedMarques.get(1).name.toString(), is("CHRYSLER"));
		Assert.assertThat(sortedMarques.get(2).name.toString(), is("DAEWOO"));
		Assert.assertThat(sortedMarques.get(3).name.toString(), is("DAIHATSU"));
		
		query.setSortOrder(marque.column(marque.name).substring(0, 3).descending());
		sortedMarques = query.getAllInstancesOf(marque);
		Assert.assertThat(sortedMarques.size(), is(22));
		Assert.assertThat(sortedMarques.get(0).name.toString(), is("VW"));
		Assert.assertThat(sortedMarques.get(1).name.toString(), is("VOLVO"));
		Assert.assertThat(sortedMarques.get(2).name.toString(), is("TOYOTA"));
		Assert.assertThat(sortedMarques.get(3).name.toString(), is("SUZUKI"));
	}
}
