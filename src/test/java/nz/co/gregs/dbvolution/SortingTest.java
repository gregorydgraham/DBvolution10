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
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.Matchers.*;
import org.hamcrest.core.IsNull;

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

	@Test
	public void sortingNullsLast() throws SQLException {
		Marque marque = new Marque();
		List<Marque> allRows = database
				.getDBTable(marque)
				.setBlankQueryAllowed(true)
				.setSortOrder(marque.column(marque.individualAllocationsAllowed)
						.ascending()
						.nullsLast()
				).getAllRows();
		Assert.assertThat(allRows.size(), is(22));

		if (database.getDefinition().supportsDifferenceBetweenNullAndEmptyString()) {
			Assert.assertThat(allRows.get(0).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(1).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(2).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(3).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(4).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(5).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(6).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(7).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(8).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(9).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(10).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(11).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(12).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(13).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(14).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(15).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(16).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(17).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(18).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(19).individualAllocationsAllowed.getValue(), is("Y"));
			Assert.assertThat(allRows.get(20).individualAllocationsAllowed.getValue(), nullValue());
			Assert.assertThat(allRows.get(21).individualAllocationsAllowed.getValue(), nullValue());
		} else {
			Assert.assertThat(allRows.get(0).individualAllocationsAllowed.getValue(), is("Y"));
			Assert.assertThat(allRows.get(1).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(2).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(3).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(4).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(5).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(6).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(7).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(8).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(9).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(10).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(11).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(12).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(13).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(14).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(15).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(16).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(17).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(18).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(19).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(20).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(21).individualAllocationsAllowed.getValue(), is(""));
		}
	}

	@Test
	public void sortingNullsFirst() throws SQLException {
		Marque marque = new Marque();
		List<Marque> allRows = database
				.getDBTable(marque)
				.setBlankQueryAllowed(true)
				.setSortOrder(marque.column(marque.individualAllocationsAllowed)
						.ascending()
						.nullsFirst()
				).getAllRows();
		Assert.assertThat(allRows.size(), is(22));

		if (database.getDefinition().supportsDifferenceBetweenNullAndEmptyString()) {
			Assert.assertThat(allRows.get(0).individualAllocationsAllowed.getValue(), nullValue());
			Assert.assertThat(allRows.get(1).individualAllocationsAllowed.getValue(), nullValue());
			Assert.assertThat(allRows.get(2).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(3).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(4).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(5).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(6).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(7).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(8).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(9).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(10).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(11).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(12).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(13).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(14).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(15).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(16).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(17).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(18).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(19).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(20).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(21).individualAllocationsAllowed.getValue(), is("Y"));
		} else {
			Assert.assertThat(allRows.get(0).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(1).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(2).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(3).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(4).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(5).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(6).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(7).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(8).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(9).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(10).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(11).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(12).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(13).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(14).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(15).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(16).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(17).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(18).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(19).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(20).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(21).individualAllocationsAllowed.getValue(), is("Y"));
		}
	}

	@Test
	public void sortingNullsHighest() throws SQLException {
		Marque marque = new Marque();
		List<Marque> allRows = database
				.getDBTable(marque)
				.setBlankQueryAllowed(true)
				.setSortOrder(marque.column(marque.individualAllocationsAllowed)
						.descending()
						.nullsHighest()
				).getAllRows();
		Assert.assertThat(allRows.size(), is(22));

		if (database.getDefinition().supportsDifferenceBetweenNullAndEmptyString()) {
			Assert.assertThat(allRows.get(0).individualAllocationsAllowed.getValue(), nullValue());
			Assert.assertThat(allRows.get(1).individualAllocationsAllowed.getValue(), nullValue());
			Assert.assertThat(allRows.get(2).individualAllocationsAllowed.getValue(), is("Y"));
			Assert.assertThat(allRows.get(3).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(4).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(5).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(6).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(7).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(8).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(9).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(10).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(11).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(12).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(13).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(14).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(15).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(16).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(17).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(18).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(19).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(20).individualAllocationsAllowed.getValue(), isEmptyString());
			Assert.assertThat(allRows.get(21).individualAllocationsAllowed.getValue(), isEmptyString());
		} else {
			Assert.assertThat(allRows.get(0).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(1).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(2).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(3).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(4).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(5).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(6).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(7).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(8).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(9).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(10).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(11).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(12).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(13).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(14).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(15).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(16).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(17).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(18).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(19).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(20).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(21).individualAllocationsAllowed.getValue(), is("Y"));
		}
	}

	@Test
	public void sortingNullsLowest() throws SQLException {
		Marque marque = new Marque();
		List<Marque> allRows = database
				.getDBTable(marque)
				.setBlankQueryAllowed(true)
				.setSortOrder(marque.column(marque.individualAllocationsAllowed)
						.ascending()
						.nullsLowest()
				).getAllRows();
		Assert.assertThat(allRows.size(), is(22));

		if (database.getDefinition().supportsDifferenceBetweenNullAndEmptyString()) {
			Assert.assertThat(allRows.get(0).individualAllocationsAllowed.getValue(), nullValue());
			Assert.assertThat(allRows.get(1).individualAllocationsAllowed.getValue(), nullValue());
			Assert.assertThat(allRows.get(2).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(3).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(4).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(5).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(6).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(7).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(8).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(9).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(10).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(11).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(12).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(13).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(14).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(15).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(16).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(17).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(18).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(19).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(20).individualAllocationsAllowed.getValue(), isEmptyString());
			Assert.assertThat(allRows.get(21).individualAllocationsAllowed.getValue(), is("Y"));
		} else {
			Assert.assertThat(allRows.get(0).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(1).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(2).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(3).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(4).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(5).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(6).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(7).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(8).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(9).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(10).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(11).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(12).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(13).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(14).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(15).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(16).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(17).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(18).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(19).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(20).individualAllocationsAllowed.getValue(), is(""));
			Assert.assertThat(allRows.get(21).individualAllocationsAllowed.getValue(), is("Y"));
		}
	}
}
