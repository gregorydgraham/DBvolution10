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
package nz.co.gregs.dbvolution;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBString;
import org.junit.Test;
import org.junit.Assert;
import static org.hamcrest.Matchers.*;

import nz.co.gregs.dbvolution.example.*;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.generic.AbstractTest;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class FindDistinctDBRowColumnValuesTest extends AbstractTest {

	public FindDistinctDBRowColumnValuesTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testManualMethod() throws SQLException {
		List<DBDate> creationDates = new ArrayList<DBDate>();
		Marque marque = new Marque();
		marque.setReturnFields(marque.creationDate);
		DBQuery dbQuery = database.getDBQuery(marque)
				.addGroupByColumn(marque, marque.column(marque.creationDate))
				.setSortOrder(marque.column(marque.creationDate));
		dbQuery.setBlankQueryAllowed(true);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(3));
		for (DBQueryRow dBQueryRow : allRows) {
			Marque get = dBQueryRow.get(marque);
			creationDates.add(get == null ? null : get.creationDate);
		}
		Assert.assertThat(creationDates.size(), is(3));
		Assert.assertThat(creationDates.get(0).isNull(), is(true));

		List<String> foundStrings = new ArrayList<String>();
		for (DBDate dBDate : creationDates) {
			if (dBDate != null && dBDate.isNotNull()) {
				Assert.assertThat(dBDate.toString(),
						anyOf(
								startsWith("2013-03-23"),
								startsWith("2011-04-02")
						)
				);
				foundStrings.add((dBDate.toString().substring(0, 10)));
			}
		}
		Assert.assertThat(foundStrings.size(), is(2));
		Assert.assertThat(foundStrings, hasItems("2013-03-23", "2011-04-02"));
	}

	@Test
	public void testDBRowMethod() throws SQLException {
		Marque marque = new Marque();
		List<DBDate> distinctValuesForColumn = marque.getDistinctValuesOfColumn(database, marque.creationDate);

		Assert.assertThat(distinctValuesForColumn.size(), is(3));
		DBDate[] toArray = distinctValuesForColumn.toArray(new DBDate[]{});
		Assert.assertThat(toArray[0].isNull(), is(true));
//		Assert.assertThat(distinctValuesForColumn, hasItem((DBDate) null));

		List<String> foundStrings = new ArrayList<String>();
		for (DBDate dBDate : distinctValuesForColumn) {
			if (dBDate != null && dBDate.isNotNull()) {
				Assert.assertThat(dBDate.toString(),
						anyOf(
								startsWith("2013-03-23"),
								startsWith("2011-04-02")
						)
				);
				foundStrings.add((dBDate.toString().substring(0, 10)));
			}
		}
		Assert.assertThat(foundStrings.size(), is(2));
		Assert.assertThat(foundStrings, hasItems("2013-03-23", "2011-04-02"));
		Assert.assertThat(foundStrings.get(0), is("2011-04-02"));
		Assert.assertThat(foundStrings.get(1), is("2013-03-23"));
	}

	@Test
	public void testDBRowMethodWithDBString() throws SQLException {
		Marque marque = new Marque();
		List<DBString> distinctValuesForColumn = marque.getDistinctValuesOfColumn(database, marque.individualAllocationsAllowed);
		if (database.getDefinition().supportsDifferenceBetweenNullAndEmptyString()) {
			Assert.assertThat(distinctValuesForColumn.size(), is(3));
		} else {
			Assert.assertThat(distinctValuesForColumn.size(), is(2));
		}
//		Assert.assertThat(distinctValuesForColumn, hasItem((DBString) null));

		List<String> foundStrings = new ArrayList<>();
		for (DBString val : distinctValuesForColumn) {
			if (val != null && val.isNotNull()) {
				Assert.assertThat(val.toString(),
						anyOf(
								is("Y"),
								is("")
						)
				);
				foundStrings.add((val.toString()));
			}
		}
		if (database.getDefinition().supportsDifferenceBetweenNullAndEmptyString()) {
			Assert.assertThat(foundStrings.size(), is(2));
			Assert.assertThat(foundStrings, hasItems("Y", ""));
			Assert.assertThat(foundStrings.get(0), is(""));
			Assert.assertThat(foundStrings.get(1), is("Y"));
		} else {
			Assert.assertThat(foundStrings.size(), is(1));
			Assert.assertThat(foundStrings, hasItems("Y"));
			Assert.assertThat(foundStrings.get(0), is("Y"));
		}
	}

	@Test
	public void testDBTableMethodWithDBString() throws SQLException {
		Marque marque = new Marque();
		final DBTable<Marque> dbTable = database.getDBTable(marque);
		List<DBString> distinctValuesForColumn = dbTable.getDistinctValuesOfColumn(marque.individualAllocationsAllowed);
		List<String> foundStrings = new ArrayList<String>();
		for (DBString val : distinctValuesForColumn) {
			if (val != null && val.isNotNull()) {
				Assert.assertThat(val.toString(),
						anyOf(
								is("Y"),
								is("")
						)
				);
			}
//			if (val != null) {
				foundStrings.add((val.toString()));
//			}
		}
		if (database.getDefinition().supportsDifferenceBetweenNullAndEmptyString()) {
			Assert.assertThat(distinctValuesForColumn.size(), is(3));
//			Assert.assertThat(distinctValuesForColumn, hasItem((DBString) null));
			Assert.assertThat(foundStrings.size(), is(3));
			Assert.assertThat(foundStrings.get(0), is(""));
			Assert.assertThat(foundStrings.get(1), is(""));
			Assert.assertThat(foundStrings.get(2), is("Y"));
		} else {
			Assert.assertThat(distinctValuesForColumn.size(), is(2));
//			Assert.assertThat(distinctValuesForColumn, hasItem((DBString) null));
			Assert.assertThat(foundStrings.size(), is(2));
			Assert.assertThat(foundStrings.get(1), is("Y"));
			Assert.assertThat(foundStrings.get(0), is(""));
		}
	}

	@Test
	public void testDBQueryVersion() throws AccidentalBlankQueryException, SQLException {
		final CarCompany carCo = new CarCompany();
		carCo.name.permittedValues("OTHER");
		final Marque marque = new Marque();
		marque.individualAllocationsAllowed.setSortOrderAscending();
		List<DBQueryRow> distinctCombinationsOfColumnValues
				= database
						.getDBQuery(carCo, marque)
						.setBlankQueryAllowed(true)
						.getDistinctCombinationsOfColumnValues(marque.individualAllocationsAllowed, carCo.name);

		if (database.getDefinition().supportsDifferenceBetweenNullAndEmptyString()) {
			Assert.assertThat(distinctCombinationsOfColumnValues.size(), is(3));
			Assert.assertThat(distinctCombinationsOfColumnValues.get(0).get(marque).isEmptyRow(), is(true));
			Assert.assertThat(distinctCombinationsOfColumnValues.get(1).get(marque), notNullValue());
			Assert.assertThat(distinctCombinationsOfColumnValues.get(1).get(marque).individualAllocationsAllowed.stringValue(), is(""));
			Assert.assertThat(distinctCombinationsOfColumnValues.get(2).get(marque), notNullValue());
			Assert.assertThat(distinctCombinationsOfColumnValues.get(2).get(marque).individualAllocationsAllowed.stringValue(), is("Y"));
			Assert.assertThat(distinctCombinationsOfColumnValues.get(0).get(carCo).name.stringValue(), is("OTHER"));
			Assert.assertThat(distinctCombinationsOfColumnValues.get(1).get(carCo).name.stringValue(), is("OTHER"));
			Assert.assertThat(distinctCombinationsOfColumnValues.get(2).get(carCo).name.stringValue(), is("OTHER"));
		} else {
			Assert.assertThat(distinctCombinationsOfColumnValues.size(), is(2));
			Assert.assertThat(distinctCombinationsOfColumnValues.get(0).get(marque).isEmptyRow(), is(true));
			Assert.assertThat(distinctCombinationsOfColumnValues.get(1).get(marque), notNullValue());
			Assert.assertThat(distinctCombinationsOfColumnValues.get(1).get(marque).individualAllocationsAllowed.stringValue(), is("Y"));
			Assert.assertThat(distinctCombinationsOfColumnValues.get(0).get(carCo).name.stringValue(), is("OTHER"));
			Assert.assertThat(distinctCombinationsOfColumnValues.get(1).get(carCo).name.stringValue(), is("OTHER"));
		}
	}

}
