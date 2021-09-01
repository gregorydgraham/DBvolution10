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
import static org.hamcrest.MatcherAssert.assertThat;
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
				.setSortOrder(marque.column(marque.creationDate).getSortProvider().nullsLowest());
		dbQuery.setBlankQueryAllowed(true);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(3));
		for (DBQueryRow dBQueryRow : allRows) {
			Marque get = dBQueryRow.get(marque);
			creationDates.add(get == null ? null : get.creationDate);
		}
		assertThat(creationDates.size(), is(3));
		assertThat(creationDates.get(0).isNull(), is(true));

		List<String> foundStrings = new ArrayList<String>();
		for (DBDate dBDate : creationDates) {
			if (dBDate != null && dBDate.isNotNull()) {
				assertThat(dBDate.toString(),
						anyOf(
								startsWith("2013-03-23"),
								startsWith("2011-04-02")
						)
				);
				foundStrings.add((dBDate.toString().substring(0, 10)));
			}
		}
		assertThat(foundStrings.size(), is(2));
		assertThat(foundStrings, hasItems("2013-03-23", "2011-04-02"));
	}

	@Test
	public void testDBRowMethod() throws SQLException {
		Marque marque = new Marque();
		List<DBDate> distinctValuesForColumn = marque.getDistinctValuesOfColumn(database, marque.creationDate);

		assertThat(distinctValuesForColumn.size(), is(3));
		DBDate[] toArray = distinctValuesForColumn.toArray(new DBDate[]{});
		assertThat(toArray[0].isNull(), is(true));

		List<String> foundStrings = new ArrayList<String>();
		for (DBDate dBDate : distinctValuesForColumn) {
			if (dBDate != null && dBDate.isNotNull()) {
				assertThat(dBDate.toString(),
						anyOf(
								startsWith("2013-03-23"),
								startsWith("2011-04-02")
						)
				);
				foundStrings.add((dBDate.toString().substring(0, 10)));
			}
		}
		assertThat(foundStrings.size(), is(2));
		assertThat(foundStrings, hasItems("2013-03-23", "2011-04-02"));
		assertThat(foundStrings.get(0), is("2011-04-02"));
		assertThat(foundStrings.get(1), is("2013-03-23"));
	}

	@Test
	public void testDBRowMethodWithDBString() throws SQLException {
		Marque marque = new Marque();
		List<DBString> distinctValuesForColumn = marque.getDistinctValuesOfColumn(database, marque.individualAllocationsAllowed);
		if (database.supportsDifferenceBetweenNullAndEmptyString()) {
			assertThat(distinctValuesForColumn.size(), is(3));
			List<String> foundStrings = new ArrayList<>();
			for (DBString val : distinctValuesForColumn) {
				if (val != null && val.isNotNull()) {
					assertThat(val.toString(),
							anyOf(
									is("Y"),
									is("")
							)
					);
					foundStrings.add((val.toString()));
				}
			}
			assertThat(foundStrings.size(), is(2));
			assertThat(foundStrings, hasItems("Y", ""));
			assertThat(foundStrings.get(0), is(""));
			assertThat(foundStrings.get(1), is("Y"));
		} else {
			assertThat(distinctValuesForColumn.size(), is(2));

			List<String> foundStrings = new ArrayList<>();
			for (DBString val : distinctValuesForColumn) {
				if (val != null && val.isNotNull()) {
					assertThat(val.toString(),
							anyOf(
									is(""),
									is("Y")
							)
					);
					foundStrings.add((val.toString()));
				}
			}
			assertThat(foundStrings.size(), is(2));
			assertThat(foundStrings, hasItems("Y", ""));
			assertThat(foundStrings.get(0), is(""));
			assertThat(foundStrings.get(1), is("Y"));
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
				assertThat(val.toString(),
						anyOf(
								is("Y"),
								is("")
						)
				);
			}
			foundStrings.add((val.toString()));
		}
		if (database.supportsDifferenceBetweenNullAndEmptyString()) {
			assertThat(distinctValuesForColumn.size(), is(3));
			assertThat(foundStrings.size(), is(3));
			assertThat(foundStrings.get(0), is(""));
			assertThat(foundStrings.get(1), is(""));
			assertThat(foundStrings.get(2), is("Y"));
		} else {
			assertThat(distinctValuesForColumn.size(), is(2));
			assertThat(foundStrings.size(), is(2));
			assertThat(foundStrings.get(1), is("Y"));
			assertThat(foundStrings.get(0), is(""));
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

		if (database.supportsDifferenceBetweenNullAndEmptyString()) {
			assertThat(distinctCombinationsOfColumnValues.size(), is(3));
			assertThat(distinctCombinationsOfColumnValues.get(0).get(marque).isEmptyRow(), is(true));
			assertThat(distinctCombinationsOfColumnValues.get(1).get(marque), notNullValue());
			assertThat(distinctCombinationsOfColumnValues.get(1).get(marque).individualAllocationsAllowed.stringValue(), is(""));
			assertThat(distinctCombinationsOfColumnValues.get(2).get(marque), notNullValue());
			assertThat(distinctCombinationsOfColumnValues.get(2).get(marque).individualAllocationsAllowed.stringValue(), is("Y"));
			assertThat(distinctCombinationsOfColumnValues.get(0).get(carCo).name.stringValue(), is("OTHER"));
			assertThat(distinctCombinationsOfColumnValues.get(1).get(carCo).name.stringValue(), is("OTHER"));
			assertThat(distinctCombinationsOfColumnValues.get(2).get(carCo).name.stringValue(), is("OTHER"));
		} else {
			assertThat(distinctCombinationsOfColumnValues.size(), is(2));
			assertThat(distinctCombinationsOfColumnValues.get(0).get(marque).isEmptyRow(), is(true));
			assertThat(distinctCombinationsOfColumnValues.get(1).get(marque), notNullValue());
			assertThat(distinctCombinationsOfColumnValues.get(1).get(marque).individualAllocationsAllowed.stringValue(), is("Y"));
			assertThat(distinctCombinationsOfColumnValues.get(0).get(carCo).name.stringValue(), is("OTHER"));
			assertThat(distinctCombinationsOfColumnValues.get(1).get(carCo).name.stringValue(), is("OTHER"));
		}
	}

}
