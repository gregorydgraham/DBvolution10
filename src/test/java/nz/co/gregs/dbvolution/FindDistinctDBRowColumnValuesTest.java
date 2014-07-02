/*
 * Copyright 2014 gregory.graham.
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
import org.junit.Test;
import org.junit.Assert;
import static org.hamcrest.Matchers.*;

import nz.co.gregs.dbvolution.example.*;
import nz.co.gregs.dbvolution.generic.AbstractTest;

/**
 *
 * @author gregory.graham
 */
public class FindDistinctDBRowColumnValuesTest extends AbstractTest {

	public FindDistinctDBRowColumnValuesTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testManualMethod() throws SQLException {
		List<DBDate> creationDates = new ArrayList<DBDate>();
		Marque marque = new Marque();
		marque.returnFieldsLimitedTo(marque.creationDate);
		DBQuery dbQuery = database.getDBQuery(marque).addGroupByColumn(marque, marque.column(marque.creationDate));
		dbQuery.setBlankQueryAllowed(true);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(3));
		for (DBQueryRow dBQueryRow : allRows) {
			Marque get = dBQueryRow.get(marque);
			creationDates.add(get == null ? null : get.creationDate);
		}
		Assert.assertThat(creationDates.size(), is(3));
		Assert.assertThat(creationDates, hasItem((DBDate) null));
		
		List<String> foundStrings = new ArrayList<String>();
		for (DBDate dBDate : creationDates) {
			if (dBDate != null) {
				System.out.println("DISTINCT CREATION DATE: " + dBDate.toString());
				Assert.assertThat(dBDate.toString(),
						anyOf(
								is("2013-03-23"),
								is("2013-04-02")
						)
				);
				foundStrings.add((dBDate.toString()));
			}
		}
		Assert.assertThat(foundStrings.size(), is(2));
		Assert.assertThat(foundStrings, hasItems("2013-03-23", "2013-04-02"));
	}

	@Test
	public void testDBRowMethod() throws SQLException {
		Marque marque = new Marque();
		List<DBDate> distinctValuesForColumn = marque.getDistinctValuesForColumn(database, marque.creationDate);
		Assert.assertThat(distinctValuesForColumn.size(), is(3));
		Assert.assertThat(distinctValuesForColumn, hasItem((DBDate) null));
		
		List<String> foundStrings = new ArrayList<String>();
		for (DBDate dBDate : distinctValuesForColumn) {
			if (dBDate != null) {
				System.out.println("DISTINCT CREATION DATE: " + dBDate.toString());
				Assert.assertThat(dBDate.toString(),
						anyOf(
								is("2013-03-23"),
								is("2013-04-02")
						)
				);
				foundStrings.add((dBDate.toString()));
			}
		}
		Assert.assertThat(foundStrings.size(), is(2));
		Assert.assertThat(foundStrings, hasItems("2013-03-23", "2013-04-02"));
	}

}
