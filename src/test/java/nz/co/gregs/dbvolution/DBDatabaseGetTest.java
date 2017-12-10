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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import net.sourceforge.tedhi.DateRange;
import net.sourceforge.tedhi.FlexibleDateFormat;
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
public class DBDatabaseGetTest extends AbstractTest {

	Marque myTableRow = new Marque();
	List<Marque> myTableRows = new ArrayList<>();

	public DBDatabaseGetTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testGetAllRows() throws SQLException {
		List<Marque> allMarques = database
				.getDBTable(new Marque())
				.setBlankQueryAllowed(true)
				.getAllRows();
		Assert.assertTrue("Incorrect number of marques retreived", allMarques.size() == marqueRows.size());
	}

	@Test
	public void testGetFirstAndPrimaryKey() throws SQLException {
		List<Marque> singleMarque = new ArrayList<>();
		DBRow row = marqueRows.get(0);
		Object primaryKey;
		if (row != null) {
			primaryKey = row.getPrimaryKeys().get(0).getValue();
			Marque marque = new Marque();
			if (primaryKey instanceof Long) {
				marque.uidMarque.permittedValues((Long) primaryKey);
			} else if (primaryKey instanceof Integer) {
				marque.uidMarque.permittedValues((Integer) primaryKey);
			} else {
				throw new ClassCastException("Primary Key should be an Integer or Long");
			}
			singleMarque = database.get(marque);
		}
		Assert.assertTrue("Incorrect number of marques retreived", singleMarque.size() == 1);
	}

	@Test
	public void newDBRowWillCauseBlankQuery() {
		Marque marque = new Marque();
		Assert.assertThat(database.willCreateBlankQuery(marque), is(true));
	}

	@Test
	public void newAlteredDBRowWillCauseBlankQuery() {
		Marque marque = new Marque();
		marque.name.permittedValues("HOLDEN");
		Assert.assertThat(database.willCreateBlankQuery(marque), is(false));
	}

	@Test
	public void testNumberIsBetween() throws SQLException {
		Marque marqueQuery = new Marque();
		marqueQuery.getUidMarque().permittedRange(0, 90000000);
		List<Marque> gotMarques = database.get(marqueQuery);
		Assert.assertTrue("Incorrect number of marques retreived", gotMarques.size() == marqueRows.size());
	}

	@Test
	public void testNumberIsBetweenExclusive() throws SQLException {
		Marque marqueQuery = new Marque();
		marqueQuery.getUidMarque().permittedRangeExclusive(0, 2);
		List<Marque> gotMarques = database.get(marqueQuery);
		Assert.assertTrue("Incorrect number of marques retreived", gotMarques.size() == 1);
	}

	@Test
	public void testNumberIsGreaterThan() throws SQLException {
		Marque marqueQuery = new Marque();
		marqueQuery.getUidMarque().permittedRangeExclusive(2, null);
		List<Marque> gotMarques = database.get(marqueQuery);
		Assert.assertThat(gotMarques.size(), is(20));
	}

	@Test
	public void testNumberIsLessThan() throws SQLException {
		Marque marqueQuery = new Marque();
		marqueQuery.getUidMarque().permittedRangeExclusive(null, 2);
		List<Marque> gotMarques = database.get(marqueQuery);
		Assert.assertThat(gotMarques.size(), is(1));
	}

	@Test
	public void testStringIsBetweenExclusive() throws SQLException {
		Marque marqueQuery = new Marque();
		marqueQuery.name.permittedRangeExclusive("FORD", "TOYOTA");
		List<Marque> gotMarques = database.get(marqueQuery);

		Assert.assertThat(gotMarques.size(), is(14));
	}

	@Test
	public void testIsLiterally() throws SQLException {
		Marque literalQuery = new Marque();
		literalQuery.getUidMarque().permittedValues(4893059);
		List<Marque> gotMarques = database.get(literalQuery);
		Assert.assertEquals(1, gotMarques.size());
		Assert.assertEquals("" + 4893059, gotMarques.get(0).getPrimaryKeys().get(0).toSQLString(database.getDefinition()));
	}

	@Test
	public void testIsNull() throws SQLException {
		Marque literalQuery = new Marque();
		literalQuery.individualAllocationsAllowed.permittedValues((Object) null);
		List<Marque> gotMarques = database.get(literalQuery);
		if (database.getDefinition().supportsDifferenceBetweenNullAndEmptyString()) {
			Assert.assertEquals(2, gotMarques.size());
		} else {
			Assert.assertEquals(gotMarques.size(), database.getDBTable(new Marque()).count() - 1);
		}
		Assert.assertEquals(true, gotMarques.get(0).individualAllocationsAllowed.isNull());
	}

	@Test
	public void testIsNotNull() throws SQLException {
		Marque literalQuery = new Marque();
		literalQuery.individualAllocationsAllowed.excludedValues((String) null);
		List<Marque> gotMarques = database.get(literalQuery);
		if (database.getDefinition().supportsDifferenceBetweenNullAndEmptyString()) {
			Assert.assertEquals(gotMarques.size(), database.getDBTable(new Marque()).count() - 2);
		} else {
			Assert.assertEquals(1, gotMarques.size());
		}
		Assert.assertEquals(true, gotMarques.get(0).individualAllocationsAllowed.isNotNull());
	}

	@Test
	public void testMultiplePermittedValues() throws SQLException {
		Marque literalQuery = new Marque();
		literalQuery.getUidMarque().permittedValues(4893059, 4893090);
		List<Marque> gotMarques = database.get(literalQuery);
		Assert.assertThat(gotMarques.size(), is(2));
		Assert.assertThat(gotMarques.get(0).getPrimaryKeys().get(0).toSQLString(database.getDefinition()), is("" + 4893059));
		Assert.assertThat(gotMarques.get(1).getPrimaryKeys().get(0).toSQLString(database.getDefinition()), is("" + 4893090));
	}

	@Test
	public void testIsIn() throws SQLException {
		Marque hummerQuery = new Marque();
		hummerQuery.getName().permittedValues("PEUGEOT", "HUMMER");
		List<Marque> gotMarques = database.get(hummerQuery);

		Assert.assertThat(gotMarques.size(), is(2));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testIsInWithList() throws SQLException {
		Marque hummerQuery = new Marque();
		List<String> permittedMarques = new ArrayList<>();
		permittedMarques.add("PEUGEOT");
		permittedMarques.add("HUMMER");
		hummerQuery.getName().permittedValues(permittedMarques);
		List<Marque> gotMarques = database.get(hummerQuery);
		Assert.assertThat(gotMarques.size(), is(2));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testIsExcludedWithList() throws SQLException {
		Marque hummerQuery = new Marque();
		List<Marque> allMarques = database.getDBTable(hummerQuery).setBlankQueryAllowed(true).getAllRows();
		List<String> permittedMarques = new ArrayList<>();
		permittedMarques.add("PEUGEOT");
		permittedMarques.add("HUMMER");
		hummerQuery.getName().excludedValues(permittedMarques);
		List<Marque> gotMarques = database.get(hummerQuery);

		Assert.assertThat(gotMarques.size(), is(allMarques.size() - 2));
	}

	@Test
	public void testDateIsBetween() throws SQLException, ParseException {

		Date beforeAllTheDates = FlexibleDateFormat.parse("July 2010", Locale.ENGLISH).toDate();
		Date afterAllTheDates = FlexibleDateFormat.parse("July 2013", Locale.ENGLISH).toDate();
		DateRange coversFirstDate = TEDHI_RANGE_FORMAT.parse("March 2013");

		Marque oldQuery = new Marque();
//        oldQuery.getCreationDate().permittedRange(new Date(0L), afterAllTheDates);
		oldQuery.getCreationDate().permittedRange(beforeAllTheDates, afterAllTheDates);
		List<Marque> gotMarques = database.get(oldQuery);
		Assert.assertTrue("Wrong number of rows selected, should be all but one of them", gotMarques.size() == marqueRows.size() - 1);

		oldQuery.getCreationDate().permittedRange(coversFirstDate.getStart(), coversFirstDate.getEnd());
		gotMarques = database.get(oldQuery);
		Assert.assertThat(gotMarques.size(), is(18));
	}

	@Test
	public void testDateIsLessThanAndGreaterThan() throws SQLException {
		GregorianCalendar gregorianCalendar = new GregorianCalendar();
		gregorianCalendar.add(Calendar.SECOND, 60);
		Date future = gregorianCalendar.getTime();
		Marque oldQuery = new Marque();
		oldQuery.getCreationDate().permittedRange(null, future);
		List<Marque> gotMarques = database.get(oldQuery);
		Assert.assertTrue("Wrong number of rows selected, should be all but one of them", gotMarques.size() == marqueRows.size() - 1);
		oldQuery.getCreationDate().permittedRange(future, null);
		gotMarques = database.get(oldQuery);

		Assert.assertTrue("Wrong number of rows selected, should be NONE of them", gotMarques.isEmpty());
		oldQuery = new Marque();
		oldQuery.getCreationDate().permittedRangeInclusive(null, future);
		gotMarques = database.get(oldQuery);

		Assert.assertTrue("Wrong number of rows selected, should be all but one of them", gotMarques.size() == marqueRows.size() - 1);
		oldQuery = new Marque();
		oldQuery.getCreationDate().permittedRangeExclusive(null, future);
		gotMarques = database.get(oldQuery);

		Assert.assertTrue("Wrong number of rows selected, should be all but one of them", gotMarques.size() == marqueRows.size() - 1);
		oldQuery.getCreationDate().permittedRange(new Date(0L), null);
		gotMarques = database.get(oldQuery);

		Assert.assertTrue("Wrong number of rows selected, should be all but one of them", gotMarques.size() == marqueRows.size() - 1);
		oldQuery.getCreationDate().permittedRange(null, new Date(0L));
		gotMarques = database.get(oldQuery);

		Assert.assertTrue("Wrong number of rows selected, should be NONE of them", gotMarques.isEmpty());
		oldQuery = new Marque();
		oldQuery.getCreationDate().permittedRangeInclusive(new Date(0L), null);
		gotMarques = database.get(oldQuery);

		Assert.assertTrue("Wrong number of rows selected, should be all but one of them", gotMarques.size() == marqueRows.size() - 1);
	}

	@Test
	public void testIgnoringColumnsOnTable() throws SQLException {
		Marque myMarqueRow = new Marque();
		myMarqueRow.setReturnFields(myMarqueRow.name, myMarqueRow.uidMarque, myMarqueRow.carCompany);
		List<Marque> rowsByExample = database.getDBTable(myMarqueRow).setBlankQueryAllowed(true).getAllRows();
		for (Marque marq : rowsByExample) {
			Assert.assertThat(marq.auto_created.isNull(), is(true));
			Assert.assertThat(marq.creationDate.isNull(), is(true));
			Assert.assertThat(marq.enabled.isNull(), is(true));
			Assert.assertThat(marq.individualAllocationsAllowed.isNull(), is(true));
			Assert.assertThat(marq.isUsedForTAFROs.isNull(), is(true));
			Assert.assertThat(marq.numericCode.isNull(), is(true));
			Assert.assertThat(marq.pricingCodePrefix.isNull(), is(true));
			Assert.assertThat(marq.reservationsAllowed.isNull(), is(true));
			Assert.assertThat(marq.statusClassID.isNull(), is(true));

			Assert.assertThat(marq.name.isNull(), is(false));
			Assert.assertThat(marq.uidMarque.isNull(), is(false));
		}
	}

	@Test
	public void testUnignoringColumnsOnTable() throws SQLException {
		Marque myMarqueRow = new Marque();
		myMarqueRow.auto_created.excludedValues((String) null);
		myMarqueRow.isUsedForTAFROs.excludedValues((String) null);
		myMarqueRow.reservationsAllowed.excludedValues((String) null);
		myMarqueRow.statusClassID.excludedValues((Number) null);
		myMarqueRow.name.excludedValues((String) null);
		myMarqueRow.uidMarque.excludedValues((Integer) null);

		myMarqueRow.setReturnFields(myMarqueRow.name, myMarqueRow.uidMarque, myMarqueRow.carCompany);
		myMarqueRow.returnAllFields();

		List<Marque> rowsByExample = database.getDBTable(myMarqueRow).getAllRows();
		for (Marque marq : rowsByExample) {
			Assert.assertThat(marq.auto_created.isNull(), is(false));
			Assert.assertThat(marq.isUsedForTAFROs.isNull(), is(false));
			Assert.assertThat(marq.reservationsAllowed.isNull(), is(false));
			Assert.assertThat(marq.statusClassID.isNull(), is(false));
			Assert.assertThat(marq.name.isNull(), is(false));
			Assert.assertThat(marq.uidMarque.isNull(), is(false));
		}
	}

	@Test
	public void testIgnoringColumnsOnQuery() throws SQLException {
		Marque myMarqueRow = new Marque();
		myMarqueRow.setReturnFields(myMarqueRow.name, myMarqueRow.uidMarque, myMarqueRow.carCompany);
		DBQuery dbQuery = database.getDBQuery(myMarqueRow, new CarCompany());
		dbQuery.setBlankQueryAllowed(true);
		List<DBQueryRow> rowsByExample = dbQuery.getAllRows();
		for (DBQueryRow row : rowsByExample) {
			Marque marq = row.get(new Marque());
			Assert.assertThat(marq.auto_created.isNull(), is(true));
			Assert.assertThat(marq.creationDate.isNull(), is(true));
			Assert.assertThat(marq.enabled.isNull(), is(true));
			Assert.assertThat(marq.individualAllocationsAllowed.isNull(), is(true));
			Assert.assertThat(marq.isUsedForTAFROs.isNull(), is(true));
			Assert.assertThat(marq.numericCode.isNull(), is(true));
			Assert.assertThat(marq.pricingCodePrefix.isNull(), is(true));
			Assert.assertThat(marq.reservationsAllowed.isNull(), is(true));
			Assert.assertThat(marq.statusClassID.isNull(), is(true));

			Assert.assertThat(marq.name.isNull(), is(false));
			Assert.assertThat(marq.uidMarque.isNull(), is(false));
		}

	}

	@Test
	@SuppressWarnings("unchecked")
	public void testUnignoringColumnsOnQuery() throws SQLException {
		Marque myMarqueRow = new Marque();
		myMarqueRow.auto_created.excludedValues((String) null);
		myMarqueRow.isUsedForTAFROs.excludedValues((String) null);
		myMarqueRow.reservationsAllowed.excludedValues((String) null);
		myMarqueRow.statusClassID.excludedValues((Number) null);
		myMarqueRow.name.excludedValues((String) null);
		myMarqueRow.uidMarque.excludedValues((Integer) null);

		myMarqueRow.setReturnFields(myMarqueRow.name, myMarqueRow.uidMarque, myMarqueRow.carCompany);
		myMarqueRow.returnAllFields();
		DBQuery dbQuery = database.getDBQuery(myMarqueRow, new CarCompany());
		dbQuery.setBlankQueryAllowed(true);
		List<DBQueryRow> rowsByExample = dbQuery.getAllRows();
		for (DBQueryRow row : rowsByExample) {
			Marque marq = row.get(new Marque());
			Assert.assertThat(marq.auto_created.isNull(), is(false));
			Assert.assertThat(marq.isUsedForTAFROs.isNull(), is(false));
			Assert.assertThat(marq.reservationsAllowed.isNull(), is(false));
			Assert.assertThat(marq.statusClassID.isNull(), is(false));
			Assert.assertThat(marq.name.isNull(), is(false));
			Assert.assertThat(marq.uidMarque.isNull(), is(false));
		}
	}
}
