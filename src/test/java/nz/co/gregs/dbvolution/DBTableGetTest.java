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
import nz.co.gregs.dbvolution.example.MarqueSelectQuery;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import nz.co.gregs.dbvolution.operators.DBBetweenOperator;
import nz.co.gregs.dbvolution.operators.DBNonOperator;
import static org.hamcrest.Matchers.*;
import org.junit.*;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBTableGetTest extends AbstractTest {
	
	Marque myTableRow = new Marque();
	List<Marque> myTableRows = new ArrayList<Marque>();
	
	public DBTableGetTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}
	
	@Test
	public void testGetAllRows() throws SQLException {
		marquesTable.setBlankQueryAllowed(true);
		Assert.assertThat(marquesTable.getAllRows().size(), is(marqueRows.size()));
	}
	
	@Test
	public void testGetFirstAndPrimaryKey() throws SQLException, ClassNotFoundException {
		DBTable<Marque> singleMarque = DBTable.getInstance(database, new Marque());
		DBRow row = marqueRows.get(0);
		String primaryKey;
		if (row != null) {
			primaryKey = row.getPrimaryKeys().get(0).toSQLString(database.getDefinition());
			singleMarque.getRowsByPrimaryKey(Long.parseLong(primaryKey));
		}
		Assert.assertTrue("Incorrect number of marques retreived", singleMarque.getAllRows().size() == 1);
	}
	
	@Test(expected = AccidentalBlankQueryException.class)
	public void newDBRowWillCauseBlankQuery() throws SQLException {
		Marque marque = new Marque();
		Assert.assertThat(marque.willCreateBlankQuery(database.getDefinition()), is(true));
		database.get(marque);
	}
	
	@Test
	public void newAlteredDBRowWillCauseBlankQuery() {
		Marque marque = new Marque();
		marque.name.permittedValues("HOLDEN");
		Assert.assertThat(marque.willCreateBlankQuery(database.getDefinition()), is(false));
	}
	
	@Test
	public void testNumberIsBetween() throws SQLException {
		Marque marqueQuery = new Marque();
		marqueQuery.getUidMarque().permittedRange(0, 90000000);
		List<Marque> rowsByExample = marquesTable.getRowsByExample(marqueQuery);
		
		Assert.assertTrue("Incorrect number of marques retreived", rowsByExample.size() == marqueRows.size());
	}
	
	@Test
	public void testUsingDBNonOperator() throws SQLException {
		Marque marqueQuery = new Marque();
		marqueQuery.getUidMarque().setOperator(new DBNonOperator());
		List<Marque> rowsByExample = marquesTable.getRowsByExample(marqueQuery);
		
		Assert.assertThat(rowsByExample.size(), is(marqueRows.size()));
	}
	
	@Test
	public void testNumberIsBetweenUsingDBBetweenOperator() throws SQLException {
		Marque marqueQuery = new Marque();
		marqueQuery.getUidMarque().setOperator(new DBBetweenOperator(0, 90000000));
		List<Marque> rowsByExample = marquesTable.getRowsByExample(marqueQuery);
		
		Assert.assertTrue("Incorrect number of marques retreived", rowsByExample.size() == marqueRows.size());
	}
	
	@Test
	public void testNumberIsBetweenExclusive() throws SQLException {
		Marque marqueQuery = new Marque();
		marqueQuery.getUidMarque().permittedRangeExclusive(0, 90000000);
		List<Marque> rowsByExample = marquesTable.getRowsByExample(marqueQuery);
		
		Assert.assertTrue("Incorrect number of marques retreived", rowsByExample.size() == marqueRows.size());
	}
	
	@Test
	public void testNumberIsBetweenInclusive() throws SQLException {
		Marque marqueQuery = new Marque();
		marqueQuery.getUidMarque().permittedRangeInclusive(0, 90000000);
		List<Marque> rowsByExample = marquesTable.getRowsByExample(marqueQuery);
		
		Assert.assertTrue("Incorrect number of marques retreived", rowsByExample.size() == marqueRows.size());
	}
	
	@Test
	public void testIsLiterally() throws SQLException {
		Marque literalQuery = new Marque();
		literalQuery.getUidMarque().permittedValues(4893059);
		List<Marque> rowsByExample = marquesTable.getRowsByExample(literalQuery);
		
		Assert.assertEquals(1, rowsByExample.size());
		Assert.assertEquals("" + 4893059, rowsByExample.get(0).getPrimaryKeys().get(0).toSQLString(database.getDefinition()));
	}
	
	@Test
	public void testIsLiterallyNotWithNull() throws SQLException {
		Marque literalQuery = new Marque();
		literalQuery.getIntIndividualAllocationsAllowed().excludedValues(null, "YES", "");
		List<Marque> rowsByExample = marquesTable.getRowsByExample(literalQuery);
		
		Assert.assertThat(rowsByExample.size(), is(1));
	}
	
	@Test
	public void testIsLike() throws SQLException {
		Marque likeQuery = new Marque();
		likeQuery.name.permittedPattern("TOY%");
		List<Marque> rowsByExample = marquesTable.getRowsByExample(likeQuery);
		
		Assert.assertEquals(1, rowsByExample.size());
		Assert.assertEquals("" + 1, rowsByExample.get(0).getPrimaryKeys().get(0).toSQLString(database.getDefinition()));
	}
	
	@Test
	public void testIsLikeCaseInsensitive() throws SQLException {
		Marque likeQuery = new Marque();
		likeQuery.name.permittedPatternIgnoreCase("TOY%");
		List<Marque> rowsByExample = marquesTable.getRowsByExample(likeQuery);
		
		Assert.assertEquals(1, rowsByExample.size());
		Assert.assertEquals("" + 1, rowsByExample.get(0).getPrimaryKeys().get(0).toSQLString(database.getDefinition()));
	}
	
	@Test
	public void testIsNotLike() throws SQLException {
		Marque likeQuery = new Marque();
		likeQuery.name.excludedPattern("%E%");
		List<Marque> rowsByExample = marquesTable.getRowsByExample(likeQuery);
		
		Assert.assertEquals(14, rowsByExample.size());
	}
	
	@Test
	public void testIsNotLikeStringExpression() throws SQLException {
		Marque likeQuery = new Marque();
		likeQuery.name.excludedPattern(new StringExpression("%e%").uppercase());
		List<Marque> rowsByExample = marquesTable.getRowsByExample(likeQuery);
		
		Assert.assertEquals(14, rowsByExample.size());
	}
	
	@Test
	public void testIsWhileIgnoringCase() throws SQLException {
		Marque literalQuery = new Marque();
		literalQuery.name.permittedValuesIgnoreCase("toYOTA");
		List<Marque> rowsByExample = marquesTable.getRowsByExample(literalQuery);
		
		Assert.assertEquals(1, rowsByExample.size());
		Assert.assertEquals("TOYOTA", rowsByExample.get(0).name.stringValue());
	}
	
	@Test
	public void testIsWhileIgnoringCaseStringExpression() throws SQLException {
		Marque literalQuery = new Marque();
		literalQuery.name.permittedValuesIgnoreCase(new StringExpression("toYOTA").lowercase());
		List<Marque> rowsByExample = marquesTable.getRowsByExample(literalQuery);
		
		Assert.assertEquals(1, rowsByExample.size());
		Assert.assertEquals("TOYOTA", rowsByExample.get(0).name.stringValue());
	}
	
	@Test
	public void testMultiplePermittedValues() throws SQLException {
		Marque literalQuery = new Marque();
		literalQuery.getUidMarque().permittedValues(4893059, 4893090);
		List<Marque> rowsByExample = marquesTable.getRowsByExample(literalQuery);
		
		Assert.assertEquals(2, rowsByExample.size());
		Assert.assertEquals("" + 4893059, rowsByExample.get(0).getPrimaryKeys().get(0).toSQLString(database.getDefinition()));
		Assert.assertEquals("" + 4893090, rowsByExample.get(1).getPrimaryKeys().get(0).toSQLString(database.getDefinition()));
	}
	
	@Test
	public void testIsIn() throws SQLException {
		Marque hummerQuery = new Marque();
		hummerQuery.getName().permittedValues("PEUGEOT", "HUMMER");
		List<Marque> rowsByExample = marquesTable.getRowsByExample(hummerQuery);
		
		Assert.assertThat(rowsByExample.size(), is(2));
	}
	
	@Test
	public void testIsInIgnoreCase() throws SQLException {
		Marque hummerQuery = new Marque();
		hummerQuery.getName().permittedValuesIgnoreCase("PEUGEOT", "HUMMER");
		List<Marque> rowsByExample = marquesTable.getRowsByExample(hummerQuery);
		
		Assert.assertThat(rowsByExample.size(), is(2));
	}
	
	@Test
	public void testIsInWithNull() throws SQLException {
		Marque hummerQuery = new Marque();
		hummerQuery.individualAllocationsAllowed.permittedValues(null, "Y", "YES");
		List<Marque> rowsByExample = marquesTable.getRowsByExample(hummerQuery);
		
		if (database.getDefinition().supportsDifferenceBetweenNullAndEmptyString()) {
			Assert.assertThat(rowsByExample.size(), is(3));
		} else {
			Assert.assertThat(rowsByExample.size(), is(22));
		}
		
		hummerQuery.individualAllocationsAllowed.permittedValues(null, "YES");
		rowsByExample = marquesTable.getRowsByExample(hummerQuery);
		
		if (database.getDefinition().supportsDifferenceBetweenNullAndEmptyString()) {
			Assert.assertThat(rowsByExample.size(), is(2));
		} else {
			Assert.assertThat(rowsByExample.size(), is(21));
		}
	}
	
	@Test
	public void testIsNotInWithNull() throws SQLException {
		Marque query = new Marque();
		query.individualAllocationsAllowed.excludedValues(null, "YES", "");
		List<Marque> rowsByExample = marquesTable.getRowsByExample(query);
		
		Assert.assertThat(rowsByExample.size(), is(1));
	}
	
	@Test
	public void testDateIsBetween() throws SQLException, ParseException {
		
		Date afterAllTheDates = FlexibleDateFormat.parse("July 2013", Locale.ENGLISH).toDate();
		DateRange coversFirstDate = TEDHI_RANGE_FORMAT.parse("March 2013");
		
		Marque oldQuery = new Marque();
		oldQuery.getCreationDate().permittedRange(new Date(0L), afterAllTheDates);
		List<Marque> rowsByExample = marquesTable.getRowsByExample(oldQuery);
		
		Assert.assertTrue("Wrong number of rows selected, should be all but one of them", rowsByExample.size() == marqueRows.size() - 1);
		
		oldQuery.getCreationDate().permittedRange(coversFirstDate.getStart(), coversFirstDate.getEnd());
		rowsByExample = marquesTable.getRowsByExample(oldQuery);
		
		Assert.assertThat(rowsByExample.size(), is(18));
	}
	
	@Test
	public void testDateIsLessThanAndGreaterThan() throws SQLException {
		GregorianCalendar gregorianCalendar = new GregorianCalendar();
		gregorianCalendar.add(Calendar.SECOND, 60);
		Date future = gregorianCalendar.getTime();
		Marque oldQuery = new Marque();
		oldQuery.getCreationDate().permittedRange(null, future);
		List<Marque> rowsByExample = marquesTable.getRowsByExample(oldQuery);
		
		Assert.assertTrue("Wrong number of rows selected, should be all but one of them", rowsByExample.size() == marqueRows.size() - 1);
		oldQuery.getCreationDate().permittedRange(future, null);
		rowsByExample = marquesTable.getRowsByExample(oldQuery);
		
		Assert.assertTrue("Wrong number of rows selected, should be NONE of them", rowsByExample.isEmpty());
		oldQuery = new Marque();
		oldQuery.getCreationDate().permittedRangeInclusive(null, future);
		rowsByExample = marquesTable.getRowsByExample(oldQuery);
		
		Assert.assertTrue("Wrong number of rows selected, should be all but one of them", rowsByExample.size() == marqueRows.size() - 1);
		oldQuery.getCreationDate().permittedRange(new Date(0L), null);
		rowsByExample = marquesTable.getRowsByExample(oldQuery);
		
		Assert.assertTrue("Wrong number of rows selected, should be all but one of them", rowsByExample.size() == marqueRows.size() - 1);
		oldQuery.getCreationDate().permittedRange(null, new Date(0L));
		rowsByExample = marquesTable.getRowsByExample(oldQuery);
		
		Assert.assertTrue("Wrong number of rows selected, should be NONE of them", rowsByExample.isEmpty());
		oldQuery = new Marque();
		oldQuery.getCreationDate().permittedRangeInclusive(new Date(0L), null);
		rowsByExample = marquesTable.getRowsByExample(oldQuery);
		
		Assert.assertTrue("Wrong number of rows selected, should be all but one of them", rowsByExample.size() == marqueRows.size() - 1);
	}
	
	@Test
	public void testRawQuery() throws SQLException {
		String rawQuery = "and lower(name) in ('peugeot','hummer')  ";
		List<Marque> rowsByRawSQL = marquesTable.setRawSQL(rawQuery).getAllRows();
		
		Assert.assertEquals(2, rowsByRawSQL.size());
	}
	
	@Test
	public void testDBSelectQuery() throws SQLException {
		DBTable<MarqueSelectQuery> msq = DBTable.getInstance(database, new MarqueSelectQuery()).setBlankQueryAllowed(true);
		msq.getAllRows();
		
		Assert.assertThat(msq.getAllRows().size(), is(22));
		MarqueSelectQuery marqueSelectQuery = new MarqueSelectQuery();
		marqueSelectQuery.uidMarque.permittedValues(1);
		msq.getRowsByExample(marqueSelectQuery);
		
		Assert.assertThat(msq.getAllRows().size(), is(1));
	}
	
	@Test
	public void testDBSelectQueryJoinedToAnotherTable() throws SQLException {
		DBQuery msq = DBQuery.getInstance(database, new MarqueSelectQuery(), new CarCompany()).setBlankQueryAllowed(true);

		String sqlString = msq.getSQLForQuery();
		Assert.assertThat(sqlString, containsString("(select uid_marque, isusedfortafros, fk_carcompany from marque)"));

		List<DBQueryRow> allRows = msq.getAllRows();
		Assert.assertThat(allRows.size(), is(22));
	}
	
	@Test
	public void testIgnoringColumnsOnTable() throws SQLException {
		Marque myMarqueRow = new Marque();
		myMarqueRow.setReturnFields(myMarqueRow.name, myMarqueRow.uidMarque, myMarqueRow.carCompany);
		List<Marque> rowsByExample = database.getDBTable(myMarqueRow).setBlankQueryAllowed(true).getRowsByExample(myMarqueRow);
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
		List<Marque> rowsByExample = database.getDBTable(myMarqueRow).setBlankQueryAllowed(true).getRowsByExample(myMarqueRow);
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
		List<Marque> rowsByExample = dbQuery.getAllInstancesOf(myMarqueRow);
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
		List<Marque> rowsByExample = dbQuery.getAllInstancesOf(myMarqueRow);
		for (Marque marq : rowsByExample) {
			Assert.assertThat(marq.auto_created.isNull(), is(false));
			Assert.assertThat(marq.isUsedForTAFROs.isNull(), is(false));
			Assert.assertThat(marq.reservationsAllowed.isNull(), is(false));
			Assert.assertThat(marq.statusClassID.isNull(), is(false));
			Assert.assertThat(marq.name.isNull(), is(false));
			Assert.assertThat(marq.uidMarque.isNull(), is(false));
		}
	}
}
