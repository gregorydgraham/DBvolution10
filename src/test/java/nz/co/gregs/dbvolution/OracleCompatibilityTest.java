/*
 * Copyright 2020 Gregory Graham.
 *
 * Commercial licenses are available, please contact info@gregs.co.nz for details.
 * 
 * This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. 
 * To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/4.0/ 
 * or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.
 * 
 * You are free to:
 *     Share - copy and redistribute the material in any medium or format
 *     Adapt - remix, transform, and build upon the material
 * 
 *     The licensor cannot revoke these freedoms as long as you follow the license terms.               
 *     Under the following terms:
 *                 
 *         Attribution - 
 *             You must give appropriate credit, provide a link to the license, and indicate if changes were made. 
 *             You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.
 *         NonCommercial - 
 *             You may not use the material for commercial purposes.
 *         ShareAlike - 
 *             If you remix, transform, or build upon the material, 
 *             you must distribute your contributions under the same license as the original.
 *         No additional restrictions - 
 *             You may not apply legal terms or technological measures that legally restrict others from doing anything the 
 *             license permits.
 * 
 * Check the Creative Commons website for any details, legalese, and updates.
 */
package nz.co.gregs.dbvolution;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author gregorygraham
 */
public class OracleCompatibilityTest extends AbstractTest {

	public OracleCompatibilityTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Before
	public void requireOracleCompatibility() {
//		database.setRequiredToProduceEmptyStringsForNull(true);
	}

	@After
	public void removeOracleCompatibility() {
//		database.setRequiredToProduceEmptyStringsForNull(false);
	}

	@Test
	public void testDBRowMethodWithDBString() throws SQLException {
		Marque marque = new Marque();
		DBQuery query = database.getDBQuery()
				.add(marque)
				.setQueryLabel("testDBRowMethodWithDBString")
				.setReturnEmptyStringForNullString(true);
		List<DBString> distinctValuesForColumn = query.getDistinctValuesOfColumn(marque.column(marque.individualAllocationsAllowed));
		Assert.assertThat(distinctValuesForColumn.size(), is(2));

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
		Assert.assertThat(foundStrings.size(), is(2));
		Assert.assertThat(foundStrings, hasItems("Y", ""));
		Assert.assertThat(foundStrings.get(0), is(""));
		Assert.assertThat(foundStrings.get(1), is("Y"));
	}

	@Test
	public void testDBTableMethodWithDBString() throws SQLException {
		Marque marque = new Marque();
		final DBTable<Marque> dbTable = database
				.getDBTable(marque)
				.setQueryLabel("testDBTableMethodWithDBString")
				.setReturnEmptyStringForNullString(true);
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
			foundStrings.add((val.toString()));
		}
		Assert.assertThat(distinctValuesForColumn.size(), is(2));
		Assert.assertThat(foundStrings.size(), is(2));
		Assert.assertThat(foundStrings.get(1), is("Y"));
		Assert.assertThat(foundStrings.get(0), is(""));
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
						.setQueryLabel("testDBQueryVersion")
						.setBlankQueryAllowed(true)
						.setReturnEmptyStringForNullString(true)
						.getDistinctCombinationsOfColumnValues(marque.individualAllocationsAllowed, carCo.name);

		Assert.assertThat(distinctCombinationsOfColumnValues.size(), is(2));
		Assert.assertThat(distinctCombinationsOfColumnValues.get(0).get(marque).isEmptyRow(), is(true));
		Assert.assertThat(distinctCombinationsOfColumnValues.get(1).get(marque), notNullValue());
		Assert.assertThat(distinctCombinationsOfColumnValues.get(1).get(marque).individualAllocationsAllowed.stringValue(), is("Y"));
		Assert.assertThat(distinctCombinationsOfColumnValues.get(0).get(carCo).name.stringValue(), is("OTHER"));
		Assert.assertThat(distinctCombinationsOfColumnValues.get(1).get(carCo).name.stringValue(), is("OTHER"));
	}

	@Test
	public void sortingNullsHighest() throws SQLException {
		Marque marque = new Marque();
		List<Marque> allRows
				= database
						.getDBTable(marque)
						.setBlankQueryAllowed(true)
						.setReturnEmptyStringForNullString(true)
						.setSortOrder(marque.column(marque.individualAllocationsAllowed)
								.descending()
								.nullsHighest()
						).getAllRows();
		Assert.assertThat(allRows.size(), is(22));

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

	@Test
	public void sortingNullsLast() throws SQLException {
		Marque marque = new Marque();
		List<Marque> allRows
				= database
						.getDBTable(marque)
						.setReturnEmptyStringForNullString(true)
						.setBlankQueryAllowed(true)
						.setSortOrder(marque.column(marque.individualAllocationsAllowed)
								.ascending()
								.nullsLast()
						).getAllRows();
		Assert.assertThat(allRows.size(), is(22));

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

	@Test
	public void testIsNotStringExpressionNull() throws SQLException {
		Marque likeQuery = new Marque();
		final StringExpression nullExpr = StringExpression.nullString();
		likeQuery.individualAllocationsAllowed.excludedValues(nullExpr);

		List<Marque> rowsByExample
				= DBTable.getInstance(database, new Marque())
						.setReturnEmptyStringForNullString(true)
						.getRowsByExample(likeQuery);

		Assert.assertEquals(1, rowsByExample.size());
	}
}
