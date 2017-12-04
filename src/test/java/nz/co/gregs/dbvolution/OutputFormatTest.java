/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.is;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class OutputFormatTest extends AbstractTest {

	public OutputFormatTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testFormatAllTSV() throws SQLException {

		DBQuery dbQuery = database.getDBQuery();
		CarCompany carCompany = new CarCompany();
		carCompany.name.permittedValues("TOYOTA");
		final Marque marque = new Marque();
		dbQuery.add(marque);
		dbQuery.add(carCompany);
		dbQuery.setSortOrder(marque.column(marque.uidMarque));
		// make sure it works
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		Assert.assertThat(allRows.size(), is(2));

		String formatDBQueryRows = OutputFormat.TSV.formatDBQueryRows("headerrow", "headercell", "rowstyle", DATETIME_FORMAT, allRows);

		final String newline = System.getProperty("line.separator");
		Assert.assertThat(formatDBQueryRows.replaceAll(".00000*", ""), is("Marque:numericCode	Marque:uidMarque	Marque:isUsedForTAFROs	Marque:statusClassID	Marque:individualAllocationsAllowed	Marque:updateCount	Marque:auto_created	Marque:name	Marque:pricingCodePrefix	Marque:reservationsAllowed	Marque:creationDate	Marque:enabled	Marque:carCompany	CarCompany:name	CarCompany:uidCarCompany" + newline
				+ "	1	False	1246974		0		TOYOTA		Y	23/Mar/2013 12:34:56	true	1	TOYOTA	1" + newline
				+ "	4896300	False	1246974		2	UV	HYUNDAI		Y	23/Mar/2013 12:34:56		1	TOYOTA	1" + newline));
	}

	@Test
	public void testFormatAllTableTSV() throws SQLException {
		final String newline = System.getProperty("line.separator");
		CarCompany carCompany = new CarCompany();
		carCompany.name.permittedValues("TOYOTA");
		DBTable<CarCompany> dbTable = database.getDBTable(carCompany);
		// make sure it works
		List<CarCompany> allRows = dbTable.getAllRows();
		Assert.assertThat(allRows.size(), is(1));
		final CarCompany[] allRowsArray = allRows.toArray(new CarCompany[]{});

		String formatDBRows = OutputFormat.TSV.formatDBRows("headerrow", "headercell", "rowstyle", DATETIME_FORMAT, allRowsArray);

		Assert.assertThat(formatDBRows, is("name	uidCarCompany" + newline + "TOYOTA	1" + newline));

		formatDBRows = OutputFormat.TSV.formatDBRows(DATETIME_FORMAT, allRowsArray);

		Assert.assertThat(formatDBRows, is("name	uidCarCompany" + newline + "TOYOTA	1" + newline));
	}

	@Test
	public void testFormatAllTableCSV() throws SQLException {
		CarCompany carCompany = new CarCompany();
		carCompany.name.permittedValues("TOYOTA");
		DBTable<CarCompany> dbTable = database.getDBTable(carCompany);
		// make sure it works
		List<CarCompany> allRows = dbTable.getAllRows();
		Assert.assertThat(allRows.size(), is(1));
		final CarCompany[] allRowsArray = allRows.toArray(new CarCompany[]{});

		final String newline = System.getProperty("line.separator");

		String formatDBRows = OutputFormat.CSV.formatDBRows("headerrow", "headercell", "rowstyle", DATETIME_FORMAT, allRowsArray);

		Assert.assertThat(formatDBRows, is("\"name\", \"uidCarCompany\"" + newline + "\"TOYOTA\", \"1\"" + newline));

		formatDBRows = OutputFormat.CSV.formatDBRows(DATETIME_FORMAT, allRowsArray);

		Assert.assertThat(formatDBRows, is("\"name\", \"uidCarCompany\"" + newline + "\"TOYOTA\", \"1\"" + newline));
	}

	@Test
	public void testFormatAllCSV() throws SQLException {

		DBQuery dbQuery = database.getDBQuery();
		CarCompany carCompany = new CarCompany();
		carCompany.name.permittedValues("TOYOTA");
		final Marque marque = new Marque();
		marque.numericCode.setSortOrderDescending();
		dbQuery.add(marque);
		dbQuery.add(carCompany);
		dbQuery.setSortOrder(marque.column(marque.uidMarque));
		// make sure it works
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		Assert.assertThat(allRows.size(), is(2));

		String formatDBQueryRows = OutputFormat.CSV.formatDBQueryRows("headerrow", "headercell", "rowstyle", DATETIME_FORMAT, allRows);

		final String newline = System.getProperty("line.separator");
		Assert.assertThat(formatDBQueryRows.replaceAll(".00000*", ""), is(("\"Marque:numericCode\", \"Marque:uidMarque\", \"Marque:isUsedForTAFROs\", \"Marque:statusClassID\", \"Marque:individualAllocationsAllowed\", \"Marque:updateCount\", \"Marque:auto_created\", \"Marque:name\", \"Marque:pricingCodePrefix\", \"Marque:reservationsAllowed\", \"Marque:creationDate\", \"Marque:enabled\", \"Marque:carCompany\", \"CarCompany:name\", \"CarCompany:uidCarCompany\"" + newline + "\"\", \"1\", \"False\", \"1246974\", \"\", \"0\", \"\", \"TOYOTA\", \"\", \"Y\", \"23/Mar/2013 12:34:56\", \"true\", \"1\", \"TOYOTA\", \"1\"" + newline + "\"\", \"4896300\", \"False\", \"1246974\", \"\", \"2\", \"UV\", \"HYUNDAI\", \"\", \"Y\", \"23/Mar/2013 12:34:56\", \"\", \"1\", \"TOYOTA\", \"1\"" + newline)));
	}

	@Test
	public void testFormatAllTableHTML() throws SQLException {
		CarCompany carCompany = new CarCompany();
		carCompany.name.permittedValues("TOYOTA");
		DBTable<CarCompany> dbTable = database.getDBTable(carCompany);
		// make sure it works
		List<CarCompany> allRows = dbTable.getAllRows();
		Assert.assertThat(allRows.size(), is(1));
		final CarCompany[] allRowsArray = allRows.toArray(new CarCompany[]{});

		final String newline = System.getProperty("line.separator");

		String formatDBRows = OutputFormat.HTMLTABLE.formatDBRows("headerrow", "headercell", "rowstyle", DATETIME_FORMAT, allRowsArray);

		Assert.assertThat(formatDBRows.replaceAll(".00000", ""), is("<tr class=\"headerrow\"><th class=\"headercell\">name</th><th class=\"headercell\">uidCarCompany</th></tr>" + newline
				+ "<tr class=\"rowstyle\"><td class=\"rowstyle\">TOYOTA</td><td class=\"rowstyle\">1</td></tr>" + newline));

		formatDBRows = OutputFormat.HTMLTABLE.formatDBRows(DATETIME_FORMAT, allRowsArray);

		Assert.assertThat(formatDBRows.replaceAll(".00000", ""), is("<tr class=\"\"><th class=\"\">name</th><th class=\"\">uidCarCompany</th></tr>" + newline
				+ "<tr class=\"\"><td class=\"\">TOYOTA</td><td class=\"\">1</td></tr>" + newline));
	}

	@Test
	public void testFormatAllHTML() throws SQLException {
		DBQuery dbQuery = database.getDBQuery();
		CarCompany carCompany = new CarCompany();
		carCompany.name.permittedValues("TOYOTA");
		final Marque marque = new Marque();
		dbQuery.add(marque);
		dbQuery.add(carCompany);
		dbQuery.setSortOrder(marque.column(marque.uidMarque));

		// make sure it works
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		Assert.assertThat(allRows.size(), is(2));

		String formatDBQueryRows = OutputFormat.HTMLTABLE.formatDBQueryRows("headerrow", "headercell", "rowstyle", DATETIME_FORMAT, allRows);

		final String newline = System.getProperty("line.separator");
		Assert.assertThat(formatDBQueryRows.replaceAll(".00000*", ""), is("<tr class=\"headerrow\"><th class=\"headercell\">Marque:numericCode</th><th class=\"headercell\">Marque:uidMarque</th><th class=\"headercell\">Marque:isUsedForTAFROs</th><th class=\"headercell\">Marque:statusClassID</th><th class=\"headercell\">Marque:individualAllocationsAllowed</th><th class=\"headercell\">Marque:updateCount</th><th class=\"headercell\">Marque:auto_created</th><th class=\"headercell\">Marque:name</th><th class=\"headercell\">Marque:pricingCodePrefix</th><th class=\"headercell\">Marque:reservationsAllowed</th><th class=\"headercell\">Marque:creationDate</th><th class=\"headercell\">Marque:enabled</th><th class=\"headercell\">Marque:carCompany</th><th class=\"headercell\">CarCompany:name</th><th class=\"headercell\">CarCompany:uidCarCompany</th></tr>" + newline
				+ "<tr class=\"rowstyle\"><td class=\"rowstyle\"></td><td class=\"rowstyle\">1</td><td class=\"rowstyle\">False</td><td class=\"rowstyle\">1246974</td><td class=\"rowstyle\"></td><td class=\"rowstyle\">0</td><td class=\"rowstyle\"></td><td class=\"rowstyle\">TOYOTA</td><td class=\"rowstyle\"></td><td class=\"rowstyle\">Y</td><td class=\"rowstyle\">23/Mar/2013 12:34:56</td><td class=\"rowstyle\">true</td><td class=\"rowstyle\">1</td><td class=\"rowstyle\">TOYOTA</td><td class=\"rowstyle\">1</td></tr>" + newline
				+ "<tr class=\"rowstyle\"><td class=\"rowstyle\"></td><td class=\"rowstyle\">4896300</td><td class=\"rowstyle\">False</td><td class=\"rowstyle\">1246974</td><td class=\"rowstyle\"></td><td class=\"rowstyle\">2</td><td class=\"rowstyle\">UV</td><td class=\"rowstyle\">HYUNDAI</td><td class=\"rowstyle\"></td><td class=\"rowstyle\">Y</td><td class=\"rowstyle\">23/Mar/2013 12:34:56</td><td class=\"rowstyle\"></td><td class=\"rowstyle\">1</td><td class=\"rowstyle\">TOYOTA</td><td class=\"rowstyle\">1</td></tr>" + newline));
	}

}
