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
import nz.co.gregs.dbvolution.query.RowDefinition;
import static org.hamcrest.Matchers.is;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author gregorygraham
 */
public class OutputFormatTest extends AbstractTest{

	public OutputFormatTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}
	
	@Test
	public void testFormatAllTSV() throws SQLException {
		System.out.println("nz.co.gregs.dbvolution.OutputFormatTest.testFormatAllTSV()");
		
		DBQuery dbQuery = database.getDBQuery();
		CarCompany carCompany = new CarCompany();
		carCompany.name.permittedValues("TOYOTA");
		dbQuery.add(new Marque());
		dbQuery.add(carCompany);
		// make sure it works
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		Assert.assertThat(allRows.size(), is(2));
		
		String formatDBQueryRows = OutputFormat.TSV.formatDBQueryRows("headerrow", "headercell", "rowstyle", DATETIME_FORMAT, allRows);
		System.out.println(formatDBQueryRows);
		final String newline = System.getProperty("line.separator");
		Assert.assertThat(formatDBQueryRows, is("Marque:numericCode	Marque:uidMarque	Marque:isUsedForTAFROs	Marque:statusClassID	Marque:individualAllocationsAllowed	Marque:updateCount	Marque:auto_created	Marque:name	Marque:pricingCodePrefix	Marque:reservationsAllowed	Marque:creationDate	Marque:enabled	Marque:carCompany	CarCompany:name	CarCompany:uidCarCompany" +newline+
"	1	False	1246974.00000		0		TOYOTA		Y	23/Mar/2013 12:34:56	true	1	TOYOTA	1" +newline+
"	4896300	False	1246974.00000		2	UV	HYUNDAI		Y	23/Mar/2013 12:34:56		1	TOYOTA	1" +newline));
	}

	@Test
	public void testFormatAllTableTSV() throws SQLException {
		System.out.println("nz.co.gregs.dbvolution.OutputFormatTest.testFormatAllTableTSV()");
		
		CarCompany carCompany = new CarCompany();
		carCompany.name.permittedValues("TOYOTA");
		DBTable<CarCompany> dbTable = database.getDBTable(carCompany);
		// make sure it works
		List<CarCompany> allRows = dbTable.getAllRows();
		Assert.assertThat(allRows.size(), is(1));
		final CarCompany[] allRowsArray = allRows.toArray(new CarCompany[]{});
		
		String formatDBRows = OutputFormat.TSV.formatDBRows("headerrow", "headercell", "rowstyle", DATETIME_FORMAT, allRowsArray);
		System.out.println(formatDBRows);
		formatDBRows = OutputFormat.TSV.formatDBRows(DATETIME_FORMAT, allRowsArray);
		System.out.println(formatDBRows);
		final String newline = System.getProperty("line.separator");
		Assert.assertThat(formatDBRows, is("name	uidCarCompany"+newline+"TOYOTA	1"+newline));
	}

	@Test
	public void testFormatAllTableCSV() throws SQLException {
		System.out.println("nz.co.gregs.dbvolution.OutputFormatTest.testFormatAllTableTSV()");
		
		CarCompany carCompany = new CarCompany();
		carCompany.name.permittedValues("TOYOTA");
		DBTable<CarCompany> dbTable = database.getDBTable(carCompany);
		// make sure it works
		List<CarCompany> allRows = dbTable.getAllRows();
		Assert.assertThat(allRows.size(), is(1));
		final CarCompany[] allRowsArray = allRows.toArray(new CarCompany[]{});
		
		final String newline = System.getProperty("line.separator");

		String formatDBRows = OutputFormat.CSV.formatDBRows("headerrow", "headercell", "rowstyle", DATETIME_FORMAT, allRowsArray);
		System.out.println(formatDBRows);
		Assert.assertThat(formatDBRows, is("\"name\", \"uidCarCompany\""+newline+"\"TOYOTA\", \"1\""+newline));

		formatDBRows = OutputFormat.CSV.formatDBRows(DATETIME_FORMAT, allRowsArray);
		System.out.println(formatDBRows);
		Assert.assertThat(formatDBRows, is("\"name\", \"uidCarCompany\""+newline+"\"TOYOTA\", \"1\""+newline));
	}

	
	@Test
	public void testFormatAllCSV() throws SQLException {
		System.out.println("nz.co.gregs.dbvolution.OutputFormatTest.testFormatAllTSV()");
		
		DBQuery dbQuery = database.getDBQuery();
		CarCompany carCompany = new CarCompany();
		carCompany.name.permittedValues("TOYOTA");
		dbQuery.add(new Marque());
		dbQuery.add(carCompany);
		// make sure it works
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		Assert.assertThat(allRows.size(), is(2));
		
		String formatDBQueryRows = OutputFormat.CSV.formatDBQueryRows("headerrow", "headercell", "rowstyle", DATETIME_FORMAT, allRows);
		System.out.println(formatDBQueryRows);
		final String newline = System.getProperty("line.separator");
		Assert.assertThat(formatDBQueryRows, is("\"Marque:numericCode\", \"Marque:uidMarque\", \"Marque:isUsedForTAFROs\", \"Marque:statusClassID\", \"Marque:individualAllocationsAllowed\", \"Marque:updateCount\", \"Marque:auto_created\", \"Marque:name\", \"Marque:pricingCodePrefix\", \"Marque:reservationsAllowed\", \"Marque:creationDate\", \"Marque:enabled\", \"Marque:carCompany\", \"CarCompany:name\", \"CarCompany:uidCarCompany\""+newline+"\"\", \"1\", \"False\", \"1246974.00000\", \"\", \"0\", \"\", \"TOYOTA\", \"\", \"Y\", \"23/Mar/2013 12:34:56\", \"true\", \"1\", \"TOYOTA\", \"1\""+newline+"\"\", \"4896300\", \"False\", \"1246974.00000\", \"\", \"2\", \"UV\", \"HYUNDAI\", \"\", \"Y\", \"23/Mar/2013 12:34:56\", \"\", \"1\", \"TOYOTA\", \"1\""+newline));
	}

	
	
}
