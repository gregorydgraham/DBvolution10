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
import java.util.List;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

public class DBReportTest extends AbstractTest {
	
	public DBReportTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}
	
	@Test
	public void createReportTest() throws SQLException {
		SimpleReport reportExample = new SimpleReport();
		System.out.println(DBReport.getSQLForQuery(database, reportExample));
		List<SimpleReport> simpleReportRows = DBReport.getAllRows(database, reportExample);
		Assert.assertThat(simpleReportRows.size(), is(21));
		for (SimpleReport simp : simpleReportRows) {
			Assert.assertThat(simp.marqueUID.stringValue(), not(isEmptyOrNullString()));
			Assert.assertThat(simp.marqueName.stringValue(), not(isEmptyOrNullString()));
			Assert.assertThat(simp.carCompanyName.stringValue(), not(isEmptyOrNullString()));
			Assert.assertThat(simp.carCompanyAndMarque.stringValue(), not(isEmptyOrNullString()));
			System.out.println("" + simp.marqueName);
			System.out.println("" + simp.carCompanyName);
			System.out.println("" + simp.carCompanyAndMarque.stringValue());
		}
	}
	
	@Test
	public void withExampleTest() throws SQLException {
		SimpleReport reportExample = new SimpleReport();
		Marque toyota = new Marque();
		toyota.name.permittedValuesIgnoreCase("TOYOTA");
		List<SimpleReport> simpleReportRows = DBReport.getRows(database, reportExample, toyota);
		Assert.assertThat(simpleReportRows.size(), is(1));
		for (SimpleReport simp : simpleReportRows) {
			Assert.assertThat(simp.marqueUID.stringValue(), not(isEmptyOrNullString()));
			Assert.assertThat(simp.carCompanyAndMarque.stringValue(), not(isEmptyOrNullString()));
			System.out.println("" + simp.marque);
			System.out.println("" + simp.carCompany);
			System.out.println("" + simp.carCompanyAndMarque.stringValue());
			Assert.assertThat(simp.marqueName.stringValue(), is("TOYOTA"));
			Assert.assertThat(simp.carCompanyName.stringValue(), is("TOYOTA"));
			Assert.assertThat(simp.carCompanyAndMarque.stringValue(), is("TOYOTA: TOYOTA"));
		}
	}
	
	@Test
	public void GroupTest() throws SQLException {
		GroupReport reportExample = new GroupReport();
		List<GroupReport> foundGroupReports = database.getRows(reportExample);
		Assert.assertThat(foundGroupReports.size(), is(4));
		for (GroupReport rep : foundGroupReports) {
			System.out.println("" + rep.carCompanyName.stringValue() + ": " + rep.average.stringValue() + ": " + rep.stddev.stringValue());
		}
	}
	
	@Test
	public void CountAllTest() throws SQLException {
		CountAllReport reportExample = new CountAllReport();
		List<CountAllReport> foundGroupReports = database.getRows(reportExample);
		Assert.assertThat(foundGroupReports.size(), is(4));
		for (CountAllReport rep : foundGroupReports) {
			System.out.println("" + rep.carCompanyName.stringValue() + ": " + rep.countAll.stringValue());
			switch (rep.countAll.intValue()) {
				case 1:
					Assert.assertThat(rep.carCompanyName.stringValue(), is("FORD"));
					break;
				case 3:
					Assert.assertThat(rep.carCompanyName.stringValue(), is("GENERAL MOTORS"));
					break;
				case 2:
					Assert.assertThat(rep.carCompanyName.stringValue(), is("TOYOTA"));
					break;
				case 15:
					Assert.assertThat(rep.carCompanyName.stringValue(), is("OTHER"));
					break;
				default:
                    ;
			}
		}
	}
	
	@Test
	public void MinMaxSumTest() throws SQLException {
		MinMaxSumReport reportExample = new MinMaxSumReport();
		List<MinMaxSumReport> foundGroupReports = database.getRows(reportExample);
		Assert.assertThat(foundGroupReports.size(), is(4));
		for (MinMaxSumReport rep : foundGroupReports) {
			System.out.println("" + rep.carCompanyName.stringValue() + ": " + rep.min.stringValue() + ": " + rep.max.stringValue() + ": " + rep.sum.stringValue());
			if (rep.carCompanyName.stringValue().equals("TOYOTA")) {
				//TOYOTA: 1: 4896300: 4896301
				Assert.assertThat(rep.min.intValue(), is(1));
				Assert.assertThat(rep.max.intValue(), is(4896300));
				Assert.assertThat(rep.sum.intValue(), is(4896301));
			}
		}
	}
	
	@Test
	public void MinMaxSumOrderedTest() throws SQLException {
		MinMaxSumReport reportExample = new MinMaxSumReport();
		reportExample.setSortOrder(reportExample.carCompanyName.setSortOrderAscending(), reportExample.min.setSortOrderAscending());
		List<MinMaxSumReport> foundGroupReports = database.getRows(reportExample);
		database.print(foundGroupReports);
		Assert.assertThat(foundGroupReports.size(), is(4));
		Assert.assertThat(foundGroupReports.get(0).carCompanyName.stringValue(), is("FORD"));
		Assert.assertThat(foundGroupReports.get(1).carCompanyName.stringValue(), is("GENERAL MOTORS"));
		Assert.assertThat(foundGroupReports.get(2).carCompanyName.stringValue(), is("OTHER"));
		Assert.assertThat(foundGroupReports.get(3).carCompanyName.stringValue(), is("TOYOTA"));
		for (MinMaxSumReport rep : foundGroupReports) {
			System.out.println("" + rep.carCompanyName.stringValue() + ": " + rep.min.stringValue() + ": " + rep.max.stringValue() + ": " + rep.sum.stringValue());
			if (rep.carCompanyName.stringValue().equals("TOYOTA")) {
				//TOYOTA: 1: 4896300: 4896301
				Assert.assertThat(rep.min.intValue(), is(1));
				Assert.assertThat(rep.max.intValue(), is(4896300));
				Assert.assertThat(rep.sum.intValue(), is(4896301));
			}
		}
		reportExample.setSortOrder(reportExample.carCompanyName.setSortOrderDescending(), reportExample.min.setSortOrderDescending());
		foundGroupReports = database.getRows(reportExample);
		Assert.assertThat(foundGroupReports.size(), is(4));
		Assert.assertThat(foundGroupReports.get(3).carCompanyName.stringValue(), is("FORD"));
		Assert.assertThat(foundGroupReports.get(2).carCompanyName.stringValue(), is("GENERAL MOTORS"));
		Assert.assertThat(foundGroupReports.get(1).carCompanyName.stringValue(), is("OTHER"));
		Assert.assertThat(foundGroupReports.get(0).carCompanyName.stringValue(), is("TOYOTA"));
	}
	
	@Test
	public void MinMaxSumOrderedWithExampleTest() throws SQLException {
		MinMaxSumReport reportExample = new MinMaxSumReport();
		reportExample.setSortOrder(reportExample.carCompanyName.setSortOrderAscending(), reportExample.min.setSortOrderAscending());
		CarCompany carCo = new CarCompany();
		carCo.name.permittedPattern("%T%");
		List<MinMaxSumReport> foundGroupReports = DBReport.getRows(database, reportExample, carCo);
		database.print(foundGroupReports);
		Assert.assertThat(foundGroupReports.size(), is(3));
		Assert.assertThat(foundGroupReports.get(0).carCompanyName.stringValue(), is("GENERAL MOTORS"));
		Assert.assertThat(foundGroupReports.get(1).carCompanyName.stringValue(), is("OTHER"));
		Assert.assertThat(foundGroupReports.get(2).carCompanyName.stringValue(), is("TOYOTA"));
		for (MinMaxSumReport rep : foundGroupReports) {
			System.out.println("" + rep.carCompanyName.stringValue() + ": " + rep.min.stringValue() + ": " + rep.max.stringValue() + ": " + rep.sum.stringValue());
			if (rep.carCompanyName.stringValue().equals("TOYOTA")) {
				//TOYOTA: 1: 4896300: 4896301
				Assert.assertThat(rep.min.intValue(), is(1));
				Assert.assertThat(rep.max.intValue(), is(4896300));
				Assert.assertThat(rep.sum.intValue(), is(4896301));
			}
		}
		reportExample.setSortOrder(reportExample.carCompanyName.setSortOrderDescending(), reportExample.min.setSortOrderAscending());
		foundGroupReports = database.getRows(reportExample, carCo);
		Assert.assertThat(foundGroupReports.size(), is(3));
		Assert.assertThat(foundGroupReports.get(2).carCompanyName.stringValue(), is("GENERAL MOTORS"));
		Assert.assertThat(foundGroupReports.get(1).carCompanyName.stringValue(), is("OTHER"));
		Assert.assertThat(foundGroupReports.get(0).carCompanyName.stringValue(), is("TOYOTA"));

		reportExample.setSortOrder(reportExample.min.setSortOrderAscending());
		foundGroupReports = database.getRows(reportExample, carCo);
		database.print(foundGroupReports);
		Assert.assertThat(foundGroupReports.size(), is(3));
		Assert.assertThat(foundGroupReports.get(0).carCompanyName.stringValue(), is("TOYOTA"));
		Assert.assertThat(foundGroupReports.get(1).carCompanyName.stringValue(), is("GENERAL MOTORS"));
		Assert.assertThat(foundGroupReports.get(2).carCompanyName.stringValue(), is("OTHER"));
	}
	
	public static class SimpleReport extends DBReport {

		private static final long serialVersionUID = 1L;
		
		public Marque marque = new Marque();
		public CarCompany carCompany = new CarCompany();
		@DBColumn
		public DBString carCompanyName = new DBString(carCompany.column(carCompany.name));
		@DBColumn
		public DBString marqueName = new DBString(marque.column(marque.name));
		@DBColumn
		public DBString carCompanyAndMarque = new DBString(carCompany.column(carCompany.name).append(": ").append(marque.column(marque.name)));
		@DBColumn
		public DBNumber marqueUID = new DBNumber(marque.column(marque.uidMarque));
		@DBColumn
		public DBDate marqueCreated = new DBDate(marque.column(marque.creationDate));
		
		{
			marque.statusClassID.permittedValues(1246974);
			carCompany.uidCarCompany.excludedValues((Long) null);
		}
	}
	
	
	public static class GroupReport extends DBReport {

		private static final long serialVersionUID = 1L;
		
		public Marque marque = new Marque();
		public CarCompany carCompany = new CarCompany();
		@DBColumn
		public DBString carCompanyName = new DBString(carCompany.column(carCompany.name).uppercase());
		@DBColumn
		public DBNumber average = new DBNumber(marque.column(marque.name).length().average());
		@DBColumn
		public DBNumber stddev = new DBNumber(marque.column(marque.name).length().standardDeviation());
		
		{
			marque.statusClassID.permittedValues(1246974);
			carCompany.uidCarCompany.excludedValues((Integer) null);
		}
	}
	
	public static class CountAllReport extends DBReport {

		private static final long serialVersionUID = 1L;
		
		public Marque marque = new Marque();
		public CarCompany carCompany = new CarCompany();
		public DBString carCompanyName = new DBString(carCompany.column(carCompany.name).uppercase());
		public DBNumber countAll = new DBNumber(NumberExpression.countAll());
		
		{
			marque.statusClassID.permittedValues(1246974);
			carCompany.uidCarCompany.excludedValues((Integer) null);
		}
	}
	
	public static class MinMaxSumReport extends DBReport {

		private static final long serialVersionUID = 1L;
		
		public Marque marque = new Marque();
		public CarCompany carCompany = new CarCompany();
		@DBColumn
		public DBString carCompanyName = new DBString(carCompany.column(carCompany.name).uppercase());
		@DBColumn
		public DBNumber min = new DBNumber(marque.column(marque.uidMarque).min());
		@DBColumn
		public DBNumber max = new DBNumber(marque.column(marque.uidMarque).max());
		@DBColumn
		public DBNumber sum = new DBNumber(marque.column(marque.uidMarque).sum());
		
		{
			marque.statusClassID.permittedValues(1246974);
			carCompany.uidCarCompany.excludedValues((Integer) null);
		}
	}
}
