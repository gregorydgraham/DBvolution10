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

		List<SimpleReport> simpleReportRows = DBReport.getAllRows(database, reportExample);
		Assert.assertThat(simpleReportRows.size(), is(21));
		for (SimpleReport simp : simpleReportRows) {
			Assert.assertThat(simp.marqueUID.stringValue(), not(isEmptyOrNullString()));
			Assert.assertThat(simp.marqueName.stringValue(), not(isEmptyOrNullString()));
			Assert.assertThat(simp.carCompanyName.stringValue(), not(isEmptyOrNullString()));
			Assert.assertThat(simp.carCompanyAndMarque.stringValue(), not(isEmptyOrNullString()));
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
			Assert.assertThat(simp.marqueName.stringValue(), is("TOYOTA"));
			Assert.assertThat(simp.carCompanyName.stringValue(), is("TOYOTA"));
			Assert.assertThat(simp.carCompanyAndMarque.stringValue(), is("TOYOTA: TOYOTA"));
			Assert.assertThat(simp.carCompanyAndANumber.stringValue(), is("TOYOTA: 5"));
		}
	}

	@Test
	public void GroupTest() throws SQLException {
		GroupReport reportExample = new GroupReport();
		List<GroupReport> foundGroupReports = database.getRows(reportExample);
		Assert.assertThat(foundGroupReports.size(), is(4));

		final GroupReport otherRow = foundGroupReports.get(2);
		// TOYOTA: 6: 0.7071067811865476
		//OTHER: 5.8667: 1.9955506062794353
		Assert.assertThat(otherRow.carCompanyName.stringValue(), is("OTHER"));
		Assert.assertThat(Math.round(otherRow.average.doubleValue() * 100), is(587L));
		Assert.assertThat(Math.round(otherRow.stddev.doubleValue() * 100), is(207L));
		Assert.assertThat(Math.round(otherRow.stats.average().doubleValue() * 100), is(587L));
		Assert.assertThat(Math.round(otherRow.stats.standardDeviation().doubleValue() * 100), is(207L));
	}

	@Test
	public void CountAllTest() throws SQLException {
		CountAllReport reportExample = new CountAllReport();
		List<CountAllReport> foundGroupReports = database.getRows(reportExample);
		Assert.assertThat(foundGroupReports.size(), is(4));
		for (CountAllReport rep : foundGroupReports) {
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

		Assert.assertThat(foundGroupReports.size(), is(4));
		Assert.assertThat(foundGroupReports.get(0).carCompanyName.stringValue(), is("FORD"));
		Assert.assertThat(foundGroupReports.get(1).carCompanyName.stringValue(), is("GENERAL MOTORS"));
		Assert.assertThat(foundGroupReports.get(2).carCompanyName.stringValue(), is("OTHER"));
		Assert.assertThat(foundGroupReports.get(3).carCompanyName.stringValue(), is("TOYOTA"));
		for (MinMaxSumReport rep : foundGroupReports) {
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

		Assert.assertThat(foundGroupReports.size(), is(3));
		Assert.assertThat(foundGroupReports.get(0).carCompanyName.stringValue(), is("GENERAL MOTORS"));
		Assert.assertThat(foundGroupReports.get(1).carCompanyName.stringValue(), is("OTHER"));
		Assert.assertThat(foundGroupReports.get(2).carCompanyName.stringValue(), is("TOYOTA"));
		for (MinMaxSumReport rep : foundGroupReports) {
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
		public DBString carCompanyAndANumber = new DBString(carCompany.column(carCompany.name).append(": ").append(5));
		@DBColumn
		public DBNumber marqueUID = new DBNumber(marque.column(marque.uidMarque));
		@DBColumn
		public DBDate marqueCreated = marque.column(marque.creationDate).asExpressionColumn();

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
		public DBNumber count = new DBNumber(NumberExpression.countAll());
		@DBColumn
		public DBNumber sum = new DBNumber(marque.column(marque.name).length().sum());
		@DBColumn
		public DBNumber average = new DBNumber(marque.column(marque.name).length().average());
		@DBColumn
		public DBNumber stddev = new DBNumber(marque.column(marque.name).length().standardDeviation());
		@DBColumn
		public DBNumberStatistics stats = new DBNumberStatistics(marque.column(marque.name).length());

		{
			marque.statusClassID.permittedValues(1246974);
			carCompany.uidCarCompany.excludedValues((Integer) null);
			this.setSortOrder(this.column(this.carCompanyName));
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

	public static class ProtectedDBRowsReport extends DBReport {

		private static final long serialVersionUID = 1L;

		Marque marque = new Marque();
		protected CarCompany carCompany = new CarCompany();
		public DBString carCompanyName = new DBString(carCompany.column(carCompany.name).uppercase());
		public DBNumber countAll = new DBNumber(NumberExpression.countAll());

		{
			carCompany.uidCarCompany.excludedValues((Integer) null);
		}
	}

	@Test
	public void nonPublicDBRowsReportTest() throws SQLException {
		ProtectedDBRowsReport reportExample = new ProtectedDBRowsReport();
		List<ProtectedDBRowsReport> foundGroupReports = database.getRows(reportExample);
		Assert.assertThat(foundGroupReports.size(), is(4));
		for (ProtectedDBRowsReport rep : foundGroupReports) {
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

	public static class PrivateDBRowsReport extends DBReport {

		private static final long serialVersionUID = 1L;

		private final Marque marque = new Marque();
		private final CarCompany carCompany = new CarCompany();
		public DBString carCompanyName = carCompany.column(carCompany.name).uppercase().asExpressionColumn();
		public DBInteger countAll = NumberExpression.countAll().asExpressionColumn();

		{
			carCompany.uidCarCompany.excludedValues((Integer) null);
		}
	}

	@Test
	public void privateDBRowsReportTest() throws SQLException {
		PrivateDBRowsReport reportExample = new PrivateDBRowsReport();
		List<PrivateDBRowsReport> foundGroupReports = database.getRows(reportExample);
		Assert.assertThat(foundGroupReports.size(), is(4));
		for (PrivateDBRowsReport rep : foundGroupReports) {
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

	public static class PrivateFieldsReport extends DBReport {

		private static final long serialVersionUID = 1L;

		private final Marque marque = new Marque();
		private final CarCompany carCompany = new CarCompany();
		private final DBString carCompanyName = carCompany.column(carCompany.name).uppercase().asExpressionColumn();
		private final DBInteger countAll = NumberExpression.countAll().asExpressionColumn();

		{
			carCompany.uidCarCompany.excludedValues((Integer) null);
		}
	}

	@Test
	public void privateFieldsReportTest() throws SQLException {
		PrivateFieldsReport reportExample = new PrivateFieldsReport();
		List<PrivateFieldsReport> foundGroupReports = database.getRows(reportExample);
		Assert.assertThat(foundGroupReports.size(), is(4));
		for (PrivateFieldsReport rep : foundGroupReports) {
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
}
