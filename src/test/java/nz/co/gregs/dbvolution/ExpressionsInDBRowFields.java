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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import nz.co.gregs.dbvolution.annotations.*;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import nz.co.gregs.dbvolution.databases.Oracle11XEContainerDB;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.expressions.DateExpression;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.*;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class ExpressionsInDBRowFields extends AbstractTest {

	public ExpressionsInDBRowFields(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@BeforeClass
	public static void setUpClass() {
	}

	@AfterClass
	public static void tearDownClass() {
	}

	@Before
	@Override
	public void setUp() throws Exception {
		super.setUp();
		database.setPrintSQLBeforeExecuting(true);
	}

	@After
	@Override
	public void tearDown() throws Exception {
		database.setPrintSQLBeforeExecuting(false);
		super.tearDown();
	}

	@Test
	public void selectDBRowExpressionWithDBQuery() throws Exception {
		final ExpressionRow exprExample = new ExpressionRow();

		exprExample.name.permittedValuesIgnoreCase("TOYOTA");
		DBQuery query = database.getDBQuery(exprExample);

		final String sqlForQuery = query.getSQLForQuery();
		if (!(database instanceof DBDatabaseCluster)) {
			Assert.assertThat(sqlForQuery, containsString(database.getDefinition().doCurrentDateOnlyTransform()));
		}
		Assert.assertThat(sqlForQuery, containsString(ExpressionRow.STRING_VALUE));
		Assert.assertThat(sqlForQuery, containsString(NumberExpression.value(5).times(3).toSQLString(database.getDefinition())));

		final List<DBQueryRow> allRows = query.getAllRows();
		for (DBQueryRow row : allRows) {
			ExpressionRow expressionRow = row.get(exprExample);
			DBDate currentDate = expressionRow.sysDateColumnOnClass;
			GregorianCalendar cal = new GregorianCalendar();
			cal.add(GregorianCalendar.MINUTE, +1);
			Date later = cal.getTime();
			cal.add(GregorianCalendar.MINUTE, -1);
			cal.add(GregorianCalendar.HOUR, -24);
			Date yesterday = cal.getTime();
			Assert.assertThat(currentDate.getValue(), lessThan(later));
			Assert.assertThat(currentDate.getValue(), greaterThan(yesterday));
			Assert.assertThat(expressionRow.stringColumnOnClass.stringValue(), is(ExpressionRow.STRING_VALUE.toUpperCase()));
			Assert.assertThat(expressionRow.numberColumnOnClass.intValue(), is(15));
			Assert.assertThat(expressionRow.marqueUIDTimes10.intValue(), is(10));
			Assert.assertThat(expressionRow.shortName.stringValue(), is("TOY"));
			Assert.assertThat(expressionRow.uidAndName.stringValue(), is("1-TOYOTA"));
		}
	}

	@Test
	public void selectDBRowExpressionWithDBQueryAndExpressionCriteria() throws Exception {
		final ExpressionRow exprExample = new ExpressionRow();
		exprExample.numberColumnOnClass.permittedValues(15);

		exprExample.name.permittedValuesIgnoreCase("TOYOTA");
		DBQuery query = database.getDBQuery(exprExample);

		if (!(database instanceof DBDatabaseCluster)) {
			String sqlForQuery = query.getSQLForQuery();
			Assert.assertThat(sqlForQuery, containsString(database.getDefinition().doCurrentDateOnlyTransform()));
		}
		for (DBQueryRow row : query.getAllRows()) {
			ExpressionRow expressionRow = row.get(exprExample);
			Assert.assertThat(expressionRow.stringColumnOnClass.stringValue(), is(ExpressionRow.STRING_VALUE.toUpperCase()));
			Assert.assertThat(expressionRow.numberColumnOnClass.intValue(), is(15));
			Assert.assertThat(expressionRow.marqueUIDTimes10.intValue(), is(10));
			Assert.assertThat(expressionRow.shortName.stringValue(), is("TOY"));
			Assert.assertThat(expressionRow.uidAndName.stringValue(), is("1-TOYOTA"));
		}

		final ExpressionRow exprExample2 = new ExpressionRow();
		exprExample2.numberColumnOnClass.excludedValues(15);
		exprExample2.name.permittedValuesIgnoreCase("TOYOTA");

		query = database.getDBQuery(exprExample2);

		if (!(database instanceof DBDatabaseCluster)) {
			String sqlForQuery = query.getSQLForQuery();
			Assert.assertThat(sqlForQuery, containsString(database.getDefinition().doCurrentDateOnlyTransform()));
		}
		final List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void selectDBRowExpressionWithDBTable() throws Exception {
		final ExpressionRow exprExample = new ExpressionRow();
		exprExample.name.permittedValuesIgnoreCase("TOYOTA");
		DBTable<ExpressionRow> table = database.getDBTable(exprExample);

		final String sqlForQuery = table.getSQLForQuery();

		if (!(database instanceof DBDatabaseCluster)) {
			Assert.assertThat(sqlForQuery, containsString(database.getDefinition().doCurrentDateOnlyTransform()));
		}

		for (ExpressionRow expressionRow : table.getAllRows()) {
			DBDate currentDate = expressionRow.sysDateColumnOnClass;
			GregorianCalendar cal = new GregorianCalendar();
			cal.add(GregorianCalendar.MINUTE, +1);
			Date later = cal.getTime();
			cal.add(GregorianCalendar.MINUTE, -1);
			cal.add(GregorianCalendar.HOUR, -24);
			Date yesterday = cal.getTime();
			Assert.assertThat(currentDate.getValue(), lessThan(later));
			Assert.assertThat(currentDate.getValue(), greaterThan(yesterday));
			Assert.assertThat(expressionRow.stringColumnOnClass.stringValue(), is(ExpressionRow.STRING_VALUE.toUpperCase()));
			Assert.assertThat(expressionRow.numberColumnOnClass.intValue(), is(15));
			Assert.assertThat(expressionRow.marqueUIDTimes10.intValue(), is(10));
			Assert.assertThat(expressionRow.shortName.stringValue(), is("TOY"));
			Assert.assertThat(expressionRow.uidAndName.stringValue(), is("1-TOYOTA"));
		}
	}

	@Test
	@SuppressWarnings("deprecation")
	public void selectDBRowExpressionAllMarques() throws Exception {
		System.out.println("nz.co.gregs.dbvolution.ExpressionsInDBRowFields.selectDBRowExpressionAllMarques()");
		final ExpressionRow expressionRow = new ExpressionRow();
		final DBTable<ExpressionRow> expressionTable = database.getDBTable(expressionRow)
				.setQueryLabel("selectDBRowExpressionAllMarques");
//		database.getDefinition().setRequiredToProduceEmptyStringsForNull(true);
//		database.getDefinition().setRequiredToProduceEmptyStringsForNull(false);
		database.setPrintSQLBeforeExecuting(true);
		final List<ExpressionRow> allMarques = expressionTable.setBlankQueryAllowed(true).getAllRows();
		database.setPrintSQLBeforeExecuting(false);

		for (ExpressionRow row : allMarques) {
			Assert.assertThat(row.uidAndName.stringValue(),
					is(row.uidMarque.stringValue() + "-" + row.name.stringValue()));
			final Date dateValue = row.creationDate.dateValue();

			if (dateValue != null) {
				String year = new SimpleDateFormat("yyyy").format(dateValue);
				Assert.assertThat(row.uidNameAndYear.stringValue(), is(
						row.uidMarque.stringValue() + "-"
						+ row.name.stringValue() + "-"
						+ year));
				Assert.assertThat(row.uidNameAndNVLYear.stringValue(), is(
						row.uidMarque.stringValue() + "-"
						+ row.name.stringValue() + "-"
						+ year));
			} else {
				System.out.println("supportsDifferenceBetweenNullAndEmptyString: " + database.supportsDifferenceBetweenNullAndEmptyString());
				System.out.println("supportsDifferenceBetweenNullAndEmptyStringNatively: " + database.getDefinition().supportsDifferenceBetweenNullAndEmptyStringNatively());
				System.out.println("NOT requiredToProduceEmptyStringsForNull: NOT " + database.getDefinition().requiredToProduceEmptyStringsForNull());
				if (database.supportsDifferenceBetweenNullAndEmptyString()) {
					Assert.assertThat(
							row.uidNameAndYear.stringValue(),
							isEmptyOrNullString()
					);
					String year = new SimpleDateFormat("yyyy").format(new Date());
					Assert.assertThat(
							row.uidNameAndNVLYear.stringValue(),
							is(row.uidMarque.stringValue() + "-" + row.name.stringValue() + "-" + year)
					);
				} else {
					if (row.creationDate.isNull()) {
						Assert.assertThat(
								row.uidNameAndYear.stringValue(),
								isEmptyOrNullString()
						);
					} else {
						System.out.println("" + row.uidMarque + "-" + row.name + "-" + row.creationDate.stringValue());
						Assert.assertThat(
								row.uidNameAndYear.stringValue(),
								is(row.uidMarque.stringValue() + "-" + row.name.stringValue() + "-")
						);
						String year = new SimpleDateFormat("yyyy").format(new Date());
						Assert.assertThat(
								row.uidNameAndNVLYear.stringValue(),
								is(row.uidMarque.stringValue() + "-" + row.name.stringValue() + "-" + year)
						);
					}
				}
			}
		}
	}

	@Test
	@SuppressWarnings("deprecation")
	public void selectDBRowExpressionAllMarquesWithinOracleCluster() throws Exception {
		System.out.println("nz.co.gregs.dbvolution.ExpressionsInDBRowFields.selectDBRowExpressionAllMarques()");

//		final Oracle11XEContainerDB oracle = Oracle11XEContainerDB.getLabelledInstance("Oracle DB:selectDBRowExpressionAllMarquesWithinOracleCluster"+Math.random());
		Oracle11XEContainerDB oracle = getOracleReferenceDatabase();
		var cluster = new DBDatabaseCluster("test required empty for null cluster", oracle, database);

		final ExpressionRow expressionRow = new ExpressionRow();
		final DBTable<ExpressionRow> expressionTable = database.getDBTable(expressionRow)
				.setQueryLabel("selectDBRowExpressionAllMarques");

		database.setPrintSQLBeforeExecuting(true);
		final List<ExpressionRow> allMarques = expressionTable.setBlankQueryAllowed(true).getAllRows();
		database.setPrintSQLBeforeExecuting(false);

		for (ExpressionRow row : allMarques) {
			Assert.assertThat(row.uidAndName.stringValue(),
					is(row.uidMarque.stringValue() + "-" + row.name.stringValue()));
			final Date dateValue = row.creationDate.dateValue();

			if (dateValue != null) {
				String year = new SimpleDateFormat("yyyy").format(dateValue);
				Assert.assertThat(row.uidNameAndYear.stringValue(), is(
						row.uidMarque.stringValue() + "-"
						+ row.name.stringValue() + "-"
						+ year));
				Assert.assertThat(row.uidNameAndNVLYear.stringValue(), is(
						row.uidMarque.stringValue() + "-"
						+ row.name.stringValue() + "-"
						+ year));
			} else {
				System.out.println("supportsDifferenceBetweenNullAndEmptyString: " + database.supportsDifferenceBetweenNullAndEmptyString());
				System.out.println("supportsDifferenceBetweenNullAndEmptyStringNatively: " + database.getDefinition().supportsDifferenceBetweenNullAndEmptyStringNatively());
				System.out.println("NOT requiredToProduceEmptyStringsForNull: NOT " + database.getDefinition().requiredToProduceEmptyStringsForNull());
				if (database.supportsDifferenceBetweenNullAndEmptyString()) {
					Assert.assertThat(
							row.uidNameAndYear.stringValue(),
							isEmptyOrNullString()
					);
					String year = new SimpleDateFormat("yyyy").format(new Date());
					Assert.assertThat(
							row.uidNameAndNVLYear.stringValue(),
							is(row.uidMarque.stringValue() + "-" + row.name.stringValue() + "-" + year)
					);
				} else {
					System.out.println("" + row.uidMarque + "-" + row.name + "-" + row.creationDate.stringValue());
					if (row.creationDate.isNull()) {
						Assert.assertThat(
								row.uidNameAndYear.stringValue(),
								isEmptyOrNullString()
						);
					} else {
						Assert.assertThat(
								row.uidNameAndYear.stringValue(),
								is(row.uidMarque.stringValue() + "-" + row.name.stringValue() + "-")
						);
						String year = new SimpleDateFormat("yyyy").format(new Date());
						Assert.assertThat(
								row.uidNameAndNVLYear.stringValue(),
								is(row.uidMarque.stringValue() + "-" + row.name.stringValue() + "-" + year)
						);
					}
				}
			}
		}
		System.out.println(cluster.getLabel());
	}

	@Test
	@SuppressWarnings("deprecation")
	public void selectDBRowExpressionAllMarquesAfterAddingToClusterAndIntroducingOracleDatabase() throws Exception {
		System.out.println("nz.co.gregs.dbvolution.ExpressionsInDBRowFields.selectDBRowExpressionAllMarques()");

//		final Oracle11XEContainerDB oracle = Oracle11XEContainerDB.getLabelledInstance("Oracle DB:selectDBRowExpressionAllMarquesAfterAddingToClusterAndIntroducingOracleDatabase" + Math.random());
		Oracle11XEContainerDB oracle = getOracleReferenceDatabase();
		var cluster = new DBDatabaseCluster("test required empty for null cluster", database);
		cluster.addDatabase(oracle);

		final ExpressionRow expressionRow = new ExpressionRow();
		final DBTable<ExpressionRow> expressionTable = database.getDBTable(expressionRow)
				.setQueryLabel("selectDBRowExpressionAllMarques");

		database.setPrintSQLBeforeExecuting(true);
		final List<ExpressionRow> allMarques = expressionTable.setBlankQueryAllowed(true).getAllRows();
		database.setPrintSQLBeforeExecuting(false);

		for (ExpressionRow row : allMarques) {
			Assert.assertThat(row.uidAndName.stringValue(),
					is(row.uidMarque.stringValue() + "-" + row.name.stringValue()));
			final Date dateValue = row.creationDate.dateValue();

			if (dateValue != null) {
				String year = new SimpleDateFormat("yyyy").format(dateValue);
				Assert.assertThat(row.uidNameAndYear.stringValue(), is(
						row.uidMarque.stringValue() + "-"
						+ row.name.stringValue() + "-"
						+ year));
				Assert.assertThat(row.uidNameAndNVLYear.stringValue(), is(
						row.uidMarque.stringValue() + "-"
						+ row.name.stringValue() + "-"
						+ year));
			} else {
				System.out.println("supportsDifferenceBetweenNullAndEmptyString: " + database.supportsDifferenceBetweenNullAndEmptyString());
				System.out.println("supportsDifferenceBetweenNullAndEmptyStringNatively: " + database.getDefinition().supportsDifferenceBetweenNullAndEmptyStringNatively());
				System.out.println("NOT requiredToProduceEmptyStringsForNull: NOT " + database.getDefinition().requiredToProduceEmptyStringsForNull());
				System.out.println("uidNameAndYear: " + row.uidNameAndYear);
				System.out.println("uidNameAndYear: " + row.uidNameAndYear.stringValue());
				if (database.supportsDifferenceBetweenNullAndEmptyString()) {
					Assert.assertThat(
							row.uidNameAndYear.stringValue(),
							isEmptyOrNullString()
					);
					String year = new SimpleDateFormat("yyyy").format(new Date());
					Assert.assertThat(
							row.uidNameAndNVLYear.stringValue(),
							is(row.uidMarque.stringValue() + "-" + row.name.stringValue() + "-" + year)
					);
				} else {
					System.out.println(row);
					System.out.println("" + row.uidMarque + "-" + row.name + "-" + row.creationDate.stringValue());
					if (row.creationDate.isNull()) {
						Assert.assertThat(
								row.uidNameAndYear.stringValue(),
								isEmptyOrNullString()
						);
					} else {
						Assert.assertThat(
								row.uidNameAndYear.stringValue(),
								is(row.uidMarque.stringValue() + "-" + row.name.stringValue() + "-")
						);
						String year = new SimpleDateFormat("yyyy").format(new Date());
						Assert.assertThat(
								row.uidNameAndNVLYear.stringValue(),
								is(row.uidMarque.stringValue() + "-" + row.name.stringValue() + "-" + year)
						);
					}
				}
			}
		}
		System.out.println(cluster.getLabel());
	}

	@DBTableName("marque")
	public static class ExpressionRow extends DBRow {

		public static final long serialVersionUID = 1L;
		public static final String STRING_VALUE = "THis ValuE";

		@DBColumn
		DBDate sysDateColumnOnClass = new DBDate(DateExpression.currentDateOnly());

		@DBColumn
		DBString stringColumnOnClass = new DBString(StringExpression.value(STRING_VALUE).uppercase());

		@DBColumn
		DBNumber numberColumnOnClass = new DBNumber(NumberExpression.value(5).times(3));

		@DBColumn("uid_marque")
		@DBPrimaryKey
		public DBNumber uidMarque = new DBNumber();

		@DBColumn
		public DBString name = new DBString();

		@DBColumn("creation_date")
		public DBDate creationDate = new DBDate();

		@DBColumn
		DBNumber marqueUIDTimes10 = new DBNumber(this.column(this.uidMarque).times(10));

		@DBColumn
		DBString marqueUIDTimes10String = new DBString(this.column(this.uidMarque).times(10).stringResult());

		@DBColumn
		DBString shortName = new DBString(this.column(this.name).substring(0, 3));

		@DBColumn
		DBString uidAndName = new DBString(this.column(this.uidMarque).append("-").append(this.column(this.name)));

		@DBColumn
		DBString uidNameAndYear = new DBString(this.column(this.uidMarque).append("-").append(this.column(this.name)).append("-").append(this.column(this.creationDate).year()));

		@DBColumn
		DBString uidNameAndNVLYear = new DBString(this.column(this.uidMarque).ifDBNull(NumberExpression.value(-1).times(NumberExpression.value(2))).append("-").append(this.column(this.name).ifDBNull("UNKNOWN")).append("-").append(this.column(this.creationDate).ifDBNull(DateExpression.currentDateOnly()).year().ifDBNull(2000)));

		@DBColumn
		DBString uidNameAndNVLYearWithDate = new DBString(this.column(this.uidMarque).ifDBNull(NumberExpression.value(-1).times(NumberExpression.value(2))).append("-").append(this.column(this.name).ifDBNull("UNKNOWN")).append("-").append(this.column(this.creationDate).ifDBNull(april2nd2011).year().ifDBNull(2000)));

	}
}
