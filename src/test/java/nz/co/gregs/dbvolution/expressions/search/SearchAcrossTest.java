/*
 * Copyright 2013 greg.
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
package nz.co.gregs.dbvolution.expressions.search;

import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.columns.NumberColumn;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

public class SearchAcrossTest extends AbstractTest {

	public SearchAcrossTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	public static class SearchAcrossMarque extends Marque {

		private static final long serialVersionUID = 1L;

		@DBColumn
		public DBNumber ranking = SearchAcross.empty()
				.addTerm("R")
				.addPreferredTerm("o")
				.addReducedTerm("l")
				.addSearchColumn(column(name), "name")
				.getRankingExpression()
				.asExpressionColumn();
//				.column(name).searchForRanking("+o", "-l", "R"));
	}

	@Test
	public void testSearchStringUsingAdds() throws SQLException {

		SearchAcrossMarque marq = new SearchAcrossMarque();
		marq.name.clear();
		DBQuery query = database.getDBQuery(marq);
		final SearchAcross searchString
				= SearchAcross
						.search(marq.column(marq.name), "name")
						.addTerm("n")
						.addQuotedTerm("ho")
						.addReducedTerm("r")
						.addSearchColumn(marq.column(marq.numericCode).stringResult(), "code");

		query.addExpressionColumn(
				this,
				searchString.getRankingExpression().asExpressionColumn()
		);
		query.setSortOrder(
				searchString.descending(),
				marq.column(marq.name).ascending()
		);

//		query.printSQLForQuery();
//		List<DBQueryRow> rows = query.setBlankQueryAllowed(true).getAllRows();
//		int i = 0;
//		for (DBQueryRow row : rows) {
//			final SearchAcrossMarque marque = row.get(marq);
//			String rank = rows.get(i).getExpressionColumnValue(this).getValue().toString();
//			System.out.println("ROW:  name: " + marque.name + " \tRANK: " + rank + " \tnumericcode:" + marque.numericCode);
//			i++;
//		}

		query.addCondition(
				searchString.getComparisonExpression()
		);
		List<DBQueryRow> got = query.getAllRows();

		Assert.assertThat(got.size(), is(4));
		Assert.assertThat((got.get(0)).get(marq).name.stringValue(), is("HOLDEN"));
		Assert.assertThat((got.get(1)).get(marq).name.stringValue(), is("HONDA"));
		Assert.assertThat((got.get(2)).get(marq).name.stringValue(), is("HYUNDAI"));
		Assert.assertThat((got.get(3)).get(marq).name.stringValue(), is("NISSAN"));

		Assert.assertThat(got.get(0).getExpressionColumnValue(this).getValue().toString(), is("15.5"));
		Assert.assertThat(got.get(1).getExpressionColumnValue(this).getValue().toString(), is("15.5"));
		Assert.assertThat(got.get(2).getExpressionColumnValue(this).getValue().toString(), is("5.5"));
		Assert.assertThat(got.get(3).getExpressionColumnValue(this).getValue().toString(), is("5.5"));
	}

	@Test
	public void testSearchStringWithAliases() throws SQLException {

		SearchAcrossMarque marq = new SearchAcrossMarque();
		marq.name.clear();
		DBQuery query = database.getDBQuery(marq);
		final SearchAcross searchString
				= new SearchAcross("name:n")
						.addQuotedTerm("ho")
						.addReducedTerm("r")
						.addSearchColumn(marq.column(marq.name), "name")
						.addSearchColumn(marq.column(marq.numericCode).stringResult(), "code");

		query.addExpressionColumn(
				this,
				searchString.getRankingExpression().asExpressionColumn()
		);
		query.setSortOrder(
				searchString.descending(),
				marq.column(marq.name).ascending()
		);

//		List<DBQueryRow> got = query.setBlankQueryAllowed(true).getAllRows();

//		int i = 0;
//		for (DBQueryRow row : got) {
//			final SearchAcrossMarque marque = row.get(marq);
//			String rank = got.get(i).getExpressionColumnValue(this).getValue().toString();
//			System.out.println("ROW:  name: " + marque.name + " \tRANK: " + rank + " \tnumericcode:" + marque.numericCode);
//			i++;
//		}

		query.addCondition(
				searchString.getComparisonExpression()
		);
		List<DBQueryRow> got = query.getAllRows();

		Assert.assertThat(got.size(), is(5));
		Assert.assertThat((got.get(0)).get(marq).name.stringValue(), is("HOLDEN"));
		Assert.assertThat((got.get(1)).get(marq).name.stringValue(), is("HONDA"));
		Assert.assertThat((got.get(2)).get(marq).name.stringValue(), is("HYUNDAI"));
		Assert.assertThat((got.get(3)).get(marq).name.stringValue(), is("NISSAN"));
		Assert.assertThat((got.get(4)).get(marq).name.stringValue(), is("LANDROVER"));

		Assert.assertThat(got.get(0).getExpressionColumnValue(this).getValue().toString(), is("65.0"));
		Assert.assertThat(got.get(1).getExpressionColumnValue(this).getValue().toString(), is("65.0"));
		Assert.assertThat(got.get(2).getExpressionColumnValue(this).getValue().toString(), is("55.0"));
		Assert.assertThat(got.get(3).getExpressionColumnValue(this).getValue().toString(), is("55.0"));
		Assert.assertThat(got.get(4).getExpressionColumnValue(this).getValue().toString(), is("16.5"));
	}

	@Test
	public void testStringSearchRanking() throws SQLException {

		SearchAcrossMarque marq = new SearchAcrossMarque();
		marq.name.clear();
		DBQuery query = database.getDBQuery(marq);
		NumberColumn rankColumn = marq.column(marq.ranking);

		query.addExpressionColumn(this, rankColumn.asExpressionColumn());
		query.setSortOrder(
				rankColumn.descending(),
				marq.column(marq.name).ascending()
		);

//		query.printSQLForQuery();
//		List<DBQueryRow> rows = query.setBlankQueryAllowed(true).getAllRows();
//
//		int i = 0;
//		for (DBQueryRow row : rows) {
//			final SearchAcrossMarque marque = row.get(marq);
//			String rank = rows.get(i).getExpressionColumnValue(this).getValue().toString();
//			System.out.println("ROW:  name: " + marque.name + " \tRANK: " + rank + " \tnumericcode:" + marque.numericCode);
//			i++;
//		}
		
		query.addCondition(rankColumn.isGreaterThan(0));

		List<SearchAcrossMarque> got = query.getAllInstancesOf(marq);

		Assert.assertThat(got.size(), is(11));
		Assert.assertThat((got.get(0)).name.stringValue(), is("FORD"));
		Assert.assertThat((got.get(1)).name.stringValue(), is("ROVER"));
		Assert.assertThat((got.get(2)).name.stringValue(), is("DAEWOO"));
		Assert.assertThat((got.get(3)).name.stringValue(), is("HONDA"));
		Assert.assertThat((got.get(4)).name.stringValue(), is("PEUGEOT"));
		Assert.assertThat((got.get(5)).name.stringValue(), is("TOYOTA"));
		Assert.assertThat((got.get(6)).name.stringValue(), is("LANDROVER"));
		Assert.assertThat((got.get(7)).name.stringValue(), is("HOLDEN"));
		Assert.assertThat((got.get(8)).name.stringValue(), is("HUMMER"));
		Assert.assertThat((got.get(9)).name.stringValue(), is("SUBARU"));
		Assert.assertThat((got.get(10)).name.stringValue(), is("VOLVO"));
		Assert.assertThat((got.get(0)).ranking.intValue(), is(71));
		Assert.assertThat((got.get(1)).ranking.intValue(), is(71));
		Assert.assertThat((got.get(2)).ranking.intValue(), is(55));
		Assert.assertThat((got.get(3)).ranking.intValue(), is(55));
		Assert.assertThat((got.get(4)).ranking.intValue(), is(55));
		Assert.assertThat((got.get(5)).ranking.intValue(), is(55));
		Assert.assertThat((got.get(6)).ranking.intValue(), is(33));
		Assert.assertThat((got.get(7)).ranking.intValue(), is(16));
		Assert.assertThat((got.get(8)).ranking.intValue(), is(16));
		Assert.assertThat((got.get(9)).ranking.intValue(), is(16));
		Assert.assertThat((got.get(10)).ranking.intValue(), is(16));
	}
}
