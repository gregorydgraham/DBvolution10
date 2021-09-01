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
import nz.co.gregs.dbvolution.columns.IntegerColumn;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Test;

public class SearchStringTest extends AbstractTest {

	public SearchStringTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	public static class SearchMarque extends Marque {

		private static final long serialVersionUID = 1L;

		@DBColumn
		public DBInteger ranking = new DBInteger(column(name).searchForRanking("+o", "-l", "R"));
	}

	@Test
	public void testStringSearch() throws SQLException {

		SearchMarque marq = new SearchMarque();
		marq.name.clear();
		DBQuery query = database.getDBQuery(marq);
		
		final BooleanExpression searchExpr = marq.column(marq.name).searchFor("+o", "-l", "R");
		final NumberExpression searchForRanking = marq.column(marq.name).searchForRanking("+o", "-l", "R");
		
		query.addExpressionColumn(this, searchForRanking.asExpressionColumn());
		query.setSortOrder(
				searchForRanking.descending(),
				marq.column(marq.name).ascending()
		);

		query.addCondition(searchExpr);

		List<SearchMarque> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(11));
		assertThat((got.get(0)).name.stringValue(), is("FORD"));
		assertThat((got.get(1)).name.stringValue(), is("ROVER"));
		assertThat((got.get(2)).name.stringValue(), is("DAEWOO"));
		assertThat((got.get(3)).name.stringValue(), is("HONDA"));
		assertThat((got.get(4)).name.stringValue(), is("PEUGEOT"));
		assertThat((got.get(5)).name.stringValue(), is("TOYOTA"));
		assertThat((got.get(6)).name.stringValue(), is("LANDROVER"));
		assertThat((got.get(7)).name.stringValue(), is("HOLDEN"));
		assertThat((got.get(8)).name.stringValue(), is("HUMMER"));
		assertThat((got.get(9)).name.stringValue(), is("SUBARU"));
		assertThat((got.get(10)).name.stringValue(), is("VOLVO"));
		assertThat((got.get(0)).ranking.intValue(), is(214));
		assertThat((got.get(1)).ranking.intValue(), is(214));
		assertThat((got.get(2)).ranking.intValue(), is(165));
		assertThat((got.get(3)).ranking.intValue(), is(165));
		assertThat((got.get(4)).ranking.intValue(), is(165));
		assertThat((got.get(5)).ranking.intValue(), is(165));
		assertThat((got.get(6)).ranking.intValue(), is(99));
		assertThat((got.get(7)).ranking.intValue(), is(49));
		assertThat((got.get(8)).ranking.intValue(), is(49));
		assertThat((got.get(9)).ranking.intValue(), is(49));
		assertThat((got.get(10)).ranking.intValue(), is(49));
	}

	@Test
	public void testSearchString() throws SQLException {

		SearchMarque marq = new SearchMarque();
		marq.name.clear();
		DBQuery query = database.getDBQuery(marq);
		final SearchString searchString = new SearchString("+o -l R");

		query.addExpressionColumn(this, marq.column(marq.name).searchForRanking(searchString).asExpressionColumn());
		
		query.setSortOrder(
				marq.column(marq.name).searchForRanking(searchString).descending(),
				marq.column(marq.name).ascending()
		);

		query.addCondition(
				marq.column(marq.name).searchFor(searchString)
		);

		List<SearchMarque> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(11));
		assertThat((got.get(0)).name.stringValue(), is("FORD"));
		assertThat((got.get(1)).name.stringValue(), is("ROVER"));
		assertThat((got.get(2)).name.stringValue(), is("DAEWOO"));
		assertThat((got.get(3)).name.stringValue(), is("HONDA"));
		assertThat((got.get(4)).name.stringValue(), is("PEUGEOT"));
		assertThat((got.get(5)).name.stringValue(), is("TOYOTA"));
		assertThat((got.get(6)).name.stringValue(), is("LANDROVER"));
		assertThat((got.get(7)).name.stringValue(), is("HOLDEN"));
		assertThat((got.get(8)).name.stringValue(), is("HUMMER"));
		assertThat((got.get(9)).name.stringValue(), is("SUBARU"));
		assertThat((got.get(10)).name.stringValue(), is("VOLVO"));
		assertThat((got.get(0)).ranking.intValue(), is(214));
		assertThat((got.get(1)).ranking.intValue(), is(214));
		assertThat((got.get(2)).ranking.intValue(), is(165));
		assertThat((got.get(3)).ranking.intValue(), is(165));
		assertThat((got.get(4)).ranking.intValue(), is(165));
		assertThat((got.get(5)).ranking.intValue(), is(165));
		assertThat((got.get(6)).ranking.intValue(), is(99));
		assertThat((got.get(7)).ranking.intValue(), is(49));
		assertThat((got.get(8)).ranking.intValue(), is(49));
		assertThat((got.get(9)).ranking.intValue(), is(49));
		assertThat((got.get(10)).ranking.intValue(), is(49));
	}

	@Test
	public void testSearchStringWithQuotes() throws SQLException {

		SearchMarque marq = new SearchMarque();
		marq.name.clear();
		DBQuery query = database.getDBQuery(marq);
		final SearchString searchString = new SearchString("+o \"HO\"");

		query.addCondition(
				marq.column(marq.name).searchFor(searchString)
		);
		query.setSortOrder(
				marq.column(marq.name).searchForRanking(searchString).descending(),
				marq.column(marq.name).ascending()
		);

		List<SearchMarque> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(9));
		assertThat((got.get(0)).name.stringValue(), is("HOLDEN"));
		assertThat((got.get(1)).name.stringValue(), is("HONDA"));
		assertThat((got.get(2)).name.stringValue(), is("DAEWOO"));
		assertThat((got.get(3)).name.stringValue(), is("FORD"));
		assertThat((got.get(4)).name.stringValue(), is("LANDROVER"));
		assertThat((got.get(5)).name.stringValue(), is("PEUGEOT"));
		assertThat((got.get(6)).name.stringValue(), is("ROVER"));
		assertThat((got.get(7)).name.stringValue(), is("TOYOTA"));
		assertThat((got.get(8)).name.stringValue(), is("VOLVO"));
	}

	@Test
	public void testSearchStringUsingAdds() throws SQLException {

		SearchMarque marq = new SearchMarque();
		marq.name.clear();
		DBQuery query = database.getDBQuery(marq);
		final SearchAcross searchString
				= new SearchAcross()
						.addTerm("n")
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

		query.addCondition(
				searchString.getComparisonExpression()
		);
		List<DBQueryRow> got = query.getAllRows();

		assertThat(got.size(), is(4));
		assertThat((got.get(0)).get(marq).name.stringValue(), is("HOLDEN"));
		assertThat((got.get(1)).get(marq).name.stringValue(), is("HONDA"));
		assertThat((got.get(2)).get(marq).name.stringValue(), is("HYUNDAI"));
		assertThat((got.get(3)).get(marq).name.stringValue(), is("NISSAN"));

		assertThat(got.get(0).getExpressionColumnValue(this).getValue().toString(), is("15.5"));
		assertThat(got.get(1).getExpressionColumnValue(this).getValue().toString(), is("15.5"));
		assertThat(got.get(2).getExpressionColumnValue(this).getValue().toString(), is("5.5"));
		assertThat(got.get(3).getExpressionColumnValue(this).getValue().toString(), is("5.5"));
	}
	
	@Test
	public void testStringSearchRanking() throws SQLException {

		SearchMarque marq = new SearchMarque();
		marq.name.clear();
		DBQuery query = database.getDBQuery(marq);
		IntegerColumn rankColumn = marq.column(marq.ranking);

		query.addExpressionColumn(this, rankColumn.asExpressionColumn());
		query.setSortOrder(
				rankColumn.descending(),
				marq.column(marq.name).ascending()
		);
		
		query.addCondition(rankColumn.isGreaterThan(0));

		List<SearchMarque> got = query.getAllInstancesOf(marq);

		assertThat(got.size(), is(11));
		assertThat((got.get(0)).name.stringValue(), is("FORD"));
		assertThat((got.get(1)).name.stringValue(), is("ROVER"));
		assertThat((got.get(2)).name.stringValue(), is("DAEWOO"));
		assertThat((got.get(3)).name.stringValue(), is("HONDA"));
		assertThat((got.get(4)).name.stringValue(), is("PEUGEOT"));
		assertThat((got.get(5)).name.stringValue(), is("TOYOTA"));
		assertThat((got.get(6)).name.stringValue(), is("LANDROVER"));
		assertThat((got.get(7)).name.stringValue(), is("HOLDEN"));
		assertThat((got.get(8)).name.stringValue(), is("HUMMER"));
		assertThat((got.get(9)).name.stringValue(), is("SUBARU"));
		assertThat((got.get(10)).name.stringValue(), is("VOLVO"));
		assertThat((got.get(0)).ranking.intValue(), is(214));
		assertThat((got.get(1)).ranking.intValue(), is(214));
		assertThat((got.get(2)).ranking.intValue(), is(165));
		assertThat((got.get(3)).ranking.intValue(), is(165));
		assertThat((got.get(4)).ranking.intValue(), is(165));
		assertThat((got.get(5)).ranking.intValue(), is(165));
		assertThat((got.get(6)).ranking.intValue(), is(99));
		assertThat((got.get(7)).ranking.intValue(), is(49));
		assertThat((got.get(8)).ranking.intValue(), is(49));
		assertThat((got.get(9)).ranking.intValue(), is(49));
		assertThat((got.get(10)).ranking.intValue(), is(49));
	}
}
