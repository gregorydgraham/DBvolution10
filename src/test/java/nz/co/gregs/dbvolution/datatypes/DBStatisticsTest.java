/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.datatypes;

import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.columns.IntegerColumn;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.expressions.IntegerExpression;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Test;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import static org.junit.Assert.assertThat;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class DBStatisticsTest extends AbstractTest {

	public DBStatisticsTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testBasic() throws SQLException {

		DBQuery dbQuery = database.getDBQuery(new StatsTest()).setBlankQueryAllowed(true);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		final StatsTest row = allRows.get(0).get(new StatsTest());
		assertThat(row.stats.count().intValue(), is(22));
	}

	@Test
	public void testModeSimpleQuery() throws SQLException {
		final Marque marque = new Marque();
		marque.setReturnFieldsToNone();
		final IntegerColumn updateCountColumn = marque.column(marque.updateCount);
		DBInteger count = updateCountColumn.count().asExpressionColumn();
		count.setSortOrderDescending();
		DBQuery query = database
				.getDBQuery(marque);
		query.setBlankQueryAllowed(true)
				.setReturnFields(updateCountColumn)
				.addExpressionColumn(this, count) 
				.setSortOrder(query.column(count))
				.setRowLimit(1)
				;
		final String sql = testableSQL(query.getSQLForQuery());
		System.out.println("" + sql);
		
		List<DBQueryRow> allRows = query.getAllRows();
//		database.print(allRows);
		QueryableDatatype<?> expressionColumnValue = allRows.get(0).getExpressionColumnValue(this);
		Assert.assertThat(expressionColumnValue.stringValue(), is("9"));
		
//		Assert.assertThat(sql, is(this.testableSQL(
//				"SELECT a1.upd_count mode, COUNT(a1.upd_count) counter \n"
//				+ "	FROM marque \n"
//				+ "	group by a1.upd_count\n"
//				+ "	order by counter desc\n"
//				+ "     limit 1 offset 0")));
	}

	public static class StatsTest extends Marque {

		private static final long serialVersionUID = 1L;

		public StatsTest() {
		}

		@DBColumn
		public DBStatistics stats = new DBStatistics(this.column(this.carCompany));

		{
			this.setReturnFields(stats);
		}
	}

}
