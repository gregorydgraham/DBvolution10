/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.datatypes;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Test;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author gregorygraham
 */
public class DBNumberStatisticsTest extends AbstractTest {

	public DBNumberStatisticsTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testBasic() throws SQLException {

		DBQuery dbQuery = database
				.getDBQuery(new DBNumberStatisticsTestDBRowClass())
				.setBlankQueryAllowed(true);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		final DBNumberStatisticsTestDBRowClass row = allRows.get(0).get(new DBNumberStatisticsTestDBRowClass());
		assertThat(row.stats.count().intValue(), is(22));
		assertThat(row.stats.sum().intValue(), is(128625259));
		assertThat(row.stats.max().intValue(), is(13224369));
		assertThat(row.stats.min().intValue(), is(1));
		assertThat(Math.rint(row.stats.standardDeviation().doubleValue() * 10000), is(28906523576D));
		BigDecimal bd = new BigDecimal(row.stats.average().doubleValue());
		bd = bd.round(new MathContext(7 + 5));
		double rounded = bd.doubleValue();
		assertThat(rounded, isOneOf(5846602.68182));
	}

	public static class DBNumberStatisticsTestDBRowClass extends Marque {

		private static final long serialVersionUID = 1L;

		public DBNumberStatisticsTestDBRowClass() {
		}

		@DBColumn
		public DBNumberStatistics stats = new DBNumberStatistics(this.column(uidMarque));

		{
			this.setReturnFields(stats);
		}
	}
}
