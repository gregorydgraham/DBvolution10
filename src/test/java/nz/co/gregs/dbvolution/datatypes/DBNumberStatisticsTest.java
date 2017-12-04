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
import static org.junit.Assert.assertThat;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class DBNumberStatisticsTest extends AbstractTest {

	public DBNumberStatisticsTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testBasic() throws SQLException {

		DBQuery dbQuery = database.getDBQuery(new StatsTest()).setBlankQueryAllowed(true);
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		assertThat(allRows.size(), is(1));
		final StatsTest row = allRows.get(0).get(new StatsTest());
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

	//@Test
	public void testMin() {
	}

	//@Test
	public void testMax() {
	}

	//@Test
	public void testMedian() {
	}

	//@Test
	public void testAverage() {
	}

	//@Test
	public void testFirstQuartile() {
	}

	//@Test
	public void testThirdQuartile() {
	}

	//@Test
	public void testSecondQuartile() {
	}

	//@Test
	public void testGetSQLDatatype() {
	}

	//@Test
	public void testIsAggregator() {
	}

	//@Test
	public void testCopy() {
	}

	//@Test
	public void testSetFromResultSet() throws Exception {
	}

	//@Test
	public void testToString() {
	}

	public static class StatsTest extends Marque {

		private static final long serialVersionUID = 1L;

		public StatsTest() {
		}

		@DBColumn
		public DBNumberStatistics stats = new DBNumberStatistics(this.column(uidMarque));

		{
			this.setReturnFields(stats);
		}
	}

}
