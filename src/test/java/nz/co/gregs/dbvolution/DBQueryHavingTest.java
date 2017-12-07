/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.Matchers.is;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class DBQueryHavingTest extends AbstractTest {

	public DBQueryHavingTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void getRowsHavingTest() throws SQLException {
		MarqueCounter marqueCounter = new MarqueCounter();
		marqueCounter.carCompany.permittedRange(0, null);
		DBQuery query = database.getDBQuery(new CarCompany(), marqueCounter).setBlankQueryAllowed(true);
		query.setSortOrder(marqueCounter.column(marqueCounter.carCompany));

		List<DBQueryRow> rows = query.getAllRows();

		rows = database
				.getDBQuery(new CarCompany(), marqueCounter)
				.setSortOrder(marqueCounter.column(marqueCounter.carCompany))
				.addCondition(marqueCounter.column(marqueCounter.counted).isGreaterThan(1))
				.getAllRows();

		Assert.assertThat(rows.size(), is(3));

		CarCompany currentCarCo = rows.get(0).get(new CarCompany());
		MarqueCounter currentMarque = rows.get(0).get(new MarqueCounter());

		Assert.assertThat(currentCarCo.name.stringValue(), is("TOYOTA"));
		Assert.assertThat(currentMarque.counted.intValue(), is(2));

		currentCarCo = rows.get(1).get(new CarCompany());
		currentMarque = rows.get(1).get(new MarqueCounter());

		Assert.assertThat(currentCarCo.name.stringValue(), is("GENERAL MOTORS"));
		Assert.assertThat(currentMarque.counted.intValue(), is(3));

		currentCarCo = rows.get(2).get(new CarCompany());
		currentMarque = rows.get(2).get(new MarqueCounter());

		Assert.assertThat(currentCarCo.name.stringValue(), is("OTHER"));
		Assert.assertThat(currentMarque.counted.intValue(), is(16));
	}

	private static class MarqueCounter extends Marque {

		private static final long serialVersionUID = 1L;

		public MarqueCounter() {
		}
		@DBColumn
		DBNumber counted = new DBNumber(NumberExpression.countAll());

		{
			setReturnFields(counted, carCompany);
		}
	}

}
