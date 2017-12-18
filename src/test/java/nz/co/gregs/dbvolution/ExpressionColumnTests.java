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

import java.util.Date;
import java.util.GregorianCalendar;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.expressions.DateExpression;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

public class ExpressionColumnTests extends AbstractTest {

	public ExpressionColumnTests(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void selectDateExpressionWithDBQuery() throws Exception {
		final CarCompany carCompany = new CarCompany();
		final Marque marque = new Marque();
		marque.name.permittedValuesIgnoreCase("TOYOTA");
		DBQuery query = database.getDBQuery(marque, carCompany);

		final Date dateKey = new Date();
		query.addExpressionColumn(dateKey, DateExpression.currentDateOnly().asExpressionColumn());

		if (!(database instanceof DBDatabaseCluster)) {
			final String sqlForQuery = query.getSQLForQuery();
			Assert.assertThat(sqlForQuery, containsString(database.getDefinition().doCurrentDateOnlyTransform().trim()));
		}
		if (!(database instanceof DBDatabaseCluster)) {
			final String sqlForQuery = query.getSQLForQuery();
			Assert.assertThat(sqlForQuery, containsString(database.getDefinition().doCurrentDateOnlyTransform().trim()));
		}
		for (DBQueryRow row : query.getAllRows()) {
			QueryableDatatype<?> expressionColumnValue = row.getExpressionColumnValue(dateKey);
			if (expressionColumnValue instanceof DBDate) {
				GregorianCalendar cal = new GregorianCalendar();
				cal.add(GregorianCalendar.MINUTE,+1);
				Date later = cal.getTime();
				cal.add(GregorianCalendar.MINUTE,-1);
				cal.add(GregorianCalendar.HOUR,-24);
				Date yesterday = cal.getTime();
				DBDate currentDate = (DBDate) expressionColumnValue;
				Assert.assertThat(currentDate.getValue(), lessThan(later));
				Assert.assertThat(currentDate.getValue(), greaterThan(yesterday));
			} else {
				throw new RuntimeException("CurrentDate Expression Failed To Create DBDate Instance");
			}
		}
	}

	@Test
	public void selectStringExpressionWithDBQuery() throws Exception {
		final CarCompany carCompany = new CarCompany();
		final Marque marque = new Marque();
		marque.name.permittedValuesIgnoreCase("TOYOTA");
		DBQuery query = database.getDBQuery(marque, carCompany);

		final String shortMarqueName = "Short marque name";
		query.addExpressionColumn(shortMarqueName, marque.column(marque.name).substring(0, 3).asExpressionColumn());

		final String sqlForQuery = query.getSQLForQuery();
		Assert.assertThat(sqlForQuery, containsString("SUBSTR"));

		for (DBQueryRow row : query.getAllRows()) {
			QueryableDatatype<?> expressionColumnValue = row.getExpressionColumnValue(shortMarqueName);
			if (expressionColumnValue instanceof DBString) {
				DBString shortName = (DBString) expressionColumnValue;
				Assert.assertThat(shortName.toString(), is("TOYOTA".substring(0, 3)));
			} else {
				throw new RuntimeException("String Expression Failed To Create DBString Instance");
			}
		}
	}

	@Test
	public void selectNumberExpressionWithDBQuery() throws Exception {
		final CarCompany carCompany = new CarCompany();
		final Marque marque = new Marque();
		marque.name.permittedValuesIgnoreCase("TOYOTA");
		DBQuery query = database.getDBQuery(marque, carCompany);

		final String strangeEquation = "strange equation";
		query.addExpressionColumn(strangeEquation, marque.column(marque.uidMarque).times(5).dividedBy(3).plus(2).asExpressionColumn());

		for (DBQueryRow row : query.getAllRows()) {
			Long uid = row.get(new Marque()).uidMarque.getValue();
			QueryableDatatype<?> expressionColumnValue = row.getExpressionColumnValue(strangeEquation);
			if (expressionColumnValue instanceof DBNumber) {
				DBNumber eqValue = (DBNumber) expressionColumnValue;
				Assert.assertThat(eqValue.longValue(), is(uid * 5 / 3 + 2));
			} else {
				throw new RuntimeException("String Expression Failed To Create DBNumber Instance");
			}
		}
	}
}
