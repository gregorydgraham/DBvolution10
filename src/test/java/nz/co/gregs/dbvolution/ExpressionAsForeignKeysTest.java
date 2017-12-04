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
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class ExpressionAsForeignKeysTest extends AbstractTest {

	public ExpressionAsForeignKeysTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testSimpleExpressionsAsForeignKeysDoesntThrowCartesianJoinException() throws SQLException {

		DBQuery dbQuery = database.getDBQuery();
		CarCompany carCompany = new CarCompany();
		carCompany.name.permittedValues("TOYOTA");
		carCompany.ignoreAllForeignKeys();
		final Marque marque = new Marque();
		marque.ignoreAllForeignKeys();
		dbQuery.add(marque);
		dbQuery.add(carCompany);
		dbQuery.addCondition(marque.column(marque.carCompany).is(carCompany.column(carCompany.uidCarCompany)));

		List<DBQueryRow> allRows = dbQuery.getAllRows();
		Assert.assertThat(allRows.size(), is(2));
	}

}
