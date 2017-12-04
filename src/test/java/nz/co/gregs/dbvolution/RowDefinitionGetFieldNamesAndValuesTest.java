/*
 * Copyright 2014 gregory.graham.
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
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.example.*;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregory.graham
 */
public class RowDefinitionGetFieldNamesAndValuesTest extends AbstractTest {

	public RowDefinitionGetFieldNamesAndValuesTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void getFieldNamesAndValuesTest() throws SQLException, IllegalArgumentException, IllegalAccessException {
		List<String> lines = new ArrayList<String>();

		Marque marque = new Marque();
		CarCompany carCompany = new CarCompany();

		marque.setReturnFields(marque.name);

		DBQuery dbQuery = database.getDBQuery(marque, carCompany);
		dbQuery.setSortOrder(marque.column(marque.name), carCompany.column(carCompany.name));
		List<DBQueryRow> rows = dbQuery.setBlankQueryAllowed(true).getAllRows();

		if (rows.size() > 0) {
			String header = "" + rows.get(0).toCSVHeader();
			Assert.assertThat(header, is("\"Marque:name\",\"CarCompany:name\",\"CarCompany:uidCarCompany\""));
			for (DBQueryRow row : rows) {
				String csvLine = "" + row.toCSVLine();
				lines.add(csvLine);

			}
		}
		Assert.assertThat(lines.get(0), is("\"BMW\",\"OTHER\",\"4\""));
	}
}
