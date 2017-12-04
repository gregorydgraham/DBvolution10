/*
 * Copyright 2015 gregory.graham.
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
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.Matchers.is;

public class TestSQLInjectionPrevention extends AbstractTest {

	public TestSQLInjectionPrevention(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testSQLInjectionPrevention() throws SQLException {
		Marque newMarque1 = new Marque();
		int newID = 998;
		newMarque1.getUidMarque().setValue(newID);
		String nastyString = "TOYOTA'; drop table marques;";
		newMarque1.getName().setValue(nastyString);
		newMarque1.getNumericCode().setValue(10);

		DBActionList changes = database.insert(newMarque1);
		List<Marque> allRows = marquesTable.setBlankQueryAllowed(true).getAllRows();
		Assert.assertThat(allRows.size(), is(23));

		newMarque1 = new Marque();
		newMarque1.uidMarque.permittedValues(newID);
		List<Marque> allRows1 = database.getDBTable(newMarque1).getAllRows();
		Assert.assertThat(allRows1.size(), is(1));
		Assert.assertThat(allRows1.get(0).name.stringValue(), is(nastyString));

	}

	@Test
	public void testSQLInjectionPreventionDuringLIKE() throws SQLException {

		String nastyString = "TOYOTA'; drop table marques;";
		Marque newMarque1 = new Marque();
		newMarque1.name.permittedPattern(nastyString);
		List<Marque> allRows1 = database.getDBTable(newMarque1).getAllRows();
		Assert.assertThat(allRows1.size(), is(0));

		List<Marque> allRows = marquesTable.setBlankQueryAllowed(true).getAllRows();
		Assert.assertThat(allRows.size(), is(22));
	}

}
