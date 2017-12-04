/*
 * Copyright 2013 Gregory Graham.
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
import java.util.Date;
import java.util.List;
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.actions.DBInsert;
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
public class DBTableInsertTest extends AbstractTest {

	Marque myTableRow = new Marque();

	public DBTableInsertTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testInsertRows() throws SQLException {
		myTableRow.getUidMarque().setValue(999);
		myTableRow.getName().setValue("TOYOTA");
		myTableRow.getNumericCode().setValue(10);
		marquesTable.insert(myTableRow);
		List<Marque> allRows = marquesTable.setBlankQueryAllowed(true).getAllRows();
		Assert.assertThat(allRows.size(), is(23));

		Date creationDate = new Date();
		List<Marque> myTableRows = new ArrayList<Marque>();
		myTableRows.add(new Marque(3, "False", 1246974, "", 3, "UV", "TVR", "", "Y", creationDate, 4, null));

		marquesTable.insert(myTableRows);
		allRows = marquesTable.getAllRows();
		Assert.assertThat(allRows.size(), is(24));
	}

	@Test
	public void testInsertIncompleteRows() throws SQLException {
		Marque marque = new Marque();
		marque.getUidMarque().setValue(999);
		marque.getName().setValue("TOYOTA");
		marque.getNumericCode().setValue(10);
		DBActionList insertActions = DBInsert.getInserts(marque);
		DBAction possibleInsert = insertActions.get(0);
		Assert.assertThat(possibleInsert.getClass().getSimpleName(), is(DBInsert.class.getSimpleName()));
		if (possibleInsert instanceof DBInsert) {
			DBInsert insert = (DBInsert) possibleInsert;
			String sql = insert.getSQLStatements(database).get(0);
			Assert.assertThat(sql.toUpperCase(), containsString("NAME"));
			Assert.assertThat(sql.toUpperCase(), not(containsString("CREATION_DATE")));
			Assert.assertThat(sql.toUpperCase(), not(containsString("FK_CARCOMPANY")));
		}
	}
}
