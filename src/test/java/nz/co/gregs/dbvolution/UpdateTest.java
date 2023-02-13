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
import java.util.List;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Test;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

public class UpdateTest extends AbstractTest {

	public UpdateTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void updateNewRow() throws SQLException, ClassNotFoundException {
		Marque myMarqueRow = new Marque();
		myMarqueRow.uidMarque.setValue(4);
		marquesTable.insert(myMarqueRow);
		Marque insertedRow = marquesTable.getRowsByPrimaryKey(4).get(0);
		insertedRow.individualAllocationsAllowed.setValue("Y");
		String sqlForUpdate = marquesTable.update(insertedRow).get(0).getSQLStatements(database).get(0);
		final String[] possibleStrings = {
			testableSQL("UPDATE MARQUE SET INTINDALLOCALLOWED = 'Y' WHERE (UID_MARQUE = 4);"),
			testableSQL("UPDATE MARQUE SET INTINDALLOCALLOWED = N'Y' WHERE (UID_MARQUE = 4);"),
			testableSQL("update [marque] set intindallocallowed = n'y' where (uid_marque = 4);"),
			testableSQL("update marque set intindallocallowed = 'y' where (uid_marque = 4)"),
			testableSQL("update marque set \"intindallocallowed\" = 'y' where (uid_marque = 4);")
		};

		assertThat(testableSQL(sqlForUpdate),
				isOneOf(possibleStrings));
//        marquesTable.update(insertedRow);
		insertedRow = marquesTable.getRowsByPrimaryKey(4).get(0);
		assertThat(insertedRow.individualAllocationsAllowed.toString(), is("Y"));
	}

	@Test
	public void updateExistingRow() throws SQLException {
		Marque marque = new Marque();
		marque.name.permittedValues("PEUGEOT");
		List<Marque> rowsByExample = marquesTable.getRowsByExample(marque);
		assertThat(rowsByExample.size(), is(1));
		Marque peugeot = rowsByExample.get(0);

		peugeot.individualAllocationsAllowed.setValue("Y");
		String sqlForUpdate = marquesTable.update(peugeot).get(0).getSQLStatements(database).get(0);
		final String updateQueryStr1 = testableSQL("UPDATE MARQUE SET INTINDALLOCALLOWED = 'Y' WHERE (UID_MARQUE = 4893059);");
		final String updateQueryStr2 = testableSQL("UPDATE MARQUE SET INTINDALLOCALLOWED = N'Y' WHERE (UID_MARQUE = 4893059);");
		final String updateQueryStr3 = testableSQL("update [marque] set intindallocallowed = n'y' where (uid_marque = 4893059);");
		final String updateQueryStrOracle = testableSQL("update marque set intindallocallowed = 'y' where (uid_marque = 4893059)");
		assertThat(testableSQL(sqlForUpdate),
				isOneOf(
						updateQueryStr1,
						updateQueryStr2,
						updateQueryStr3,
						updateQueryStrOracle
				));
		marquesTable.update(peugeot);
		Marque updatePeugeot = marquesTable.getRowsByExample(marque).get(0);
		assertThat(updatePeugeot.individualAllocationsAllowed.toString(), is("Y"));
	}
}
