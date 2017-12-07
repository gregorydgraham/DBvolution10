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
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.Matchers.*;

public class DBTableUpdateTest extends AbstractTest {

	public DBTableUpdateTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void changingPrimaryKey() throws SQLException, UnexpectedNumberOfRowsException {
		Marque marqueExample = new Marque();
		marqueExample.getUidMarque().permittedValues(1);

		marquesTable.getRowsByExample(marqueExample);
		Marque toyota = marquesTable.getOnlyRowByExample(marqueExample);
		toyota.uidMarque.setValue(99999);
		DBActionList updateList = marquesTable.update(toyota);
		final String standardSQL = "UPDATE MARQUE SET UID_MARQUE = 99999 WHERE (UID_MARQUE = 1);";
		final String oracleSQL = "update OO1081299805 set uid_marque = 99999 where (uid_marque = 1)";
		Assert.assertThat(testableSQL(updateList.get(0).getSQLStatements(database).get(0)),
				anyOf(
						is(testableSQL(standardSQL)),
						is(testableSQL(oracleSQL))
				));
		marquesTable.update(toyota);
		toyota.name.setValue("NOTOYOTA");
		Assert.assertThat(testableSQL(marquesTable.update(toyota).get(0).getSQLStatements(database).get(0)),
				isIn(new String[]{
			testableSQL("UPDATE MARQUE SET NAME = 'NOTOYOTA' WHERE (UID_MARQUE = 99999);"),
			testableSQL("UPDATE MARQUE SET NAME = N'NOTOYOTA' WHERE (UID_MARQUE = 99999);"),}));

		marqueExample = new Marque();
		marqueExample.uidMarque.permittedValues(99999);
		toyota = marquesTable.getOnlyRowByExample(marqueExample);
		Assert.assertThat(toyota.name.toString(), is("NOTOYOTA"));
	}

	@Test
	public void testInsertRows() throws SQLException {
		Marque myTableRow = new Marque();
		myTableRow.getUidMarque().permittedValues(1);

		marquesTable.getRowsByExample(myTableRow);

		Marque toyota = marquesTable.getFirstRow();

		Assert.assertEquals("The row retrieved should be TOYOTA", "TOYOTA", toyota.name.toString());

		toyota.name.setValue("NOTTOYOTA");
		String sqlForUpdate = marquesTable.update(toyota).get(0).getSQLStatements(database).get(0);
//		Assert.assertEquals("Update statement doesn't look right:", testableSQL("UPDATE MARQUE SET NAME = 'NOTTOYOTA' WHERE (UID_MARQUE = 1);"), testableSQL(sqlForUpdate));
		Assert.assertThat(testableSQL(sqlForUpdate),
				isIn(new String[]{
			testableSQL("UPDATE MARQUE SET NAME = 'NOTTOYOTA' WHERE (UID_MARQUE = 1);"),
			testableSQL("UPDATE MARQUE SET NAME = N'NOTTOYOTA' WHERE (UID_MARQUE = 1);")
		}
				));

		marquesTable.update(toyota);

		marquesTable.getRowsByExample(myTableRow);

		toyota = marquesTable.getFirstRow();
		Assert.assertEquals("The row retrieved should be NOTTOYOTA", "NOTTOYOTA", toyota.name.toString());
	}
}
