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
package nz.co.gregs.dbvolution.exceptions;

import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.DBScript;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Test;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class AutoCommitActionDuringTransactionExceptionTest extends AbstractTest {

	public AutoCommitActionDuringTransactionExceptionTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test(expected = AutoCommitActionDuringTransactionException.class)
	public void testDropTableThrowsAutoCommitException() throws Exception {
		DBScript badScript = new DBScript() {

			@Override
			public DBActionList script(DBDatabase db) throws Exception {
				db.dropTable(new Marque());
				return new DBActionList();
			}
		};
		database.test(badScript);
	}

	@Test(expected = AutoCommitActionDuringTransactionException.class)
	public void testCreateTableThrowsAutoCommitException() throws Exception {
		DBScript badScript = new DBScript() {

			@Override
			public DBActionList script(DBDatabase db) throws Exception {
				db.createTable(new Marque());
				return new DBActionList();
			}
		};
		database.test(badScript);
	}

	@Test(expected = AutoCommitActionDuringTransactionException.class)
	public void testDropDatabaseThrowsAutoCommitException() throws Exception {
		DBScript badScript = new DBScript() {

			@Override
			public DBActionList script(DBDatabase db) throws Exception {
				db.dropDatabase(false);
				return new DBActionList();
			}
		};
		database.test(badScript);
	}

}
