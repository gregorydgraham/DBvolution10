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
package nz.co.gregs.dbvolution.transactions;

import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class RawSQLTransactionTest extends AbstractTest {

	public RawSQLTransactionTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testRawSQLTransactionRollback() throws Exception {
		DBRawSQLTransaction sqlTrans = new DBRawSQLTransaction("update marque set name = 'Peugeot' where name = 'PEUGEOT'");
		Boolean doneTrans = database.doTransaction(sqlTrans, Boolean.FALSE);
		Assert.assertThat(doneTrans, is(true));
		Marque mrq = new Marque();
		mrq.name.permittedValues("PEUGEOT");
		Marque peugeot = database.getDBTable(mrq).getOnlyRowByExample(mrq);
		Assert.assertThat(peugeot, is(not(nullValue())));
	}

	@Test
	public void testRawSQLTransactionCommit() throws Exception {
		DBRawSQLTransaction sqlTrans = new DBRawSQLTransaction("update marque set name = 'Peugeot' where name = 'PEUGEOT'");
		Boolean doneTrans = database.doTransaction(sqlTrans, Boolean.TRUE);
		Assert.assertThat(doneTrans, is(true));
		Marque mrq = new Marque();
		mrq.name.permittedValues("Peugeot");
		Marque peugeot = database.getDBTable(mrq).getOnlyRowByExample(mrq);
		Assert.assertThat(peugeot, is(not(nullValue())));
	}

	@Test
	public void testRawSQLTransactionWithManyStatements() throws Exception {
		DBRawSQLTransaction sqlTrans = new DBRawSQLTransaction(
				"update marque set name = 'Peugeot' where name = 'PEUGEOT'");
		Boolean doneTrans = database.doTransaction(sqlTrans, Boolean.TRUE);
		sqlTrans = new DBRawSQLTransaction(
				"update marque set name = 'Toyota' where name = 'TOYOTA'");
		doneTrans = database.doTransaction(sqlTrans, Boolean.TRUE);
		Assert.assertThat(doneTrans, is(true));
		Marque mrq = new Marque();
		mrq.name.permittedValues("Peugeot", "Toyota");
		DBTable<Marque> rows = database.getDBTable(mrq);
		Assert.assertThat(rows.toList(), is(not(Matchers.empty())));
		Assert.assertThat(rows.toList().size(), is(2));
	}
}
