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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.exceptions.ExceptionThrownDuringTransaction;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBTableTransactionTest extends AbstractTest {

	Marque myTableRow = new Marque();

	public DBTableTransactionTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testInsertRowsSucceeds() throws SQLException, Exception {
		List<Marque> original = marquesTable.setBlankQueryAllowed(true).getRowsByExample(new Marque());
		DBTable<Marque> transacted = database.doTransaction(new DBTransaction<DBTable<Marque>>() {
			@Override
			public DBTable<Marque> doTransaction(DBDatabase dbDatabase) throws ExceptionThrownDuringTransaction {
				try {
					Marque myTableRow = new Marque();
					DBTable<Marque> marques = DBTable.getInstance(dbDatabase, myTableRow);
					myTableRow.getUidMarque().setValue(999);
					myTableRow.getName().setValue("TOYOTA");
					myTableRow.getNumericCode().setValue(10);
					marques.insert(myTableRow);
					marques.setBlankQueryAllowed(true).getAllRows();
					
					List<Marque> myTableRows = new ArrayList<Marque>();
					myTableRows.add(new Marque(3, "False", 1246974, "", 3, "UV", "TVR", "", "Y", new Date(), 4, null));
					
					marques.insert(myTableRows);
					
					marques.getAllRows();
					return marques;
				} catch (SQLException ex) {
					throw new ExceptionThrownDuringTransaction(ex);
				}
			}
		}, true);
		List<Marque> added = marquesTable.getRowsByExample(new Marque());
		Assert.assertTrue("Length of list after insert should be longer than the original", added.size() == original.size() + 2);
	}

	@Test
	public void testInsertRowsFailure() throws SQLException {
		List<Marque> original = marquesTable.setBlankQueryAllowed(true).getRowsByExample(new Marque());
		try {
			DBTable<Marque> transacted = database.doTransaction(new DBTransaction<DBTable<Marque>>() {
				@Override
				public DBTable<Marque> doTransaction(DBDatabase dbDatabase) throws ExceptionThrownDuringTransaction {
					try {
						Marque myTableRow = new Marque();
						DBTable<Marque> marques = DBTable.getInstance(dbDatabase, myTableRow);
						myTableRow.getUidMarque().permittedValues(999);
						myTableRow.getName().permittedValues("TOYOTA");
						myTableRow.getNumericCode().permittedValues(10);
						marques.insert(myTableRow);
						
						List<Marque> myTableRows = new ArrayList<Marque>();
						myTableRows.add(new Marque(999, "False", 1246974, "", 3, "UV", "TVR", "", "Y", new Date(), 4, null));
						
						marques.insert(myTableRows);
						
						marques.getAllRows();
						return marques;
					} catch (Exception ex) {
						throw new ExceptionThrownDuringTransaction(ex);
					}
				}
			}, true);
		} catch (SQLException | ExceptionThrownDuringTransaction e) {
		}
		final List<Marque> addedRows = marquesTable.getRowsByExample(new Marque());
		List<Marque> added = marquesTable.toList();
		Assert.assertTrue("Length of list after insert should be the same as the original", added.size() == original.size());

	}
}
