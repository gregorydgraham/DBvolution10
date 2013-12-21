/*
 * Copyright 2013 gregory.graham.
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
package nz.co.gregs.dbvolution.generic;

import nz.co.gregs.dbvolution.transactions.DBTransaction;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.example.Marque;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author gregory.graham
 */
public class DBTableTransactionTest extends AbstractTest {

    Marque myTableRow = new Marque();

    public DBTableTransactionTest(Object testIterationName, Object db) {
        super(testIterationName, db);
    }

    @Test
    public void testInsertRowsSucceeds() throws SQLException, Exception{
        List<Marque> original = marques.setBlankQueryAllowed(true).getRowsByExample(new Marque()).toList();
        System.out.println("original.toList().size(): " + original.size());
        DBTable<Marque> transacted = database.doTransaction(new DBTransaction<DBTable<Marque>>() {
            @Override
            public DBTable<Marque> doTransaction(DBDatabase dbDatabase) throws SQLException {
                Marque myTableRow = new Marque();
                DBTable<Marque> marques = DBTable.getInstance(dbDatabase, myTableRow);
                myTableRow.getUidMarque().permittedValues(999);
                myTableRow.getName().permittedValues("TOYOTA");
                myTableRow.getNumericCode().permittedValues(10);
                marques.insert(myTableRow);
                marques.setBlankQueryAllowed(true).getAllRows();
                marques.print();

                List<Marque> myTableRows = new ArrayList<Marque>();
                myTableRows.add(new Marque(3, "False", 1246974, "", 3, "UV", "TVR", "", "Y", new Date(), 4,null));

                System.out.println("EXPECT A UNIQUE CONSTRAINT VIOLATION EXCEPTION HERE:");
                marques.insert(myTableRows);

                marques.getAllRows();
                marques.print();
                return marques;
            }
        });
        List<Marque> added = marques.getRowsByExample(new Marque()).toList();
        System.out.println("original.toList().size(): " + original.size());
        System.out.println("added.toList().size(): " + added.size());
        Assert.assertTrue("Length of list after insert should be longer than the original", added.size() == original.size() + 2);
    }

    @Test
    public void testInsertRowsFailure() throws SQLException {
        List<Marque> original = marques.setBlankQueryAllowed(true).getRowsByExample(new Marque()).toList();
        System.out.println("original.toList().size(): " + original.size());
        try{
        DBTable<Marque> transacted = database.doTransaction(new DBTransaction<DBTable<Marque>>() {
            @Override
            public DBTable<Marque> doTransaction(DBDatabase dbDatabase) throws SQLException{
                Marque myTableRow = new Marque();
                DBTable<Marque> marques = DBTable.getInstance(dbDatabase, myTableRow);
                myTableRow.getUidMarque().permittedValues(999);
                myTableRow.getName().permittedValues("TOYOTA");
                myTableRow.getNumericCode().permittedValues(10);
                marques.insert(myTableRow);
//                marques.getAllRows();
//                marques.printRows();

                List<Marque> myTableRows = new ArrayList<Marque>();
                myTableRows.add(new Marque(999, "False", 1246974, "", 3, "UV", "TVR", "", "Y", new Date(), 4,null));

                marques.insert(myTableRows);

                marques.getAllRows();
                marques.print();
                return marques;
            }

        });
        }catch (Exception e){
            e.printStackTrace();
        }
        final DBTable<Marque> addedRows = marques.getRowsByExample(new Marque());
        addedRows.print();
        List<Marque> added = addedRows.toList();
        System.out.println("original.toList().size(): " + original.size());
        System.out.println("added.toList().size(): " + added.size());
        Assert.assertTrue("Length of list after insert should be the same as the original", added.size() == original.size());

    }
}
