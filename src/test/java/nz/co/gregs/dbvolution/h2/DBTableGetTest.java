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
package nz.co.gregs.dbvolution.h2;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import junit.framework.TestCase;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.DBTableRow;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.H2DB;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.example.MarqueSelectQuery;

/**
 *
 * @author gregory.graham
 */
public class DBTableGetTest extends TestCase {

    DBDatabase myDatabase = new H2DB("jdbc:h2:~/dbvolutionGetTest", "", "");
    Marque myTableRow = new Marque();
    DBTable<Marque> marques;
    List<Marque> myTableRows = new ArrayList<Marque>();

    public DBTableGetTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        myDatabase.dropTableNoExceptions(myTableRow);
        myDatabase.createTable(myTableRow);
        DBTable.setPrintSQLBeforeExecuting(false);
        marques = new DBTable<Marque>(myTableRow, myDatabase);


        myTableRows.add(new Marque(4893059, "False", 1246974, "", 3, "UV", "PEUGEOT", "", "Y"));
        myTableRows.add(new Marque(4893090, "False", 1246974, "", 1, "UV", "FORD", "", "Y"));
        myTableRows.add(new Marque(4893101, "False", 1246974, "", 2, "UV", "HOLDEN", "", "Y"));
        myTableRows.add(new Marque(4893112, "False", 1246974, "", 2, "UV", "MITSUBISHI", "", "Y"));
        myTableRows.add(new Marque(4893150, "False", 1246974, "", 3, "UV", "SUZUKI", "", "Y"));
        myTableRows.add(new Marque(4893263, "False", 1246974, "", 2, "UV", "HONDA", "", "Y"));
        myTableRows.add(new Marque(4893353, "False", 1246974, "", 4, "UV", "NISSAN", "", "Y"));
        myTableRows.add(new Marque(4893557, "False", 1246974, "", 2, "UV", "SUBARU", "", "Y"));
        myTableRows.add(new Marque(4894018, "False", 1246974, "", 2, "UV", "MAZDA", "", "Y"));
        myTableRows.add(new Marque(4895203, "False", 1246974, "", 2, "UV", "ROVER", "", "Y"));
        myTableRows.add(new Marque(4896300, "False", 1246974, "", 2, "UV", "HYUNDAI", "", "Y"));
        myTableRows.add(new Marque(4899527, "False", 1246974, "", 1, "UV", "JEEP", "", "Y"));
        myTableRows.add(new Marque(7659280, "False", 1246972, "Y", 3, "", "DAIHATSU", "", "Y"));
        myTableRows.add(new Marque(7681544, "False", 1246974, "", 2, "UV", "LANDROVER", "", "Y"));
        myTableRows.add(new Marque(7730022, "False", 1246974, "", 2, "UV", "VOLVO", "", "Y"));
        myTableRows.add(new Marque(8376505, "False", 1246974, "", 0, "", "ISUZU", "", "Y"));
        myTableRows.add(new Marque(8587147, "False", 1246974, "", 0, "", "DAEWOO", "", "Y"));
        myTableRows.add(new Marque(9971178, "False", 1246974, "", 1, "", "CHRYSLER", "", "Y"));
        myTableRows.add(new Marque(13224369, "False", 1246974, "", 0, "", "VW", "", "Y"));
        myTableRows.add(new Marque(6664478, "False", 1246974, "", 0, "", "BMW", "", "Y"));

        marques.insert(myTableRows);
        DBTable.setPrintSQLBeforeExecuting(true);
    }

    @Override
    protected void tearDown() throws Exception {
        myDatabase.dropTable(myTableRow);

        super.tearDown();
    }
    // TODO add test methods here. The name must begin with 'test'. For example:
    // public void testHello() {}

    public void testGetAllRows() throws IllegalArgumentException, IllegalAccessException, IntrospectionException, InvocationTargetException, SQLException, InstantiationException, NoSuchMethodException {
        marques.getAllRows();
        for (DBTableRow row : marques) {
            System.out.println(row);
        }
        assertTrue("Incorrect number of marques retreived", marques.size() == myTableRows.size());
    }

    public void testGetFirstAndPrimaryKey() throws SQLException, IllegalArgumentException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, IntrospectionException, InstantiationException, SQLException, ClassNotFoundException {
        DBTable<Marque> singleMarque = new DBTable<Marque>(new Marque(), myDatabase);
        DBTableRow row = myTableRows.get(0);
        String primaryKey;
        if (row != null) {
            primaryKey = row.getPrimaryKey();
            singleMarque.getByPrimaryKey(Long.parseLong(primaryKey));
            singleMarque.printAllRows();
        }
        assertTrue("Incorrect number of marques retreived", singleMarque.size() == 1);
    }

    public void testNumberIsBetween() throws IllegalArgumentException, IllegalAccessException, SQLException, InstantiationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException, IntrospectionException {
        Marque marqueQuery = new Marque();
        marqueQuery.getUidMarque().isBetween(0, 90000000);
        //System.out.println(marques.getSQLForExample(marqueQuery));
        marques = marques.getByExample(marqueQuery);
        for (Marque row : marques) {
            System.out.println(row);
        }
        assertTrue("Incorrect number of marques retreived", marques.size() == myTableRows.size());
    }

    public void testIsLiterally() throws IllegalArgumentException, IllegalAccessException, SQLException, InstantiationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException, IntrospectionException {
        Marque literalQuery = new Marque();
        literalQuery.getUidMarque().isLiterally(4893059);
        marques = marques.getByExample(literalQuery);
        marques.printAllRows();
        assertEquals(marques.size(), 1);
        assertEquals("" + 4893059, marques.get(0).getPrimaryKey());
    }

    public void testIsIn() throws IllegalArgumentException, IllegalAccessException, SQLException, InstantiationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException, IntrospectionException {
        Marque hummerQuery = new Marque();
        hummerQuery.getUidMarque().blankQuery();
        hummerQuery.getName().isIn(new String[]{"PEUGEOT", "HUMMER"});
        marques = marques.getByExample(hummerQuery);
        marques.printAllRows();
        assertEquals(marques.size(), 1);
    }

    public void testDateIsBetween() throws IllegalArgumentException, IllegalAccessException, SQLException, InstantiationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException, IntrospectionException {
        Marque oldQuery = new Marque();
        oldQuery.getCreationDate().isBetween(new Date(0L), new Date());
        marques = marques.getByExample(oldQuery);
        marques.printAllRows();
        assertTrue("Wrong number of rows selected, should be all of them", marques.size() == myTableRows.size());
    }

    public void testRawQuery() throws IllegalArgumentException, IllegalAccessException, SQLException, InstantiationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException, IntrospectionException {
        String rawQuery = "and lower(name) in ('peugeot','hummer') ;  ";
        marques = marques.getByRawSQL(rawQuery);
        marques.printAllRows();
        assertEquals(marques.size(), 1);
    }
    
    public void testDBSelectQuery() throws SQLException, InstantiationException, IllegalArgumentException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, IntrospectionException{
        DBTable<MarqueSelectQuery> msq = new DBTable<MarqueSelectQuery>(new MarqueSelectQuery(), myDatabase);
        msq.getAllRows();
        msq.printAllRows();
    }
}
