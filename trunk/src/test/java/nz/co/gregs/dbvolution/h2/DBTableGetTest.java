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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import nz.co.gregs.dbvolution.DBDate;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.DBTableRow;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.example.MarqueSelectQuery;

/**
 *
 * @author gregory.graham
 */
public class DBTableGetTest extends AbstractTest {

    Marque myTableRow = new Marque();
    List<Marque> myTableRows = new ArrayList<Marque>();

    public DBTableGetTest(String testName) {
        super(testName);
    }

    public void testGetAllRows() throws SQLException {
        marques.getAllRows();
        for (DBTableRow row : marques.toList()) {
            System.out.println(row);
        }
        assertTrue("Incorrect number of marques retreived", marques.toList().size() == marqueRows.size());
    }

    public void testGetFirstAndPrimaryKey() throws SQLException {
        DBTable<Marque> singleMarque = new DBTable<Marque>(myDatabase, new Marque());
        DBTableRow row = marqueRows.get(0);
        String primaryKey;
        if (row != null) {
            primaryKey = row.getPrimaryKeySQLStringValue();
            singleMarque.getRowsByPrimaryKey(Long.parseLong(primaryKey));
            singleMarque.printAllRows();
        }
        assertTrue("Incorrect number of marques retreived", singleMarque.toList().size() == 1);
    }

    public void testNumberIsBetween() throws SQLException{
        Marque marqueQuery = new Marque();
        marqueQuery.getUidMarque().isBetween(0, 90000000);
        //System.out.println(marques.getSQLForExample(marqueQuery));
        marques = marques.getRowsByExample(marqueQuery);
        for (Marque row : marques.toList()) {
            System.out.println(row);
        }
        assertTrue("Incorrect number of marques retreived", marques.toList().size() == marqueRows.size());
    }

    public void testIsLiterally() throws SQLException{
        Marque literalQuery = new Marque();
        literalQuery.getUidMarque().isLiterally(4893059);
        marques = marques.getRowsByExample(literalQuery);
        marques.printAllRows();
        assertEquals(marques.toList().size(), 1);
        assertEquals("" + 4893059, marques.toList().get(0).getPrimaryKeySQLStringValue());
    }

    public void testIsIn() throws SQLException{
        Marque hummerQuery = new Marque();
        hummerQuery.getUidMarque().blankQuery();
        hummerQuery.getName().isIn(new String[]{"PEUGEOT", "HUMMER"});
        marques = marques.getRowsByExample(hummerQuery);
        marques.printAllRows();
        assertEquals(marques.toList().size(), 2);
    }

    public void testDateIsBetween() throws SQLException{
        GregorianCalendar gregorianCalendar = new GregorianCalendar();
        gregorianCalendar.add(Calendar.SECOND, 60);
        Date future = gregorianCalendar.getTime();

        Marque oldQuery = new Marque();
        oldQuery.getCreationDate().isBetween(new Date(0L), future);
        marques = marques.getRowsByExample(oldQuery);
        marques.printAllRows();
        assertTrue("Wrong number of rows selected, should be all but one of them", marques.toList().size() == marqueRows.size()-1);
    }

    public void testDateIsLessThanAndGreaterThan() throws SQLException{
        GregorianCalendar gregorianCalendar = new GregorianCalendar();
        gregorianCalendar.add(Calendar.SECOND, 60);
        Date future = gregorianCalendar.getTime();
        Marque oldQuery = new Marque();
        oldQuery.getCreationDate().isLessThan(new DBDate(future));
        marques = marques.getRowsByExample(oldQuery);
        marques.printAllRows();
        assertTrue("Wrong number of rows selected, should be all but one of them", marques.toList().size() == marqueRows.size()-1);
        oldQuery.getCreationDate().isGreaterThan(new DBDate(future));
        marques = marques.getRowsByExample(oldQuery);
        marques.printAllRows();
        assertTrue("Wrong number of rows selected, should be NONE of them", marques.toList().isEmpty());
        oldQuery = new Marque();
        oldQuery.getCreationDate().isLessThanOrEqualTo(new DBDate(future));
        marques = marques.getRowsByExample(oldQuery);
        marques.printAllRows();
        assertTrue("Wrong number of rows selected, should be all but one of them", marques.toList().size() == marqueRows.size()-1);
        oldQuery.getCreationDate().isGreaterThan(new DBDate(new Date(0L)));
        marques = marques.getRowsByExample(oldQuery);
        marques.printAllRows();
        assertTrue("Wrong number of rows selected, should be all but one of them", marques.toList().size() == marqueRows.size()-1);
        oldQuery.getCreationDate().isLessThan(new DBDate(new Date(0L)));
        marques = marques.getRowsByExample(oldQuery);
        marques.printAllRows();
        assertTrue("Wrong number of rows selected, should be NONE of them", marques.toList().isEmpty());
        oldQuery = new Marque();
        oldQuery.getCreationDate().isGreaterThanOrEqualTo(new DBDate(new Date(0L)));
        marques = marques.getRowsByExample(oldQuery);
        marques.printAllRows();
        assertTrue("Wrong number of rows selected, should be all but one of them", marques.toList().size() == marqueRows.size()-1);
    }

    public void testRawQuery() throws SQLException{
        String rawQuery = "and lower(name) in ('peugeot','hummer')  ";
        marques = marques.getByRawSQL(rawQuery);
        marques.printAllRows();
        assertEquals(marques.toList().size(), 2);
    }

    public void testDBSelectQuery() throws SQLException {
        DBTable<MarqueSelectQuery> msq = new DBTable<MarqueSelectQuery>(myDatabase, new MarqueSelectQuery());
        msq.getAllRows();
        msq.printAllRows();

        MarqueSelectQuery marqueSelectQuery = new MarqueSelectQuery();
        marqueSelectQuery.uidMarque.isLiterally(1);
        msq.getRowsByExample(marqueSelectQuery);
        msq.printAllRows();
    }
}
