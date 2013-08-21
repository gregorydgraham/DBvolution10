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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import net.sourceforge.tedhi.DateRange;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.example.MarqueSelectQuery;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author gregory.graham
 */
public class DBTableGetTest extends AbstractTest {

    Marque myTableRow = new Marque();
    List<Marque> myTableRows = new ArrayList<Marque>();

    @Test
    public void testGetAllRows() throws SQLException {
        marques.getAllRows();
        for (DBRow row : marques.toList()) {
            System.out.println(row);
        }
        Assert.assertTrue("Incorrect number of marques retreived", marques.toList().size() == marqueRows.size());
    }

    @Test
    public void testGetFirstAndPrimaryKey() throws SQLException {
        DBTable<Marque> singleMarque = DBTable.getInstance(myDatabase, new Marque());
        DBRow row = marqueRows.get(0);
        String primaryKey;
        if (row != null) {
            primaryKey = row.getPrimaryKeySQLStringValue(myDatabase);
            singleMarque.getRowsByPrimaryKey(Long.parseLong(primaryKey));
            singleMarque.print();
        }
        Assert.assertTrue("Incorrect number of marques retreived", singleMarque.toList().size() == 1);
    }

    @Test
    public void newDBRowWillCauseBlankQuery() {
        Marque marque = new Marque();
        Assert.assertThat(marque.willCreateBlankQuery(myDatabase), is(true));
    }

    @Test
    public void newAlteredDBRowWillCauseBlankQuery() {
        Marque marque = new Marque();
        marque.name.permittedValues("HOLDEN");
        Assert.assertThat(marque.willCreateBlankQuery(myDatabase), is(false));
    }

    @Test
    public void testNumberIsBetween() throws SQLException {
        Marque marqueQuery = new Marque();
        marqueQuery.getUidMarque().permittedRange(0, 90000000);
        //System.out.println(marques.getSQLForExample(marqueQuery));
        marques = marques.getRowsByExample(marqueQuery);
        for (Marque row : marques.toList()) {
            System.out.println(row);
        }
        Assert.assertTrue("Incorrect number of marques retreived", marques.toList().size() == marqueRows.size());
    }

    @Test
    public void testIsLiterally() throws SQLException {
        Marque literalQuery = new Marque();
        literalQuery.getUidMarque().permittedValues(4893059);
        marques = marques.getRowsByExample(literalQuery);
        marques.print();
        Assert.assertEquals(marques.toList().size(), 1);
        Assert.assertEquals("" + 4893059, marques.toList().get(0).getPrimaryKeySQLStringValue(myDatabase));
    }

    @Test
    public void testIsIn() throws SQLException {
        Marque hummerQuery = new Marque();
        hummerQuery.getUidMarque().blankQuery();
        hummerQuery.getName().permittedValues("PEUGEOT", "HUMMER");
        marques = marques.getRowsByExample(hummerQuery);
        marques.print();
        Assert.assertThat(marques.toList().size(), is(2));
    }

    @Test
    public void testDateIsBetween() throws SQLException, ParseException {

        Date afterAllTheDates = tedhiFormat.parse("July 2013").asDate();
        DateRange coversFirstDate = tedhiRangeFormat.parse("March 2013");

        Marque oldQuery = new Marque();
        oldQuery.getCreationDate().permittedRange(new Date(0L), afterAllTheDates);
        marques = marques.getRowsByExample(oldQuery);
        marques.print();
        Assert.assertTrue("Wrong number of rows selected, should be all but one of them", marques.toList().size() == marqueRows.size() - 1);

        oldQuery.getCreationDate().permittedRange(coversFirstDate.getStart(), coversFirstDate.getEnd());
        marques = marques.getRowsByExample(oldQuery);
        marques.print();
        Assert.assertThat(marques.toList().size(), is(18));
    }

    @Test
    public void testDateIsLessThanAndGreaterThan() throws SQLException {
        GregorianCalendar gregorianCalendar = new GregorianCalendar();
        gregorianCalendar.add(Calendar.SECOND, 60);
        Date future = gregorianCalendar.getTime();
        Marque oldQuery = new Marque();
        oldQuery.getCreationDate().permittedRange(null, future);
        marques = marques.getRowsByExample(oldQuery);
        marques.print();
        Assert.assertTrue("Wrong number of rows selected, should be all but one of them", marques.toList().size() == marqueRows.size() - 1);
        oldQuery.getCreationDate().permittedRange(future, null);
        marques = marques.getRowsByExample(oldQuery);
        marques.print();
        Assert.assertTrue("Wrong number of rows selected, should be NONE of them", marques.toList().isEmpty());
        oldQuery = new Marque();
        oldQuery.getCreationDate().permittedRangeInclusive(null, future);
        marques = marques.getRowsByExample(oldQuery);
        marques.print();
        Assert.assertTrue("Wrong number of rows selected, should be all but one of them", marques.toList().size() == marqueRows.size() - 1);
        oldQuery.getCreationDate().permittedRange(new Date(0L), null);
        marques = marques.getRowsByExample(oldQuery);
        marques.print();
        Assert.assertTrue("Wrong number of rows selected, should be all but one of them", marques.toList().size() == marqueRows.size() - 1);
        oldQuery.getCreationDate().permittedRange(null, new Date(0L));
        marques = marques.getRowsByExample(oldQuery);
        marques.print();
        Assert.assertTrue("Wrong number of rows selected, should be NONE of them", marques.toList().isEmpty());
        oldQuery = new Marque();
        oldQuery.getCreationDate().permittedRangeInclusive(new Date(0L), null);
        marques = marques.getRowsByExample(oldQuery);
        marques.print();
        Assert.assertTrue("Wrong number of rows selected, should be all but one of them", marques.toList().size() == marqueRows.size() - 1);
    }

    @Test
    public void testRawQuery() throws SQLException {
        String rawQuery = "and lower(name) in ('peugeot','hummer')  ";
        marques = marques.getRowsByRawSQL(rawQuery);
        marques.print();
        Assert.assertEquals(marques.toList().size(), 2);
    }

    @Test
    public void testDBSelectQuery() throws SQLException {
        DBTable<MarqueSelectQuery> msq = DBTable.getInstance(myDatabase, new MarqueSelectQuery());
        msq.getAllRows();
        msq.print();

        MarqueSelectQuery marqueSelectQuery = new MarqueSelectQuery();
        marqueSelectQuery.uidMarque.permittedValues(1);
        msq.getRowsByExample(marqueSelectQuery);
        msq.print();
    }

    @Test
    public void testIgnoringColumnsOnTable() throws SQLException {
        myMarqueRow.returnFieldsLimitedTo(myMarqueRow.name, myMarqueRow.uidMarque, myMarqueRow.carCompany);
        List<Marque> rowsByExample = myDatabase.getDBTable(myMarqueRow).getRowsByExample(myMarqueRow).toList();
        for (Marque marq : rowsByExample) {
            System.out.println("" + marq);
            Assert.assertThat(marq.auto_created.isNull(), is(true));
            Assert.assertThat(marq.creationDate.isNull(), is(true));
            Assert.assertThat(marq.enabled.isNull(), is(true));
            Assert.assertThat(marq.individualAllocationsAllowed.isNull(), is(true));
            Assert.assertThat(marq.isUsedForTAFROs.isNull(), is(true));
            Assert.assertThat(marq.numericCode.isNull(), is(true));
            Assert.assertThat(marq.pricingCodePrefix.isNull(), is(true));
            Assert.assertThat(marq.reservationsAllowed.isNull(), is(true));
            Assert.assertThat(marq.toyotaStatusClassID.isNull(), is(true));

            Assert.assertThat(marq.name.isNull(), is(false));
            Assert.assertThat(marq.uidMarque.isNull(), is(false));
        }
    }
    
    @Test
    public void testUnignoringColumnsOnTable() throws SQLException {
        myMarqueRow.returnFieldsLimitedTo(myMarqueRow.name, myMarqueRow.uidMarque, myMarqueRow.carCompany);
        myMarqueRow.returnAllFields();
        List<Marque> rowsByExample = myDatabase.getDBTable(myMarqueRow).getRowsByExample(myMarqueRow).toList();
        for (Marque marq : rowsByExample) {
            System.out.println("" + marq);
            Assert.assertThat(marq.auto_created.isNull(), is(false));
            Assert.assertThat(marq.isUsedForTAFROs.isNull(), is(false));
            Assert.assertThat(marq.reservationsAllowed.isNull(), is(false));
            Assert.assertThat(marq.toyotaStatusClassID.isNull(), is(false));
            Assert.assertThat(marq.name.isNull(), is(false));
            Assert.assertThat(marq.uidMarque.isNull(), is(false));
        }
    }
    @Test
    public void testIgnoringColumnsOnQuery() throws SQLException {
        myMarqueRow.returnFieldsLimitedTo(myMarqueRow.name, myMarqueRow.uidMarque, myMarqueRow.carCompany);
        List<Marque> rowsByExample = myDatabase.getDBQuery(myMarqueRow,new CarCompany()).getAllInstancesOf(myMarqueRow);
        for (Marque marq : rowsByExample) {
            System.out.println("" + marq);
            Assert.assertThat(marq.auto_created.isNull(), is(true));
            Assert.assertThat(marq.creationDate.isNull(), is(true));
            Assert.assertThat(marq.enabled.isNull(), is(true));
            Assert.assertThat(marq.individualAllocationsAllowed.isNull(), is(true));
            Assert.assertThat(marq.isUsedForTAFROs.isNull(), is(true));
            Assert.assertThat(marq.numericCode.isNull(), is(true));
            Assert.assertThat(marq.pricingCodePrefix.isNull(), is(true));
            Assert.assertThat(marq.reservationsAllowed.isNull(), is(true));
            Assert.assertThat(marq.toyotaStatusClassID.isNull(), is(true));

            Assert.assertThat(marq.name.isNull(), is(false));
            Assert.assertThat(marq.uidMarque.isNull(), is(false));
        }

    }
    
    @Test
    public void testUnignoringColumnsOnQuery() throws SQLException {
        myMarqueRow.returnFieldsLimitedTo(myMarqueRow.name, myMarqueRow.uidMarque, myMarqueRow.carCompany);
        myMarqueRow.returnAllFields();
        List<Marque> rowsByExample = myDatabase.getDBQuery(myMarqueRow,new CarCompany()).getAllInstancesOf(myMarqueRow);
        for (Marque marq : rowsByExample) {
            System.out.println("" + marq);
            Assert.assertThat(marq.auto_created.isNull(), is(false));
            Assert.assertThat(marq.isUsedForTAFROs.isNull(), is(false));
            Assert.assertThat(marq.reservationsAllowed.isNull(), is(false));
            Assert.assertThat(marq.toyotaStatusClassID.isNull(), is(false));
            Assert.assertThat(marq.name.isNull(), is(false));
            Assert.assertThat(marq.uidMarque.isNull(), is(false));
        }
    }
}
