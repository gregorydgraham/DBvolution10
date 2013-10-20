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

import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import net.sourceforge.tedhi.DateRange;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author gregory.graham
 */
public class DBDatabaseGetTest extends AbstractTest {

    Marque myTableRow = new Marque();
    List<Marque> myTableRows = new ArrayList<Marque>();

    public DBDatabaseGetTest(Object db) {
        super(db);
    }

    @Test
    public void testGetAllRows() throws SQLException {
        List<Marque> allMarques = database.get(new Marque());
        for (DBRow row : allMarques) {
//            System.out.println(row);
        }
        Assert.assertTrue("Incorrect number of marques retreived", allMarques.size() == marqueRows.size());
    }

    @Test
    public void testGetFirstAndPrimaryKey() throws SQLException {
        List<Marque> allMarques = database.get(new Marque());
        List<Marque> singleMarque = new ArrayList<Marque>();
        DBRow row = marqueRows.get(0);
        Long primaryKey;
        if (row != null) {
            primaryKey = row.getPrimaryKey().longValue();
            Marque marque = new Marque();
            marque.uidMarque.permittedValues(primaryKey);
            singleMarque = database.get(marque);
            for (Marque mrq : singleMarque) {
//                System.out.println("" + mrq);
            }
        }
        Assert.assertTrue("Incorrect number of marques retreived", singleMarque.size() == 1);
    }

    @Test
    public void newDBRowWillCauseBlankQuery() {
        Marque marque = new Marque();
        Assert.assertThat(database.willCreateBlankQuery(marque), is(true));
    }

    @Test
    public void newAlteredDBRowWillCauseBlankQuery() {
        Marque marque = new Marque();
        marque.name.permittedValues("HOLDEN");
        Assert.assertThat(database.willCreateBlankQuery(marque), is(false));
    }

    @Test
    public void testNumberIsBetween() throws SQLException {
        Marque marqueQuery = new Marque();
        marqueQuery.getUidMarque().permittedRange(0, 90000000);
        List<Marque> gotMarques = database.get(marqueQuery);
        for (Marque row : gotMarques) {
//            System.out.println(row);
        }
        Assert.assertTrue("Incorrect number of marques retreived", gotMarques.size() == marqueRows.size());
    }

    @Test
    public void testIsLiterally() throws SQLException {
        Marque literalQuery = new Marque();
        literalQuery.getUidMarque().permittedValues(4893059);
        List<Marque> gotMarques = database.get(literalQuery);
        for (Marque row : gotMarques) {
//            System.out.println(row);
        }
        Assert.assertEquals(gotMarques.size(), 1);
        Assert.assertEquals("" + 4893059, gotMarques.get(0).getPrimaryKey().getSQLValue(database));
    }

    @Test
    public void testMultiplePermittedValues() throws SQLException {
        Marque literalQuery = new Marque();
        literalQuery.getUidMarque().permittedValues(4893059, 4893090);
        List<Marque> gotMarques = database.get(literalQuery);
        for (Marque row : gotMarques) {
//            System.out.println(row);
        }
        Assert.assertEquals(gotMarques.size(), 2);
        Assert.assertEquals("" + 4893059, gotMarques.get(0).getPrimaryKey().getSQLValue(database));
        Assert.assertEquals("" + 4893090, gotMarques.get(1).getPrimaryKey().getSQLValue(database));
    }

    @Test
    public void testIsIn() throws SQLException {
        Marque hummerQuery = new Marque();
        hummerQuery.getUidMarque().blankQuery();
        hummerQuery.getName().permittedValues("PEUGEOT", "HUMMER");
        List<Marque> gotMarques = database.get(hummerQuery);
        for (Marque row : gotMarques) {
            System.out.println(row);
        }
        Assert.assertThat(gotMarques.size(), is(2));
    }

    @Test
    public void testIsInWithList() throws SQLException {
        Marque hummerQuery = new Marque();
        hummerQuery.getUidMarque().blankQuery();
        List<String> permittedMarques = new ArrayList<String>();
        permittedMarques.add("PEUGEOT");
        permittedMarques.add("HUMMER");
        hummerQuery.getName().permittedValues(permittedMarques);
        List<Marque> gotMarques = database.get(hummerQuery);
        for (Marque row : gotMarques) {
//            System.out.println(row);
        }
        Assert.assertThat(gotMarques.size(), is(2));
    }

    @Test
    public void testIsExcludedWithList() throws SQLException {
        Marque hummerQuery = new Marque();
        hummerQuery.getUidMarque().blankQuery();
        List<Marque> allMarques = database.get(hummerQuery);
        List<String> permittedMarques = new ArrayList<String>();
        permittedMarques.add("PEUGEOT");
        permittedMarques.add("HUMMER");
        hummerQuery.getName().excludedValues(permittedMarques);
        List<Marque> gotMarques = database.get(hummerQuery);
        for (Marque row : gotMarques) {
//            System.out.println(row);
        }
        Assert.assertThat(gotMarques.size(), is(allMarques.size()-2));
    }

    @Test
    public void testDateIsBetween() throws SQLException, ParseException {

        Date afterAllTheDates = tedhiFormat.parse("July 2013").asDate();
        DateRange coversFirstDate = tedhiRangeFormat.parse("March 2013");

        Marque oldQuery = new Marque();
        oldQuery.getCreationDate().permittedRange(new Date(0L), afterAllTheDates);
        List<Marque> gotMarques = database.get(oldQuery);
        for (Marque row : gotMarques) {
//            System.out.println(row);
        }
        Assert.assertTrue("Wrong number of rows selected, should be all but one of them", gotMarques.size() == marqueRows.size() - 1);

        oldQuery.getCreationDate().permittedRange(coversFirstDate.getStart(), coversFirstDate.getEnd());
        gotMarques = database.get(oldQuery);
        for (Marque row : gotMarques) {
//            System.out.println(row);
        }
        Assert.assertThat(gotMarques.size(), is(18));
    }

    @Test
    public void testDateIsLessThanAndGreaterThan() throws SQLException {
        GregorianCalendar gregorianCalendar = new GregorianCalendar();
        gregorianCalendar.add(Calendar.SECOND, 60);
        Date future = gregorianCalendar.getTime();
        Marque oldQuery = new Marque();
        oldQuery.getCreationDate().permittedRange(null, future);
        List<Marque> gotMarques = database.get(oldQuery);
        for (Marque row : gotMarques) {
//            System.out.println(row);
        }
        Assert.assertTrue("Wrong number of rows selected, should be all but one of them", gotMarques.size() == marqueRows.size() - 1);
        oldQuery.getCreationDate().permittedRange(future, null);
        gotMarques = database.get(oldQuery);
        for (Marque row : gotMarques) {
            System.out.println(row);
        }
        Assert.assertTrue("Wrong number of rows selected, should be NONE of them", gotMarques.isEmpty());
        oldQuery = new Marque();
        oldQuery.getCreationDate().permittedRangeInclusive(null, future);
        gotMarques = database.get(oldQuery);
        for (Marque row : gotMarques) {
            System.out.println(row);
        }
        Assert.assertTrue("Wrong number of rows selected, should be all but one of them", gotMarques.size() == marqueRows.size() - 1);
        oldQuery.getCreationDate().permittedRange(new Date(0L), null);
        gotMarques = database.get(oldQuery);
        for (Marque row : gotMarques) {
            System.out.println(row);
        }
        Assert.assertTrue("Wrong number of rows selected, should be all but one of them", gotMarques.size() == marqueRows.size() - 1);
        oldQuery.getCreationDate().permittedRange(null, new Date(0L));
        gotMarques = database.get(oldQuery);
        for (Marque row : gotMarques) {
            System.out.println(row);
        }
        Assert.assertTrue("Wrong number of rows selected, should be NONE of them", gotMarques.isEmpty());
        oldQuery = new Marque();
        oldQuery.getCreationDate().permittedRangeInclusive(new Date(0L), null);
        gotMarques = database.get(oldQuery);
        for (Marque row : gotMarques) {
            System.out.println(row);
        }
        Assert.assertTrue("Wrong number of rows selected, should be all but one of them", gotMarques.size() == marqueRows.size() - 1);
    }

    @Test
    public void testIgnoringColumnsOnTable() throws SQLException {
        myMarqueRow.returnFieldsLimitedTo(myMarqueRow.name, myMarqueRow.uidMarque, myMarqueRow.carCompany);
        List<Marque> rowsByExample = database.get(myMarqueRow);
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
        List<Marque> rowsByExample = database.get(myMarqueRow);
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
        DBQuery dbQuery = database.getDBQuery(myMarqueRow, new CarCompany());
        dbQuery.setBlankQueryAllowed(true);
        List<DBQueryRow> rowsByExample = dbQuery.getAllRows();
        for (DBQueryRow row : rowsByExample) {
            Marque marq = row.get(new Marque());
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
        DBQuery dbQuery = database.getDBQuery(myMarqueRow, new CarCompany());
        dbQuery.setBlankQueryAllowed(true);
        List<DBQueryRow> rowsByExample = dbQuery.getAllRows();
        for (DBQueryRow row : rowsByExample) {
            Marque marq = row.get(new Marque());
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
