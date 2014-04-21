/*
 * Copyright 2013 greg.
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
package nz.co.gregs.dbvolution.actions;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.example.CompanyLogo;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.Matchers.*;

public class DBActionListCreationTest extends AbstractTest {

    public DBActionListCreationTest(Object testIterationName, Object db) {
        super(testIterationName, db);
    }

    @Test
    public void simpleActionCreation() throws SQLException, UnexpectedNumberOfRowsException {
        Marque marqueExample = new Marque();
        marqueExample.getUidMarque().permittedValues(1);
        Marque toyota = marquesTable.getOnlyRowByExample(marqueExample);

        toyota.uidMarque.setValue(99999);
        DBActionList updateList = marquesTable.update(toyota);
        Assert.assertThat(updateList.size(), is(1));

        final DBAction firstAction = updateList.get(0);
        final DBActionList revertList = firstAction.getRevertDBActionList();
        Assert.assertThat(firstAction, instanceOf(DBUpdateSimpleTypes.class));
        Assert.assertThat(revertList.size(), is(1));
        Assert.assertThat(revertList.get(0), instanceOf(DBUpdateToPreviousValues.class));
        List<String> revertStrings = revertList.get(0).getSQLStatements(database);
        Assert.assertThat(revertStrings.size(), is(1));
        Assert.assertThat(this.testableSQLWithoutColumnAliases(revertStrings.get(0)),
                is(this.testableSQLWithoutColumnAliases("UPDATE MARQUE SET UID_MARQUE = 1 WHERE UID_MARQUE = 99999;")));
        System.out.println("REVERT:");
        for (String revert : revertStrings) {
            System.out.println(revert);
        }
    }

    @Test
    public void multiActionCreation() throws SQLException, UnexpectedNumberOfRowsException {
        Marque marqueExample = new Marque();
        final Long toyotaUID = 1L;
        marqueExample.getUidMarque().permittedValues(toyotaUID);

        Marque toyota = marquesTable.getOnlyRowByExample(marqueExample);
        toyota.uidMarque.setValue(999999);

        marqueExample = new Marque();
        marqueExample.name.permittedValuesIgnoreCase("ford");
        Marque ford = marquesTable.getOnlyRowByExample(marqueExample);
        final Long fordOriginalUpdateCount = ford.updateCount.getValue();
        ford.updateCount.setValue(fordOriginalUpdateCount + 10);

        DBActionList updates = database.update(toyota, ford);
        Assert.assertThat(updates.size(), is(2));
        Assert.assertThat(updates.get(0), instanceOf(DBUpdateSimpleTypes.class));
        Assert.assertThat(updates.get(1), instanceOf(DBUpdateSimpleTypes.class));

        DBActionList reverts = updates.getRevertActionList();
        Assert.assertThat(reverts.size(), is(2));
        System.out.println("REVERTS: ");
        for (String revert : reverts.getSQL(database)) {
            System.out.println(revert);
        }

        reverts.execute(database);
        marqueExample = new Marque();
        marqueExample.getUidMarque().permittedValues(toyotaUID);

        toyota = marquesTable.getOnlyRowByExample(marqueExample);

        marqueExample = new Marque();
        marqueExample.name.permittedValuesIgnoreCase("ford");
        ford = marquesTable.getOnlyRowByExample(marqueExample);

        Assert.assertThat(toyota.uidMarque.getValue(), is(toyotaUID));
        Assert.assertThat(ford.updateCount.getValue(), is(fordOriginalUpdateCount));
    }

    @Test
    public void insertAndRevertTest() throws SQLException {
        Marque marque = new Marque(3, "False", 1246974, "", 3, "UV", "TVR", "", "Y", new Date(), 4, null);
        DBActionList insertActions = database.insert(marque);
        Marque example = new Marque();
        example.name.permittedValuesIgnoreCase("TVR");
        List<Marque> foundTVR = database.get(example);
        Assert.assertThat(foundTVR.size(), is(1));
        DBActionList revertActionList = insertActions.getRevertActionList();
        revertActionList.execute(database);
        foundTVR = database.get(example);
        Assert.assertThat(foundTVR.size(), is(0));

    }
    
    @Test
    public void insertLargeObjectAndRevertTest() throws SQLException, FileNotFoundException, IOException {
        CompanyLogo logo = new CompanyLogo();
        logo.carCompany.setValue(2);
        logo.imageFilename.setValue("some logo file.jpg");
        logo.logoID.setValue(798);
        logo.imageBytes.setFromFileSystem("found_toyota_logo.jpg");
        DBActionList insertActions = database.insert(logo);
        CompanyLogo example = new CompanyLogo();
        example.logoID.permittedValues(798);
        List<CompanyLogo> foundLogo = database.get(example);
        Assert.assertThat(foundLogo.size(), is(1));
        DBActionList revertActionList = insertActions.getRevertActionList();
        revertActionList.execute(database);
        foundLogo = database.get(example);
        Assert.assertThat(foundLogo.size(), is(0));

    }

    @Test
    public void deleteAndRevertTest() throws SQLException {
        database.setPrintSQLBeforeExecuting(true);
        Marque example = new Marque();
        example.name.permittedValuesIgnoreCase("toyota");
        List<Marque> foundToyota = database.get(example);
        Assert.assertThat(foundToyota.size(), is(1));

        DBActionList deleteActions = database.delete(example);
        Assert.assertThat(deleteActions.size(), is(1));
        Assert.assertThat(deleteActions.get(0), instanceOf(DBDeleteByExample.class));

        foundToyota = database.get(example);
        Assert.assertThat(foundToyota.size(), is(0));

        DBActionList revertActionList = deleteActions.getRevertActionList();
        Assert.assertThat(revertActionList.size(), is(1));
        Assert.assertThat(revertActionList.get(0), instanceOf(DBInsert.class));

        revertActionList.execute(database);
        foundToyota = database.get(example);
        Assert.assertThat(foundToyota.size(), is(1));
    }

    @Test
    public void lotsOfDifferentActionsTest() throws SQLException, UnexpectedNumberOfRowsException {
        Marque example = new Marque();
        final int toyotaUID = 1;
        example.getUidMarque().permittedValues(toyotaUID);

        Marque toyota = marquesTable.getOnlyRowByExample(example);
        toyota.uidMarque.setValue(999999);

        example = new Marque();
        example.name.permittedValuesIgnoreCase("ford");
        Marque ford = marquesTable.getOnlyRowByExample(example);
        final Integer fordOriginalUpdateCount = ford.updateCount.getValue().intValue();
        ford.updateCount.setValue(fordOriginalUpdateCount + 10);

        DBActionList dataChanges = database.update(toyota, ford);

        Marque marque = new Marque(3, "False", 1246974, "", 3, "UV", "TVR", "", "Y", new Date(), 4, null);
        dataChanges.addAll(database.insert(marque));

        example = new Marque();
        example.name.permittedValuesIgnoreCase("honda");
        List<Marque> foundToyota = database.get(example);
        Assert.assertThat(foundToyota.size(), is(1));

        dataChanges.addAll(database.delete(example));
        
        System.out.println("Data Changes: ");
        for(String sql : dataChanges.getSQL(database)){
            System.out.println(sql);
        }

        final DBActionList revertActionList = dataChanges.getRevertActionList();
        System.out.println("Revert Actions: ");
        for(String sql : revertActionList.getSQL(database)){
            System.out.println(sql);
        }

        revertActionList.execute(database);

        example = new Marque();
        example.name.permittedValuesIgnoreCase("toyota");
        toyota = database.getDBTable(example).getOnlyRowByExample(example);
        Assert.assertThat(toyota.uidMarque.getValue().intValue(), is(toyotaUID));
        
        example = new Marque();
        example.name.permittedValuesIgnoreCase("ford");
        ford = database.getDBTable(example).getOnlyRowByExample(example);
        Assert.assertThat(ford.updateCount.getValue().intValue(), is(fordOriginalUpdateCount));
        
        example = new Marque();
        example.name.permittedValuesIgnoreCase("tvr");
        DBTable<Marque> rowsByExample = database.getDBTable(example);
        Assert.assertThat(rowsByExample.getAllRows().size(), is(0));
        
        example = new Marque();
        example.name.permittedValuesIgnoreCase("honda");
        rowsByExample = database.getDBTable(example);
        Assert.assertThat(rowsByExample.getAllRows().size(), is(1));
    }

    @Test
    public void simpleDeferredActionCreation() throws SQLException, UnexpectedNumberOfRowsException {
        Marque marqueExample = new Marque();
        marqueExample.getUidMarque().permittedValues(1);
        Marque toyota = marquesTable.getOnlyRowByExample(marqueExample);

        toyota.uidMarque.setValue(99999);
        DBActionList updateList = DBUpdate.getUpdates(toyota);
        Assert.assertThat(updateList.size(), is(1));

        final DBAction firstAction = updateList.get(0);
        final DBActionList revertList = firstAction.getRevertDBActionList();
        Assert.assertThat(firstAction, instanceOf(DBUpdateSimpleTypes.class));
        Assert.assertThat(revertList.size(), is(1));
        Assert.assertThat(revertList.get(0), instanceOf(DBUpdateToPreviousValues.class));
        List<String> revertStrings = revertList.get(0).getSQLStatements(database);
        Assert.assertThat(revertStrings.size(), is(1));
        Assert.assertThat(this.testableSQLWithoutColumnAliases(revertStrings.get(0)),
                is(this.testableSQLWithoutColumnAliases("UPDATE MARQUE SET UID_MARQUE = 1 WHERE UID_MARQUE = 99999;")));
        System.out.println("REVERT:");
        for (String revert : revertStrings) {
            System.out.println(revert);
        }
    }


    @Test
    public void revertListIsReversedTest() throws SQLException, UnexpectedNumberOfRowsException, FileNotFoundException, IOException {
        Marque example = new Marque();
        DBActionList dataChanges = new DBActionList();

        Marque marque = new Marque(3, "False", 1246974, "", 3, "UV", "TVR", "", "Y", new Date(), 4, null);
        dataChanges.addAll(database.insert(marque));

        example.name.permittedValuesIgnoreCase("TVR");
        List<Marque> foundTVR = database.get(example);
        Assert.assertThat(foundTVR.size(), is(1));
        Marque tvr = foundTVR.get(0);
        tvr.name.setValue("TVR ROCKS!!!");

        dataChanges.addAll(database.update(tvr));
        
        dataChanges.addAll(database.delete(tvr));
        
        CompanyLogo logo = new CompanyLogo();
        logo.carCompany.setValue(2);
        logo.imageFilename.setValue("some logo file.jpg");
        logo.logoID.setValue(798);
        logo.imageBytes.setFromFileSystem("found_toyota_logo.jpg");
        dataChanges.addAll(database.insert(logo));
        
        System.out.println("Data Changes: ");
        for(String sql : dataChanges.getSQL(database)){
            System.out.println(sql);
        }

        final DBActionList revertActionList = dataChanges.getRevertActionList();
        System.out.println("Revert Actions: ");
        for(String sql : revertActionList.getSQL(database)){
            System.out.println(sql);
        }
        
        Assert.assertThat(revertActionList.get(0), instanceOf(DBDelete.class));
        Assert.assertThat(revertActionList.get(1), instanceOf(DBInsert.class));
        Assert.assertThat(revertActionList.get(2), instanceOf(DBUpdate.class));
        Assert.assertThat(revertActionList.get(3), instanceOf(DBDelete.class));

        revertActionList.execute(database);
    }
}
