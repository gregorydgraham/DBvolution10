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
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.CompanyLogo;
import nz.co.gregs.dbvolution.example.LinkCarCompanyAndLogo;
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
		final String standardUpdateSQL = "UPDATE MARQUE SET UID_MARQUE = 1 WHERE UID_MARQUE = 99999;";
		final String microsoftUpdateSQL = "UPDATE [MARQUE] SET UID_MARQUE = 1 WHERE UID_MARQUE = 99999;";
		final String oracleUpdateSQL = "UPDATE OO1081299805 SET UID_MARQUE = 1 WHERE UID_MARQUE = 99999";
		final String actualUpdateSQL = this.testableSQLWithoutColumnAliases(revertStrings.get(0));

		Assert.assertThat(actualUpdateSQL,
				anyOf(
						is(this.testableSQLWithoutColumnAliases(standardUpdateSQL)),
						is(this.testableSQLWithoutColumnAliases(microsoftUpdateSQL)),
						is(this.testableSQLWithoutColumnAliases(oracleUpdateSQL))
				));
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
		CarCompany tvr = new CarCompany("TVR", 33);
		DBActionList insertActions = database.insert(tvr);
		CarCompany example = new CarCompany();
		example.name.permittedValuesIgnoreCase("TVR");
		List<CarCompany> foundTVR = database.get(example);
		Assert.assertThat(foundTVR.size(), is(1));
		DBActionList revertActionList = insertActions.getRevertActionList();
		revertActionList.execute(database);
		foundTVR = database.get(example);
		Assert.assertThat(foundTVR.size(), is(0));

	}

	@Test
	public void insertAndRevertWithAutoIncrementTest() throws SQLException {

		CarCompanyWithAutoIncrement tvr = new CarCompanyWithAutoIncrement();
		tvr.name.setValue("TVR");

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(tvr);
		database.createTable(tvr);
		
		DBActionList insertTVRActions = database.insert(tvr);
		Assert.assertThat(tvr.carcoId.intValue(), is(1));

		CarCompanyWithAutoIncrement hulme = new CarCompanyWithAutoIncrement();
		hulme.name.setValue("HULME");
		database.insert(hulme);
		Assert.assertThat(hulme.carcoId.intValue(), is(2));

		CarCompanyWithAutoIncrement tvrExample = new CarCompanyWithAutoIncrement();
		tvrExample.name.permittedValuesIgnoreCase("TVR");
		CarCompanyWithAutoIncrement hulmeExample = new CarCompanyWithAutoIncrement();
		hulmeExample.name.permittedValuesIgnoreCase("HULME");
		List<CarCompanyWithAutoIncrement> foundTVR = database.get(tvrExample);
		Assert.assertThat(foundTVR.size(), is(1));
		List<CarCompanyWithAutoIncrement> foundHulme = database.get(hulmeExample);
		Assert.assertThat(foundHulme.size(), is(1));

		DBActionList revertTVRInsertActionList = insertTVRActions.getRevertActionList();
		revertTVRInsertActionList.execute(database);
		foundTVR = database.get(tvrExample);
		Assert.assertThat(foundTVR.size(), is(0));
		foundHulme = database.get(hulmeExample);
		Assert.assertThat(foundHulme.size(), is(1));
	}

	@DBTableName("carcompany_auto")
	public static class CarCompanyWithAutoIncrement extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		public DBInteger carcoId = new DBInteger();

		@DBColumn
		DBString name = new DBString();
	}

	@Test
	public void insertLargeObjectAndRevertTest() throws SQLException, FileNotFoundException, IOException {
		CompanyLogo logo = new CompanyLogo();
		logo.carCompany.setValue(2);
		logo.imageFilename.setValue("some logo file.jpg");
		logo.logoID.setValue(798);
		logo.imageBytes.setFromFileSystem("toyota_share_logo.jpg");
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
	public void deleteUsingAllColumnsAndRevertTest() throws SQLException {
		LinkCarCompanyAndLogo example = new LinkCarCompanyAndLogo();
		example.fkCarCompany.setValue(1);
		example.fkCompanyLogo.setValue(1);
		database.getDBTable(example).insert(example);
		List<LinkCarCompanyAndLogo> foundLinks = database.get(example);
		Assert.assertThat(foundLinks.size(), is(1));

		DBActionList deleteActions = database.delete(foundLinks.get(0));
		Assert.assertThat(deleteActions.size(), is(1));
		Assert.assertThat(deleteActions.get(0), instanceOf(DBDeleteUsingAllColumns.class));

		foundLinks = database.get(example);
		Assert.assertThat(foundLinks.size(), is(0));

		DBActionList revertActionList = deleteActions.getRevertActionList();
		Assert.assertThat(revertActionList.size(), is(1));
		Assert.assertThat(revertActionList.get(0), instanceOf(DBInsert.class));

		revertActionList.execute(database);
		foundLinks = database.get(example);
		Assert.assertThat(foundLinks.size(), is(1));
		database.delete(example);
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

		final DBActionList revertActionList = dataChanges.getRevertActionList();

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
		final String standardSQL = "UPDATE MARQUE SET UID_MARQUE = 1 WHERE UID_MARQUE = 99999;";
		final String microsoftSQL = "UPDATE [MARQUE] SET UID_MARQUE = 1 WHERE UID_MARQUE = 99999;";
		final String oracleSQL = "update __1081299805 set uid_marque = 1 where uid_marque = 99999";
		final String oracleStandardSQL = "UPDATE MARQUE SET UID_MARQUE = 1 WHERE UID_MARQUE = 99999";
		Assert.assertThat(this.testableSQLWithoutColumnAliases(revertStrings.get(0)),
				anyOf(
						is(this.testableSQLWithoutColumnAliases(standardSQL)),
						is(this.testableSQLWithoutColumnAliases(microsoftSQL)),
						is(this.testableSQLWithoutColumnAliases(oracleSQL)),
						is(this.testableSQLWithoutColumnAliases(oracleStandardSQL))
				)
		);
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
		logo.imageBytes.setFromFileSystem("toyota_share_logo.jpg");
		dataChanges.addAll(database.insert(logo));

		Assert.assertThat(dataChanges.get(0), instanceOf(DBInsert.class));
		Assert.assertThat(dataChanges.get(1), instanceOf(DBUpdateSimpleTypes.class));
		Assert.assertThat(dataChanges.get(2), instanceOf(DBDeleteByPrimaryKey.class));
		Assert.assertThat(dataChanges.get(3), instanceOf(DBInsert.class));
		Assert.assertThat(dataChanges.get(4), instanceOf(DBUpdateLargeObjects.class));

		final DBActionList revertActionList = dataChanges.getRevertActionList();

		Assert.assertThat(revertActionList.get(0), instanceOf(DBDelete.class));
		Assert.assertThat(revertActionList.get(1), instanceOf(DBInsert.class));
		Assert.assertThat(revertActionList.get(2), instanceOf(DBUpdate.class));
		Assert.assertThat(revertActionList.get(3), instanceOf(DBDelete.class));

		revertActionList.execute(database);
	}
}
