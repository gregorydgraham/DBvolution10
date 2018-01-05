/*
 * Copyright 2017 gregorygraham.
 *
 * This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. 
 * To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/4.0/ 
 * or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.
 * 
 * You are free to:
 *     Share - copy and redistribute the material in any medium or format
 *     Adapt - remix, transform, and build upon the material
 * 
 *     The licensor cannot revoke these freedoms as long as you follow the license terms.               
 *     Under the following terms:
 *                 
 *         Attribution - 
 *             You must give appropriate credit, provide a link to the license, and indicate if changes were made. 
 *             You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.
 *         NonCommercial - 
 *             You may not use the material for commercial purposes.
 *         ShareAlike - 
 *             If you remix, transform, or build upon the material, 
 *             you must distribute your contributions under the same license as the original.
 *         No additional restrictions - 
 *             You may not apply legal terms or technological measures that legally restrict others from doing anything the 
 *             license permits.
 * 
 * Check the Creative Commons website for any details, legalese, and updates.
 */
package nz.co.gregs.dbvolution;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBRequiredTable;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import nz.co.gregs.dbvolution.databases.H2MemoryDB;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author gregorygraham
 */
public class DBDatabaseClusterTest extends AbstractTest {

	public DBDatabaseClusterTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testAutomaticDataCreation() throws SQLException {
		H2MemoryDB soloDB = new H2MemoryDB("DBDatabaseClusterTest", "who", "what", true);
		final DBDatabaseClusterTestTable testTable = new DBDatabaseClusterTestTable();
		Assert.assertTrue(soloDB.tableExists(testTable));

		DBDatabaseCluster cluster = new DBDatabaseCluster(database);
		Assert.assertTrue(cluster.tableExists(testTable));
		
		cluster.delete(cluster
				.getDBTable(testTable)
				.setBlankQueryAllowed(true)
				.getAllRows());

		Date firstDate = new Date();
		Date secondDate = new Date();
		List<DBDatabaseClusterTestTable> data = createData(firstDate, secondDate);

		cluster.insert(data);

		Assert.assertThat(cluster.getDBTable(testTable).count(), is(22l));
		Assert.assertThat(soloDB.getDBTable(testTable).count(), is(0l));

		cluster.addDatabase(soloDB);

		Assert.assertThat(cluster.getDBTable(testTable).count(), is(22l));
		Assert.assertThat(soloDB.getDBTable(testTable).count(), is(22l));

		H2MemoryDB soloDB2 = new H2MemoryDB("DBDatabaseClusterTest2", "who", "what", true);
		Assert.assertThat(soloDB2.getDBTable(testTable).count(), is(0l));

		cluster.addDatabase(soloDB2);
		
		Assert.assertThat(soloDB2.getDBTable(testTable).count(), is(22l));

		cluster.delete(cluster.getDBTable(testTable)
						.setBlankQueryAllowed(true)
						.getAllRows()
		);

		Assert.assertThat(cluster.getDBTable(testTable).count(), is(0l));
		Assert.assertThat(cluster.getDBTable(testTable).count(), is(0l));
		Assert.assertThat(cluster.getDBTable(testTable).count(), is(0l));
		Assert.assertThat(cluster.getDBTable(testTable).count(), is(0l));
		
		Assert.assertThat(soloDB.getDBTable(testTable).count(), is(0l));
		Assert.assertThat(soloDB2.getDBTable(testTable).count(), is(0l));
	}

	private List<DBDatabaseClusterTestTable> createData(Date firstDate, Date secondDate) {
		List<DBDatabaseClusterTestTable> data = new ArrayList<>();
		data.add(new DBDatabaseClusterTestTable(4893059, "True", 1246974, null, 3, "UV", "PEUGEOT", null, "Y", null, 4, true));
		data.add(new DBDatabaseClusterTestTable(4893090, "False", 1246974, "", 1, "UV", "FORD", "", "Y", firstDate, 2, false));
		data.add(new DBDatabaseClusterTestTable(4893101, "False", 1246974, "", 2, "UV", "HOLDEN", "", "Y", firstDate, 3, null));
		data.add(new DBDatabaseClusterTestTable(4893112, "False", 1246974, "", 2, "UV", "MITSUBISHI", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(4893150, "False", 1246974, "", 3, "UV", "SUZUKI", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(4893263, "False", 1246974, "", 2, "UV", "HONDA", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(4893353, "False", 1246974, "", 4, "UV", "NISSAN", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(4893557, "False", 1246974, "", 2, "UV", "SUBARU", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(4894018, "False", 1246974, "", 2, "UV", "MAZDA", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(4895203, "False", 1246974, "", 2, "UV", "ROVER", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(4896300, "False", 1246974, null, 2, "UV", "HYUNDAI", null, "Y", firstDate, 1, null));
		data.add(new DBDatabaseClusterTestTable(4899527, "False", 1246974, "", 1, "UV", "JEEP", "", "Y", firstDate, 3, null));
		data.add(new DBDatabaseClusterTestTable(7659280, "False", 1246972, "Y", 3, "", "DAIHATSU", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(7681544, "False", 1246974, "", 2, "UV", "LANDROVER", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(7730022, "False", 1246974, "", 2, "UV", "VOLVO", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(8376505, "False", 1246974, "", null, "", "ISUZU", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(8587147, "False", 1246974, "", null, "", "DAEWOO", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(9971178, "False", 1246974, "", 1, "", "CHRYSLER", "", "Y", firstDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(13224369, "False", 1246974, "", 0, "", "VW", "", "Y", secondDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(6664478, "False", 1246974, "", 0, "", "BMW", "", "Y", secondDate, 4, null));
		data.add(new DBDatabaseClusterTestTable(1, "False", 1246974, "", 0, "", "TOYOTA", "", "Y", firstDate, 1, true));
		data.add(new DBDatabaseClusterTestTable(2, "False", 1246974, "", 0, "", "HUMMER", "", "Y", secondDate, 3, null));

		return data;
	}

	@DBRequiredTable
	public static class DBDatabaseClusterTestTable extends DBRow {

		public static final long serialVersionUID = 1L;
		@DBColumn(value = "NUMERIC_CODE")
		public nz.co.gregs.dbvolution.datatypes.DBNumber numericCode = new DBNumber();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "UID_MARQUE")
		@nz.co.gregs.dbvolution.annotations.DBPrimaryKey
		public nz.co.gregs.dbvolution.datatypes.DBInteger uidMarque = new DBInteger();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "ISUSEDFORTAFROS")
		public nz.co.gregs.dbvolution.datatypes.DBString isUsedForTAFROs = new DBString();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "FK_TOYSTATUSCLASS")
		public nz.co.gregs.dbvolution.datatypes.DBNumber statusClassID = new DBNumber();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "INTINDALLOCALLOWED")
		public nz.co.gregs.dbvolution.datatypes.DBString individualAllocationsAllowed = new DBString();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "UPD_COUNT")
		public nz.co.gregs.dbvolution.datatypes.DBInteger updateCount = new DBInteger();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "AUTO_CREATED")
		public nz.co.gregs.dbvolution.datatypes.DBString auto_created = new DBString();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "NAME")
		public nz.co.gregs.dbvolution.datatypes.DBString name = new DBString();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "PRICINGCODEPREFIX")
		public nz.co.gregs.dbvolution.datatypes.DBString pricingCodePrefix = new DBString();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "RESERVATIONSALWD")
		public nz.co.gregs.dbvolution.datatypes.DBString reservationsAllowed = new DBString();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "CREATION_DATE")
		public nz.co.gregs.dbvolution.datatypes.DBDate creationDate = new DBDate();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "ENABLED")
		public nz.co.gregs.dbvolution.datatypes.DBBoolean enabled = new DBBoolean();
		@nz.co.gregs.dbvolution.annotations.DBColumn(value = "FK_CARCOMPANY")
		public nz.co.gregs.dbvolution.datatypes.DBInteger carCompany = new DBInteger();

		public DBDatabaseClusterTestTable() {
		}

		public DBDatabaseClusterTestTable(int uidMarque, String isUsedForTAFROs, int statusClass, String intIndividualAllocationsAllowed, Integer updateCount, String autoCreated, String name, String pricingCodePrefix, String reservationsAllowed, Date creationDate, int carCompany, Boolean enabled) {
			this.uidMarque.setValue(uidMarque);
			this.isUsedForTAFROs.setValue(isUsedForTAFROs);
			this.statusClassID.setValue(statusClass);
			this.individualAllocationsAllowed.setValue(intIndividualAllocationsAllowed);
			this.updateCount.setValue(updateCount);
			this.auto_created.setValue(autoCreated);
			this.name.setValue(name);
			this.pricingCodePrefix.setValue(pricingCodePrefix);
			this.reservationsAllowed.setValue(reservationsAllowed);
			this.creationDate.setValue(creationDate);
			this.carCompany.setValue(carCompany);
			this.enabled.setValue(enabled);
		}
	}

}
