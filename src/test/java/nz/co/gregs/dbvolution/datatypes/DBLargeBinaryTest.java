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
package nz.co.gregs.dbvolution.datatypes;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.sql.SQLException;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.CompanyLogo;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import nz.co.gregs.dbvolution.utility.ImageCompare;
import org.apache.commons.codec.binary.Hex;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.Matchers.*;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBLargeBinaryTest extends AbstractTest {

	public DBLargeBinaryTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void createRowWithBinaryObject() throws FileNotFoundException, IOException, SQLException, UnexpectedNumberOfRowsException {

		CompanyLogo companyLogo = new CompanyLogo();
		companyLogo.logoID.setValue(1);
		companyLogo.carCompany.setValue(1);//Toyota
		companyLogo.imageFilename.setValue("toyota_logo.jpg");
		companyLogo.imageBytes.setFromFileSystem("toyota_share_logo.jpg");
		database.insert(companyLogo);

		CarCompany carCompany = new CarCompany();
		carCompany.name.permittedValuesIgnoreCase("FORD");
		CarCompany ford = database.getDBTable(carCompany).getOnlyRowByExample(carCompany);

		companyLogo.logoID.setValue(2);
		companyLogo.carCompany.setValue(ford.uidCarCompany.getValue());
		companyLogo.imageFilename.setValue("ford_logo.jpg");
		File fordLogoFile = new File("ford_logo.jpg");
		companyLogo.imageBytes.setValue(fordLogoFile);
		database.insert(companyLogo);

		CompanyLogo logoExample = new CompanyLogo();
		logoExample.carCompany.permittedValues(ford.uidCarCompany);
		List<CompanyLogo> foundLogos = database.get(logoExample);

		Assert.assertThat(foundLogos.size(), is(1));
		CompanyLogo foundLogo = foundLogos.get(0);
		Assert.assertThat(foundLogo.logoID.intValue(), is(2));
		Assert.assertThat(foundLogo.imageFilename.stringValue(), is("ford_logo.jpg"));
		Assert.assertThat(foundLogo.imageBytes.isNull(), is(false));
		File tempFile = new File("tempfileForCreateRowWithBinaryObject.jpg");
		foundLogo.imageBytes.writeToFileSystem(tempFile.getAbsoluteFile());
		Assert.assertThat(tempFile.length(), is(fordLogoFile.length()));
		ImageCompare imgcomp = new ImageCompare(tempFile, fordLogoFile);
		imgcomp.setParameters(8, 6, 5, 10);
		Assert.assertThat(imgcomp.match(), is(true));
		tempFile.delete();
	}

	@Test
	public void testHexValueOfStoredFile() throws FileNotFoundException, IOException, SQLException, UnexpectedNumberOfRowsException, ClassNotFoundException, InstantiationException {

		CarCompany carCompany = new CarCompany();
		carCompany.name.permittedValuesIgnoreCase("FORD");
		CarCompany ford = database.getDBTable(carCompany).getOnlyRowByExample(carCompany);

		CompanyLogo companyLogo = new CompanyLogo();
		companyLogo.logoID.setValue(2);
		companyLogo.carCompany.setValue(ford.uidCarCompany.getValue());
		companyLogo.imageFilename.setValue("ford_logo.jpg");
		File fordLogoFile = new File("ford_logo.jpg");
		companyLogo.imageBytes.setValue(fordLogoFile);
		database.insert(companyLogo);

		CompanyLogo logoExample = new CompanyLogo();
		logoExample.carCompany.permittedValues(ford.uidCarCompany);
		List<CompanyLogo> foundLogos = database.get(logoExample);

		final CompanyLogo foundLogo = foundLogos.get(0);

		Assert.assertThat(foundLogos.size(), is(1));
		String hexValueFromDatabase = "0x" + Hex.encodeHexString(foundLogo.imageBytes.getBytes());
		DBLargeBinary dbBinary = new DBLargeBinary();
		dbBinary.setFromFileSystem("ford_logo.jpg");
		byte[] fil = dbBinary.getBytes();
		String hexValueFromFileSystem = "0x" + Hex.encodeHexString(fil);
		Assert.assertThat(hexValueFromFileSystem, is(hexValueFromDatabase));
	}

	@Test
	public void retrieveRowWithBinaryObject() throws FileNotFoundException, IOException, SQLException, UnexpectedNumberOfRowsException, ClassNotFoundException, InstantiationException {

		CompanyLogoForRetreivingBinaryObject blobTable = new CompanyLogoForRetreivingBinaryObject();

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(blobTable);
		database.createTable(blobTable);

		int primaryKey = 3;
		blobTable.logoID.setValue(primaryKey);
		blobTable.carCompany.setValue(1);//Toyota
		blobTable.imageFilename.setValue("toyota_logo.jpg");
		File image = new File("toyota_share_logo.jpg");
		blobTable.imageBytes.setValue(image);
		database.insert(blobTable);

		File newFile = new File("retrieveRowWithBinaryObject.jpg");
		try {
			newFile.delete();
		} catch (Exception exp) {
			;// I just need it gone
		}

		blobTable = new CompanyLogoForRetreivingBinaryObject();
		CompanyLogoForRetreivingBinaryObject firstRow = database.getDBTable(blobTable).getRowsByPrimaryKey(primaryKey).get(0);

		firstRow.imageBytes.writeToFileSystem(newFile);
		Assert.assertThat(newFile.length(), is(image.length()));
		ImageCompare imgcomp = new ImageCompare(newFile, image);
		imgcomp.setParameters(8, 6, 5, 10);
		Assert.assertThat(imgcomp.match(), is(true));
	}

	@Test
	public void retrieveRowWithBinaryObjectUsingInputStream() throws FileNotFoundException, IOException, SQLException, UnexpectedNumberOfRowsException, ClassNotFoundException, InstantiationException {

		CompanyLogoForRetreivingBinaryObject blobTable = new CompanyLogoForRetreivingBinaryObject();

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(blobTable);
		database.createTable(blobTable);

		int primaryKey = 3;
		blobTable.logoID.setValue(primaryKey);
		blobTable.carCompany.setValue(1);//Toyota
		blobTable.imageFilename.setValue("toyota_logo.jpg");
		File image = new File("toyota_share_logo.jpg");
		blobTable.imageBytes.setValue(new FileInputStream(image));
		database.insert(blobTable);

		File newFile = new File("retrieveRowWithBinaryObjectUsingInputStream.jpg");
		try {
			newFile.delete();
		} catch (Exception exp) {
			;// I just need it gone
		}

		blobTable = new CompanyLogoForRetreivingBinaryObject();
		CompanyLogoForRetreivingBinaryObject firstRow = database.getDBTable(blobTable).getRowsByPrimaryKey(primaryKey).get(0);

		firstRow.imageBytes.writeToFileSystem(newFile);
		Assert.assertThat(newFile.length(), is(image.length()));
		ImageCompare imgcomp = new ImageCompare(newFile, image);
		imgcomp.setParameters(8, 6, 5, 10);
		Assert.assertThat(imgcomp.match(), is(true));
	}

	@Test
	public void retrieveRowWithBinaryObjectUsingInputStreamAndLargeFile() throws FileNotFoundException, IOException, SQLException, UnexpectedNumberOfRowsException, ClassNotFoundException, InstantiationException {

		CompanyLogoForRetreivingBinaryObject blobTable = new CompanyLogoForRetreivingBinaryObject();

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(blobTable);
		database.createTable(blobTable);

		int primaryKey = 31;
		blobTable.logoID.setValue(primaryKey);
		blobTable.carCompany.setValue(1);//Toyota
		blobTable.imageFilename.setValue("mclaren.jpg");
		File image = new File("mclaren.jpg");
		blobTable.imageBytes.setValue(new FileInputStream(image));
		database.insert(blobTable);

		File newFile = new File("retrieveRowWithBinaryObjectUsingInputStreamAndLargeFile.jpg");
		try {
			newFile.delete();
		} catch (Exception exp) {
			;// I just need it gone
		}

		blobTable = new CompanyLogoForRetreivingBinaryObject();
		CompanyLogoForRetreivingBinaryObject firstRow = database.getDBTable(blobTable).getRowsByPrimaryKey(primaryKey).get(0);

		firstRow.imageBytes.writeToFileSystem(newFile);
		Assert.assertThat(newFile.length(), is(image.length()));
		ImageCompare imgcomp = new ImageCompare(newFile, image);
		imgcomp.setParameters(8, 6, 5, 10);
		Assert.assertThat(imgcomp.match(), is(true));

		blobTable = new CompanyLogoForRetreivingBinaryObject();
		firstRow = database.getDBTable(blobTable).getRowsByPrimaryKey(primaryKey).get(0);
		firstRow.imageBytes.writeToFileSystem(newFile);
		imgcomp = new ImageCompare(newFile, image);
		imgcomp.setParameters(8, 6, 5, 10);
		Assert.assertThat(imgcomp.match(), is(true));

		blobTable = new CompanyLogoForRetreivingBinaryObject();
		firstRow = database.getDBTable(blobTable).getRowsByPrimaryKey(primaryKey).get(0);
		firstRow.imageBytes.writeToFileSystem(newFile);
		imgcomp = new ImageCompare(newFile, image);
		imgcomp.setParameters(8, 6, 5, 10);
		Assert.assertThat(imgcomp.match(), is(true));
	}

	@Test
	public void retrieveStringWithBinaryObject() throws FileNotFoundException, IOException, SQLException, UnexpectedNumberOfRowsException, ClassNotFoundException, InstantiationException {

		CompanyLogoForRetreivingString clobTable = new CompanyLogoForRetreivingString();

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(clobTable);
		database.createTable(clobTable);

		int primaryKey = 4;
		clobTable.logoID.setValue(primaryKey);
		clobTable.carCompany.setValue(1);
		clobTable.imageFilename.setValue("toyota_logo.jpg");
		clobTable.imageBytes.setValue(SOURCE_DATA_AS_STRING);
		database.insert(clobTable);

		CompanyLogoForRetreivingString firstRow = database.getDBTable(new CompanyLogoForRetreivingString()).getRowsByPrimaryKey(primaryKey).get(0);
		String stringValue = firstRow.imageBytes.stringValue();
		Assert.assertThat(stringValue, is(SOURCE_DATA_AS_STRING));
	}

	@Test
	public void retrieveStringWithBinaryObjectAndAutoIncrement() throws FileNotFoundException, IOException, SQLException, UnexpectedNumberOfRowsException, ClassNotFoundException, InstantiationException {

		BinaryObjectWithAutoIncrement testRow = new BinaryObjectWithAutoIncrement();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(testRow);
		database.createTable(testRow);

		testRow.carCompany.setValue(1);
		testRow.imageFilename.setValue("toyota_logo.jpg");
		testRow.imageBytes.setValue(SOURCE_DATA_AS_STRING);
		database.insert(testRow);

		BinaryObjectWithAutoIncrement firstRow = database.getDBTable(new BinaryObjectWithAutoIncrement()).setBlankQueryAllowed(true).getOnlyRow();
		String stringValue = firstRow.imageBytes.stringValue();
		Assert.assertThat(stringValue, is(SOURCE_DATA_AS_STRING));
	}

	@DBTableName("bytearraywithautoincrement")
	public static class BinaryObjectWithAutoIncrement extends DBRow {

		public static final long serialVersionUID = 1L;

		@DBPrimaryKey
		@DBColumn("logo_id")
		@DBAutoIncrement
		public DBInteger logoID = new DBInteger();

		@DBForeignKey(CarCompany.class)
		@DBColumn("car_company_fk")
		public DBInteger carCompany = new DBInteger();

		@DBColumn("image_file")
		public DBLargeBinary imageBytes = new DBLargeBinary();

		@DBColumn("image_name")
		public DBString imageFilename = new DBString();
	}

	@DBTableName("bigblob")
	public static class CompanyLogoForRetreivingBinaryObject extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBPrimaryKey
		@DBColumn("logo_id")
		public DBInteger logoID = new DBInteger();

		@DBForeignKey(CarCompany.class)
		@DBColumn("car_company_fk")
		public DBInteger carCompany = new DBInteger();

		@DBColumn("image_file")
		public DBLargeBinary imageBytes = new DBLargeBinary();

		@DBColumn("image_name")
		public DBString imageFilename = new DBString();
	}

	@DBTableName("bigstring")
	public static class CompanyLogoForRetreivingString extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBPrimaryKey
		@DBColumn("logo_id")
		public DBInteger logoID = new DBInteger();

		@DBForeignKey(CarCompany.class)
		@DBColumn("car_company_fk")
		public DBInteger carCompany = new DBInteger();

		@DBColumn("image_file")
		public DBLargeBinary imageBytes = new DBLargeBinary();

		@DBColumn("image_name")
		public DBString imageFilename = new DBString();
	}

	static final String SOURCE_DATA_AS_STRING = "\n"
			+ "-------------------------------------------------------\n"
			+ " T E S T S\n"
			+ "-------------------------------------------------------\n"
			+ "Running nz.co.gregs.dbvolution.datatypes.DBBinaryObjectTest\n"
			+ "INFO  DBStatement - EXECUTING: DROP TABLE marque;\n"
			+ "INFO  DBStatement - EXECUTING: CREATE TABLE marque(\n"
			+ "numeric_code NUMERIC(15,5), \n"
			+ "uid_marque INTEGER, \n"
			+ "isusedfortafros  VARCHAR(1000) CHARACTER SET utf8 COLLATE utf8_bin , \n"
			+ "fk_toystatusclass NUMERIC(15,5), \n"
			+ "intindallocallowed  VARCHAR(1000) CHARACTER SET utf8 COLLATE utf8_bin , \n"
			+ "upd_count INTEGER, \n"
			+ "auto_created  VARCHAR(1000) CHARACTER SET utf8 COLLATE utf8_bin , \n"
			+ "name  VARCHAR(1000) CHARACTER SET utf8 COLLATE utf8_bin , \n"
			+ "pricingcodeprefix  VARCHAR(1000) CHARACTER SET utf8 COLLATE utf8_bin , \n"
			+ "reservationsalwd  VARCHAR(1000) CHARACTER SET utf8 COLLATE utf8_bin , \n"
			+ "creation_date  DATETIME , \n"
			+ "enabled BIT(1), \n"
			+ "fk_carcompany INTEGER\n"
			+ ",PRIMARY KEY (uid_marque)\n"
			+ ")\n"
			+ ";\n"
			+ "INFO  DBStatement - EXECUTING: DROP TABLE car_company;\n"
			+ "INFO  DBStatement - EXECUTING: CREATE TABLE car_company(\n"
			+ "name  VARCHAR(1000) CHARACTER SET utf8 COLLATE utf8_bin , \n"
			+ "uid_carcompany INTEGER\n"
			+ ",PRIMARY KEY (uid_carcompany)\n"
			+ ")\n"
			+ ";\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO car_company( name, uid_carcompany)  VALUES ( 'TOYOTA',1);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO car_company( name, uid_carcompany)  VALUES ( 'Ford',2);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO car_company( name, uid_carcompany)  VALUES ( 'GENERAL MOTORS',3);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO car_company( name, uid_carcompany)  VALUES ( 'OTHER',4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4893059,'True',1246974, NULL ,3,'UV','PEUGEOT', NULL ,'Y', NULL , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4893090,'False',1246974,'',1,'UV','FORD','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,2);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4893101,'False',1246974,'',2,'UV','HOLDEN','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,3);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4893112,'False',1246974,'',2,'UV','MITSUBISHI','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4893150,'False',1246974,'',3,'UV','SUZUKI','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4893263,'False',1246974,'',2,'UV','HONDA','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4893353,'False',1246974,'',4,'UV','NISSAN','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4893557,'False',1246974,'',2,'UV','SUBARU','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4894018,'False',1246974,'',2,'UV','MAZDA','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4895203,'False',1246974,'',2,'UV','ROVER','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4896300,'False',1246974, NULL ,2,'UV','HYUNDAI', NULL ,'Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,1);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4899527,'False',1246974,'',1,'UV','JEEP','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,3);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 7659280,'False',1246972,'Y',3,'','DAIHATSU','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 7681544,'False',1246974,'',2,'UV','LANDROVER','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 7730022,'False',1246974,'',2,'UV','VOLVO','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 8376505,'False',1246974,'',0,'','ISUZU','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 8587147,'False',1246974,'',0,'','DAEWOO','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 9971178,'False',1246974,'',1,'','CHRYSLER','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 13224369,'False',1246974,'',0,'','VW','','Y', STR_TO_DATE('02,04,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 6664478,'False',1246974,'',0,'','BMW','','Y', STR_TO_DATE('02,04,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 1,'False',1246974,'',0,'','TOYOTA','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,1);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 2,'False',1246974,'',0,'','HUMMER','','Y', STR_TO_DATE('02,04,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,3);\n"
			+ "INFO  DBStatement - EXECUTING: DROP TABLE CompanyLogo;\n"
			+ "INFO  DBStatement - EXECUTING: CREATE TABLE CompanyLogo(\n"
			+ "logo_id INTEGER, \n"
			+ "car_company_fk INTEGER, \n"
			+ "image_file  LONGBLOB , \n"
			+ "image_name  VARCHAR(1000) CHARACTER SET utf8 COLLATE utf8_bin \n"
			+ ",PRIMARY KEY (logo_id)\n"
			+ ")\n"
			+ ";\n"
			+ "INFO  DBStatement - EXECUTING: DROP TABLE lt_carco_logo;\n"
			+ "INFO  DBStatement - EXECUTING: CREATE TABLE lt_carco_logo(\n"
			+ "fk_car_company INTEGER, \n"
			+ "fk_company_logo INTEGER)\n"
			+ ";\n"
			+ "EXECUTING: INSERT INTO CompanyLogo( logo_id, car_company_fk, image_name)  VALUES ( 1,1,'toyota_logo.jpg');\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO CompanyLogo( logo_id, car_company_fk, image_name)  VALUES ( 1,1,'toyota_logo.jpg');\n"
			+ "INFO  DBUpdateLargeObjects - UPDATE CompanyLogo SET image_file =  ?  WHERE logo_id = 1;\n"
			+ "EXECUTING QUERY:  SELECT _1159239592.logo_id DB_1579317226,\n"
			+ "_1159239592.car_company_fk DB1430605643,\n"
			+ "_1159239592.image_file DB1622411417,\n"
			+ "_1159239592.image_name DB1622642088\n"
			+ " FROM  CompanyLogo AS _1159239592 \n"
			+ " WHERE  1=1  AND (_1159239592.logo_id = 1)\n"
			+ ";\n"
			+ "INFO  DBStatement - EXECUTING QUERY:  SELECT _1159239592.logo_id DB_1579317226,\n"
			+ "_1159239592.car_company_fk DB1430605643,\n"
			+ "_1159239592.image_file DB1622411417,\n"
			+ "_1159239592.image_name DB1622642088\n"
			+ " FROM  CompanyLogo AS _1159239592 \n"
			+ " WHERE  1=1  AND (_1159239592.logo_id = 1)\n"
			+ ";\n"
			+ "row = CompanyLogo logoID:1, carCompany:1, imageBytes:/*BINARY DATA*/, imageFilename:toyota_logo.jpg\n"
			+ "INFO  DBStatement - EXECUTING: DROP TABLE lt_carco_logo;\n"
			+ "INFO  DBStatement - EXECUTING: DROP TABLE CompanyLogo;\n"
			+ "INFO  DBStatement - EXECUTING: DROP TABLE marque;\n"
			+ "INFO  DBStatement - EXECUTING: DROP TABLE car_company;\n"
			+ "INFO  DBStatement - EXECUTING: DROP TABLE marque;\n"
			+ "INFO  DBStatement - EXECUTING: CREATE TABLE marque(\n"
			+ "numeric_code NUMERIC(15,5), \n"
			+ "uid_marque INTEGER, \n"
			+ "isusedfortafros  VARCHAR(1000) CHARACTER SET utf8 COLLATE utf8_bin , \n"
			+ "fk_toystatusclass NUMERIC(15,5), \n"
			+ "intindallocallowed  VARCHAR(1000) CHARACTER SET utf8 COLLATE utf8_bin , \n"
			+ "upd_count INTEGER, \n"
			+ "auto_created  VARCHAR(1000) CHARACTER SET utf8 COLLATE utf8_bin , \n"
			+ "name  VARCHAR(1000) CHARACTER SET utf8 COLLATE utf8_bin , \n"
			+ "pricingcodeprefix  VARCHAR(1000) CHARACTER SET utf8 COLLATE utf8_bin , \n"
			+ "reservationsalwd  VARCHAR(1000) CHARACTER SET utf8 COLLATE utf8_bin , \n"
			+ "creation_date  DATETIME , \n"
			+ "enabled BIT(1), \n"
			+ "fk_carcompany INTEGER\n"
			+ ",PRIMARY KEY (uid_marque)\n"
			+ ")\n"
			+ ";\n"
			+ "INFO  DBStatement - EXECUTING: DROP TABLE car_company;\n"
			+ "INFO  DBStatement - EXECUTING: CREATE TABLE car_company(\n"
			+ "name  VARCHAR(1000) CHARACTER SET utf8 COLLATE utf8_bin , \n"
			+ "uid_carcompany INTEGER\n"
			+ ",PRIMARY KEY (uid_carcompany)\n"
			+ ")\n"
			+ ";\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO car_company( name, uid_carcompany)  VALUES ( 'TOYOTA',1);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO car_company( name, uid_carcompany)  VALUES ( 'Ford',2);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO car_company( name, uid_carcompany)  VALUES ( 'GENERAL MOTORS',3);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO car_company( name, uid_carcompany)  VALUES ( 'OTHER',4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4893059,'True',1246974, NULL ,3,'UV','PEUGEOT', NULL ,'Y', NULL , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4893090,'False',1246974,'',1,'UV','FORD','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,2);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4893101,'False',1246974,'',2,'UV','HOLDEN','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,3);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4893112,'False',1246974,'',2,'UV','MITSUBISHI','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4893150,'False',1246974,'',3,'UV','SUZUKI','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4893263,'False',1246974,'',2,'UV','HONDA','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4893353,'False',1246974,'',4,'UV','NISSAN','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4893557,'False',1246974,'',2,'UV','SUBARU','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4894018,'False',1246974,'',2,'UV','MAZDA','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4895203,'False',1246974,'',2,'UV','ROVER','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4896300,'False',1246974, NULL ,2,'UV','HYUNDAI', NULL ,'Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,1);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4899527,'False',1246974,'',1,'UV','JEEP','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,3);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 7659280,'False',1246972,'Y',3,'','DAIHATSU','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 7681544,'False',1246974,'',2,'UV','LANDROVER','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 7730022,'False',1246974,'',2,'UV','VOLVO','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 8376505,'False',1246974,'',0,'','ISUZU','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 8587147,'False',1246974,'',0,'','DAEWOO','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 9971178,'False',1246974,'',1,'','CHRYSLER','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 13224369,'False',1246974,'',0,'','VW','','Y', STR_TO_DATE('02,04,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 6664478,'False',1246974,'',0,'','BMW','','Y', STR_TO_DATE('02,04,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 1,'False',1246974,'',0,'','TOYOTA','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,1);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 2,'False',1246974,'',0,'','HUMMER','','Y', STR_TO_DATE('02,04,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,3);\n"
			+ "INFO  DBStatement - EXECUTING: DROP TABLE CompanyLogo;\n"
			+ "INFO  DBStatement - EXECUTING: CREATE TABLE CompanyLogo(\n"
			+ "logo_id INTEGER, \n"
			+ "car_company_fk INTEGER, \n"
			+ "image_file  LONGBLOB , \n"
			+ "image_name  VARCHAR(1000) CHARACTER SET utf8 COLLATE utf8_bin \n"
			+ ",PRIMARY KEY (logo_id)\n"
			+ ")\n"
			+ ";\n"
			+ "INFO  DBStatement - EXECUTING: DROP TABLE lt_carco_logo;\n"
			+ "INFO  DBStatement - EXECUTING: CREATE TABLE lt_carco_logo(\n"
			+ "fk_car_company INTEGER, \n"
			+ "fk_company_logo INTEGER)\n"
			+ ";\n"
			+ "FILE: /Users/gregorygraham/Projects/DBvolution-https/trunk/toyota_share_logo.jpg\n"
			+ "EXECUTING: INSERT INTO CompanyLogo( logo_id, car_company_fk, image_name)  VALUES ( 1,1,'toyota_logo.jpg');\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO CompanyLogo( logo_id, car_company_fk, image_name)  VALUES ( 1,1,'toyota_logo.jpg');\n"
			+ "INFO  DBUpdateLargeObjects - UPDATE CompanyLogo SET image_file =  ?  WHERE logo_id = 1;\n"
			+ "EXECUTING QUERY:  SELECT _1159239592.logo_id DB_1579317226,\n"
			+ "_1159239592.car_company_fk DB1430605643,\n"
			+ "_1159239592.image_file DB1622411417,\n"
			+ "_1159239592.image_name DB1622642088\n"
			+ " FROM  CompanyLogo AS _1159239592 \n"
			+ " WHERE  1=1  AND (_1159239592.logo_id = 1)\n"
			+ ";\n"
			+ "INFO  DBStatement - EXECUTING QUERY:  SELECT _1159239592.logo_id DB_1579317226,\n"
			+ "_1159239592.car_company_fk DB1430605643,\n"
			+ "_1159239592.image_file DB1622411417,\n"
			+ "_1159239592.image_name DB1622642088\n"
			+ " FROM  CompanyLogo AS _1159239592 \n"
			+ " WHERE  1=1  AND (_1159239592.logo_id = 1)\n"
			+ ";\n"
			+ "row = CompanyLogo logoID:1, carCompany:1, imageBytes:/*BINARY DATA*/, imageFilename:toyota_logo.jpg\n"
			+ "FILE: /Users/gregorygraham/Projects/DBvolution-https/trunk/found_toyota_logo.jpg\n"
			+ "INFO  DBStatement - EXECUTING: DROP TABLE lt_carco_logo;\n"
			+ "INFO  DBStatement - EXECUTING: DROP TABLE CompanyLogo;\n"
			+ "INFO  DBStatement - EXECUTING: DROP TABLE marque;\n"
			+ "INFO  DBStatement - EXECUTING: DROP TABLE car_company;\n"
			+ "INFO  DBStatement - EXECUTING: DROP TABLE marque;\n"
			+ "INFO  DBStatement - EXECUTING: CREATE TABLE marque(\n"
			+ "numeric_code NUMERIC(15,5), \n"
			+ "uid_marque INTEGER, \n"
			+ "isusedfortafros  VARCHAR(1000) CHARACTER SET utf8 COLLATE utf8_bin , \n"
			+ "fk_toystatusclass NUMERIC(15,5), \n"
			+ "intindallocallowed  VARCHAR(1000) CHARACTER SET utf8 COLLATE utf8_bin , \n"
			+ "upd_count INTEGER, \n"
			+ "auto_created  VARCHAR(1000) CHARACTER SET utf8 COLLATE utf8_bin , \n"
			+ "name  VARCHAR(1000) CHARACTER SET utf8 COLLATE utf8_bin , \n"
			+ "pricingcodeprefix  VARCHAR(1000) CHARACTER SET utf8 COLLATE utf8_bin , \n"
			+ "reservationsalwd  VARCHAR(1000) CHARACTER SET utf8 COLLATE utf8_bin , \n"
			+ "creation_date  DATETIME , \n"
			+ "enabled BIT(1), \n"
			+ "fk_carcompany INTEGER\n"
			+ ",PRIMARY KEY (uid_marque)\n"
			+ ")\n"
			+ ";\n"
			+ "INFO  DBStatement - EXECUTING: DROP TABLE car_company;\n"
			+ "INFO  DBStatement - EXECUTING: CREATE TABLE car_company(\n"
			+ "name  VARCHAR(1000) CHARACTER SET utf8 COLLATE utf8_bin , \n"
			+ "uid_carcompany INTEGER\n"
			+ ",PRIMARY KEY (uid_carcompany)\n"
			+ ")\n"
			+ ";\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO car_company( name, uid_carcompany)  VALUES ( 'TOYOTA',1);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO car_company( name, uid_carcompany)  VALUES ( 'Ford',2);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO car_company( name, uid_carcompany)  VALUES ( 'GENERAL MOTORS',3);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO car_company( name, uid_carcompany)  VALUES ( 'OTHER',4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4893059,'True',1246974, NULL ,3,'UV','PEUGEOT', NULL ,'Y', NULL , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4893090,'False',1246974,'',1,'UV','FORD','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,2);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4893101,'False',1246974,'',2,'UV','HOLDEN','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,3);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4893112,'False',1246974,'',2,'UV','MITSUBISHI','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4893150,'False',1246974,'',3,'UV','SUZUKI','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4893263,'False',1246974,'',2,'UV','HONDA','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4893353,'False',1246974,'',4,'UV','NISSAN','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4893557,'False',1246974,'',2,'UV','SUBARU','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4894018,'False',1246974,'',2,'UV','MAZDA','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4895203,'False',1246974,'',2,'UV','ROVER','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4896300,'False',1246974, NULL ,2,'UV','HYUNDAI', NULL ,'Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,1);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 4899527,'False',1246974,'',1,'UV','JEEP','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,3);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 7659280,'False',1246972,'Y',3,'','DAIHATSU','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 7681544,'False',1246974,'',2,'UV','LANDROVER','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 7730022,'False',1246974,'',2,'UV','VOLVO','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 8376505,'False',1246974,'',0,'','ISUZU','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 8587147,'False',1246974,'',0,'','DAEWOO','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 9971178,'False',1246974,'',1,'','CHRYSLER','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 13224369,'False',1246974,'',0,'','VW','','Y', STR_TO_DATE('02,04,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 6664478,'False',1246974,'',0,'','BMW','','Y', STR_TO_DATE('02,04,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,4);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 1,'False',1246974,'',0,'','TOYOTA','','Y', STR_TO_DATE('23,03,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,1);\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO marque( uid_marque, isusedfortafros, fk_toystatusclass, intindallocallowed, upd_count, auto_created, name, pricingcodeprefix, reservationsalwd, creation_date, enabled, fk_carcompany)  VALUES ( 2,'False',1246974,'',0,'','HUMMER','','Y', STR_TO_DATE('02,04,2013 00:00:00', '%d,%m,%Y %H:%i:%s') , NULL ,3);\n"
			+ "INFO  DBStatement - EXECUTING: DROP TABLE CompanyLogo;\n"
			+ "INFO  DBStatement - EXECUTING: CREATE TABLE CompanyLogo(\n"
			+ "logo_id INTEGER, \n"
			+ "car_company_fk INTEGER, \n"
			+ "image_file  LONGBLOB , \n"
			+ "image_name  VARCHAR(1000) CHARACTER SET utf8 COLLATE utf8_bin \n"
			+ ",PRIMARY KEY (logo_id)\n"
			+ ")\n"
			+ ";\n"
			+ "INFO  DBStatement - EXECUTING: DROP TABLE lt_carco_logo;\n"
			+ "INFO  DBStatement - EXECUTING: CREATE TABLE lt_carco_logo(\n"
			+ "fk_car_company INTEGER, \n"
			+ "fk_company_logo INTEGER)\n"
			+ ";\n"
			+ "FILE: /Users/gregorygraham/Projects/DBvolution-https/trunk/toyota_share_logo.jpg\n"
			+ "EXECUTING: INSERT INTO CompanyLogo( logo_id, car_company_fk, image_name)  VALUES ( 1,1,'toyota_logo.jpg');\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO CompanyLogo( logo_id, car_company_fk, image_name)  VALUES ( 1,1,'toyota_logo.jpg');\n"
			+ "INFO  DBUpdateLargeObjects - UPDATE CompanyLogo SET image_file =  ?  WHERE logo_id = 1;\n"
			+ "EXECUTING QUERY:  SELECT __78874071.name DB_241667647,\n"
			+ "__78874071.uid_carcompany DB112832814\n"
			+ " FROM  car_company AS __78874071 \n"
			+ " WHERE  1=1  AND ( lower(__78874071.name) =  lower('FORD') )\n"
			+ ";\n"
			+ "INFO  DBStatement - EXECUTING QUERY:  SELECT __78874071.name DB_241667647,\n"
			+ "__78874071.uid_carcompany DB112832814\n"
			+ " FROM  car_company AS __78874071 \n"
			+ " WHERE  1=1  AND ( lower(__78874071.name) =  lower('FORD') )\n"
			+ ";\n"
			+ "FILE: /Users/gregorygraham/Projects/DBvolution-https/trunk/ford_logo.jpg\n"
			+ "EXECUTING: INSERT INTO CompanyLogo( logo_id, car_company_fk, image_name)  VALUES ( 2,2,'ford_logo.jpg');\n"
			+ "INFO  DBStatement - EXECUTING: INSERT INTO CompanyLogo( logo_id, car_company_fk, image_name)  VALUES ( 2,2,'ford_logo.jpg');\n"
			+ "INFO  DBUpdateLargeObjects - UPDATE CompanyLogo SET image_file =  ?  WHERE logo_id = 2;\n"
			+ "EXECUTING QUERY:  SELECT _1159239592.logo_id DB_1579317226,\n"
			+ "_1159239592.car_company_fk DB1430605643,\n"
			+ "_1159239592.image_file DB1622411417,\n"
			+ "_1159239592.image_name DB1622642088\n"
			+ " FROM  CompanyLogo AS _1159239592 \n"
			+ " WHERE  1=1  AND (_1159239592.car_company_fk = 2)\n"
			+ ";\n"
			+ "INFO  DBStatement - EXECUTING QUERY:  SELECT _1159239592.logo_id DB_1579317226,\n"
			+ "_1159239592.car_company_fk DB1430605643,\n"
			+ "_1159239592.image_file DB1622411417,\n"
			+ "_1159239592.image_name DB1622642088\n"
			+ " FROM  CompanyLogo AS _1159239592 \n"
			+ " WHERE  1=1  AND (_1159239592.car_company_fk = 2)\n"
			+ ";\n"
			+ "CompanyLogo logoID:2, carCompany:2, imageBytes:/*BINARY DATA*/, imageFilename:ford_logo.jpg\n"
			+ "INFO  DBStatement - EXECUTING: DROP TABLE lt_carco_logo;\n"
			+ "INFO  DBStatement - EXECUTING: DROP TABLE CompanyLogo;\n"
			+ "INFO  DBStatement - EXECUTING: DROP TABLE marque;\n"
			+ "INFO  DBStatement - EXECUTING: DROP TABLE car_company;\n"
			+ "Tests run: 3, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 6.338 sec - in nz.co.gregs.dbvolution.datatypes.DBBinaryObjectTest\n"
			+ "\n"
			+ "Results :\n"
			+ "\n"
			+ "Tests run: 3, Failures: 0, Errors: 0, Skipped: 0";
}
