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
package nz.co.gregs.dbvolution.generation;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.tools.*;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.H2MemoryDB;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBString;

import org.junit.Test;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 *
 *
 *
 * @author Gregory Graham
 */
public class DataRepoGeneratorTest {

	public DataRepoGeneratorTest() {
	}

	@Test
	public void testGetSchema() throws SQLException, IOException {
		DBDatabase database = H2MemoryDB.createANewRandomDatabase();
		database.createTable(new Marque());
		database.createTable(new CarCompany());
		database.createTable(new Companylogo());
		database.createTable(new LtCarcoLogo());

		database.createTable(new TestAutoIncrementDetection());
		int classesTested = 0;
		DataRepo generateSchema;
		List<String> testClassNames = Arrays.asList(new String[]{"CarCompany", "Companylogo", "LtCarcoLogo", "Marque", "TestAutoIncrementDetection"});
		List<String> testClasses = new ArrayList<String>();
		testClasses.add("package nz.co.gregs.dbvolution.generation.tables;\n"
				+ "\n"
				+ "import java.lang.Long;\n"
				+ "import nz.co.gregs.dbvolution.DBRow;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBColumn;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBTableName;\n"
				+ "import nz.co.gregs.dbvolution.datatypes.DBInteger;\n"
				+ "import nz.co.gregs.dbvolution.datatypes.DBString;\n"
				+ "\n"
				+ "@DBTableName ( value = \"CAR_COMPANY\" )\n"
				+ "public class CarCompany extends DBRow { \n"
				+ "\n"
				+ "	public static final Long serialVersionUID = 1L;\n"
				+ "	@DBColumn ( value = \"NAME\" )\n"
				+ "	public DBString name = new DBString();\n"
				+ "	@DBColumn ( value = \"UID_CARCOMPANY\" )\n"
				+ "	@DBPrimaryKey\n"
				+ "	public DBInteger uidCarcompany = new DBInteger(); \n"
				+ "\n"
				+ "	public CarCompany() { \n"
				+ "	} \n"
				+ "\n"
				+ "}");
		testClasses.add("package nz.co.gregs.dbvolution.generation.tables;\n"
				+ "\n"
				+ "import java.lang.Long;\n"
				+ "import nz.co.gregs.dbvolution.DBRow;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBColumn;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBTableName;\n"
				+ "import nz.co.gregs.dbvolution.datatypes.DBInteger;\n"
				+ "import nz.co.gregs.dbvolution.datatypes.DBLargeBinary;\n"
				+ "import nz.co.gregs.dbvolution.datatypes.DBString;\n"
				+ "\n"
				+ "@DBTableName ( value = \"COMPANYLOGO\" )\n"
				+ "public class Companylogo extends DBRow { \n"
				+ "\n"
				+ "	public static final Long serialVersionUID = 1L;\n"
				+ "	@DBColumn ( value = \"LOGO_ID\" )\n"
				+ "	@DBPrimaryKey\n"
				+ "	public DBInteger logoId = new DBInteger();\n"
				+ "	@DBColumn ( value = \"CAR_COMPANY_FK\" )\n"
				+ "	public DBInteger carCompanyFk = new DBInteger();\n"
				+ "	@DBColumn ( value = \"IMAGE_FILE\" )\n"
				+ "	public DBLargeBinary imageFile = new DBLargeBinary();\n"
				+ "	@DBColumn ( value = \"IMAGE_NAME\" )\n"
				+ "	public DBString imageName = new DBString(); \n"
				+ "\n"
				+ "	public Companylogo() { \n"
				+ "	} \n"
				+ "\n"
				+ "}");
		testClasses.add("\"package nz.co.gregs.dbvolution.generation.tables;\n"
				+ "\n"
				+ "import java.lang.Long;\n"
				+ "import nz.co.gregs.dbvolution.DBRow;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBColumn;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBTableName;\n"
				+ "import nz.co.gregs.dbvolution.datatypes.DBInteger;\n"
				+ "\n"
				+ "@DBTableName ( value = \"LT_CARCO_LOGO\" )\n"
				+ "public class LtCarcoLogo extends DBRow { \n"
				+ "\n"
				+ "	public static final Long serialVersionUID = 1L;\n"
				+ "	@DBColumn ( value = \"FK_CAR_COMPANY\" )\n"
				+ "	public DBInteger fkCarCompany = new DBInteger();\n"
				+ "	@DBColumn ( value = \"FK_COMPANY_LOGO\" )\n"
				+ "	public DBInteger fkCompanyLogo = new DBInteger(); \n"
				+ "\n"
				+ "	public LtCarcoLogo() { \n"
				+ "	} \n"
				+ "\n"
				+ "}");
		testClasses.add("package nz.co.gregs.dbvolution.generation.tables;\n"
				+ "\n"
				+ "import java.lang.Long;\n"
				+ "import nz.co.gregs.dbvolution.DBRow;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBColumn;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBTableName;\n"
				+ "import nz.co.gregs.dbvolution.datatypes.DBInteger;\n"
				+ "\n"
				+ "@DBTableName ( value = \"LT_CARCO_LOGO\" )\n"
				+ "public class LtCarcoLogo extends DBRow { \n"
				+ "\n"
				+ "	public static final Long serialVersionUID = 1L;\n"
				+ "	@DBColumn ( value = \"FK_CAR_COMPANY\" )\n"
				+ "	public DBInteger fkCarCompany = new DBInteger();\n"
				+ "	@DBColumn ( value = \"FK_COMPANY_LOGO\" )\n"
				+ "	public DBInteger fkCompanyLogo = new DBInteger(); \n"
				+ "\n"
				+ "	public LtCarcoLogo() { \n"
				+ "	} \n"
				+ "\n"
				+ "}");
		testClasses.add("package nz.co.gregs.dbvolution.generation.tables;\n"
				+ "\n"
				+ "import java.lang.Long;\n"
				+ "import nz.co.gregs.dbvolution.DBRow;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBColumn;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBTableName;\n"
				+ "import nz.co.gregs.dbvolution.datatypes.DBBoolean;\n"
				+ "import nz.co.gregs.dbvolution.datatypes.DBDate;\n"
				+ "import nz.co.gregs.dbvolution.datatypes.DBInteger;\n"
				+ "import nz.co.gregs.dbvolution.datatypes.DBNumber;\n"
				+ "import nz.co.gregs.dbvolution.datatypes.DBString;\n"
				+ "\n"
				+ "@DBTableName ( value = \"MARQUE\" )\n"
				+ "public class Marque extends DBRow { \n"
				+ "\n"
				+ "	public static final Long serialVersionUID = 1L;\n"
				+ "	@DBColumn ( value = \"NUMERIC_CODE\" )\n"
				+ "	public DBNumber numericCode = new DBNumber();\n"
				+ "	@DBColumn ( value = \"UID_MARQUE\" )\n"
				+ "	@DBPrimaryKey\n"
				+ "	public DBInteger uidMarque = new DBInteger();\n"
				+ "	@DBColumn ( value = \"ISUSEDFORTAFROS\" )\n"
				+ "	public DBString isusedfortafros = new DBString();\n"
				+ "	@DBColumn ( value = \"FK_TOYSTATUSCLASS\" )\n"
				+ "	public DBNumber fkToystatusclass = new DBNumber();\n"
				+ "	@DBColumn ( value = \"INTINDALLOCALLOWED\" )\n"
				+ "	public DBString intindallocallowed = new DBString();\n"
				+ "	@DBColumn ( value = \"UPD_COUNT\" )\n"
				+ "	public DBInteger updCount = new DBInteger();\n"
				+ "	@DBColumn ( value = \"AUTO_CREATED\" )\n"
				+ "	public DBString autoCreated = new DBString();\n"
				+ "	@DBColumn ( value = \"NAME\" )\n"
				+ "	public DBString name = new DBString();\n"
				+ "	@DBColumn ( value = \"PRICINGCODEPREFIX\" )\n"
				+ "	public DBString pricingcodeprefix = new DBString();\n"
				+ "	@DBColumn ( value = \"RESERVATIONSALWD\" )\n"
				+ "	public DBString reservationsalwd = new DBString();\n"
				+ "	@DBColumn ( value = \"CREATION_DATE\" )\n"
				+ "	public DBDate creationDate = new DBDate();\n"
				+ "	@DBColumn ( value = \"ENABLED\" )\n"
				+ "	public DBBoolean enabled = new DBBoolean();\n"
				+ "	@DBColumn ( value = \"FK_CARCOMPANY\" )\n"
				+ "	public DBInteger fkCarcompany = new DBInteger(); \n"
				+ "\n"
				+ "	public Marque() { \n"
				+ "	} \n"
				+ "\n"
				+ "}");
		testClasses.add("package nz.co.gregs.dbvolution.generation.tables;\n"
				+ "\n"
				+ "import java.lang.Long;\n"
				+ "import nz.co.gregs.dbvolution.DBRow;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBColumn;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBTableName;\n"
				+ "import nz.co.gregs.dbvolution.datatypes.DBInteger;\n"
				+ "import nz.co.gregs.dbvolution.datatypes.DBString;\n"
				+ "\n"
				+ "@DBTableName ( value = \"TEST_AUTO_INCREMENT_DETECTION\" )\n"
				+ "public class TestAutoIncrementDetection extends DBRow { \n"
				+ "\n"
				+ "	public static final Long serialVersionUID = 1L;\n"
				+ "	@DBColumn ( value = \"PK_UID\" )\n"
				+ "	@DBPrimaryKey\n"
				+ "	@DBAutoIncrement\n"
				+ "	public DBInteger pkUid = new DBInteger();\n"
				+ "	@DBColumn ( value = \"NAME\" )\n"
				+ "	public DBString name = new DBString(); \n"
				+ "\n"
				+ "	public TestAutoIncrementDetection() { \n"
				+ "	} \n"
				+ "\n"
				+ "}");
		generateSchema = DataRepoGenerator.generateClasses(database, "nz.co.gregs.dbvolution.generation");
		for (DBTableClass dbcl : generateSchema.getTables()) {
			if (testClassNames.contains(dbcl.getClassName())) {
				classesTested++;
				boolean found = false;
				for (String str : testClasses) {
					final String testcaseLowercase = str.toLowerCase().replaceAll("[ \n\r\t]+", " ");
					final String sourceLowercase = dbcl.getJavaSource().toLowerCase().replaceAll("[ \n\r\t]+", " ");
					if (testcaseLowercase.equals(sourceLowercase)) {
						found = true;
					}
				}
				assertTrue("Unable to find: \n\"" + dbcl.getJavaSource() + "\"", found);
			}
		}
		assertThat(classesTested, is(5));

		database.preventDroppingOfTables(false);
		database.dropTable(new TestAutoIncrementDetection());
	}

	@Test
	public void testGetSchemaOfGeneratedForeignKeys() throws SQLException, IOException {
		DBDatabase database = H2MemoryDB.createANewRandomDatabase();
		database.createTable(new Marque());
		database.createTable(new CarCompany());
		database.createTableWithForeignKeys(new CreateTableForeignKey());

		int classesTested = 0;
		List<String> testClassNames = Arrays.asList(new String[]{"CreateTableForeignKey"});
		List<String> testClasses = new ArrayList<String>();
		testClasses.add(
				"package nz.co.gregs.dbvolution.generation2.tables;\n"
				+ "\n"
				+ "import java.lang.Long;\n"
				+ "import nz.co.gregs.dbvolution.DBRow;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBColumn;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBForeignKey;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBTableName;\n"
				+ "import nz.co.gregs.dbvolution.datatypes.DBInteger;\n"
				+ "import nz.co.gregs.dbvolution.datatypes.DBString;\n"
				+ "\n"
				+ "@DBTableName ( value = \"CREATE_TABLE_FOREIGN_KEY\" )\n"
				+ "public class CreateTableForeignKey extends DBRow { \n"
				+ "\n"
				+ "	public static final Long serialVersionUID = 1L;\n"
				+ "	@DBColumn ( value = \"NAME\" )\n"
				+ "	public DBString name = new DBString();\n"
				+ "	@DBColumn ( value = \"MARQUEFOREIGNKEY\" )\n"
				+ "	@DBForeignKey ( value = nz.co.gregs.dbvolution.generation2.tables.Marque.class )\n"
				+ "	public DBInteger marqueforeignkey = new DBInteger();\n"
				+ "	@DBColumn ( value = \"CARCOFOREIGNKEY\" )\n"
				+ "	@DBForeignKey ( value = nz.co.gregs.dbvolution.generation2.tables.CarCompany.class )\n"
				+ "	public DBInteger carcoforeignkey = new DBInteger(); \n"
				+ "\n"
				+ "	public CreateTableForeignKey() { \n"
				+ "	} \n"
				+ "\n"
				+ "}");

		var generateSchema = DataRepoGenerator.generateClasses(database, "nz.co.gregs.dbvolution.generation2", new Options());
		for (DBTableClass dbcl : generateSchema.getTables()) {
			if (testClassNames.contains(dbcl.getClassName())) {
				classesTested++;
				boolean found = false;
				for (String str : testClasses) {
					final String testcaseLowercase = str.toLowerCase().replaceAll("[ \n\r\t]+", " ");
					final String sourceLowercase = dbcl.getJavaSource().toLowerCase().replaceAll("[ \n\r\t]+", " ");

					if (testcaseLowercase.equals(sourceLowercase)) {
						found = true;
					}
				}
				assertTrue("Unable to find: \n\"" + dbcl.getJavaSource() + "\"", found);
			}
		}
		assertThat(classesTested, is(1));

		database.preventDroppingOfTables(false);
		database.dropTable(new CreateTableForeignKey());
	}

	@Test
	public void testGetSchemaOfGeneratedForeignKeysWithIncludedColumnNames() throws SQLException, IOException {
		DBDatabase database = H2MemoryDB.createANewRandomDatabase();
		database.createTable(new Marque());
		database.createTable(new CarCompany());
		database.createTableWithForeignKeys(new CreateTableForeignKeyy());

		int classesTested = 0;
		List<String> testClassNames = Arrays.asList(new String[]{"CreateTableForeignKeyy"});
		List<String> testClasses = new ArrayList<String>();
		testClasses.add(
				"package nz.co.gregs.dbvolution.generation.tables;\n"
				+ "\n"
				+ "import java.lang.Long;\n"
				+ "import nz.co.gregs.dbvolution.DBRow;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBColumn;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBForeignKey;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBTableName;\n"
				+ "import nz.co.gregs.dbvolution.datatypes.DBInteger;\n"
				+ "import nz.co.gregs.dbvolution.datatypes.DBString;\n"
				+ "\n"
				+ "@DBTableName ( value = \"CREATE_TABLE_FOREIGN_KEYY\" )\n"
				+ "public class CreateTableForeignKeyy extends DBRow { \n"
				+ "\n"
				+ "	public static final Long serialVersionUID = 1L;\n"
				+ "	@DBColumn ( value = \"NAME\" )\n"
				+ "	public DBString name = new DBString();\n"
				+ "	@DBColumn ( value = \"MARQUEFOREIGNKEY\" )\n"
				+ "	@DBForeignKey ( value = nz.co.gregs.dbvolution.generation.tables.Marque.class, column = \"UID_MARQUE\" )\n"
				+ "	public DBInteger marqueforeignkey = new DBInteger();\n"
				+ "	@DBColumn ( value = \"CARCOFOREIGNKEY\" )\n"
				+ "	@DBForeignKey ( value = nz.co.gregs.dbvolution.generation.tables.CarCompany.class, column = \"UID_CARCOMPANY\" )\n"
				+ "	public DBInteger carcoforeignkey = new DBInteger(); \n"
				+ "\n"
				+ "	public CreateTableForeignKeyy() { \n"
				+ "	} \n"
				+ "\n"
				+ "}");
		final Options options = new Options();
		options.setIncludeForeignKeyColumnName(true);
		var generateSchema = DataRepoGenerator.generateClasses(database, "nz.co.gregs.dbvolution.generation", options);
		for (DBTableClass dbcl : generateSchema.getTables()) {
			if (testClassNames.contains(dbcl.getClassName())) {
				classesTested++;
				boolean found = false;
				for (String str : testClasses) {
					final String testcaseLowercase = str.toLowerCase().replaceAll("[ \n\r\t]+", " ");
					final String sourceLowercase = dbcl.getJavaSource().toLowerCase().replaceAll("[ \n\r\t]+", " ");

					if (testcaseLowercase.equals(sourceLowercase)) {
						found = true;
					}
				}
				assertTrue("Unable to find: \n\"" + dbcl.getJavaSource() + "\"", found);
			}
		}
		assertThat(classesTested, is(1));

		database.preventDroppingOfTables(false);
		database.dropTable(new CreateTableForeignKeyy());
	}

	@Test
	public void testGetSchemaWithRecognisor() throws SQLException, IOException {
		DBDatabase database = H2MemoryDB.createANewRandomDatabase();
		database.createTable(new Marque());
		database.createTable(new CarCompany());
		database.createTable(new Companylogo());
		database.createTable(new LtCarcoLogo());

		int classesTested = 0;

		List<String> testClassNames = Arrays.asList(new String[]{"CarCompany", "Companylogo", "LtCarcoLogo", "Marque"});
		List<String> testGetSchemaWithRecognisorTestClasses = new ArrayList<String>();
		testGetSchemaWithRecognisorTestClasses.add(
				"package nz.co.gregs.dbvolution.generation.tables;\n"
				+ "\n"
				+ "import java.lang.Long;\n"
				+ "import nz.co.gregs.dbvolution.DBRow;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBColumn;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBTableName;\n"
				+ "import nz.co.gregs.dbvolution.datatypes.DBInteger;\n"
				+ "import nz.co.gregs.dbvolution.datatypes.DBString;\n"
				+ "\n"
				+ "@DBTableName ( value = \"CAR_COMPANY\" )\n"
				+ "public class CarCompany extends DBRow { \n"
				+ "\n"
				+ "	public static final Long serialVersionUID = 1L;\n"
				+ "	@DBColumn ( value = \"NAME\" )\n"
				+ "	public DBString name = new DBString();\n"
				+ "	@DBColumn ( value = \"UID_CARCOMPANY\" )\n"
				+ "	@DBPrimaryKey\n"
				+ "	public DBInteger uidCarcompany = new DBInteger(); \n"
				+ "\n"
				+ "	public CarCompany() { \n"
				+ "	} \n"
				+ "\n"
				+ "}");
		testGetSchemaWithRecognisorTestClasses.add(
				"package nz.co.gregs.dbvolution.generation.tables;\n"
				+ "\n"
				+ "import java.lang.Long;\n"
				+ "import nz.co.gregs.dbvolution.DBRow;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBColumn;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBTableName;\n"
				+ "import nz.co.gregs.dbvolution.datatypes.DBInteger;\n"
				+ "import nz.co.gregs.dbvolution.datatypes.DBLargeBinary;\n"
				+ "import nz.co.gregs.dbvolution.datatypes.DBString;\n"
				+ "\n"
				+ "@DBTableName ( value = \"COMPANYLOGO\" )\n"
				+ "public class Companylogo extends DBRow { \n"
				+ "\n"
				+ "	public static final Long serialVersionUID = 1L;\n"
				+ "	@DBColumn ( value = \"LOGO_ID\" )\n"
				+ "	@DBPrimaryKey\n"
				+ "	public DBInteger logoId = new DBInteger();\n"
				+ "	@DBColumn ( value = \"CAR_COMPANY_FK\" )\n"
				+ "	public DBInteger carCompanyFk = new DBInteger();\n"
				+ "	@DBColumn ( value = \"IMAGE_FILE\" )\n"
				+ "	public DBLargeBinary imageFile = new DBLargeBinary();\n"
				+ "	@DBColumn ( value = \"IMAGE_NAME\" )\n"
				+ "	public DBString imageName = new DBString(); \n"
				+ "\n"
				+ "	public Companylogo() { \n"
				+ "	} \n"
				+ "\n"
				+ "}");
		testGetSchemaWithRecognisorTestClasses.add(
				"package nz.co.gregs.dbvolution.generation.tables;\n"
				+ "\n"
				+ "import java.lang.Long;\n"
				+ "import nz.co.gregs.dbvolution.DBRow;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBColumn;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBForeignKey;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBTableName;\n"
				+ "import nz.co.gregs.dbvolution.datatypes.DBInteger;\n"
				+ "\n"
				+ "@DBTableName ( value = \"LT_CARCO_LOGO\" )\n"
				+ "public class LtCarcoLogo extends DBRow { \n"
				+ "\n"
				+ "	public static final Long serialVersionUID = 1L;\n"
				+ "	@DBColumn ( value = \"FK_CAR_COMPANY\" )\n"
				+ "	@DBForeignKey ( value = nz.co.gregs.dbvolution.generation.tables.CarCompany.class )\n"
				+ "	public DBInteger fkCarCompany = new DBInteger();\n"
				+ "	@DBColumn ( value = \"FK_COMPANY_LOGO\" )\n"
				+ "	@DBForeignKey ( value = nz.co.gregs.dbvolution.generation.tables.Companylogo.class )\n"
				+ "	public DBInteger fkCompanyLogo = new DBInteger(); \n"
				+ "\n"
				+ "	public LtCarcoLogo() { \n"
				+ "	} \n"
				+ "\n"
				+ "}");
		testGetSchemaWithRecognisorTestClasses.add(
				"package nz.co.gregs.dbvolution.generation.tables;\n"
				+ "\n"
				+ "import java.lang.Long;\n"
				+ "import nz.co.gregs.dbvolution.DBRow;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBColumn;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBForeignKey;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;\n"
				+ "import nz.co.gregs.dbvolution.annotations.DBTableName;\n"
				+ "import nz.co.gregs.dbvolution.datatypes.DBBoolean;\n"
				+ "import nz.co.gregs.dbvolution.datatypes.DBDate;\n"
				+ "import nz.co.gregs.dbvolution.datatypes.DBInteger;\n"
				+ "import nz.co.gregs.dbvolution.datatypes.DBNumber;\n"
				+ "import nz.co.gregs.dbvolution.datatypes.DBString;\n"
				+ "\n"
				+ "@DBTableName ( value = \"MARQUE\" )\n"
				+ "public class Marque extends DBRow { \n"
				+ "\n"
				+ "	public static final Long serialVersionUID = 1L;\n"
				+ "	@DBColumn ( value = \"NUMERIC_CODE\" )\n"
				+ "	public DBNumber numericCode = new DBNumber();\n"
				+ "	@DBColumn ( value = \"UID_MARQUE\" )\n"
				+ "	@DBPrimaryKey\n"
				+ "	public DBInteger uidMarque = new DBInteger();\n"
				+ "	@DBColumn ( value = \"ISUSEDFORTAFROS\" )\n"
				+ "	public DBString isusedfortafros = new DBString();\n"
				+ "	@DBColumn ( value = \"FK_TOYSTATUSCLASS\" )\n"
				+ "	@DBForeignKey ( value = nz.co.gregs.dbvolution.generation.tables.Toystatusclass.class )\n"
				+ "	public DBNumber fkToystatusclass = new DBNumber();\n"
				+ "	@DBColumn ( value = \"INTINDALLOCALLOWED\" )\n"
				+ "	public DBString intindallocallowed = new DBString();\n"
				+ "	@DBColumn ( value = \"UPD_COUNT\" )\n"
				+ "	public DBInteger updCount = new DBInteger();\n"
				+ "	@DBColumn ( value = \"AUTO_CREATED\" )\n"
				+ "	public DBString autoCreated = new DBString();\n"
				+ "	@DBColumn ( value = \"NAME\" )\n"
				+ "	public DBString name = new DBString();\n"
				+ "	@DBColumn ( value = \"PRICINGCODEPREFIX\" )\n"
				+ "	public DBString pricingcodeprefix = new DBString();\n"
				+ "	@DBColumn ( value = \"RESERVATIONSALWD\" )\n"
				+ "	public DBString reservationsalwd = new DBString();\n"
				+ "	@DBColumn ( value = \"CREATION_DATE\" )\n"
				+ "	public DBDate creationDate = new DBDate();\n"
				+ "	@DBColumn ( value = \"ENABLED\" )\n"
				+ "	public DBBoolean enabled = new DBBoolean();\n"
				+ "	@DBColumn ( value = \"FK_CARCOMPANY\" )\n"
				+ "	@DBForeignKey ( value = nz.co.gregs.dbvolution.generation.tables.CarCompany.class )\n"
				+ "	public DBInteger fkCarcompany = new DBInteger(); \n"
				+ "\n"
				+ "	public Marque() { \n"
				+ "	} \n"
				+ "\n"
				+ "}");

		Options options = new Options();
		options.setPkRecog(new UIDBasedPKRecognisor());
		options.setFkRecog(new FKBasedFKRecognisor());
		var generateSchema = DataRepoGenerator.generateClasses(database, "nz.co.gregs.dbvolution.generation", options);
		for (DBTableClass dbcl : generateSchema.getTables()) {
			if (testClassNames.contains(dbcl.getClassName())) {
				classesTested++;
				boolean found = false;
				for (String str : testGetSchemaWithRecognisorTestClasses) {
					if (str.contains(dbcl.getClassName())) {
						final String testcaseLowercase = str.replaceAll("[ \n\r\t]*", "").toLowerCase();
						final String sourceLowerCase = dbcl.getJavaSource().replaceAll("[ \n\r\t]*", "").toLowerCase();
						if (testcaseLowercase.equals(sourceLowerCase)) {
							found = true;
						}
					}
				}
				assertTrue("Unable to find: \n\"" + dbcl.getJavaSource() + "\"", found);
			}
		}
		assertThat(classesTested, is(4));
	}

	/**
	 * A file object used to represent source coming from a string.
	 */
//	public class JavaSourceFromString extends SimpleJavaFileObject {
//
//		/**
//		 * The source code of this "file".
//		 */
//		final String code;
//
//		/**
//		 * Constructs a new JavaSourceFromString.
//		 *
//		 * @param name the name of the compilation unit represented by this file
//		 * object
//		 * @param code the source code for the compilation unit represented by this
//		 * file object
//		 */
//		JavaSourceFromString(String name, String code) {
//			super(URI.create("string:///" + name.replace('.', '/') + Kind.SOURCE.extension),
//					Kind.SOURCE);
//			this.code = code;
//		}
//
//		@Override
//		public CharSequence getCharContent(boolean ignoreEncodingErrors) {
//			return code;
//		}
//	}

	@Test
	public void testToClassCase() {
		String test = "T_31";
		String expected = "T_31";
		String result = DataRepoGenerator.toClassCase(test);
		assertEquals(result, expected);

		test = "T_3_1";
		expected = "T_3_1";
		result = DataRepoGenerator.toClassCase(test);
		assertEquals(result, expected);

		test = "car_company";
		expected = "CarCompany";
		result = DataRepoGenerator.toClassCase(test);
		assertEquals(result, expected);

		test = "CAR_COMPANY";
		expected = "CarCompany";
		result = DataRepoGenerator.toClassCase(test);
		assertThat(result, is(expected));

		test = "CARCOMPANY";
		expected = "Carcompany";
		result = DataRepoGenerator.toClassCase(test);
		assertThat(result, is(expected));

		test = "CarCompany";
		expected = "CarCompany";
		result = DataRepoGenerator.toClassCase(test);
		assertThat(result, is(expected));
	}

	@DBTableName("test_auto_increment_detection")
	public static class TestAutoIncrementDetection extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBPrimaryKey
		@DBColumn
		@DBAutoIncrement
		public DBInteger pk_uid = new DBInteger();

		@DBColumn
		public DBString name = new DBString();

	}

	@DBTableName("create_table_foreign_key")
	public static class CreateTableForeignKey extends DBRow {

		public static final long serialVersionUID = 1L;

		@DBColumn
		DBString name = new DBString();

		@DBColumn
		@DBForeignKey(Marque.class)
		DBInteger marqueForeignKey = new DBInteger();

		@DBColumn
		@DBForeignKey(CarCompany.class)
		DBInteger carCoForeignKey = new DBInteger();
	}

	@DBTableName("create_table_foreign_keyy")
	public static class CreateTableForeignKeyy extends DBRow {

		public static final long serialVersionUID = 1L;

		@DBColumn
		DBString name = new DBString();

		@DBColumn
		@DBForeignKey(Marque.class)
		DBInteger marqueForeignKey = new DBInteger();

		@DBColumn
		@DBForeignKey(CarCompany.class)
		DBInteger carCoForeignKey = new DBInteger();
	}

}
