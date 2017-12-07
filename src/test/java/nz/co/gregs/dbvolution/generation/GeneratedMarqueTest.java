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

import java.io.File;
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
import nz.co.gregs.dbvolution.databases.H2MemoryDB;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.FKBasedFKRecognisor;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.example.UIDBasedPKRecognisor;
import nz.co.gregs.dbvolution.generation.DBTableClassGenerator.Options;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.freshvanilla.compile.CachedCompiler;
import org.freshvanilla.compile.CompilerUtils;
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
public class GeneratedMarqueTest extends AbstractTest {

	public GeneratedMarqueTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testGetSchema() throws SQLException {
		if (database instanceof H2MemoryDB) {
			database.createTable(new TestAutoIncrementDetection());
			int classesTested = 0;
			List<DBTableClass> generateSchema;
			List<String> testClassNames = Arrays.asList(new String[]{"CarCompany", "Companylogo", "LtCarcoLogo", "Marque", "TestAutoIncrementDetection"});
			List<String> testClasses = new ArrayList<String>();
			testClasses.add("package nz.co.gregs.dbvolution.generation;\n\nimport nz.co.gregs.dbvolution.*;\nimport nz.co.gregs.dbvolution.datatypes.*;\nimport nz.co.gregs.dbvolution.datatypes.spatial2D.*;\nimport nz.co.gregs.dbvolution.annotations.*;\n\n@DBTableName(\"CAR_COMPANY\") \npublic class CarCompany extends DBRow {\n\n    public static final long serialVersionUID = 1L;\n\n    @DBColumn(\"NAME\")\n    public DBString name = new DBString();\n\n    @DBColumn(\"UID_CARCOMPANY\")\n    @DBPrimaryKey\n    public DBInteger uidCarcompany = new DBInteger();\n\n}\n\n");
			testClasses.add("package nz.co.gregs.dbvolution.generation;\n\nimport nz.co.gregs.dbvolution.*;\nimport nz.co.gregs.dbvolution.datatypes.*;\nimport nz.co.gregs.dbvolution.datatypes.spatial2D.*;\nimport nz.co.gregs.dbvolution.annotations.*;\n\n@DBTableName(\"COMPANYLOGO\") \npublic class Companylogo extends DBRow {\n\n    public static final long serialVersionUID = 1L;\n\n    @DBColumn(\"LOGO_ID\")\n    @DBPrimaryKey\n    public DBInteger logoId = new DBInteger();\n\n    @DBColumn(\"CAR_COMPANY_FK\")\n    public DBInteger carCompanyFk = new DBInteger();\n\n    @DBColumn(\"IMAGE_FILE\")\n    public DBLargeBinary imageFile = new DBLargeBinary();\n\n    @DBColumn(\"IMAGE_NAME\")\n    public DBString imageName = new DBString();\n\n}\n\n");
			testClasses.add("package nz.co.gregs.dbvolution.generation;\n\nimport nz.co.gregs.dbvolution.*;\nimport nz.co.gregs.dbvolution.datatypes.*;\nimport nz.co.gregs.dbvolution.datatypes.spatial2D.*;\nimport nz.co.gregs.dbvolution.annotations.*;\n\n@DBTableName(\"LT_CARCO_LOGO\") \npublic class LtCarcoLogo extends DBRow {\n\n    public static final long serialVersionUID = 1L;\n\n    @DBColumn(\"FK_CAR_COMPANY\")\n    public DBInteger fkCarCompany = new DBInteger();\n\n    @DBColumn(\"FK_COMPANY_LOGO\")\n    public DBInteger fkCompanyLogo = new DBInteger();\n\n}\n\n");
			testClasses.add("package nz.co.gregs.dbvolution.generation;\n" + "\n" + "import nz.co.gregs.dbvolution.*;\n" + "import nz.co.gregs.dbvolution.datatypes.*;\nimport nz.co.gregs.dbvolution.datatypes.spatial2D.*;\n" + "import nz.co.gregs.dbvolution.annotations.*;\n" + "\n" + "@DBTableName(\"MARQUE\") \n" + "public class Marque extends DBRow {\n\n    public static final long serialVersionUID = 1L;\n\n    @DBColumn(\"NUMERIC_CODE\")\n    public DBNumber numericCode = new DBNumber();\n\n    @DBColumn(\"UID_MARQUE\")\n    @DBPrimaryKey\n    public DBInteger uidMarque = new DBInteger();\n\n    @DBColumn(\"ISUSEDFORTAFROS\")\n    public DBString isusedfortafros = new DBString();\n\n    @DBColumn(\"FK_TOYSTATUSCLASS\")\n    public DBNumber fkToystatusclass = new DBNumber();\n\n    @DBColumn(\"INTINDALLOCALLOWED\")\n    public DBString intindallocallowed = new DBString();\n\n    @DBColumn(\"UPD_COUNT\")\n    public DBInteger updCount = new DBInteger();\n\n    @DBColumn(\"AUTO_CREATED\")\n    public DBString autoCreated = new DBString();\n\n    @DBColumn(\"NAME\")\n    public DBString name = new DBString();\n\n    @DBColumn(\"PRICINGCODEPREFIX\")\n    public DBString pricingcodeprefix = new DBString();\n\n    @DBColumn(\"RESERVATIONSALWD\")\n    public DBString reservationsalwd = new DBString();\n\n    @DBColumn(\"CREATION_DATE\")\n    public DBDate creationDate = new DBDate();\n\n    @DBColumn(\"ENABLED\")\n    public DBBoolean enabled = new DBBoolean();\n\n    @DBColumn(\"FK_CARCOMPANY\")\n    public DBInteger fkCarcompany = new DBInteger();\n\n}\n\n");
			testClasses.add("package nz.co.gregs.dbvolution.generation;\n\nimport nz.co.gregs.dbvolution.*;\nimport nz.co.gregs.dbvolution.datatypes.*;\nimport nz.co.gregs.dbvolution.datatypes.spatial2D.*;\nimport nz.co.gregs.dbvolution.annotations.*;\n\n@DBTableName(\"TEST_AUTO_INCREMENT_DETECTION\") \npublic class TestAutoIncrementDetection extends DBRow {\n\n    public static final long serialVersionUID = 1L;\n\n    @DBColumn(\"PK_UID\")\n    @DBPrimaryKey\n    @DBAutoIncrement\n    public DBInteger pkUid = new DBInteger();\n\n    @DBColumn(\"NAME\")\n    public DBString name = new DBString();\n\n}\n\n");
			generateSchema = DBTableClassGenerator.generateClassesOfTables(database, "nz.co.gregs.dbvolution.generation", new Options());
			for (DBTableClass dbcl : generateSchema) {
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
					Assert.assertTrue("Unable to find: \n\"" + dbcl.getJavaSource() + "\"", found);
				}
			}
			Assert.assertThat(classesTested, is(5));

			database.preventDroppingOfTables(false);
			database.dropTable(new TestAutoIncrementDetection());
		}
	}

	@Test
	public void testGetSchemaOfGeneratedForeignKeys() throws SQLException {
		if (database instanceof H2MemoryDB) {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(new CreateTableForeignKey());
			database.createTableWithForeignKeys(new CreateTableForeignKey());
			int classesTested = 0;
			List<DBTableClass> generateSchema;
			List<String> testClassNames = Arrays.asList(new String[]{"CreateTableForeignKey"});
			List<String> testClasses = new ArrayList<String>();
			testClasses.add("package nz.co.gregs.dbvolution.generation;\n\nimport nz.co.gregs.dbvolution.*;\nimport nz.co.gregs.dbvolution.datatypes.*;\nimport nz.co.gregs.dbvolution.datatypes.spatial2D.*;\nimport nz.co.gregs.dbvolution.annotations.*;\n\n@DBTableName(\"CREATE_TABLE_FOREIGN_KEY\") \npublic class CreateTableForeignKey extends DBRow {\n\n    public static final long serialVersionUID = 1L;\n\n    @DBColumn(\"NAME\")\n    public DBString name = new DBString();\n\n    @DBColumn(\"MARQUEFOREIGNKEY\")\n    @DBForeignKey(Marque.class)\n    public DBInteger marqueforeignkey = new DBInteger();\n\n    @DBColumn(\"CARCOFOREIGNKEY\")\n    @DBForeignKey(CarCompany.class)\n    public DBInteger carcoforeignkey = new DBInteger();\n\n}\n\n");

			generateSchema = DBTableClassGenerator.generateClassesOfTables(database, "nz.co.gregs.dbvolution.generation", new DBTableClassGenerator.Options());
			for (DBTableClass dbcl : generateSchema) {
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
					Assert.assertTrue("Unable to find: \n\"" + dbcl.getJavaSource() + "\"", found);
				}
			}
			Assert.assertThat(classesTested, is(1));

			database.preventDroppingOfTables(false);
			database.dropTable(new CreateTableForeignKey());
		}
	}

	@Test
	public void testGetSchemaOfGeneratedForeignKeysWithIncludedColumnNames() throws SQLException {
		if (database instanceof H2MemoryDB) {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(new CreateTableForeignKeyy());
			database.createTableWithForeignKeys(new CreateTableForeignKeyy());
			int classesTested = 0;
			List<DBTableClass> generateSchema;
			List<String> testClassNames = Arrays.asList(new String[]{"CreateTableForeignKeyy"});
			List<String> testClasses = new ArrayList<String>();
//			testClasses.add("package nz.co.gregs.dbvolution.generation;\n\nimport nz.co.gregs.dbvolution.*;\nimport nz.co.gregs.dbvolution.datatypes.*;\nimport nz.co.gregs.dbvolution.datatypes.spatial2D.*;\nimport nz.co.gregs.dbvolution.annotations.*;\n\n@DBTableName(\"CREATE_TABLE_FOREIGN_KEY\") \npublic class CreateTableForeignKey extends DBRow {\n\n    public static final long serialVersionUID = 1L;\n\n    @DBColumn(\"NAME\")\n    public DBString name = new DBString();\n\n    @DBColumn(\"MARQUEFOREIGNKEY\")\n    @DBForeignKey(Marque.class)\n    public DBInteger marqueforeignkey = new DBInteger();\n\n    @DBColumn(\"CARCOFOREIGNKEY\")\n    @DBForeignKey(CarCompany.class)\n    public DBInteger carcoforeignkey = new DBInteger();\n\n}\n\n");
			testClasses.add("package nz.co.gregs.dbvolution.generation;\n\nimport nz.co.gregs.dbvolution.*;\nimport nz.co.gregs.dbvolution.datatypes.*;\nimport nz.co.gregs.dbvolution.datatypes.spatial2D.*;\nimport nz.co.gregs.dbvolution.annotations.*;\n\n@DBTableName(\"CREATE_TABLE_FOREIGN_KEYY\") \npublic class CreateTableForeignKeyy extends DBRow {\n\n    public static final long serialVersionUID = 1L;\n\n    @DBColumn(\"NAME\")\n    public DBString name = new DBString();\n\n    @DBColumn(\"MARQUEFOREIGNKEY\")\n    @DBForeignKey(value = Marque.class, column = \"UID_MARQUE\")\n    public DBInteger marqueforeignkey = new DBInteger();\n\n    @DBColumn(\"CARCOFOREIGNKEY\")\n    @DBForeignKey(value = CarCompany.class, column = \"UID_CARCOMPANY\")\n    public DBInteger carcoforeignkey = new DBInteger();\n\n}\n\n");
			final Options options = new DBTableClassGenerator.Options();
			options.includeForeignKeyColumnName = true;
			generateSchema = DBTableClassGenerator.generateClassesOfTables(database, "nz.co.gregs.dbvolution.generation", options);
			for (DBTableClass dbcl : generateSchema) {
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
					Assert.assertTrue("Unable to find: \n\"" + dbcl.getJavaSource() + "\"", found);
				}
			}
			Assert.assertThat(classesTested, is(1));

			database.preventDroppingOfTables(false);
			database.dropTable(new CreateTableForeignKeyy());
		}
	}

	@Test
	public void testGetSchemaWithRecognisor() throws SQLException {
		if (database instanceof H2MemoryDB) {
			int classesTested = 0;
			List<DBTableClass> generateSchema;
			List<String> testClassNames = Arrays.asList(new String[]{"CarCompany", "Companylogo", "LtCarcoLogo", "Marque"});
			List<String> testGetSchemaWithRecognisorTestClasses = new ArrayList<String>();
			testGetSchemaWithRecognisorTestClasses.add("package nz.co.gregs.dbvolution.generation;\n\nimport nz.co.gregs.dbvolution.*;\nimport nz.co.gregs.dbvolution.datatypes.*;\nimport nz.co.gregs.dbvolution.datatypes.spatial2D.*;\nimport nz.co.gregs.dbvolution.annotations.*;\n\n@DBTableName(\"CAR_COMPANY\") \npublic class CarCompany extends DBRow {\n\n    public static final long serialVersionUID = 1L;\n\n    @DBColumn(\"NAME\")\n    public DBString name = new DBString();\n\n    @DBColumn(\"UID_CARCOMPANY\")\n    @DBPrimaryKey\n    public DBInteger uidCarcompany = new DBInteger();\n\n}\n\n");
			testGetSchemaWithRecognisorTestClasses.add("package nz.co.gregs.dbvolution.generation;\n\nimport nz.co.gregs.dbvolution.*;\nimport nz.co.gregs.dbvolution.datatypes.*;\nimport nz.co.gregs.dbvolution.datatypes.spatial2D.*;\nimport nz.co.gregs.dbvolution.annotations.*;\n\n@DBTableName(\"COMPANYLOGO\") \npublic class Companylogo extends DBRow {\n\n    public static final long serialVersionUID = 1L;\n\n    @DBColumn(\"LOGO_ID\")\n    @DBPrimaryKey\n    public DBInteger logoId = new DBInteger();\n\n    @DBColumn(\"CAR_COMPANY_FK\")\n    public DBInteger carCompanyFk = new DBInteger();\n\n    @DBColumn(\"IMAGE_FILE\")\n    public DBLargeBinary imageFile = new DBLargeBinary();\n\n    @DBColumn(\"IMAGE_NAME\")\n    public DBString imageName = new DBString();\n\n}\n\n");
			testGetSchemaWithRecognisorTestClasses.add("package nz.co.gregs.dbvolution.generation;\n\nimport nz.co.gregs.dbvolution.*;\nimport nz.co.gregs.dbvolution.datatypes.*;\nimport nz.co.gregs.dbvolution.datatypes.spatial2D.*;\nimport nz.co.gregs.dbvolution.annotations.*;\n\n@DBTableName(\"LT_CARCO_LOGO\") \npublic class LtCarcoLogo extends DBRow {\n\n    public static final long serialVersionUID = 1L;\n\n    @DBColumn(\"FK_CAR_COMPANY\")\n    @DBForeignKey(CarCompany.class)\n    public DBInteger fkCarCompany = new DBInteger();\n\n    @DBColumn(\"FK_COMPANY_LOGO\")\n    @DBForeignKey(Companylogo.class)\n    public DBInteger fkCompanyLogo = new DBInteger();\n\n}\n\n");
			testGetSchemaWithRecognisorTestClasses.add("package nz.co.gregs.dbvolution.generation;\n\nimport nz.co.gregs.dbvolution.*;\nimport nz.co.gregs.dbvolution.datatypes.*;\nimport nz.co.gregs.dbvolution.datatypes.spatial2D.*;\nimport nz.co.gregs.dbvolution.annotations.*;\n\n@DBTableName(\"MARQUE\") \npublic class Marque extends DBRow {\n\n    public static final long serialVersionUID = 1L;\n\n    @DBColumn(\"NUMERIC_CODE\")\n    public DBNumber numericCode = new DBNumber();\n\n    @DBColumn(\"UID_MARQUE\")\n    @DBPrimaryKey\n    public DBInteger uidMarque = new DBInteger();\n\n    @DBColumn(\"ISUSEDFORTAFROS\")\n    public DBString isusedfortafros = new DBString();\n\n    @DBColumn(\"FK_TOYSTATUSCLASS\")\n    @DBForeignKey(Toystatusclass.class)\n    public DBNumber fkToystatusclass = new DBNumber();\n\n    @DBColumn(\"INTINDALLOCALLOWED\")\n    public DBString intindallocallowed = new DBString();\n\n    @DBColumn(\"UPD_COUNT\")\n    public DBInteger updCount = new DBInteger();\n\n    @DBColumn(\"AUTO_CREATED\")\n    public DBString autoCreated = new DBString();\n\n    @DBColumn(\"NAME\")\n    public DBString name = new DBString();\n\n    @DBColumn(\"PRICINGCODEPREFIX\")\n    public DBString pricingcodeprefix = new DBString();\n\n    @DBColumn(\"RESERVATIONSALWD\")\n    public DBString reservationsalwd = new DBString();\n\n    @DBColumn(\"CREATION_DATE\")\n    public DBDate creationDate = new DBDate();\n\n    @DBColumn(\"ENABLED\")\n    public DBBoolean enabled = new DBBoolean();\n\n    @DBColumn(\"FK_CARCOMPANY\")\n    @DBForeignKey(CarCompany.class)\n    public DBInteger fkCarcompany = new DBInteger();\n\n}\n\n");

			DBTableClassGenerator.Options options = new DBTableClassGenerator.Options();
			options.pkRecog = new UIDBasedPKRecognisor();
			options.fkRecog = new FKBasedFKRecognisor();
			generateSchema = DBTableClassGenerator.generateClassesOfTables(database, "nz.co.gregs.dbvolution.generation", options);
			for (DBTableClass dbcl : generateSchema) {
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
					Assert.assertTrue("Unable to find: \n\"" + dbcl.getJavaSource() + "\"", found);
				}
			}
			Assert.assertThat(classesTested, is(4));
		}
	}

	@Test
	public void testEssenceCompiling() throws SQLException, InstantiationException, IllegalAccessException, Exception {
		if (database instanceof H2MemoryDB && database.getDatabaseName().equalsIgnoreCase("blank")) {
			CachedCompiler cc = CompilerUtils.DEBUGGING
					? new CachedCompiler(new File(System.getProperty("user.dir"), "src/test/java"), new File(System.getProperty("user.dir"), "target/compiled"))
					: CompilerUtils.CACHED_COMPILER;

			List<DBTableClass> generateSchema = DBTableClassGenerator.generateClassesOfTables(database, "nz.co.gregs.dbvolution.generation.test", new Options());
			for (DBTableClass dbcl : generateSchema) {
				if (dbcl.getTableName().equals("CAR_COMPANY")
						|| dbcl.getTableName().equals("MARQUE")
						|| dbcl.getTableName().equals("LT_CARCO_LOGO")
						|| dbcl.getTableName().equals("COMPANYLOGO")) {
					final String javaSource = dbcl.getJavaSource();

					Class<?> compiledClass = cc.loadFromJava(dbcl.getFullyQualifiedName(), javaSource);
					Object newInstance = compiledClass.newInstance();
					DBRow row = (DBRow) newInstance;
					List<DBRow> rows = database.getDBTable(row).setBlankQueryAllowed(true).getAllRows();

					switch (row.getTableName()) {
						case "CAR_COMPANY":
							Assert.assertThat(rows.size(), is(4));
							break;
						case "MARQUE":
							Assert.assertThat(rows.size(), is(22));
							break;
						case "LT_CARCO_LOGO":
							Assert.assertThat(rows.size(), is(0));
							break;
						case "COMPANYLOGO":
							Assert.assertThat(rows.size(), is(0));
							break;
						default:
							break;
					}
				}
			}
		}
	}

	@Test
	public void testCompiling() throws SQLException, IOException, Exception {
		List<JavaSourceFromString> compilationUnits = new ArrayList<JavaSourceFromString>(); // input for first compilation task
		List<DBTableClass> generateSchema = DBTableClassGenerator.generateClassesOfTables(database, "nz.co.gregs.dbvolution.generation", new Options());
		for (DBTableClass dbcl : generateSchema) {
			compilationUnits.add(new JavaSourceFromString(dbcl.getFullyQualifiedName(), dbcl.getJavaSource()));
		}
		JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
		DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();
		Boolean succeeded;
		// Try to add the classes to the TARGET directory
		try (StandardJavaFileManager fileManager = compiler.getStandardFileManager(diagnostics, null, null)) {
			// Try to add the classes to the TARGET directory
			List<File> locations = new ArrayList<File>();
			File file = new File(System.getProperty("user.dir"), "target");
			if (file.exists()) {
				locations.add(file);
			} else {
				locations.add(new File(System.getProperty("user.dir")));
			}
			fileManager.setLocation(StandardLocation.CLASS_OUTPUT, locations);
			JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager, diagnostics, null, null, compilationUnits);
			succeeded = task.call();
			for (Diagnostic<?> diagnostic : diagnostics.getDiagnostics()) {
				succeeded = false;
			}
		}
		Assert.assertThat(succeeded, is(true));
	}

	/**
	 * A file object used to represent source coming from a string.
	 */
	public class JavaSourceFromString extends SimpleJavaFileObject {

		/**
		 * The source code of this "file".
		 */
		final String code;

		/**
		 * Constructs a new JavaSourceFromString.
		 *
		 * @param name the name of the compilation unit represented by this file
		 * object
		 * @param code the source code for the compilation unit represented by this
		 * file object
		 */
		JavaSourceFromString(String name, String code) {
			super(URI.create("string:///" + name.replace('.', '/') + Kind.SOURCE.extension),
					Kind.SOURCE);
			this.code = code;
		}

		@Override
		public CharSequence getCharContent(boolean ignoreEncodingErrors) {
			return code;
		}
	}

	@Test
	public void testToClassCase() {
		String test = "T_31";
		String expected = "T_31";
		String result = DBTableClassGenerator.toClassCase(test);
		Assert.assertEquals(result, expected);

		test = "T_3_1";
		expected = "T_3_1";
		result = DBTableClassGenerator.toClassCase(test);
		Assert.assertEquals(result, expected);

		test = "car_company";
		expected = "CarCompany";
		result = DBTableClassGenerator.toClassCase(test);
		Assert.assertEquals(result, expected);

		test = "CAR_COMPANY";
		expected = "CarCompany";
		result = DBTableClassGenerator.toClassCase(test);
		Assert.assertEquals(result, expected);

		test = "CARCOMPANY";
		expected = "Carcompany";
		result = DBTableClassGenerator.toClassCase(test);
		Assert.assertEquals(result, expected);
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
