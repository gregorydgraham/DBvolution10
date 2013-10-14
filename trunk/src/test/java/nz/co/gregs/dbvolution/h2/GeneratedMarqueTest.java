/*
 * Copyright 2013 gregorygraham.
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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.example.FKBasedFKRecognisor;
import nz.co.gregs.dbvolution.example.UIDBasedPKRecognisor;
import nz.co.gregs.dbvolution.generation.DBTableClassGenerator;
import nz.co.gregs.dbvolution.generation.DBTableClass;
import nz.co.gregs.dbvolution.generation.ForeignKeyRecognisor;
import nz.co.gregs.dbvolution.generation.PrimaryKeyRecognisor;
import org.freshvanilla.compile.CachedCompiler;
import org.freshvanilla.compile.CompilerUtils;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.Matchers.*;

/**
 *
 * @author gregorygraham
 */
public class GeneratedMarqueTest extends AbstractTest {

    nz.co.gregs.dbvolution.example.Marque myTableRow = new nz.co.gregs.dbvolution.example.Marque();

    public GeneratedMarqueTest(Object db) {
        super(db);
    }

    @Test
    public void testGetSchema() throws SQLException {
        List<DBTableClass> generateSchema;
        List<String> testClasses = new ArrayList<String>();
        testClasses.add("package nz.co.gregs.dbvolution.generation;\n\nimport nz.co.gregs.dbvolution.*;\nimport nz.co.gregs.dbvolution.datatypes.*;\nimport nz.co.gregs.dbvolution.annotations.*;\n\n@DBTableName(\"CAR_COMPANY\") \npublic class CarCompany extends DBRow {\n\n    @DBColumn(\"NAME\")\n    public DBString name = new DBString();\n\n    @DBColumn(\"UID_CARCOMPANY\")\n    @DBPrimaryKey\n    public DBInteger uidCarcompany = new DBInteger();\n\n}\n\n");
        testClasses.add("package nz.co.gregs.dbvolution.generation;\n\nimport nz.co.gregs.dbvolution.*;\nimport nz.co.gregs.dbvolution.datatypes.*;\nimport nz.co.gregs.dbvolution.annotations.*;\n\n@DBTableName(\"COMPANYLOGO\") \npublic class Companylogo extends DBRow {\n\n    @DBColumn(\"LOGO_ID\")\n    @DBPrimaryKey\n    public DBInteger logoId = new DBInteger();\n\n    @DBColumn(\"CAR_COMPANY_FK\")\n    public DBInteger carCompanyFk = new DBInteger();\n\n    @DBColumn(\"IMAGE_FILE\")\n    public DBByteArray imageFile = new DBByteArray();\n\n    @DBColumn(\"IMAGE_NAME\")\n    public DBString imageName = new DBString();\n\n}\n\n");
        testClasses.add("package nz.co.gregs.dbvolution.generation;\n\nimport nz.co.gregs.dbvolution.*;\nimport nz.co.gregs.dbvolution.datatypes.*;\nimport nz.co.gregs.dbvolution.annotations.*;\n\n@DBTableName(\"LT_CARCO_LOGO\") \npublic class LtCarcoLogo extends DBRow {\n\n    @DBColumn(\"FK_CAR_COMPANY\")\n    public DBInteger fkCarCompany = new DBInteger();\n\n    @DBColumn(\"FK_COMPANY_LOGO\")\n    public DBInteger fkCompanyLogo = new DBInteger();\n\n}\n\n");
        testClasses.add("package nz.co.gregs.dbvolution.generation;\n" + "\n" + "import nz.co.gregs.dbvolution.*;\n" + "import nz.co.gregs.dbvolution.datatypes.*;\n" + "import nz.co.gregs.dbvolution.annotations.*;\n" + "\n" + "@DBTableName(\"MARQUE\") \n" + "public class Marque extends DBRow {\n" + "\n" + "    @DBColumn(\"NUMERIC_CODE\")\n" + "    public DBNumber numericCode = new DBNumber();\n" + "\n" + "    @DBColumn(\"UID_MARQUE\")\n" + "    @DBPrimaryKey\n" + "    public DBInteger uidMarque = new DBInteger();\n" + "\n" + "    @DBColumn(\"ISUSEDFORTAFROS\")\n" + "    public DBString isusedfortafros = new DBString();\n" + "\n" + "    @DBColumn(\"FK_TOYSTATUSCLASS\")\n" + "    public DBNumber fkToystatusclass = new DBNumber();\n\n    @DBColumn(\"INTINDALLOCALLOWED\")\n    public DBString intindallocallowed = new DBString();\n\n    @DBColumn(\"UPD_COUNT\")\n    public DBInteger updCount = new DBInteger();\n\n    @DBColumn(\"AUTO_CREATED\")\n    public DBString autoCreated = new DBString();\n\n    @DBColumn(\"NAME\")\n    public DBString name = new DBString();\n\n    @DBColumn(\"PRICINGCODEPREFIX\")\n    public DBString pricingcodeprefix = new DBString();\n\n    @DBColumn(\"RESERVATIONSALWD\")\n    public DBString reservationsalwd = new DBString();\n\n    @DBColumn(\"CREATION_DATE\")\n    public DBDate creationDate = new DBDate();\n\n    @DBColumn(\"ENABLED\")\n    public DBInteger enabled = new DBInteger();\n\n    @DBColumn(\"FK_CARCOMPANY\")\n    public DBInteger fkCarcompany = new DBInteger();\n\n}\n\n");
        generateSchema = DBTableClassGenerator.generateClassesOfTables(database, "nz.co.gregs.dbvolution.generation", new PrimaryKeyRecognisor(), new ForeignKeyRecognisor());
        for (DBTableClass dbcl : generateSchema) {
            System.out.print("" + dbcl.javaSource);
            boolean found = false;
            for (String str : testClasses) {
                if (str.replaceAll("[ \n\r\t]*", " ").equals(dbcl.javaSource.replaceAll("[ \n\r\t]*", " "))) {
                    found = true;
                }
            }
            Assert.assertTrue("Unable to find: \n\"" + dbcl.javaSource + "\"", found);
        }
    }

    @Test
    public void testGetSchemaWithRecognisor() throws SQLException {
        List<DBTableClass> generateSchema;
        List<String> testGetSchemaWithRecognisorTestClasses = new ArrayList<String>();
        testGetSchemaWithRecognisorTestClasses.add("package nz.co.gregs.dbvolution.generation;\n\nimport nz.co.gregs.dbvolution.*;\nimport nz.co.gregs.dbvolution.datatypes.*;\nimport nz.co.gregs.dbvolution.annotations.*;\n\n@DBTableName(\"CAR_COMPANY\") \npublic class CarCompany extends DBRow {\n\n    @DBColumn(\"NAME\")\n    public DBString name = new DBString();\n\n    @DBColumn(\"UID_CARCOMPANY\")\n    @DBPrimaryKey\n    public DBInteger uidCarcompany = new DBInteger();\n\n}\n\n");
        testGetSchemaWithRecognisorTestClasses.add("package nz.co.gregs.dbvolution.generation;\n\nimport nz.co.gregs.dbvolution.*;\nimport nz.co.gregs.dbvolution.datatypes.*;\nimport nz.co.gregs.dbvolution.annotations.*;\n\n@DBTableName(\"COMPANYLOGO\") \npublic class Companylogo extends DBRow {\n\n    @DBColumn(\"LOGO_ID\")\n    @DBPrimaryKey\n    public DBInteger logoId = new DBInteger();\n\n    @DBColumn(\"CAR_COMPANY_FK\")\n    public DBInteger carCompanyFk = new DBInteger();\n\n    @DBColumn(\"IMAGE_FILE\")\n    public DBByteArray imageFile = new DBByteArray();\n\n    @DBColumn(\"IMAGE_NAME\")\n    public DBString imageName = new DBString();\n\n}\n\n");
        testGetSchemaWithRecognisorTestClasses.add("package nz.co.gregs.dbvolution.generation;\n\nimport nz.co.gregs.dbvolution.*;\nimport nz.co.gregs.dbvolution.datatypes.*;\nimport nz.co.gregs.dbvolution.annotations.*;\n\n@DBTableName(\"LT_CARCO_LOGO\") \npublic class LtCarcoLogo extends DBRow {\n\n    @DBColumn(\"FK_CAR_COMPANY\")\n    @DBForeignKey(CarCompany.class)\n    public DBInteger fkCarCompany = new DBInteger();\n\n    @DBColumn(\"FK_COMPANY_LOGO\")\n    @DBForeignKey(Companylogo.class)\n    public DBInteger fkCompanyLogo = new DBInteger();\n\n}\n\n");
        testGetSchemaWithRecognisorTestClasses.add("package nz.co.gregs.dbvolution.generation;\n\nimport nz.co.gregs.dbvolution.*;\nimport nz.co.gregs.dbvolution.datatypes.*;\nimport nz.co.gregs.dbvolution.annotations.*;\n\n@DBTableName(\"MARQUE\") \npublic class Marque extends DBRow {\n\n    @DBColumn(\"NUMERIC_CODE\")\n    public DBNumber numericCode = new DBNumber();\n\n    @DBColumn(\"UID_MARQUE\")\n    @DBPrimaryKey\n    public DBInteger uidMarque = new DBInteger();\n\n    @DBColumn(\"ISUSEDFORTAFROS\")\n    public DBString isusedfortafros = new DBString();\n\n    @DBColumn(\"FK_TOYSTATUSCLASS\")\n    @DBForeignKey(Toystatusclass.class)\n    public DBNumber fkToystatusclass = new DBNumber();\n\n    @DBColumn(\"INTINDALLOCALLOWED\")\n    public DBString intindallocallowed = new DBString();\n\n    @DBColumn(\"UPD_COUNT\")\n    public DBInteger updCount = new DBInteger();\n\n    @DBColumn(\"AUTO_CREATED\")\n    public DBString autoCreated = new DBString();\n\n    @DBColumn(\"NAME\")\n    public DBString name = new DBString();\n\n    @DBColumn(\"PRICINGCODEPREFIX\")\n    public DBString pricingcodeprefix = new DBString();\n\n    @DBColumn(\"RESERVATIONSALWD\")\n    public DBString reservationsalwd = new DBString();\n\n    @DBColumn(\"CREATION_DATE\")\n    public DBDate creationDate = new DBDate();\n\n    @DBColumn(\"ENABLED\")\n    public DBInteger enabled = new DBInteger();\n\n    @DBColumn(\"FK_CARCOMPANY\")\n    @DBForeignKey(CarCompany.class)\n    public DBInteger fkCarcompany = new DBInteger();\n\n}\n\n");

        generateSchema = DBTableClassGenerator.generateClassesOfTables(database, "nz.co.gregs.dbvolution.generation", new UIDBasedPKRecognisor(), new FKBasedFKRecognisor());
        for (DBTableClass dbcl : generateSchema) {
            System.out.println(dbcl.javaSource);
            boolean found = false;
            for (String str : testGetSchemaWithRecognisorTestClasses) {
                if (str.replaceAll("[ \n\r\t]*", " ").equals(dbcl.javaSource.replaceAll("[ \n\r\t]*", " "))) {
                    found = true;
                }
            }
            Assert.assertTrue("Unable to find: \n\"" + dbcl.javaSource + "\"", found);
        }
    }

    @Test
    public void testEssenceCompiling() throws SQLException, InstantiationException, IllegalAccessException, Exception {
        CachedCompiler cc = CompilerUtils.DEBUGGING
                ? new CachedCompiler(new File(System.getProperty("user.dir"), "src/test/java"), new File(System.getProperty("user.dir"), "target/compiled"))
                : CompilerUtils.CACHED_COMPILER;

        List<DBTableClass> generateSchema = DBTableClassGenerator.generateClassesOfTables(database, "nz.co.gregs.dbvolution.generation", new PrimaryKeyRecognisor(), new ForeignKeyRecognisor());
        for (DBTableClass dbcl : generateSchema) {
            Class<?> compiledClass = cc.loadFromJava(dbcl.getFullyQualifiedName(), dbcl.javaSource);
            Object newInstance = compiledClass.newInstance();
            DBRow row = (DBRow) newInstance;
            List<DBRow> rows = database.get(row);
            database.print(rows);
            if (row.getTableName().equals("CAR_COMPANY")) {
                Assert.assertThat(rows.size(), is(4));
            } else if (row.getTableName().equals("MARQUE")) {
                Assert.assertThat(rows.size(), is(22));
            } else if (row.getTableName().equals("LT_CARCO_LOGO")) {
                Assert.assertThat(rows.size(), is(0));
            } else if (row.getTableName().equals("COMPANYLOGO")) {
                Assert.assertThat(rows.size(), is(0));
            } else {
                throw new Exception("UNKNOWN CLASS FOUND: " + row.getTableName());
            }
        }
    }

    @Test
    public void testCompiling() throws SQLException, IOException, Exception {
        List<JavaSourceFromString> compilationUnits = new ArrayList<JavaSourceFromString>(); // input for first compilation task
        List<DBTableClass> generateSchema = DBTableClassGenerator.generateClassesOfTables(database, "nz.co.gregs.dbvolution.generation", new PrimaryKeyRecognisor(), new ForeignKeyRecognisor());
        for (DBTableClass dbcl : generateSchema) {
            compilationUnits.add(new JavaSourceFromString(dbcl.getFullyQualifiedName(), dbcl.javaSource));
        }
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();
        StandardJavaFileManager fileManager = compiler.getStandardFileManager(diagnostics, null, null);

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
        Boolean succeeded = task.call();

        for (Diagnostic<?> diagnostic : diagnostics.getDiagnostics()) {
            System.out.println("Error on line " + diagnostic.getLineNumber() + " in \"" + diagnostic.getSource() + "\"");
            succeeded = false;
        }
        fileManager.close();
        if (succeeded) {
            System.out.println("Everything compiled correctly");
        } else {
            throw new Exception("There were compilation ERRORS");


        }
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
         * @param code the source code for the compilation unit represented by
         * this file object
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
        System.out.println(test + " => " + result + "(" + expected + ")");
        Assert.assertEquals(result, expected);
        test = "T_3_1";
        expected = "T_3_1";
        result = DBTableClassGenerator.toClassCase(test);
        System.out.println(test + " => " + result + "(" + expected + ")");
        Assert.assertEquals(result, expected);
        test = "car_company";
        expected = "CarCompany";
        result = DBTableClassGenerator.toClassCase(test);
        System.out.println(test + " => " + result + "(" + expected + ")");
        Assert.assertEquals(result, expected);
        test = "CAR_COMPANY";
        expected = "CarCompany";
        result = DBTableClassGenerator.toClassCase(test);
        System.out.println(test + " => " + result + "(" + expected + ")");
        Assert.assertEquals(result, expected);
        test = "CARCOMPANY";
        expected = "Carcompany";
        result = DBTableClassGenerator.toClassCase(test);
        System.out.println(test + " => " + result + "(" + expected + ")");
        Assert.assertEquals(result, expected);

    }
}
