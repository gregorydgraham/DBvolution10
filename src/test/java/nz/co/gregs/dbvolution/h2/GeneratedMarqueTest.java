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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.example.FKBasedFKRecognisor;
import nz.co.gregs.dbvolution.example.UIDBasedPKRecognisor;
import nz.co.gregs.dbvolution.generation.DBTableClassGenerator;
import nz.co.gregs.dbvolution.generation.DBTableClass;
import nz.co.gregs.dbvolution.generation.ForeignKeyRecognisor;
import nz.co.gregs.dbvolution.generation.Marque;
import nz.co.gregs.dbvolution.generation.PrimaryKeyRecognisor;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author gregorygraham
 */
public class GeneratedMarqueTest extends AbstractTest {

    nz.co.gregs.dbvolution.example.Marque myTableRow = new nz.co.gregs.dbvolution.example.Marque();

    public GeneratedMarqueTest(Object db) {
        super(db);
    }

//    @Test
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
                if (str.replaceAll("[ /n/r/t]*", " ").equals(dbcl.javaSource.replaceAll("[ /n/r/t]*", " "))) {
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
                if (str.replaceAll("[ /n/r/t]*", " ").equals(dbcl.javaSource.replaceAll("[ /n/r/t]*", " "))) {
                    found = true;
                }
            }
            Assert.assertTrue("Unable to find: \n\"" + dbcl.javaSource + "\"", found);
        }
    }

//    @Test
    public void testGetAllRows() throws SQLException {
        DBTable<Marque> marq = DBTable.getInstance(database, new Marque());
        marq.getAllRows();
        for (DBRow row : marq.toList()) {
            System.out.println(row);
        }
    }

//    @Test
    public void testGetFirstAndPrimaryKey() throws SQLException {
        DBTable<Marque> marq = DBTable.getInstance(database, new Marque());
        DBRow row = marq.getFirstRow();
        if (row != null) {
            String primaryKey = row.getPrimaryKey().getSQLValue(database);
            DBTable<Marque> singleMarque = DBTable.getInstance(database, new Marque());
            singleMarque.getRowsByPrimaryKey(primaryKey).print();
        }
    }

//    @Test
    public void testRawQuery() throws SQLException {
        DBTable<Marque> marq = DBTable.getInstance(database, new Marque());
        String rawQuery = "and lower(name) in ('toyota','hummer') ;  ";
        marq = marq.getRowsByRawSQL(rawQuery);
        marq.print();

    }

//    @Test
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
