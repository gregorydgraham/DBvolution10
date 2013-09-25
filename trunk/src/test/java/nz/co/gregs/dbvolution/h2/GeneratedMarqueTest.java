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

    @Test
    public void testGetSchema() throws SQLException{
        List<DBTableClass> generateSchema;
        generateSchema = DBTableClassGenerator.generateClassesOfTables(database, "nz.co.gregs.dbvolution.generation", new PrimaryKeyRecognisor(), new ForeignKeyRecognisor());
        for (DBTableClass dbcl : generateSchema) {
            System.out.print("" + dbcl.javaSource);
        }
    }

    @Test
    public void testGetSchemaWithRecognisor() throws SQLException{
        List<DBTableClass> generateSchema;
        generateSchema = DBTableClassGenerator.generateClassesOfTables(database, "nz.co.gregs.dbvolution.generation", new UIDBasedPKRecognisor(), new FKBasedFKRecognisor());
        for (DBTableClass dbcl : generateSchema) {
            System.out.print("" + dbcl.javaSource);
        }
    }

    @Test
    public void testGetAllRows() throws SQLException{
        DBTable<Marque> marq = DBTable.getInstance(database, new Marque());
        marq.getAllRows();
        for (DBRow row : marq.toList()) {
            System.out.println(row);
        }
    }

    @Test
    public void testGetFirstAndPrimaryKey() throws SQLException{
        DBTable<Marque> marq = DBTable.getInstance(database, new Marque());
        DBRow row = marq.getFirstRow();
        if (row != null) {
            String primaryKey = row.getPrimaryKey().getSQLValue();
            DBTable<Marque> singleMarque = DBTable.getInstance(database, new Marque());
            singleMarque.getRowsByPrimaryKey(primaryKey).print();
        }
    }

    @Test
    public void testRawQuery() throws SQLException {
        DBTable<Marque> marq = DBTable.getInstance(database, new Marque());
        String rawQuery = "and lower(name) in ('toyota','hummer') ;  ";
        marq = marq.getRowsByRawSQL(rawQuery);
        marq.print();

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
