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
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.CompanyLogo;
import nz.co.gregs.dbvolution.example.LinkCarCompanyAndLogo;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.exceptions.AccidentalCartesianJoinException;
import org.junit.Test;

/**
 *
 * @author gregorygraham
 */
public class PrimaryKeyLessTablesTest extends AbstractTest {

    public void SetUp() throws SQLException {
        myDatabase.dropTableNoExceptions(new LinkCarCompanyAndLogo());
        myDatabase.createTable(new LinkCarCompanyAndLogo());
    }

    public void TearDown() throws SQLException {
        myDatabase.dropTable(new LinkCarCompanyAndLogo());
    }

    @Test
    public void linkIntoTableTest() throws SQLException {
        CarCompany carCompany = new CarCompany();
        LinkCarCompanyAndLogo link = new LinkCarCompanyAndLogo();
        DBQuery dbQuery = myDatabase.getDBQuery(carCompany, link);
        dbQuery.print();
        dbQuery = myDatabase.getDBQuery(new CompanyLogo(), link);
        dbQuery.print();
    }

    @Test
    public void linkThruTableTest() throws SQLException {
        CarCompany carCompany = new CarCompany();
        LinkCarCompanyAndLogo link = new LinkCarCompanyAndLogo();

        DBQuery dbQuery = myDatabase.getDBQuery(carCompany, link, new CompanyLogo());
        dbQuery.print();
    }

    @Test
    public void testCartesianJoinProtection() throws SQLException, Exception {
        try {
            myDatabase.getDBQuery(new Marque(), new CompanyLogo()).print();
            throw new Exception("Should have thrown an AccidentalCartesianJoinException here.");
        } catch (AccidentalCartesianJoinException ex) {
        }
    }

    @Test
    public void testAdHocRelations() throws SQLException, Exception {
        final CompanyLogo companyLogo = new CompanyLogo();
        myMarqueRow.addRelationship(myMarqueRow.carCompany, companyLogo,companyLogo.carCompany);
        myDatabase.getDBQuery(myMarqueRow, companyLogo).print();
    }
}
