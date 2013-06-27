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

import java.util.ArrayList;
import java.util.List;
import junit.framework.TestCase;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.H2DB;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;

/**
 *
 * @author gregorygraham
 */
public class AbstractTest extends TestCase {

    DBDatabase myDatabase = new H2DB("jdbc:h2:~/dbvolutionTest", "", "");
    Marque myMarqueRow = new Marque();
    CarCompany myCarCompanyRow = new CarCompany();
    DBTable<Marque> marques;
    DBTable<CarCompany> carCompanies;
    public List<Marque> marqueRows;
    public List<CarCompany> carTableRows;
    
    public AbstractTest(String name){
        super(name);
        carTableRows = new ArrayList<CarCompany>();
        marqueRows = new ArrayList<Marque>();
    }

    @Override
    @SuppressWarnings("empty-statement")
    protected void setUp() throws Exception {
        super.setUp();

        myDatabase.setPrintSQLBeforeExecuting(false);
        myDatabase.dropTableNoExceptions(new Marque());
        myDatabase.createTable(myMarqueRow);

        myDatabase.dropTableNoExceptions(myCarCompanyRow);
        myDatabase.createTable(myCarCompanyRow);

        DBTable.setPrintSQLBeforeExecuting(false);
        marques = new DBTable<Marque>(myMarqueRow, myDatabase);
        carCompanies = new DBTable<CarCompany>(myCarCompanyRow, myDatabase);
        carTableRows.add(new CarCompany("TOYOTA", 1));
        carTableRows.add(new CarCompany("FORD", 2));
        carTableRows.add(new CarCompany("GENERAL MOTORS", 3));
        carTableRows.add(new CarCompany("OTHER", 4));
        carCompanies.insert(carTableRows);
        
        marqueRows.add(new Marque(4893059, "True", 1246974, "", 3, "UV", "PEUGEOT", "", "Y", 4));
        marqueRows.add(new Marque(4893090, "False", 1246974, "", 1, "UV", "FORD", "", "Y", 2));
        marqueRows.add(new Marque(4893101, "False", 1246974, "", 2, "UV", "HOLDEN", "", "Y", 3));
        marqueRows.add(new Marque(4893112, "False", 1246974, "", 2, "UV", "MITSUBISHI", "", "Y", 4));
        marqueRows.add(new Marque(4893150, "False", 1246974, "", 3, "UV", "SUZUKI", "", "Y", 4));
        marqueRows.add(new Marque(4893263, "False", 1246974, "", 2, "UV", "HONDA", "", "Y", 4));
        marqueRows.add(new Marque(4893353, "False", 1246974, "", 4, "UV", "NISSAN", "", "Y", 4));
        marqueRows.add(new Marque(4893557, "False", 1246974, "", 2, "UV", "SUBARU", "", "Y", 4));
        marqueRows.add(new Marque(4894018, "False", 1246974, "", 2, "UV", "MAZDA", "", "Y", 4));
        marqueRows.add(new Marque(4895203, "False", 1246974, "", 2, "UV", "ROVER", "", "Y", 4));
        marqueRows.add(new Marque(4896300, "False", 1246974, "", 2, "UV", "HYUNDAI", "", "Y", 1));
        marqueRows.add(new Marque(4899527, "False", 1246974, "", 1, "UV", "JEEP", "", "Y", 3));
        marqueRows.add(new Marque(7659280, "False", 1246972, "Y", 3, "", "DAIHATSU", "", "Y", 4));
        marqueRows.add(new Marque(7681544, "False", 1246974, "", 2, "UV", "LANDROVER", "", "Y", 4));
        marqueRows.add(new Marque(7730022, "False", 1246974, "", 2, "UV", "VOLVO", "", "Y", 4));
        marqueRows.add(new Marque(8376505, "False", 1246974, "", 0, "", "ISUZU", "", "Y", 4));
        marqueRows.add(new Marque(8587147, "False", 1246974, "", 0, "", "DAEWOO", "", "Y", 4));
        marqueRows.add(new Marque(9971178, "False", 1246974, "", 1, "", "CHRYSLER", "", "Y", 4));
        marqueRows.add(new Marque(13224369, "False", 1246974, "", 0, "", "VW", "", "Y", 4));
        marqueRows.add(new Marque(6664478, "False", 1246974, "", 0, "", "BMW", "", "Y", 4));
        marqueRows.add(new Marque(1, "False", 1246974, "", 0, "", "TOYOTA", "", "Y", 1));
        marqueRows.add(new Marque(2, "False", 1246974, "", 0, "", "HUMMER", "", "Y", 3));

        marques.insert(marqueRows);
        DBTable.setPrintSQLBeforeExecuting(true);

    }

    @Override
    protected void tearDown() throws Exception {
        myDatabase.dropTable(myMarqueRow);
        myDatabase.dropTable(myCarCompanyRow);


        super.tearDown();
    }
    
        public void testCreateTable() {
        }
}
