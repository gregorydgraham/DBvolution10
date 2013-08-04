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
import java.util.Date;
import java.util.List;
import net.sourceforge.tedhi.FlexibleDateFormat;
import net.sourceforge.tedhi.FlexibleDateRangeFormat;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.H2DB;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import org.junit.After;
import org.junit.Before;

/**
 *
 * @author gregorygraham
 */
public class AbstractTest {

    DBDatabase myDatabase = new H2DB("jdbc:h2:~/dbvolutionTest", "", "");
    Marque myMarqueRow = new Marque();
    CarCompany myCarCompanyRow = new CarCompany();
    DBTable<Marque> marques;
    DBTable<CarCompany> carCompanies;
    public List<Marque> marqueRows = new ArrayList<Marque>();
    public List<CarCompany> carTableRows = new ArrayList<CarCompany>();
    
    static final FlexibleDateFormat tedhiFormat = FlexibleDateFormat.getPatternInstance("dd/M/yyyy");
    static final FlexibleDateRangeFormat tedhiRangeFormat = FlexibleDateRangeFormat.getPatternInstance("dd/M/yyyy");
    public String firstDateStr = "23 March 2013";
    public String secondDateStr = "2 April 2013";

    @Before
    @SuppressWarnings("empty-statement")
    public void setUp() throws Exception {

        myDatabase.setPrintSQLBeforeExecuting(false);
        myDatabase.dropTableNoExceptions(new Marque());
        myDatabase.createTable(myMarqueRow);

        myDatabase.dropTableNoExceptions(myCarCompanyRow);
        myDatabase.createTable(myCarCompanyRow);

        DBTable.setPrintSQLBeforeExecuting(false);
        marques = DBTable.getInstance(myDatabase, myMarqueRow);
        carCompanies = DBTable.getInstance(myDatabase, myCarCompanyRow);
        carTableRows.add(new CarCompany("TOYOTA", 1));
        carTableRows.add(new CarCompany("FORD", 2));
        carTableRows.add(new CarCompany("GENERAL MOTORS", 3));
        carTableRows.add(new CarCompany("OTHER", 4));
        carCompanies.insert(carTableRows);

        Date firstDate = tedhiFormat.parse(firstDateStr);
        Date secondDate = tedhiFormat.parse(secondDateStr);


        marqueRows.add(new Marque(4893059, "True", 1246974, null, 3, "UV", "PEUGEOT", null, "Y", null, 4));
        marqueRows.add(new Marque(4893090, "False", 1246974, "", 1, "UV", "FORD", "", "Y", firstDate, 2));
        marqueRows.add(new Marque(4893101, "False", 1246974, "", 2, "UV", "HOLDEN", "", "Y", firstDate, 3));
        marqueRows.add(new Marque(4893112, "False", 1246974, "", 2, "UV", "MITSUBISHI", "", "Y", firstDate, 4));
        marqueRows.add(new Marque(4893150, "False", 1246974, "", 3, "UV", "SUZUKI", "", "Y", firstDate, 4));
        marqueRows.add(new Marque(4893263, "False", 1246974, "", 2, "UV", "HONDA", "", "Y", firstDate, 4));
        marqueRows.add(new Marque(4893353, "False", 1246974, "", 4, "UV", "NISSAN", "", "Y", firstDate, 4));
        marqueRows.add(new Marque(4893557, "False", 1246974, "", 2, "UV", "SUBARU", "", "Y", firstDate, 4));
        marqueRows.add(new Marque(4894018, "False", 1246974, "", 2, "UV", "MAZDA", "", "Y", firstDate, 4));
        marqueRows.add(new Marque(4895203, "False", 1246974, "", 2, "UV", "ROVER", "", "Y", firstDate, 4));
        marqueRows.add(new Marque(4896300, "False", 1246974, null, 2, "UV", "HYUNDAI", null, "Y", firstDate, 1));
        marqueRows.add(new Marque(4899527, "False", 1246974, "", 1, "UV", "JEEP", "", "Y", firstDate, 3));
        marqueRows.add(new Marque(7659280, "False", 1246972, "Y", 3, "", "DAIHATSU", "", "Y", firstDate, 4));
        marqueRows.add(new Marque(7681544, "False", 1246974, "", 2, "UV", "LANDROVER", "", "Y", firstDate, 4));
        marqueRows.add(new Marque(7730022, "False", 1246974, "", 2, "UV", "VOLVO", "", "Y", firstDate, 4));
        marqueRows.add(new Marque(8376505, "False", 1246974, "", 0, "", "ISUZU", "", "Y", firstDate, 4));
        marqueRows.add(new Marque(8587147, "False", 1246974, "", 0, "", "DAEWOO", "", "Y", firstDate, 4));
        marqueRows.add(new Marque(9971178, "False", 1246974, "", 1, "", "CHRYSLER", "", "Y", firstDate, 4));
        marqueRows.add(new Marque(13224369, "False", 1246974, "", 0, "", "VW", "", "Y", secondDate, 4));
        marqueRows.add(new Marque(6664478, "False", 1246974, "", 0, "", "BMW", "", "Y", secondDate, 4));
        marqueRows.add(new Marque(1, "False", 1246974, "", 0, "", "TOYOTA", "", "Y", firstDate, 1));
        marqueRows.add(new Marque(2, "False", 1246974, "", 0, "", "HUMMER", "", "Y", secondDate, 3));

        marques.insert(marqueRows);
        DBTable.setPrintSQLBeforeExecuting(true);
        myDatabase.setPrintSQLBeforeExecuting(true);

    }

    @After
    public void tearDown() throws Exception {
        myDatabase.setPrintSQLBeforeExecuting(false);
        myDatabase.dropTable(myMarqueRow);
        myDatabase.dropTable(myCarCompanyRow);
    }
}
