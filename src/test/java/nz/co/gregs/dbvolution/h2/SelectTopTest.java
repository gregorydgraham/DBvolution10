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
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.Matchers.*;


public class SelectTopTest extends AbstractTest {
    
    @Test
    public void selectTop2CarCompanies() throws SQLException {
        CarCompany carCompany = new CarCompany();
        DBTable<CarCompany> carCoTable = myDatabase.getDBTable(carCompany);
        carCoTable.setRowLimit(2);
        DBTable<CarCompany> allRows = carCoTable.getAllRows();
        allRows.print();
        Assert.assertThat(allRows.toList().size(), is(2));
    }
    
    @Test
    public void selectTop3CarCompanies() throws SQLException {
        CarCompany carCompany = new CarCompany();
        DBTable<CarCompany> carCoTable = myDatabase.getDBTable(carCompany);
        carCoTable.setRowLimit(3);
        DBTable<CarCompany> allRows = carCoTable.getAllRows();
        allRows.print();
        Assert.assertThat(allRows.toList().size(), is(3));
    }
    
    
    @Test
    public void testClearingTheRowLimit() throws SQLException {
        CarCompany carCompany = new CarCompany();
        DBTable<CarCompany> carCoTable = myDatabase.getDBTable(carCompany);
        carCoTable.setRowLimit(2);
        DBTable<CarCompany> allRows = carCoTable.getAllRows();
        allRows.print();
        carCoTable.clearRowLimit();
        allRows = carCoTable.getAllRows();
        allRows.print();
        Assert.assertThat(allRows.toList().size(), is(4));
    }
    
    @Test
    public void queryTopTest() throws SQLException {
        CarCompany carCompany = new CarCompany();
        DBQuery query = myDatabase.getDBQuery(carCompany, new Marque());
        query.setRowLimit(2);
        List<DBQueryRow> allRows = query.getAllRows();
        query.printAllRows();
        Assert.assertThat(allRows.size(), is(2));
        query.setRowLimit(3);
        allRows = query.getAllRows();
        query.printAllRows();
        Assert.assertThat(allRows.size(), is(3));
        query.clearRowLimit();
        allRows = query.getAllRows();
        query.printAllRows();
        Assert.assertThat(allRows.size(), is(22));
    }
    
    
}
