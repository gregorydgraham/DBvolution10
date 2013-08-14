/*
 * Copyright 2013 gregory.graham.
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
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author gregory.graham
 */
public class IgnoreForeignKeyTest extends AbstractTest {

    @Test
    public void testIgnoreForeignKey() throws SQLException{
        Marque marque = new Marque();
        CarCompany carCompany = new CarCompany();
        DBQuery dbQuery = myDatabase.getDBQuery(carCompany, marque);
        List<DBQueryRow> allRows = dbQuery.getAllRows();
        dbQuery.print();
        Assert.assertTrue("Number of rows should be 22", allRows.size() == 22);

        marque.ignoreForeignKey(marque.getCarCompany());
        dbQuery = myDatabase.getDBQuery(carCompany, marque);
        allRows = dbQuery.getAllRows();
        dbQuery.print();
        Assert.assertTrue("Number of rows should be 88", allRows.size() == 88);

        marque.useAllForeignKeys();
        dbQuery = myDatabase.getDBQuery(carCompany, marque);
        allRows = dbQuery.getAllRows();
        dbQuery.print();
        Assert.assertTrue("Number of rows should be 88", allRows.size() == 22);

        marque.ignoreAllForeignKeys();
        dbQuery = myDatabase.getDBQuery(carCompany, marque);
        allRows = dbQuery.getAllRows();
        dbQuery.print();
        Assert.assertTrue("Number of rows should be 88", allRows.size() == 88);

    }
}
