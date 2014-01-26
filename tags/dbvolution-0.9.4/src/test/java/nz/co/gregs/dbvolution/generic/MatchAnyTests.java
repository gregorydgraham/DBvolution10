/*
 * Copyright 2014 greg.
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
package nz.co.gregs.dbvolution.generic;

import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.example.Marque;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.Matchers.*;

/**
 *
 * @author greg
 */
public class MatchAnyTests extends AbstractTest {

    public MatchAnyTests(Object testIterationName, Object db) {
        super(testIterationName, db);
    }

    @Test
    public void testSimpleTableQuery() throws SQLException {
        Marque marq = new Marque();
        marq.name.permittedValuesIgnoreCase("toyota");
        marq.uidMarque.permittedValues(2);
        DBTable<Marque> dbQuery = database.getDBTable(marq);
        List<Marque> marquesFound = dbQuery.getRowsByExample(marq).toList();
        Assert.assertThat(marquesFound.size(), is(0));

        dbQuery.setToMatchAnyCondition();
        marquesFound = dbQuery.getRowsByExample(marq).toList();
        Assert.assertThat(marquesFound.size(), is(2));
    }

    @Test
    public void testSimpleQuery() throws SQLException {
        Marque marq = new Marque();
        marq.name.permittedValuesIgnoreCase("toyota");
        marq.uidMarque.permittedValues(2);
        DBQuery dbQuery = database.getDBQuery(marq);
        List<Marque> marquesFound = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(marquesFound.size(), is(0));

        dbQuery.setToMatchAnyCondition();
        marquesFound = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(marquesFound.size(), is(2));
    }
}
