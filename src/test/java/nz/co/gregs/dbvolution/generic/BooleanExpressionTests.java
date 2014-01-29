/*
 * Copyright 2014 gregory.graham.
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
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.example.Marque;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author gregory.graham
 */

public class BooleanExpressionTests extends AbstractTest {

    public BooleanExpressionTests(Object testIterationName, Object db) {
        super(testIterationName, db);
    }

    @Test
    public void testStringLike() throws SQLException {
        Marque marque = new Marque();
        DBQuery dbQuery = database.getDBQuery(marque);
        
        dbQuery.addCondition(marque.column(marque.name).isLike("TOY%"));
        
        List<DBQueryRow> allRows = dbQuery.getAllRows();
        database.print(allRows);
        Assert.assertThat(allRows.size(), is(1));
    }
    
}