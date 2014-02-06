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
package nz.co.gregs.dbvolution;

import java.sql.SQLException;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.Matchers.*;

public class DBTableUpdateTest extends AbstractTest {

    public DBTableUpdateTest(Object testIterationName, Object db) {
        super(testIterationName, db);
    }

    @Test
    public void changingPrimaryKey() throws SQLException, UnexpectedNumberOfRowsException {
        Marque marqueExample = new Marque();
        marqueExample.getUidMarque().permittedValues(1);

        marques.getRowsByExample(marqueExample);
        marques.print();
        Marque toyota = marques.getOnlyRowByExample(marqueExample);
        toyota.uidMarque.setValue(99999);
        DBActionList updateList = marques.update(toyota);
        Assert.assertThat(testableSQL(updateList.get(0).getSQLStatements(database).get(0)),
                is(testableSQL("UPDATE MARQUE SET UID_MARQUE = 99999 WHERE UID_MARQUE = 1;")));
        marques.update(toyota);
        toyota.name.setValue("NOTOYOTA");
        Assert.assertThat(testableSQL(marques.update(toyota).get(0).getSQLStatements(database).get(0)), 
                is(testableSQL("UPDATE MARQUE SET NAME = 'NOTOYOTA' WHERE UID_MARQUE = 99999;")));
        
        marqueExample = new Marque();
        marqueExample.uidMarque.permittedValuesIgnoreCase("99999");
        toyota = marques.getOnlyRowByExample(marqueExample);
        Assert.assertThat(toyota.name.toString(), is("NOTOYOTA"));
    }
    
    @Test
    public void testInsertRows() throws SQLException {
        Marque myTableRow = new Marque();
        myTableRow.getUidMarque().permittedValues(1);

        marques.getRowsByExample(myTableRow);
        marques.print();
        Marque toyota = marques.toList().get(0);
        System.out.println("===" + toyota.name.toString());
        Assert.assertEquals("The row retrieved should be TOYOTA", "TOYOTA", toyota.name.toString());

        toyota.name.setValue("NOTTOYOTA");
        String sqlForUpdate = marques.update(toyota).get(0).getSQLStatements(database).get(0);
        Assert.assertEquals("Update statement doesn't look right:", testableSQL("UPDATE MARQUE SET NAME = 'NOTTOYOTA' WHERE UID_MARQUE = 1;"), testableSQL(sqlForUpdate));

        marques.update(toyota);
        
        marques.getRowsByExample(myTableRow);
        marques.print();
        toyota = marques.toList().get(0);
        Assert.assertEquals("The row retrieved should be NOTTOYOTA", "NOTTOYOTA", toyota.name.toString());
    }
}
