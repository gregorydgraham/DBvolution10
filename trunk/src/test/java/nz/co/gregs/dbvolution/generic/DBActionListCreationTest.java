/*
 * Copyright 2013 greg.
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
import nz.co.gregs.dbvolution.actions.*;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.Matchers.*;

public class DBActionListCreationTest extends AbstractTest {

    public DBActionListCreationTest(Object testIterationName, Object db) {
        super(testIterationName, db);
    }

    @Test
    public void simpleActionCreation() throws SQLException, UnexpectedNumberOfRowsException {
        Marque marqueExample = new Marque();
        marqueExample.getUidMarque().permittedValues(1);
        Marque toyota = marques.getOnlyRowByExample(marqueExample);

        toyota.uidMarque.permittedValues(99999);
        DBActionList updateList = marques.update(toyota);
        Assert.assertThat(updateList.size(), is(1));

        final DBAction firstAction = updateList.get(0);
        final DBActionList revertList = firstAction.getRevertDBActionList();
        Assert.assertThat(firstAction, instanceOf(DBUpdateSimpleTypes.class));
        Assert.assertThat(revertList.size(), is(1));
        Assert.assertThat(revertList.get(0), instanceOf(DBUpdateToPreviousValues.class));
        List<String> revertStrings = revertList.get(0).getSQLStatements(database);
        Assert.assertThat(revertStrings.size(), is(1));
        Assert.assertThat(this.testableSQLWithoutColumnAliases(revertStrings.get(0)),
                is(this.testableSQLWithoutColumnAliases("UPDATE MARQUE SET UID_MARQUE = 1 WHERE UID_MARQUE = 99999;")));
        System.out.println("REVERT:");
        for (String revert : revertStrings) {
            System.out.println(revert);
        }
    }

    @Test
    public void multiActionCreation() throws SQLException, UnexpectedNumberOfRowsException {
        Marque marqueExample = new Marque();
        marqueExample.getUidMarque().permittedValues(1);

        Marque toyota = marques.getOnlyRowByExample(marqueExample);
        toyota.uidMarque.permittedValues(99999);

        marqueExample.clear();
        marqueExample.name.permittedValuesIgnoreCase("ford");
        Marque ford = marques.getOnlyRowByExample(marqueExample);
        ford.updateCount.setValue(ford.updateCount.intValue() + 10);

        DBActionList update = database.update(toyota, ford);
        Assert.assertThat(update.size(), is(2));
        Assert.assertThat(update.get(0), instanceOf(DBUpdateSimpleTypes.class));
        Assert.assertThat(update.get(1), instanceOf(DBUpdateSimpleTypes.class));

    }
}
