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
package nz.co.gregs.dbvolution.generic;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import nz.co.gregs.dbvolution.example.Marque;
import org.junit.Test;

/**
 *
 * @author gregory.graham
 */
public class DBTableInsertTest extends AbstractTest {

    Marque myTableRow = new Marque();

    public DBTableInsertTest(Object db) {
        super(db);
    }

    @Test
    public void testInsertRows() throws SQLException {
        myTableRow.getUidMarque().permittedValues(999);
        myTableRow.getName().permittedValues("TOYOTA");
        myTableRow.getNumericCode().permittedValues(10);
        marques.insert(myTableRow);
        marques.getAllRows();
        marques.print();

        Date creationDate = new Date();
        List<Marque> myTableRows = new ArrayList<Marque>();
        myTableRows.add(new Marque(3, "False", 1246974, "", 3, "UV", "TVR", "", "Y", creationDate, 4, null));

        marques.insert(myTableRows);
        marques.getAllRows();
        marques.print();
    }
}
