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

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import static junit.framework.TestCase.assertEquals;
import nz.co.gregs.dbvolution.example.Marque;

public class DBTableUpdateTest extends AbstractTest {

    public DBTableUpdateTest(String name) {
        super(name);
    }

    public void testInsertRows() throws IllegalArgumentException, IllegalAccessException, IntrospectionException, InvocationTargetException, SQLException, InstantiationException, NoSuchMethodException, ClassNotFoundException {
        Marque myTableRow = new Marque();
        myTableRow.getUidMarque().isLiterally(1);

        marques.getRowsByExample(myTableRow);
        marques.printRows();
        Marque toyota = marques.toList().get(0);
        System.out.println("===" + toyota.name.toString());
        assertEquals("The row retrieved should be TOYOTA", "TOYOTA", toyota.name.toString());

        toyota.name.isLiterally("NOTTOYOTA");
        marques.update(toyota);

        marques.getRowsByExample(myTableRow);
        marques.printRows();
        toyota = marques.toList().get(0);
        assertEquals("The row retrieved should be NOTTOYOTA", "NOTTOYOTA", toyota.name.toString());
    }
}
