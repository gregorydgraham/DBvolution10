/*
 * Copyright 2013 Gregory Graham.
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

import static org.hamcrest.Matchers.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Gregory Graham
 */
public class DBTableDeleteTest extends AbstractTest {

    public DBTableDeleteTest(Object testIterationName, Object db) {
        super(testIterationName, db);
    }

    @Test
    public void testDeleteListOfRows() throws SQLException {
        marques.setBlankQueryAllowed(true).getAllRows();
        List<Marque> rowList = marques.toList();
        int originalSize = rowList.size();
        System.out.println("rowList.size()==" + rowList.size());
        ArrayList<Marque> deleteList = new ArrayList<Marque>();
        for (Marque row : rowList) {
            if (row.getIsUsedForTAFROs().toString().equals("False")) {
                deleteList.add(row);
            }
        }
        marques.delete(deleteList);
        marques.getAllRows();
        System.out.println("rowList.size()==" + marques.toList().size());
        //assertThat("All 'False' rows have not been deleted", originalSize - deleteList.size() == marques.toList().size());
        Assert.assertThat(originalSize - deleteList.size(), is(marques.toList().size()));

    }

    @Test
    public void testDeleteByExample() throws SQLException {
        List<Marque> beforeList = marques.setBlankQueryAllowed(true).getAllRows().toList();
        System.out.println("rowList.size()==" + marques.toList().size());
        Marque marq = new Marque();
        marq.name.permittedValues("PEUGEOT", "HUMMER");
        marques.delete(marq);
        List<Marque> afterList = marques.getAllRows().toList();
        System.out.println("rowList.size()==" + marques.toList().size());
        //assertThat("All 'False' rows have not been deleted", originalSize - deleteList.size() == marques.toList().size());
        Assert.assertThat(beforeList.size(), is(afterList.size() + 2));

    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDeleteByExampleUsingList() throws SQLException {
        List<Marque> beforeList = marques.setBlankQueryAllowed(true).getAllRows().toList();
        System.out.println("rowList.size()==" + marques.toList().size());
        Marque marq = new Marque();
        ArrayList<String> arrayList = new ArrayList<String>();
        arrayList.add("PEUGEOT");
        arrayList.add("HUMMER");
        marq.name.permittedValues(arrayList);
        marques.delete(marq);
        List<Marque> afterList = marques.getAllRows().toList();
        System.out.println("rowList.size()==" + marques.toList().size());
        //assertThat("All 'False' rows have not been deleted", originalSize - deleteList.size() == marques.toList().size());
        Assert.assertThat(beforeList.size(), is(afterList.size() + 2));

    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDeleteByExampleUsingSet() throws SQLException {
        List<Marque> beforeList = marques.setBlankQueryAllowed(true).getAllRows().toList();
        System.out.println("rowList.size()==" + marques.toList().size());
        Marque marq = new Marque();
        HashSet<String> hashSet = new HashSet<String>();
        hashSet.add("PEUGEOT");
        hashSet.add("HUMMER");
        marq.name.permittedValues(hashSet);
        marques.delete(marq);
        List<Marque> afterList = marques.getAllRows().toList();
        System.out.println("rowList.size()==" + marques.toList().size());
        //assertThat("All 'False' rows have not been deleted", originalSize - deleteList.size() == marques.toList().size());
        Assert.assertThat(beforeList.size(), is(afterList.size() + 2));

    }
}
