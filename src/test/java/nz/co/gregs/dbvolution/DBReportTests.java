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
package nz.co.gregs.dbvolution;

import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

public class DBReportTests extends AbstractTest {

    public DBReportTests(Object testIterationName, Object db) {
        super(testIterationName, db);
    }

    @Test
    public void createReportTest() throws SQLException {
        SimpleReport reportExample = new SimpleReport();
        List<SimpleReport> simpleReportRows = DBReport.getAllRows(database, reportExample);
        Assert.assertThat(simpleReportRows.size(), is(22));
        for (SimpleReport simp : simpleReportRows) {
            Assert.assertThat(simp.marque.uidMarque.stringValue(), not(isEmptyOrNullString()));
            Assert.assertThat(simp.carCompany.uidCarCompany.stringValue(), not(isEmptyOrNullString()));
            Assert.assertThat(simp.carCompanyAndMarque.stringValue(), not(isEmptyOrNullString()));
            System.out.println("" + simp.marque);
            System.out.println("" + simp.carCompany);
            System.out.println("" + simp.carCompanyAndMarque.stringValue());
        }
    }

    @Test
    public void withExampleTest() throws SQLException {
        SimpleReport reportExample = new SimpleReport();
        Marque toyota = new Marque();
        toyota.name.permittedValuesIgnoreCase("TOYOTA");
        List<SimpleReport> simpleReportRows = DBReport.getRows(database, reportExample, toyota);
        Assert.assertThat(simpleReportRows.size(), is(1));
        for (SimpleReport simp : simpleReportRows) {
            Assert.assertThat(simp.marque.uidMarque.stringValue(), not(isEmptyOrNullString()));
            Assert.assertThat(simp.carCompany.uidCarCompany.stringValue(), not(isEmptyOrNullString()));
            Assert.assertThat(simp.carCompanyAndMarque.stringValue(), not(isEmptyOrNullString()));
            System.out.println("" + simp.marque);
            System.out.println("" + simp.carCompany);
            System.out.println("" + simp.carCompanyAndMarque.stringValue());            
            Assert.assertThat(simp.marque.name.stringValue(), is("TOYOTA"));
            Assert.assertThat(simp.carCompany.name.stringValue(), is("TOYOTA"));
            Assert.assertThat(simp.carCompanyAndMarque.stringValue(), is("TOYOTA: TOYOTA"));
        }
    }

    public static class SimpleReport extends DBReport {

        public Marque marque = new Marque();
        public CarCompany carCompany = new CarCompany();

        @DBColumn
        public DBString carCompanyAndMarque = new DBString(carCompany.column(carCompany.name).append(": ").append(marque.column(marque.name)));

        public SimpleReport() {
            super();
        }
    }
}
