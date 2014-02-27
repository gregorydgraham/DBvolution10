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
import org.junit.Test;

public class DBReportTests extends AbstractTest {

    public DBReportTests(Object testIterationName, Object db) {
        super(testIterationName, db);
    }

    @Test
    public void createReportTest() throws SQLException {
        SimpleReport reportExample = new SimpleReport();
        List<SimpleReport> simpleReportRows = DBReport.getRows(database, reportExample);
        for (SimpleReport simp : simpleReportRows) {
            System.out.println("" + simp.marque);
            System.out.println("" + simp.carCompany);
            System.out.println("" + simp.carCompanyAndMarque.stringValue());
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
