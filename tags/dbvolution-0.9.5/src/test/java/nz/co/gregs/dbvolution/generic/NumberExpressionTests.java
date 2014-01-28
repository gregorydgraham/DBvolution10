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
import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.operators.DBEqualsOperator;
import nz.co.gregs.dbvolution.operators.DBGreaterThanOperator;
import static org.hamcrest.Matchers.is;
import org.junit.Assert;
import org.junit.Test;

public class NumberExpressionTests extends AbstractTest {

    public NumberExpressionTests(Object testIterationName, Object db) {
        super(testIterationName, db);
    }

    @Test
    public void testSimpleEquation() throws SQLException {
//        database.setPrintSQLBeforeExecuting(true);

        Marque marq = new Marque();
        DBQuery dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(
                marq.column(marq.uidMarque).mod(2),
                new DBEqualsOperator(0));
        List<DBQueryRow> allRows = dbQuery.getAllRows();
//        database.print(allRows);
        Assert.assertThat(allRows.size(), is(11));
        for (Marque marque : dbQuery.getAllInstancesOf(marq)) {
            Assert.assertThat(marque.uidMarque.intValue() % 2, is(0));
        }

    }

    @Test
    public void testAllArithmetic() throws SQLException {
        Marque marq = new Marque();
        DBQuery dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(
                marq.column(marq.uidMarque)
                .plus(2)
                .minus(4)
                .times(6)
                .dividedBy(3)
                .mod(5),
                new DBEqualsOperator(0));
        List<DBQueryRow> allRows = dbQuery.getAllRows();
//        database.print(allRows);
        Assert.assertThat(allRows.size(), is(1));
        Marque marque = allRows.get(0).get(marq);
        Assert.assertThat(marque.uidMarque.intValue(), is(1));
    }

    @Test
    public void testBrackets() throws SQLException {
        Marque marq = new Marque();
        DBQuery dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(
                marq.column(marq.uidMarque).plus(2).minus(4).bracket().times(6).bracket().dividedBy(3),
                new DBEqualsOperator(-2));
        List<DBQueryRow> allRows = dbQuery.getAllRows();
//        database.print(allRows);
        Assert.assertThat(allRows.size(), is(1));
        Marque marque = allRows.get(0).get(marq);
        Assert.assertThat(marque.uidMarque.intValue(), is(1));

        dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(
                marq.column(marq.uidMarque)
                .plus(2).minus(4).bracket()
                .times(6).bracket()
                .dividedBy(3),
                new DBEqualsOperator(-2));
        allRows = dbQuery.getAllRows();
//        database.print(allRows);
        Assert.assertThat(allRows.size(), is(1));
        marque = allRows.get(0).get(marq);
        Assert.assertThat(marque.uidMarque.intValue(), is(1));
    }

    @Test
    public void testACOS() throws SQLException {
        Marque marq = new Marque();
        DBQuery dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(
                marq.column(marq.uidMarque).arccos(),
                new DBEqualsOperator(0));
        List<DBQueryRow> allRows = dbQuery.getAllRows();
//        database.print(allRows);
        Assert.assertThat(allRows.size(), is(1));
        Marque marque = allRows.get(0).get(marq);
        Assert.assertThat(marque.uidMarque.intValue(), is(1));
    }

    @Test
    public void testCOS() throws SQLException {
        Marque marq = new Marque();
        DBQuery dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(
                marq.column(marq.uidMarque).minus(1).cos(),
                new DBEqualsOperator(1));
        List<DBQueryRow> allRows = dbQuery.getAllRows();
//        database.print(allRows);
        Assert.assertThat(allRows.size(), is(1));
        Marque marque = allRows.get(0).get(marq);
        Assert.assertThat(marque.uidMarque.intValue(), is(1));
    }

    @Test
    public void testEXP() throws SQLException {
        CarCompany carCo = new CarCompany();
        DBQuery dbQuery = database.getDBQuery(carCo);
        dbQuery.addComparison(
                //                DBMath.trunc(DBMath.exp(DBMath.column(carCo, carCo.uidCarCompany)).times(1000)),
                carCo.column(carCo.uidCarCompany).exp().times(1000).trunc(),
                new DBEqualsOperator(7389));
        List<DBQueryRow> allRows = dbQuery.getAllRows();
//        database.print(allRows);
        Assert.assertThat(allRows.size(), is(1));
        CarCompany carCompany = allRows.get(0).get(carCo);
        Assert.assertThat(carCompany.uidCarCompany.intValue(), is(2));
    }

    @Test
    public void testDegrees() throws SQLException {
        CarCompany carCo = new CarCompany();
        DBQuery dbQuery = database.getDBQuery(carCo);
        dbQuery.addComparison(
                //                DBMath.tan(DBMath.degrees(DBMath.column(carCo, carCo.uidCarCompany))),
                carCo.column(carCo.uidCarCompany).degrees().tan(),
                new DBGreaterThanOperator(new DBNumber(0)));
        List<DBQueryRow> allRows = dbQuery.getAllRows();
        database.print(allRows);
        Assert.assertThat(allRows.size(), is(2));
        for (CarCompany carCompany : dbQuery.getAllInstancesOf(carCo)) {
            Assert.assertThat(Math.tan(Math.toDegrees(carCompany.uidCarCompany.doubleValue())) > 0,
                    is(true));
        }
    }

    @Test
    public void testRadians() throws SQLException {
        CarCompany carCo = new CarCompany();
        DBQuery dbQuery = database.getDBQuery(carCo);
        dbQuery.addComparison(
                //                DBMath.tan(DBMath.degrees(DBMath.radians(DBMath.degrees(DBMath.column(carCo, carCo.uidCarCompany))))),
                carCo.column(carCo.uidCarCompany).degrees().radians().degrees().tan(),
                new DBGreaterThanOperator(new DBNumber(0)));
        List<DBQueryRow> allRows = dbQuery.getAllRows();
        database.print(allRows);
        Assert.assertThat(allRows.size(), is(2));
        for (CarCompany carCompany : dbQuery.getAllInstancesOf(carCo)) {
            Assert.assertThat(Math.tan(Math.toDegrees(carCompany.uidCarCompany.doubleValue())) > 0,
                    is(true));
        }
    }

    @Test
    public void testLength() throws SQLException {
        CarCompany carCo = new CarCompany();
        carCo.uidCarCompany.permittedValues(carCo.column(carCo.name).length().minus(1));
        DBQuery dbQuery = database.getDBQuery(carCo);
        List<DBQueryRow> allRows = dbQuery.getAllRows();
        database.print(allRows);
        Assert.assertThat(allRows.size(), is(1));
        for (CarCompany carCompany : dbQuery.getAllInstancesOf(carCo)) {
            Assert.assertThat(carCompany.uidCarCompany.intValue(),
                    is(4));
        }
    }

    @Test
    public void testLocationOf() throws SQLException {
        CarCompany carCo = new CarCompany();
        carCo.uidCarCompany.permittedValues(carCo.column(carCo.name).locationOf("ord"));
        DBQuery dbQuery = database.getDBQuery(carCo);
        List<DBQueryRow> allRows = dbQuery.getAllRows();
        database.print(allRows);
        Assert.assertThat(allRows.size(), is(1));
        for (CarCompany carCompany : dbQuery.getAllInstancesOf(carCo)) {
            Assert.assertThat(carCompany.uidCarCompany.intValue(),
                    is(2));
        }
    }
}
