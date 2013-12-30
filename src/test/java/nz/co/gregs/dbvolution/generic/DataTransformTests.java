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
import java.util.Date;
import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datagenerators.Column;
import nz.co.gregs.dbvolution.datagenerators.Value;
import nz.co.gregs.dbvolution.datatransforms.*;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.operators.DBEqualsOperator;
import nz.co.gregs.dbvolution.operators.DBOperator;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

public class DataTransformTests extends AbstractTest {

    public DataTransformTests(Object testIterationName, Object db) {
        super(testIterationName, db);
    }

    @Test
    public void testTrimTransform() throws SQLException {
        database.setPrintSQLBeforeExecuting(true);
        database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));
        Marque marq = new Marque();
        marq.name.permittedValuesIgnoreCase("HUMMER");
        DBQuery dbQuery = database.getDBQuery(marq);
        List<Marque> got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(1));

        marq.clear();
        dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(
                new Trim(new Column(marq, marq.name)),
                new DBEqualsOperator(new Value("HUMMER"))
        );
        got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(2));
    }

    @Test
    public void testLeftAndRightTrimTransforms() throws SQLException {
        database.setPrintSQLBeforeExecuting(true);
        database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));
        Marque marq = new Marque();
        marq.name.permittedValuesIgnoreCase("HUMMER");
        DBQuery dbQuery = database.getDBQuery(marq);
        List<Marque> got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(1));
        final DBOperator hummerAnyCaseOp = marq.name.getOperator();
        marq.name.clear();

//        marq.name.permittedValuesIgnoreCase(new LeftTrim(), "HUMMER");
        dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(
                new LeftTrim(new Column(marq, marq.name)),
                hummerAnyCaseOp
        );
        got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(1));

//        marq.name.permittedValuesIgnoreCase(new RightTrim(), "HUMMER");
        dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(
                        new RightTrim(new Column(marq, marq.name)),
                        hummerAnyCaseOp
        );
        got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(1));

//        marq.name.permittedValuesIgnoreCase(new LeftTrim(new RightTrim()), "HUMMER");
        dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(
                        new LeftTrim(new RightTrim(new Column(marq, marq.name))),
                        hummerAnyCaseOp
        );
        got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(2));
    }

    @Test
    public void testUpperAndLowercaseTransforms() throws SQLException {
        database.setPrintSQLBeforeExecuting(true);
        Marque marq = new Marque();
        marq.name.permittedValues("hummer");
        DBOperator hummerLowerCaseOp = marq.name.getOperator();
        marq.name.permittedValues("HUMMER");
        final DBOperator hummerUpperCaseOp = marq.name.getOperator();

        DBQuery dbQuery = database.getDBQuery(marq);
        List<Marque> got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(1));

        marq.clear();
        
//        marq.name.permittedValues(new Lowercase(), "HUMMER");
        dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(
                        new Lowercase(new Column(marq, marq.name)),
                        hummerUpperCaseOp
        );
        got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(0));
        
        dbQuery = database.getDBQuery(marq);
//        marq.name.permittedValues(new Lowercase(), "hummer");dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(new Lowercase(new Column(marq, marq.name)), hummerLowerCaseOp);
        got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(1));
        
        dbQuery = database.getDBQuery(marq);
//        marq.name.permittedValues(new Uppercase(), "hummer");dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(new Uppercase(new Column(marq, marq.name)), hummerLowerCaseOp);
        got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(0));
        
        dbQuery = database.getDBQuery(marq);
//        marq.name.permittedValues(new Uppercase(new Lowercase()), "HUMMER");dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(new Uppercase(new Lowercase(new Column(marq, marq.name))), hummerUpperCaseOp);
        got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(1));
    }

    @Test
    public void testSubstringTransform() throws SQLException {
        database.setPrintSQLBeforeExecuting(true);
        Marque marq = new Marque();
        marq.name.permittedValues("HUMMER".substring(3, 6));
        DBOperator last3LettersOfHUMMER = marq.name.getOperator();
        marq.name.permittedValues("HUMMER".substring(0, 3));
        DBOperator first3LettersOfHUMMER = marq.name.getOperator();

        DBQuery dbQuery = database.getDBQuery(marq);
        List<Marque> got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(0));

        marq.name.clear();
        
//        marq.name.permittedValues(new Substring(0,3), "HUMMER".substring(0, 3));dbQuery = database.getDBQuery(marq);
        
        dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(new Substring(new Column(marq, marq.name), 0, 3), first3LettersOfHUMMER);
        got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(1));
        Assert.assertThat(got.get(0).name.stringValue(), is("HUMMER"));

//        marq.name.permittedValues(new Substring(3,6), "HUM");dbQuery = database.getDBQuery(marq);
        
        dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(new Substring(new Column(marq, marq.name), 3, 6), first3LettersOfHUMMER);
        got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(0));

//        marq.name.permittedValues(new Substring(3,6), "HUMMER".substring(3, 6));dbQuery = database.getDBQuery(marq);
        
        dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(new Substring(new Column(marq, marq.name), 3, 6), last3LettersOfHUMMER);
        got = dbQuery.getAllInstancesOf(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(1));

//        marq.name.permittedValues(new Lowercase(marq.name.getLeftHandSide()), "HUMMER".substring(3, 6));dbQuery = database.getDBQuery(marq);
        
        dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(new Lowercase(new Substring(new Column(marq, marq.name), 3, 6)), last3LettersOfHUMMER);
        got = dbQuery.getAllInstancesOf(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(0));

//        marq.name.permittedValues(new Uppercase(marq.name.getLeftHandSide()), "HUMMER".substring(3, 6));dbQuery = database.getDBQuery(marq);
        
        dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(new Uppercase(new Lowercase(new Substring(new Column(marq, marq.name), 3, 6))), last3LettersOfHUMMER);
        got = dbQuery.getAllInstancesOf(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(1));

        database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));
//        marq.name.permittedValues(new Substring(new Uppercase(new Trim()),3,6), "HUMMER".substring(3, 6));dbQuery = database.getDBQuery(marq);
        
        dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(new Substring(new Uppercase(new Trim(new Column(marq, marq.name))), 3, 6), last3LettersOfHUMMER);
        got = dbQuery.getAllInstancesOf(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(2));
    }

    @Test
    public void testStringLengthTransform() throws SQLException {
        database.setPrintSQLBeforeExecuting(true);
        Marque marq = new Marque();
//        marq.name.permittedRangeInclusive(new StringLength(), 1,3);
        marq.name.permittedRangeInclusive(1, 3);
        final DBOperator rangeBetween1And3 = marq.name.getOperator();
        marq.name.clear();
        DBQuery query = database.getDBQuery(marq);
        query.addComparison(new StringLength(new Column(marq, marq.name)), rangeBetween1And3);
        query.setSortOrder(new DBRow[]{marq}, marq.name);
        List<Marque>got = query.getAllInstancesOf(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(2));
        Assert.assertThat(got.get(0).name.stringValue(), is("BMW"));
        Assert.assertThat(got.get(1).name.stringValue(), is("VW"));
        Assert.assertThat(got.size(), is(2));

//        marq.name.permittedRangeInclusive(new StringLength(), "1","3");
        marq.name.permittedRangeInclusive("1", "3");
        final DBOperator rangeFrom1to3 = marq.name.getOperator();
        marq.name.clear();
        query = database.getDBQuery(marq);
        query.addComparison(new StringLength(new Column(marq, marq.name)), rangeFrom1to3);
        query.setSortOrder(new DBRow[]{marq}, marq.name);
        got = query.getAllInstancesOf(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(2));
        Assert.assertThat(got.get(0).name.stringValue(), is("BMW"));
        Assert.assertThat(got.get(1).name.stringValue(), is("VW"));
        Assert.assertThat(got.size(), is(2));
    }

}
