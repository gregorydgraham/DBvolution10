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

import nz.co.gregs.dbvolution.transforms.string.LeftTrim;
import nz.co.gregs.dbvolution.transforms.string.StringLength;
import nz.co.gregs.dbvolution.transforms.string.Replace;
import nz.co.gregs.dbvolution.transforms.string.Uppercase;
import nz.co.gregs.dbvolution.transforms.string.Trim;
import nz.co.gregs.dbvolution.transforms.string.Lowercase;
import nz.co.gregs.dbvolution.transforms.string.RightTrim;
import nz.co.gregs.dbvolution.transforms.string.Substring;
import nz.co.gregs.dbvolution.transforms.string.Append;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.variables.StringValue;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.columns.StringColumn;
import nz.co.gregs.dbvolution.variables.StringValue;
import nz.co.gregs.dbvolution.operators.*;
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
                new Trim(new StringColumn(marq, marq.name)),
                new DBEqualsOperator("HUMMER")
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
        
        marq.name.clear();

        dbQuery = database.getDBQuery(marq);
        final DBOperator hummerAnyCaseOp = new DBPermittedValuesIgnoreCaseOperator("HUMMER");
        dbQuery.addComparison(
                new LeftTrim(marq.column(marq.name)),
                hummerAnyCaseOp
        );
        got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(1));

//        marq.name.permittedValuesIgnoreCase(new RightTrim(), "HUMMER");
        dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(
                new RightTrim(marq.column(marq.name)),
                hummerAnyCaseOp
        );
        got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(1));

//        marq.name.permittedValuesIgnoreCase(new LeftTrim(new RightTrim()), "HUMMER");
        dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(
                new LeftTrim(new RightTrim(marq.column(marq.name))),
                hummerAnyCaseOp
        );
        got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(2));
    }

    @Test
    public void testUpperAndLowercaseTransforms() throws SQLException {
        database.setPrintSQLBeforeExecuting(true);
        Marque marq = new Marque();

        marq.name.permittedValues("HUMMER");
        DBQuery dbQuery = database.getDBQuery(marq);
        List<Marque> got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(1));

        marq.clear();

        DBOperator hummerLowerCaseOp = new DBPermittedValuesOperator("hummer");
        final DBOperator hummerUpperCaseOp = new DBPermittedValuesOperator("HUMMER");
        
        dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(
                new Lowercase(marq.column(marq.name)),
                hummerUpperCaseOp
        );
        got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(0));

        dbQuery = database.getDBQuery(marq);
//        marq.name.permittedValues(new Lowercase(), "hummer");dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(new Lowercase(marq.column(marq.name)), hummerLowerCaseOp);
        got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(1));

        dbQuery = database.getDBQuery(marq);
//        marq.name.permittedValues(new Uppercase(), "hummer");dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(new Uppercase(marq.column(marq.name)), hummerLowerCaseOp);
        got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(0));

        dbQuery = database.getDBQuery(marq);
//        marq.name.permittedValues(new Uppercase(new Lowercase()), "HUMMER");dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(new Uppercase(new Lowercase(marq.column(marq.name))), hummerUpperCaseOp);
        got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(1));
    }

    @Test
    public void testSubstringTransform() throws SQLException {
        database.setPrintSQLBeforeExecuting(true);
        Marque marq = new Marque();
        marq.name.permittedValues("HUMMER".substring(0, 3));
        DBQuery dbQuery = database.getDBQuery(marq);
        List<Marque> got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(0));

        marq.name.clear();
        
        DBOperator last3LettersOfHUMMER = new DBPermittedValuesOperator("HUMMER".substring(3, 6));
        DBOperator first3LettersOfHUMMER = new DBPermittedValuesOperator("HUMMER".substring(0, 3));

        dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(new Substring(marq.column(marq.name), 0, 3), first3LettersOfHUMMER);
        got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(1));
        Assert.assertThat(got.get(0).name.stringValue(), is("HUMMER"));

//        marq.name.permittedValues(new Substring(3,6), "HUM");dbQuery = database.getDBQuery(marq);
        dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(new Substring(marq.column(marq.name), 3, 6), first3LettersOfHUMMER);
        got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(0));

//        marq.name.permittedValues(new Substring(3,6), "HUMMER".substring(3, 6));dbQuery = database.getDBQuery(marq);
        dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(new Substring(marq.column(marq.name), 3, 6), last3LettersOfHUMMER);
        got = dbQuery.getAllInstancesOf(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(1));

//        marq.name.permittedValues(new Lowercase(marq.name.getLeftHandSide()), "HUMMER".substring(3, 6));dbQuery = database.getDBQuery(marq);
        dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(new Lowercase(new Substring(marq.column(marq.name), 3, 6)), last3LettersOfHUMMER);
        got = dbQuery.getAllInstancesOf(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(0));

//        marq.name.permittedValues(new Uppercase(marq.name.getLeftHandSide()), "HUMMER".substring(3, 6));dbQuery = database.getDBQuery(marq);
        dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(new Uppercase(new Lowercase(new Substring(marq.column(marq.name), 3, 6))), last3LettersOfHUMMER);
        got = dbQuery.getAllInstancesOf(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(1));

        database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));
//        marq.name.permittedValues(new Substring(new Uppercase(new Trim()),3,6), "HUMMER".substring(3, 6));dbQuery = database.getDBQuery(marq);

        dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(new Substring(new Uppercase(new Trim(marq.column(marq.name))), 3, 6), last3LettersOfHUMMER);
        got = dbQuery.getAllInstancesOf(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(2));
    }

    @Test
    public void testStringLengthTransform() throws SQLException {
        database.setPrintSQLBeforeExecuting(true);
        Marque marq = new Marque();
        marq.name.clear();
        DBQuery query = database.getDBQuery(marq);
        
        final DBOperator rangeBetween1And3 = new DBPermittedRangeInclusiveOperator(1,3);
        query.addComparison(new StringLength(marq.column(marq.name)), rangeBetween1And3);
        query.setSortOrder(marq.column(marq.name));
        List<Marque> got = query.getAllInstancesOf(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(2));
        Assert.assertThat(got.get(0).name.stringValue(), is("BMW"));
        Assert.assertThat(got.get(1).name.stringValue(), is("VW"));

        final DBOperator rangeFrom1to3 = new DBPermittedRangeInclusiveOperator("1", "3");
        marq.name.clear();
        query = database.getDBQuery(marq);
        query.addComparison(new StringLength(marq.column(marq.name)), rangeFrom1to3);
        query.setSortOrder(marq.column(marq.name));
        got = query.getAllInstancesOf(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(2));
        Assert.assertThat(got.get(0).name.stringValue(), is("BMW"));
        Assert.assertThat(got.get(1).name.stringValue(), is("VW"));
    }

    @Test
    public void testReplaceTransform() throws SQLException {
        database.setPrintSQLBeforeExecuting(true);
        Marque marq = new Marque();
        DBQuery query = database.getDBQuery(marq);
        final StringColumn nameColumn = marq.column(marq.name);
        
        query.addComparison(nameColumn, new DBPermittedValuesOperator("TOY"));
        List<Marque> got = query.getAllInstancesOf(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(0));

        query = database.getDBQuery(marq);
        query.addComparison(new Replace(nameColumn,"OTA",""), new DBPermittedValuesOperator("TOY"));
        query.setSortOrder(marq.column(marq.name));
        got = query.getAllInstancesOf(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(1));
        Assert.assertThat(got.get(0).name.stringValue(), is("TOYOTA"));
        
        query = database.getDBQuery(marq);
        query.addComparison(new Replace(nameColumn,"BM","V"), new DBPermittedValuesOperator("VW"));
        query.setSortOrder(marq.column(marq.name));
        got = query.getAllInstancesOf(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(2));
        Assert.assertThat(got.get(0).name.stringValue(), is("BMW"));
        Assert.assertThat(got.get(1).name.stringValue(), is("VW"));
        
        // A rather compilicated way to find out how many marques start with V
        query = database.getDBQuery(marq);
        query.addComparison(new Replace(nameColumn, new Substring(nameColumn,1),new StringValue("")), new DBPermittedValuesOperator("V"));
        query.setSortOrder(marq.column(marq.name));
        got = query.getAllInstancesOf(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(2));
        Assert.assertThat(got.get(0).name.stringValue(), is("VOLVO"));
        Assert.assertThat(got.get(1).name.stringValue(), is("VW"));
    }

    @Test
    public void testConcatTransform() throws SQLException {
        database.setPrintSQLBeforeExecuting(true);
        Marque marq = new Marque();
        DBQuery query;
        List<Marque> got;
        final StringColumn nameColumn = marq.column(marq.name);
        
        query = database.getDBQuery(marq);
        query.addComparison(new Append(new Replace(new Replace(nameColumn,"BM","V"),"W",""),"W"), new DBPermittedValuesOperator("VW"));
        query.setSortOrder(marq.column(marq.name));
        got = query.getAllInstancesOf(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(2));
        Assert.assertThat(got.get(0).name.stringValue(), is("BMW"));
        Assert.assertThat(got.get(1).name.stringValue(), is("VW"));
        
        query = database.getDBQuery(marq);
        query.addComparison(new StringLength(nameColumn), new DBPermittedValuesOperator(6));
        query.addComparison(new Append(new Substring(nameColumn, 3,6), new Substring(nameColumn, 0,3)),  new DBPermittedValuesOperator("OTATOY"));
        query.setSortOrder(marq.column(marq.name));
        got = query.getAllInstancesOf(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(1));
        Assert.assertThat(got.get(0).name.stringValue(), is("TOYOTA"));
    }
}
