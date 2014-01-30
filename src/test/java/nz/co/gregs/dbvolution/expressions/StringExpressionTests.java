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
package nz.co.gregs.dbvolution.expressions;

import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.columns.StringColumn;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import nz.co.gregs.dbvolution.operators.*;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

public class StringExpressionTests extends AbstractTest {

    public StringExpressionTests(Object testIterationName, Object db) {
        super(testIterationName, db);
    }

    @Test
    public void testUserFunctions() throws SQLException {
//        database.setPrintSQLBeforeExecuting(true);
        Marque marq = new Marque();
        marq.name.permittedValues(StringExpression.currentUser());
        DBQuery query = database.getDBQuery(marq);
        List<Marque> got = query.getAllInstancesOf(marq);
        database.print(got);
        System.out.println(query.getSQLForQuery());
        Assert.assertThat(got.size(), is(0));
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
                marq.column(marq.name).trim(),
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
                marq.column(marq.name).leftTrim(),
                hummerAnyCaseOp
        );
        got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(1));

//        marq.name.permittedValuesIgnoreCase(new RightTrim(), "HUMMER");
        dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(
                marq.column(marq.name).rightTrim(),
                hummerAnyCaseOp
        );
        got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(1));

//        marq.name.permittedValuesIgnoreCase(new LeftTrim(new RightTrim()), "HUMMER");
        dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(
                marq.column(marq.name).rightTrim().leftTrim(),
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
                marq.column(marq.name).lowercase(),
                hummerUpperCaseOp
        );
        got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(0));

        dbQuery = database.getDBQuery(marq);
//        marq.name.permittedValues(new Lowercase(), "hummer");dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(marq.column(marq.name).lowercase(), hummerLowerCaseOp);
        got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(1));

        dbQuery = database.getDBQuery(marq);
//        marq.name.permittedValues(new Uppercase(), "hummer");dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(marq.column(marq.name).uppercase(), hummerLowerCaseOp);
        got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(0));

        dbQuery = database.getDBQuery(marq);
//        marq.name.permittedValues(new Uppercase(new Lowercase()), "HUMMER");dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(marq.column(marq.name).lowercase().uppercase(), hummerUpperCaseOp);
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
        dbQuery.addComparison(marq.column(marq.name).substring(0, 3), first3LettersOfHUMMER);
        got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(1));
        Assert.assertThat(got.get(0).name.stringValue(), is("HUMMER"));

        dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(marq.column(marq.name).substring(3, 6), first3LettersOfHUMMER);
        got = dbQuery.getAllInstancesOf(marq);
        Assert.assertThat(got.size(), is(0));

        dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(marq.column(marq.name).substring(3, 6), last3LettersOfHUMMER);
        got = dbQuery.getAllInstancesOf(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(1));

        dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(marq.column(marq.name).substring(3, 6).lowercase(), last3LettersOfHUMMER);
        got = dbQuery.getAllInstancesOf(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(0));

        dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(marq.column(marq.name).substring(3, 6).lowercase().uppercase(), last3LettersOfHUMMER);
        got = dbQuery.getAllInstancesOf(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(1));

        database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));

        dbQuery = database.getDBQuery(marq);
        dbQuery.addComparison(marq.column(marq.name).trim().uppercase().substring(3, 6), last3LettersOfHUMMER);
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
        query.addComparison(marq.column(marq.name).length(), rangeBetween1And3);
        query.setSortOrder(marq.column(marq.name));
        List<Marque> got = query.getAllInstancesOf(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(2));
        Assert.assertThat(got.get(0).name.stringValue(), is("BMW"));
        Assert.assertThat(got.get(1).name.stringValue(), is("VW"));

        final DBOperator rangeFrom1to3 = new DBPermittedRangeInclusiveOperator("1", "3");
        marq.name.clear();
        query = database.getDBQuery(marq);
        query.addComparison(marq.column(marq.name).length(), rangeFrom1to3);
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
        final StringExpression nameValue = nameColumn;
        
        query.addComparison(nameValue, new DBPermittedValuesOperator("TOY"));
        List<Marque> got = query.getAllInstancesOf(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(0));

        query = database.getDBQuery(marq);
        query.addComparison(nameValue.replace("OTA",""), new DBPermittedValuesOperator("TOY"));
        query.setSortOrder(nameColumn);
        got = query.getAllInstancesOf(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(1));
        Assert.assertThat(got.get(0).name.stringValue(), is("TOYOTA"));
        
        query = database.getDBQuery(marq);
        query.addComparison(nameValue.replace("BM","V"), new DBPermittedValuesOperator("VW"));
        query.setSortOrder(nameColumn);
        got = query.getAllInstancesOf(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(2));
        Assert.assertThat(got.get(0).name.stringValue(), is("BMW"));
        Assert.assertThat(got.get(1).name.stringValue(), is("VW"));
        
        // A rather compilicated way to find out how many marques start with V
        query = database.getDBQuery(marq);
        System.out.println("VALUE OF DBInteger(1): "+(new DBInteger(1)).toSQLString(database));
        query.addComparison(nameValue.replace(nameValue.substring(1),StringExpression.value("")), new DBPermittedValuesOperator("V"));
        query.setSortOrder(nameColumn);
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
        final StringExpression nameValue = nameColumn;
        
        query = database.getDBQuery(marq);
        // Find VW and BMW by appending V and W around the replaced brands
        query.addComparison(StringExpression.value("V").append(nameValue.replace("BMW","").replace("VW", "")).append("W"), new DBPermittedValuesOperator("VW"));
        query.setSortOrder(nameColumn);
        got = query.getAllInstancesOf(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(2));
        Assert.assertThat(got.get(0).name.stringValue(), is("BMW"));
        Assert.assertThat(got.get(1).name.stringValue(), is("VW"));
        
        query = database.getDBQuery(marq);
        query.addComparison(nameValue.length(), new DBPermittedValuesOperator(6));
        query.addComparison(nameValue.substring(3,6).append(nameValue.substring(0,3)),  new DBPermittedValuesOperator("OTATOY"));
        query.setSortOrder(nameColumn);
        System.out.println(query.getSQLForQuery());
        got = query.getAllInstancesOf(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(1));
        Assert.assertThat(got.get(0).name.stringValue(), is("TOYOTA"));
    }
}
