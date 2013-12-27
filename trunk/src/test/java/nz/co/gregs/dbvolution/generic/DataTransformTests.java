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
import nz.co.gregs.dbvolution.datatransforms.*;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.operators.DBOperator;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;


public class DataTransformTests extends AbstractTest {

    public DataTransformTests(Object testIterationName, Object db) {
        super(testIterationName, db);
    }
    
    @Test
    public void testTrimTransform() throws SQLException{
        database.setPrintSQLBeforeExecuting(true);
        database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));
        Marque marq = new Marque();
        marq.name.permittedValuesIgnoreCase("HUMMER");
        List<Marque> got = database.get(marq);
        Assert.assertThat(got.size(), is(1));
        
        marq.name.permittedComparison(
                new DBDataComparison(
                        new TrimTransform(), 
                        marq.name.getOperator()
                ));
        got = database.get(marq);
        Assert.assertThat(got.size(), is(2));
    }
    
    @Test
    public void testLeftAndRightTrimTransforms() throws SQLException{
        database.setPrintSQLBeforeExecuting(true);
        database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));
        Marque marq = new Marque();
        marq.name.permittedValuesIgnoreCase("HUMMER");
        List<Marque> got = database.get(marq);
        Assert.assertThat(got.size(), is(1));
        final DBOperator hummerAnyCaseOp = marq.name.getOperator();
        
//        marq.name.permittedValuesIgnoreCase(new LeftTrimTransform(), "HUMMER");
        marq.name.permittedComparison(
                new DBDataComparison(
                        new LeftTrimTransform(), 
                        hummerAnyCaseOp)
        );
        got = database.get(marq);
        Assert.assertThat(got.size(), is(1));

//        marq.name.permittedValuesIgnoreCase(new RightTrimTransform(), "HUMMER");
        marq.name.permittedComparison(
                new DBDataComparison(
                        new RightTrimTransform(), 
                        hummerAnyCaseOp)
        );
        got = database.get(marq);
        Assert.assertThat(got.size(), is(1));

//        marq.name.permittedValuesIgnoreCase(new LeftTrimTransform(new RightTrimTransform()), "HUMMER");
        marq.name.permittedComparison(
                new DBDataComparison(
                        new LeftTrimTransform(new RightTrimTransform()), 
                        hummerAnyCaseOp)
        );
        got = database.get(marq);
        Assert.assertThat(got.size(), is(2));
    }
    
    @Test
    public void testUpperAndLowercaseTransforms() throws SQLException{
        database.setPrintSQLBeforeExecuting(true);
        Marque marq = new Marque();
        marq.name.permittedValues("hummer");
        DBOperator hummerLowerCaseOp = marq.name.getOperator();
        marq.name.permittedValues("HUMMER");
        final DBOperator hummerUpperCaseOp = marq.name.getOperator();
        
        List<Marque> got = database.get(marq);
        Assert.assertThat(got.size(), is(1));
        
//        marq.name.permittedValues(new LowercaseTransform(), "HUMMER");
        marq.name.permittedComparison(
                new DBDataComparison(
                        new LowercaseTransform(), 
                        hummerUpperCaseOp
                )
        );
        got = database.get(marq);
        Assert.assertThat(got.size(), is(0));
        
//        marq.name.permittedValues(new LowercaseTransform(), "hummer");
        marq.name.permittedComparison(new DBDataComparison(new LowercaseTransform(), hummerLowerCaseOp));
        got = database.get(marq);
        Assert.assertThat(got.size(), is(1));

//        marq.name.permittedValues(new UppercaseTransform(), "hummer");
        marq.name.permittedComparison(new DBDataComparison(new UppercaseTransform(), hummerLowerCaseOp));
        got = database.get(marq);
        Assert.assertThat(got.size(), is(0));

//        marq.name.permittedValues(new UppercaseTransform(new LowercaseTransform()), "HUMMER");
        marq.name.permittedComparison(new DBDataComparison(new UppercaseTransform(new LowercaseTransform()), hummerUpperCaseOp));
        got = database.get(marq);
        Assert.assertThat(got.size(), is(1));
    }
    
    @Test
    public void testSubstringTransform() throws SQLException{
        database.setPrintSQLBeforeExecuting(true);
        Marque marq = new Marque();
        marq.name.permittedValues("HUMMER".substring(3, 6));
        DBOperator last3LettersOfHUMMER = marq.name.getOperator();
        marq.name.permittedValues("HUMMER".substring(0, 3));
        DBOperator first3LettersOfHUMMER = marq.name.getOperator();
        
        List<Marque> got = database.get(marq);
        Assert.assertThat(got.size(), is(0));
        
//        marq.name.permittedValues(new SubstringTransform(0,3), "HUMMER".substring(0, 3));
        marq.name.permittedComparison(new DBDataComparison(new SubstringTransform(0,3), first3LettersOfHUMMER));
        got = database.get(marq);
        Assert.assertThat(got.size(), is(1));
        Assert.assertThat(got.get(0).name.stringValue(), is("HUMMER"));
        
//        marq.name.permittedValues(new SubstringTransform(3,6), "HUM");
        marq.name.permittedComparison(new DBDataComparison(new SubstringTransform(3,6), first3LettersOfHUMMER));
        got = database.get(marq);
        Assert.assertThat(got.size(), is(0));
        
//        marq.name.permittedValues(new SubstringTransform(3,6), "HUMMER".substring(3, 6));
        marq.name.permittedComparison(new DBDataComparison(new SubstringTransform(3,6), last3LettersOfHUMMER));
        got = database.get(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(1));
        
//        marq.name.permittedValues(new LowercaseTransform(marq.name.getTransform()), "HUMMER".substring(3, 6));
        marq.name.permittedComparison(new DBDataComparison(new LowercaseTransform(marq.name.getTransform()), last3LettersOfHUMMER));
        got = database.get(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(0));
        
//        marq.name.permittedValues(new UppercaseTransform(marq.name.getTransform()), "HUMMER".substring(3, 6));
        marq.name.permittedComparison(new DBDataComparison(new UppercaseTransform(marq.name.getTransform()), last3LettersOfHUMMER));
        got = database.get(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(1));
        
        database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));
//        marq.name.permittedValues(new SubstringTransform(new UppercaseTransform(new TrimTransform()),3,6), "HUMMER".substring(3, 6));
        marq.name.permittedComparison(new DBDataComparison(new SubstringTransform(new UppercaseTransform(new TrimTransform()),3,6), last3LettersOfHUMMER));
        got = database.get(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(2));
    }
    
    @Test
    public void testStringLengthTransform() throws SQLException{
        database.setPrintSQLBeforeExecuting(true);
        Marque marq = new Marque();
//        marq.name.permittedRangeInclusive(new StringLengthTransform(), 1,3);
        marq.name.permittedRangeInclusive(1,3);
        marq.name.permittedComparison(new DBDataComparison(new StringLengthTransform(),marq.name.getOperator()));
        DBQuery query = database.getDBQuery(marq);
        query.setSortOrder(new DBRow[]{marq}, marq.name);
        List<Marque> got = query.getAllInstancesOf(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(2));
        Assert.assertThat(got.get(0).name.stringValue(), is("BMW"));
        Assert.assertThat(got.get(1).name.stringValue(), is("VW"));
        Assert.assertThat(got.size(), is(2));

//        marq.name.permittedRangeInclusive(new StringLengthTransform(), "1","3");
        marq.name.permittedRangeInclusive("1","3");
        marq.name.permittedComparison(new DBDataComparison(new StringLengthTransform(),marq.name.getOperator()));
        query = database.getDBQuery(marq);
        query.setSortOrder(new DBRow[]{marq}, marq.name);
        got = query.getAllInstancesOf(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(2));
        Assert.assertThat(got.get(0).name.stringValue(), is("BMW"));
        Assert.assertThat(got.get(1).name.stringValue(), is("VW"));
        Assert.assertThat(got.size(), is(2));
    }
    
}
