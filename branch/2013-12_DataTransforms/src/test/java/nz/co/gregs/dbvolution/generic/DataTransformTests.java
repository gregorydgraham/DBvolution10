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
import nz.co.gregs.dbvolution.datatransforms.LeftTrimTransform;
import nz.co.gregs.dbvolution.datatransforms.LowercaseTransform;
import nz.co.gregs.dbvolution.datatransforms.RightTrimTransform;
import nz.co.gregs.dbvolution.datatransforms.SubstringTransform;
import nz.co.gregs.dbvolution.datatransforms.TrimTransform;
import nz.co.gregs.dbvolution.datatransforms.UppercaseTransform;
import nz.co.gregs.dbvolution.example.Marque;
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
        
        marq.name.setTransform(new TrimTransform());
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
        
        marq.name.setTransform(new LeftTrimTransform());
        got = database.get(marq);
        Assert.assertThat(got.size(), is(1));

        marq.name.setTransform(new RightTrimTransform());
        got = database.get(marq);
        Assert.assertThat(got.size(), is(1));

        marq.name.setTransform(new LeftTrimTransform(new RightTrimTransform()));
        got = database.get(marq);
        Assert.assertThat(got.size(), is(2));
    }
    
    @Test
    public void testUpperAndLowercaseTransforms() throws SQLException{
        database.setPrintSQLBeforeExecuting(true);
        Marque marq = new Marque();
        marq.name.permittedValues("HUMMER");
        List<Marque> got = database.get(marq);
        Assert.assertThat(got.size(), is(1));
        
        marq.name.setTransform(new LowercaseTransform());
        got = database.get(marq);
        Assert.assertThat(got.size(), is(0));
        
        marq.name.permittedValues("hummer");
        got = database.get(marq);
        Assert.assertThat(got.size(), is(1));

        marq.name.setTransform(new UppercaseTransform());
        got = database.get(marq);
        Assert.assertThat(got.size(), is(0));

        marq.name.permittedValues("HUMMER");
        marq.name.setTransform(new UppercaseTransform(new LowercaseTransform()));
        got = database.get(marq);
        Assert.assertThat(got.size(), is(1));
    }
    
    @Test
    public void testSubstringTransform() throws SQLException{
        database.setPrintSQLBeforeExecuting(true);
        Marque marq = new Marque();
        marq.name.permittedValues("HUMMER".substring(0, 3));
        List<Marque> got = database.get(marq);
        Assert.assertThat(got.size(), is(0));
        
        marq.name.setTransform(new SubstringTransform(0,3));
        got = database.get(marq);
        Assert.assertThat(got.size(), is(1));
        Assert.assertThat(got.get(0).name.stringValue(), is("HUMMER"));
        
        marq.name.permittedValues("HUM");
        marq.name.setTransform(new SubstringTransform(3,6));
        got = database.get(marq);
        Assert.assertThat(got.size(), is(0));
        
        marq.name.permittedValues("HUMMER".substring(3, 6));
        got = database.get(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(1));
        
        marq.name.permittedValues("HUMMER".substring(3, 6));
        marq.name.setTransform(new LowercaseTransform(marq.name.getTransform()));
        got = database.get(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(0));
        
        marq.name.permittedValues("HUMMER".substring(3, 6));
        marq.name.setTransform(new UppercaseTransform(marq.name.getTransform()));
        got = database.get(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(1));
        
        database.insert(new Marque(3, "False", 1246974, "", 0, "", "     HUMMER               ", "", "Y", new Date(), 3, null));
        marq.name.permittedValues("HUMMER".substring(3, 6));
        marq.name.setTransform(new SubstringTransform(new UppercaseTransform(new TrimTransform()),3,6));
        got = database.get(marq);
        database.print(got);
        Assert.assertThat(got.size(), is(2));
    }
    
}
