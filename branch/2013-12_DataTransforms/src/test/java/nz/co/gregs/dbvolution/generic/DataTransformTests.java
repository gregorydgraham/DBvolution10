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
import nz.co.gregs.dbvolution.datatransforms.TrimDBString;
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
        
        marq.name.setTranform(new TrimDBString());
        got = database.get(marq);
        Assert.assertThat(got.size(), is(2));
    }
    
}
