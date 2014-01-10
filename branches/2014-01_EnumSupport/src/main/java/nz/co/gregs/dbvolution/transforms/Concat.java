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

package nz.co.gregs.dbvolution.transforms;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.generators.StringGenerator;
import nz.co.gregs.dbvolution.generators.StringValue;


public class Concat extends BaseTransform implements StringGenerator{
    private final StringGenerator secondString;

    public Concat(String firstString, String secondString) {
        super(new StringValue(firstString));
        this.secondString = new StringValue(secondString);
    }
    
    public Concat(StringGenerator firstString, String secondString) {
        super(firstString);
        this.secondString = new StringValue(secondString);
    }
    
    public Concat(String firstString, StringGenerator secondString) {
        super(new StringValue(firstString));
        this.secondString = secondString;
    }
    
    public Concat(StringGenerator firstString, StringGenerator secondString) {
        super(firstString);
        this.secondString = secondString;
    }
    
    @Override
    protected String doTransform(DBDatabase db, String enclosedValue) {
        return db.getDefinition().doConcatTransform(enclosedValue, secondString.toSQLString(db));
    }

}
