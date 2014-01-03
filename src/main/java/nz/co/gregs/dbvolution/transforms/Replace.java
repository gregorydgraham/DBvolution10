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
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.generators.Column;
import nz.co.gregs.dbvolution.generators.StringGenerator;
import nz.co.gregs.dbvolution.generators.StringValue;
import nz.co.gregs.dbvolution.generators.Value;

public class Replace extends BaseTransform implements StringGenerator {
    private final StringGenerator find;
    private final StringGenerator replace;

    public Replace(StringGenerator innerTransform, String find, String replace) {
        super(innerTransform);
        this.find = new StringValue(find);
        this.replace = new StringValue(replace);
    }
    
    public Replace(String value, String find, String replace) {
        super(new Value(value));
        this.find = new StringValue(find);
        this.replace = new StringValue(replace);
    }
    
    public Replace(String value, StringGenerator find, String replace) {
        super(new Value(value));
        this.find = find;
        this.replace = new StringValue(replace);
    }
    
    public Replace(String value, String find, StringGenerator replace) {
        super(new Value(value));
        this.find = new StringValue(find);
        this.replace = replace;
    }
    
    public Replace(StringGenerator transform, StringGenerator find, String replace) {
        super(transform);
        this.find = find;
        this.replace = new StringValue(replace);
    }
    
    public Replace(String value, StringGenerator find, StringGenerator replace) {
        super(new Value(value));
        this.find = find;
        this.replace = replace;
    }
    
    public Replace(StringGenerator transform, String find, StringGenerator replace) {
        super(transform);
        this.find = new StringValue(find);
        this.replace = replace;
    }
    
    public Replace(StringGenerator transform, StringGenerator find, StringGenerator replace) {
        super(transform);
        this.find = find;
        this.replace = replace;
    }

    @Override
    protected String doTransform(DBDatabase db, String enclosedValue) {
        return " REPLACE( "+enclosedValue+", "+find.toSQLString(db)+", "+replace.toSQLString(db)+") ";
    }

    @Override
    public boolean isNull() {
        return false;
    }
}
