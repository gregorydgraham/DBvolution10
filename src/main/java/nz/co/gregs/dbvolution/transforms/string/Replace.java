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

package nz.co.gregs.dbvolution.transforms.string;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.variables.StringVariable;
import nz.co.gregs.dbvolution.variables.StringExpression;

public class Replace extends BaseTransform implements StringVariable {
    private final StringVariable find;
    private final StringVariable replace;

    public Replace(StringVariable innerTransform, String find, String replace) {
        super(innerTransform);
        this.find = new StringExpression(find);
        this.replace = new StringExpression(replace);
    }
    
    public Replace(String value, String find, String replace) {
        super(new StringExpression(value));
        this.find = new StringExpression(find);
        this.replace = new StringExpression(replace);
    }
    
    public Replace(String value, StringVariable find, String replace) {
        super(new StringExpression(value));
        this.find = find;
        this.replace = new StringExpression(replace);
    }
    
    public Replace(String value, String find, StringVariable replace) {
        super(new StringExpression(value));
        this.find = new StringExpression(find);
        this.replace = replace;
    }
    
    public Replace(StringVariable transform, StringVariable find, String replace) {
        super(transform);
        this.find = find;
        this.replace = new StringExpression(replace);
    }
    
    public Replace(String value, StringVariable find, StringVariable replace) {
        super(new StringExpression(value));
        this.find = find;
        this.replace = replace;
    }
    
    public Replace(StringVariable transform, String find, StringVariable replace) {
        super(transform);
        this.find = new StringExpression(find);
        this.replace = replace;
    }
    
    public Replace(StringVariable transform, StringVariable find, StringVariable replace) {
        super(transform);
        this.find = find;
        this.replace = replace;
    }

    @Override
    protected String doTransform(DBDatabase db, String enclosedValue) {
        return " REPLACE( "+enclosedValue+", "+find.toSQLString(db)+", "+replace.toSQLString(db)+") ";
    }

    @Override
    public Replace copy() {
        return (Replace)super.copy();
    }
}
