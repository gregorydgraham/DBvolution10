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

package nz.co.gregs.dbvolution.transforms.string;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.variables.StringVariable;
import nz.co.gregs.dbvolution.variables.StringValue;


public class RightTrim extends BaseTransform implements StringVariable{

    public RightTrim(StringVariable innerTransform) {
        super(innerTransform);
    }
    
    public RightTrim(String value) {
        super(new StringValue(value));
    }
    
    @Override
    protected String doTransform(DBDatabase db, String enclosedValue) {
    return db.getDefinition().doRightTrimTransform(enclosedValue);
    }    

    @Override
    public RightTrim copy() {
        return (RightTrim)super.copy();
    }
}
