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
package nz.co.gregs.dbvolution.transforms.string;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.variables.StringVariable;
import nz.co.gregs.dbvolution.variables.StringValue;

/**
 * Implements the TRIM() function for String values
 * 
 * <p>use this transform to remove the leading any trailing spaces from strings 
 * and to ensure the CHAR columns are not padded with spaces.
 *
 * @author gregory.graham
 */
public class Trim extends BaseTransform implements StringVariable {
    
    public Trim(String value) {
        super(new StringValue(value));
    }

    public Trim(StringVariable innerTransform) {
        super(innerTransform);
    }

    @Override
    protected String doTransform(DBDatabase db, String enclosedValue) {
        return db.getDefinition().doTrimFunction(enclosedValue);
    }

    @Override
    public Trim copy() {
        return (Trim)super.copy();
    }
    
}
