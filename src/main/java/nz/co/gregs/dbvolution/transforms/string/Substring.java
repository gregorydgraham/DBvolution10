/*
 * Copyright 2013 Gregory Graham.
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
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.variables.NumberResult;
import nz.co.gregs.dbvolution.variables.StringResult;
import nz.co.gregs.dbvolution.variables.StringExpression;

public class Substring extends StringExpression implements StringResult{
    
    private final NumberResult startingPosition;
    private final NumberResult length;

    public Substring(StringResult stringInput, Number startingIndex0Based) {
        super(stringInput);
        this.startingPosition = new DBNumber(startingIndex0Based);
        this.length = null;
    }

    public Substring(StringResult stringInput, NumberResult startingIndex0Based) {
        super(stringInput);
        this.startingPosition = startingIndex0Based.copy();
        this.length = null;
    }

    public Substring(StringResult stringInput, Number startingIndex0Based, Number endIndex0Based) {
        super(stringInput);
        this.startingPosition = new DBNumber(startingIndex0Based);
        this.length = new DBNumber(endIndex0Based);
    }
//
//    @Deprecated
//    public Substring(String stringInput, Number startingIndex0Based, Number endIndex0Based) {
//        super(new StringExpression(stringInput));
//        this.startingPosition = new DBNumber(startingIndex0Based);
//        this.length = new DBNumber(endIndex0Based);
//    }

    public Substring(StringResult stringInput, NumberResult startingIndex0Based, NumberResult endIndex0Based) {
        super(stringInput);
        this.startingPosition = startingIndex0Based.copy();
        this.length = endIndex0Based.copy();
    }
    
//    @Deprecated
//    public Substring(String value, NumberVariable startingIndex0Based, NumberVariable endIndex0Based) {
//        super(new DBString(value));
//        this.startingPosition = startingIndex0Based.copy();
//        this.length = endIndex0Based.copy();
//    }

    @Override
    public Substring copy() {
        return (Substring)super.copy();
    }

    @Override
    public String toSQLString(DBDatabase db) {
        if (getStringInput() == null) {
            return "";
        } else {
            return doSubstringTransform(db, getStringInput(), startingPosition, length);
        }
    }
    
        public String doSubstringTransform(DBDatabase db, StringResult enclosedValue, NumberResult startingPosition, NumberResult substringLength){
            return " SUBSTRING("
                +enclosedValue.toSQLString(db)
                +" FROM " 
                +(startingPosition.toSQLString(db)+" + 1") 
                +( substringLength != null ? " for " + (substringLength.toSQLString(db)+" - "+startingPosition.toSQLString(db)) : "")
                + ") ";
    }
}
