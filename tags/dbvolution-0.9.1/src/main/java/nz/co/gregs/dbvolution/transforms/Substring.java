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
package nz.co.gregs.dbvolution.transforms;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.generators.StringGenerator;
import nz.co.gregs.dbvolution.generators.Value;

public class Substring extends BaseTransform implements StringGenerator{

    private final DBInteger startingPosition;
    private final DBInteger length;

    public Substring(StringGenerator innerTransform, Integer startingIndex0Based) {
        super(innerTransform);
        this.startingPosition = new DBInteger(startingIndex0Based);
        this.length = null;
    }

    public Substring(StringGenerator innerTransform, DBInteger startingIndex0Based) {
        super(innerTransform);
        this.startingPosition = (DBInteger) startingIndex0Based.copy();
        this.length = null;
    }

    public Substring(StringGenerator innerTransform, Integer startingIndex0Based, Integer endIndex0Based) {
        super(innerTransform);
        this.startingPosition = new DBInteger(startingIndex0Based);
        this.length = new DBInteger(endIndex0Based);
    }

    public Substring(String value, Integer startingIndex0Based, Integer endIndex0Based) {
        super(new Value(value));
        this.startingPosition = new DBInteger(startingIndex0Based);
        this.length = new DBInteger(endIndex0Based);
    }

    public Substring(StringGenerator innerTransform, DBInteger startingIndex0Based, DBInteger endIndex0Based) {
        super(innerTransform);
        this.startingPosition = (DBInteger) startingIndex0Based.copy();
        this.length = (DBInteger)endIndex0Based.copy();
    }

    public Substring(String value, DBInteger startingIndex0Based, DBInteger endIndex0Based) {
        super(new Value(value));
        this.startingPosition = (DBInteger) startingIndex0Based.copy();
        this.length = (DBInteger)endIndex0Based.copy();
    }

    @Override
    protected String doTransform(DBDatabase db, String enclosedValue) {
        return db.getDefinition().doSubstringTransform(enclosedValue, startingPosition, length);
    }

}
