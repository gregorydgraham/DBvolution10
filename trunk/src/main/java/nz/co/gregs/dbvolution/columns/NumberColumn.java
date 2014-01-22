/*
 * Copyright 2014 gregorygraham.
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
package nz.co.gregs.dbvolution.columns;

import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.variables.NumberValue;
import nz.co.gregs.dbvolution.variables.NumberVariable;

public class NumberColumn extends Column implements NumberVariable {

    public NumberColumn(DBRow row, DBNumber numberColumn) {
        super(row, numberColumn);
    }

    public NumberColumn(DBRow row, Number numberColumn) {
        super(row, numberColumn);
    }

    @Override
    public NumberVariable copy() {
        return (NumberColumn) super.copy();
    }

    @Override
    public NumberValue asValue() {
        return new NumberValue(this);
    }
}
