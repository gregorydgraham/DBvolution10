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
package nz.co.gregs.dbvolution.exceptions;

import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;

/**
 *
 * @author Gregory Graham
 */
@SuppressWarnings("serial")
public class UndefinedPrimaryKeyException extends RuntimeException {

    public UndefinedPrimaryKeyException() {
    }

    public <E extends DBRow> UndefinedPrimaryKeyException(Class<E> thisClass) {
        super("Primary Key Field Not Defined: Please define the primary key field of " + thisClass.getSimpleName() + " using the @"+ DBPrimaryKey.class.getSimpleName()+" annotation.");
    }

    public <E extends DBRow> UndefinedPrimaryKeyException(E thisRow) {
        this(thisRow.getClass());
    }
}
