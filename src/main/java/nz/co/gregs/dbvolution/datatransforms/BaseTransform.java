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
package nz.co.gregs.dbvolution.datatransforms;

import java.io.Serializable;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.datagenerators.DataGenerator;

/**
 *
 * @author greg
 */
public abstract class BaseTransform implements DataGenerator, Serializable {

    public static final long serialVersionUID = 1L;

    protected DataGenerator innerTransform = new NonGenerator();

    public BaseTransform(DataGenerator innerTransform) {
        this.innerTransform = innerTransform;
    }

    public BaseTransform() {
    }

    @Override
    public String toSQLString(DBDatabase db) {
        if (innerTransform == null) {
            return "";
        } else {
            return insertBeforeValue()
                    + innerTransform.toSQLString(db)
                    + insertAfterValue();
        }
    }

    @Override
    public DataGenerator copy() {
        return this;
    }

    protected abstract String insertAfterValue();

    protected abstract String insertBeforeValue();
}
