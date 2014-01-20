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
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.generators.DataGenerator;

/**
 *
 * @author greg
 */
public abstract class BaseTransform implements DataGenerator {

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
            return doTransform(db, innerTransform.toSQLString(db));
        }
    }

    @Override
    public DataGenerator copy() {
        BaseTransform newInstance = null;
        try {
            newInstance = this.getClass().newInstance();
            newInstance.innerTransform = this.innerTransform.copy();
        } catch (InstantiationException ex) {
            throw new DBRuntimeException("Unable To Copy BaseTransform: please ensure it has a public no-parameter constructor.", ex);
        } catch (IllegalAccessException ex) {
            throw new DBRuntimeException("Unable To Copy BaseTransform: please ensure it has a public no-parameter constructor.", ex);
        }
        return newInstance;
    }

//    protected abstract String insertAfterValue();
    protected abstract String doTransform(DBDatabase db, String enclosedValue);
}
