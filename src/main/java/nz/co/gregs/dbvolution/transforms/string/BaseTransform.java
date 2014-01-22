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

import java.lang.reflect.InvocationTargetException;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.variables.DBExpression;

/**
 *
 * @author greg
 */
public abstract class BaseTransform implements DBExpression {

    protected final DBExpression innerTransform;

    public BaseTransform(DBExpression innerTransform) {
        this.innerTransform = innerTransform;
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
    public DBExpression copy() {
        BaseTransform newInstance = null;
        try {
            newInstance = this.getClass().getConstructor(this.innerTransform.getClass()).newInstance(this.innerTransform);
        } catch (InstantiationException ex) {
            throw new DBRuntimeException("Unable To Copy "+this.getClass().getSimpleName()+": please ensure it has a public single parameter constructor.", ex);
        } catch (IllegalAccessException ex) {
            throw new DBRuntimeException("Unable To Copy "+this.getClass().getSimpleName()+": please ensure it has a public single parameter constructor.", ex);
        } catch (NoSuchMethodException ex) {
            throw new DBRuntimeException("Unable To Copy "+this.getClass().getSimpleName()+": please ensure it has a public single parameter constructor.", ex);
        } catch (SecurityException ex) {
            throw new DBRuntimeException("Unable To Copy "+this.getClass().getSimpleName()+": please ensure it has a public single parameter constructor.", ex);
        } catch (IllegalArgumentException ex) {
            throw new DBRuntimeException("Unable To Copy "+this.getClass().getSimpleName()+": please ensure it has a public single parameter constructor.", ex);
        } catch (InvocationTargetException ex) {
            throw new DBRuntimeException("Unable To Copy "+this.getClass().getSimpleName()+": please ensure it has a public single parameter constructor.", ex);
        }
        return newInstance;
    }

    protected abstract String doTransform(DBDatabase db, String enclosedValue);
}
