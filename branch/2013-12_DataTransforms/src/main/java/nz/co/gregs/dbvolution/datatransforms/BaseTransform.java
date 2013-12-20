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

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author greg
 */
public abstract class BaseTransform implements DataTransform {

    protected DataTransform innerTransform = new NullTransform();

    public BaseTransform(DataTransform innerTransform) {
        this.innerTransform = innerTransform;
    }

    public BaseTransform() {
    }

    @Override
    public String transform(String formattedValueForSQLStatement) {
        if (innerTransform == null) {
            return formattedValueForSQLStatement;
        } else {
            return insertBeforeValue()
                    + innerTransform.transform(formattedValueForSQLStatement)
                    + insertAfterValue();
        }
    }

    @Override
    public void setInnerTransform(DataTransform innerTransform) {
        if (innerTransform == null) {
            this.innerTransform = new NullTransform();
        } else {
            this.innerTransform = innerTransform;
        }
    }

    @Override
    public DataTransform copy() {
        DataTransform newCopy = this;
        try {
            newCopy = (DataTransform) this.getClass().newInstance();
        } catch (InstantiationException ex) {
            Logger.getLogger(BaseTransform.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(BaseTransform.class.getName()).log(Level.SEVERE, null, ex);
        }
        if (this.innerTransform != null) {
            newCopy.setInnerTransform(this.innerTransform.copy());
        }
        return newCopy;
    }

    protected abstract String insertAfterValue();

    protected abstract String insertBeforeValue();
}
