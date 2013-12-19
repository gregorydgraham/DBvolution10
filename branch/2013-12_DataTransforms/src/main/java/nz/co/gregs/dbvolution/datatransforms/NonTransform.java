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
public class NonTransform implements DataTransform {

    protected DataTransform innerTransform = null;

    public NonTransform(DataTransform innerTransform) {
        this.innerTransform = innerTransform;
    }

    public NonTransform() {
    }

    @Override
    public String transform(String formattedValueForSQLStatement) {
        if (innerTransform == null) {
            return formattedValueForSQLStatement;
        } else {
            return innerTransform.transform(formattedValueForSQLStatement);
        }
    }

    @Override
    public void setTransform(DataTransform innerTransform) {
        this.innerTransform = innerTransform;
    }

    @Override
    public DataTransform getTransform() {
        return innerTransform;
    }

    @Override
    public DataTransform copy() {
        DataTransform newCopy = this;
        try {
            newCopy = (DataTransform) this.getClass().newInstance();
        } catch (InstantiationException ex) {
            Logger.getLogger(NonTransform.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(NonTransform.class.getName()).log(Level.SEVERE, null, ex);
        }
        if (this.innerTransform != null) {
            newCopy.setTransform(this.innerTransform.copy());
        }
        return newCopy;
    }
}
