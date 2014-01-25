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

package nz.co.gregs.dbvolution.expressions;

import nz.co.gregs.dbvolution.DBDatabase;

/**
 *
 * @author gregory.graham
 */
public interface DBExpression {

    /**
     *
     * @param db
     * @return the DBValue formatted as a SQL snippet
     */
    public String toSQLString(DBDatabase db);

    /**
     * A Complete Copy Of This DBValue
     * 
     * <p>Immutability in DBvolution is maintain by internally copying objects. 
     * 
     * <p>This method enables immutability by performing a deep copy of the object.
     * 
     * <p>Singletons may return themselves but all other objects must return 
     * a new instance with copies of all mutable fields.
     *
     * @return a copy of this {@code DBValue}
     */
    public DBExpression copy();    
}
