/*
 * Copyright 2013 gregorygraham.
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
package nz.co.gregs.dbvolution;

import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Contains all the instances of DBRow that are associated with one line of a
 * DBQuery request.
 *
 * <p>
 * DBvolution is available on <a
 * href="https://sourceforge.net/projects/dbvolution/">SourceForge</a> complete
 * with <a href="https://sourceforge.net/p/dbvolution/blog/">BLOG</a>
 *
 * <p>DBQueryRow represents an individual line within the results of a query.
 * However the results within the line are contained in instances of all the
 * DBRow subclasses included in the DBQuery.
 * 
 * <p>Each instance is accessible thru the {@link #get(nz.co.gregs.dbvolution.DBRow) get(DBRow) method}.
 *
 * @author gregorygraham
 *
 */
public class DBQueryRow extends HashMap<Class<?>, DBRow> {

    private static final long serialVersionUID = 1;
    private final Map<Object, QueryableDatatype> expressionColumnValues = new LinkedHashMap<Object, QueryableDatatype>();

    /**
     * Returns the instance of exemplar contained within this DBQueryRow.
     *
     * <p>Finds the instance of the class supplied that is relevant to the DBRow
     * and returns it.
     *
     * <p>Criteria set on the exemplar are ignored.
     *
     * <p>For example: Marque thisMarque = myQueryRow.get(new Marque());
     *
     * @param <E>
     * @param exemplar
     * @return the instance of exemplar that is in the DBQueryRow instance
     */
    @SuppressWarnings("unchecked")
    public <E extends DBRow> E get(E exemplar) {
        return (E) get(exemplar.getClass());
    }

    /**
     * Print the specified columns to the specified PrintStream as one line.
     *
     * @param ps
     * @param columns
     */
    public void print(PrintStream ps, QueryableDatatype... columns) {
        for (QueryableDatatype qdt : columns) {
            ps.print("" + qdt + " ");
        }
        ps.println();
    }

    /**
     * Print the all columns to the specified PrintStream as one line.
     *
     * @param ps
     */
    public void print(PrintStream ps) {
        for (DBRow row : values()) {
            ps.print("" + row);
        }
    }

    void addExpressionColumnValue(Object key, QueryableDatatype expressionQDT) {
        expressionColumnValues.put(key, expressionQDT);
    }

    public QueryableDatatype getExpressionColumnValue(Object key) {
        return expressionColumnValues.get(key);
    }
}
