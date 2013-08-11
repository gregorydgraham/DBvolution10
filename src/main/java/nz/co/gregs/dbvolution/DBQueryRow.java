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

import java.io.PrintStream;
import java.util.HashMap;

/**
 *
 * @author gregorygraham
 *
 */
public class DBQueryRow extends HashMap<Class<?>, DBRow> {

    public static final long serialVersionUID = 1;

    /**
     *
     * @param exemplar
     * @return
     */
    @SuppressWarnings("unchecked")
    public <E extends DBRow> E get(E exemplar) {
        return (E) get(exemplar.getClass());
    }

    public void print(PrintStream ps, QueryableDatatype... columns) {
        for (QueryableDatatype qdt : columns) {
            ps.print("" + qdt + " ");
        }
        ps.println();
    }
}
