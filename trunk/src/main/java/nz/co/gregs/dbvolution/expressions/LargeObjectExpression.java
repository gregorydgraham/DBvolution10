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
package nz.co.gregs.dbvolution.expressions;

import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.DBByteArray;

public class LargeObjectExpression implements LargeObjectResult {

    private final LargeObjectResult qdt;

    protected LargeObjectExpression() {
        qdt = new DBByteArray();
    }

    public LargeObjectExpression(LargeObjectResult copy) {
        qdt = copy;
    }

    @Override
    public String toSQLString(DBDatabase db) {
        return qdt.toSQLString(db);
    }

    @Override
    public LargeObjectExpression copy() {
        return new LargeObjectExpression(qdt.copy());
    }

    @Override
    public DBByteArray getQueryableDatatypeForExpressionValue() {
        return new DBByteArray();
    }

    @Override
    public boolean isAggregator() {
        return qdt.isAggregator();
    }

    @Override
    public Set<DBRow> getTablesInvolved() {
        HashSet<DBRow> hashSet = new HashSet<DBRow>();
        if (qdt != null) {
            hashSet.addAll(qdt.getTablesInvolved());
        }
        return hashSet;
    }

}
