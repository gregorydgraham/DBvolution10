/*
 * Copyright 2014 gregory.graham.
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
import nz.co.gregs.dbvolution.datatypes.DBBoolean;

public class BooleanExpression implements BooleanResult {

    private BooleanResult bool1;

    public BooleanExpression() {
    }

    public BooleanExpression(BooleanResult dateVariable) {
        bool1 = dateVariable;
    }

    public BooleanExpression(Boolean bool) {
        bool1 = new DBBoolean(bool);
    }

    @Override
    public String toSQLString(DBDatabase db) {
        return bool1.toSQLString(db);
    }

    @Override
    public BooleanExpression copy() {
        return new BooleanExpression(this.bool1);
    }

    /**
     * Create An Appropriate BooleanExpression Object For This Object
     *
     * <p>The expression framework requires a *Expression to work with. The
     * easiest way to get that is the {@code DBRow.column()} method.
     *
     * <p>However if you wish your expression to start with a literal value it
     * is a little trickier.
     *
     * <p>This method provides the easy route to a *Expression from a literal
     * value. Just call, for instance,
     * {@code StringExpression.value("STARTING STRING")} to get a
     * StringExpression and start the expression chain.
     *
     * <ul>
     * <li>Only object classes that are appropriate need to be handle by the
     * DBExpression subclass.<li>
     * <li>The implementation should be {@code static}</li>
     *
     * @param bool
     * @return a DBExpression instance that is appropriate to the subclass and
     * the value supplied.
     */
    public static BooleanExpression value(Boolean bool) {
        return new BooleanExpression(bool);
    }
}
