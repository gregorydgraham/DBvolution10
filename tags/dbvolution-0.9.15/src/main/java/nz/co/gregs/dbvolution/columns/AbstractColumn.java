/*
 * Copyright 2013 Gregory Graham.
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
package nz.co.gregs.dbvolution.columns;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.exceptions.IncorrectDBRowInstanceSuppliedException;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;

public class AbstractColumn implements DBExpression {

    private final PropertyWrapper propertyWrapper;
    protected final DBRow dbrow;
    protected final Object field;

    public AbstractColumn(DBRow row, Object field) {
        this.dbrow = row;
        this.field = field;
        this.propertyWrapper = row.getPropertyWrapperOf(field);
        if (propertyWrapper == null) {
            throw IncorrectDBRowInstanceSuppliedException.newMultiRowInstance(field);
        }
    }

    @Override
    public String toSQLString(DBDatabase db) {
        return db.getDefinition().formatTableAliasAndColumnName(this.dbrow, propertyWrapper.columnName());
    }
    
    @Override
    public AbstractColumn copy() {
        try {
            Constructor<? extends AbstractColumn> constructor = this.getClass().getConstructor(dbrow.getClass(), field.getClass());
            AbstractColumn newInstance = constructor.newInstance(dbrow, field);
            return newInstance;
        } catch (NoSuchMethodException ex) {
            throw new DBRuntimeException("Unable To Copy "+this.getClass().getSimpleName()+": please ensure it has a public "+this.getClass().getSimpleName()+"(DBRow, Object) constructor.", ex);
        } catch (SecurityException ex) {
            throw new DBRuntimeException("Unable To Copy "+this.getClass().getSimpleName()+": please ensure it has a public "+this.getClass().getSimpleName()+"(DBRow, Object) constructor.", ex);
        } catch (InstantiationException ex) {
            throw new DBRuntimeException("Unable To Copy "+this.getClass().getSimpleName()+": please ensure it has a public "+this.getClass().getSimpleName()+"(DBRow, Object) constructor.", ex);
        } catch (IllegalAccessException ex) {
            throw new DBRuntimeException("Unable To Copy "+this.getClass().getSimpleName()+": please ensure it has a public "+this.getClass().getSimpleName()+"(DBRow, Object) constructor.", ex);
        } catch (IllegalArgumentException ex) {
            throw new DBRuntimeException("Unable To Copy "+this.getClass().getSimpleName()+": please ensure it has a public "+this.getClass().getSimpleName()+"(DBRow, Object) constructor.", ex);
        } catch (InvocationTargetException ex) {
            throw new DBRuntimeException("Unable To Copy "+this.getClass().getSimpleName()+": please ensure it has a public "+this.getClass().getSimpleName()+"(DBRow, Object) constructor.", ex);
        }
    }

    /**
     * @return the propertyWrapperOfQDT
     */
    public PropertyWrapper getPropertyWrapper() {
        return propertyWrapper;
    }
    
    /**
     * Wrap this column in the equivalent DBValue subclass
     * 
     * <p> Probably this should be implemented as:<br>
     * public MyValue asValue(){return new MyValue(this);}
     *
     * @return this instance as a StringValue, NumberValue, DateValue, or LargeObjectValue as appropriate
     */
    public DBExpression asValue(){
        return this;
    }

    @Override
    public QueryableDatatype getQueryableDatatypeForExpressionValue() {
        return QueryableDatatype.getQueryableDatatypeForObject(field);
    }

    @Override
    public boolean isAggregator() {
        return false;
    }

    @Override
    public Set<DBRow> getTablesInvolved() {
        HashSet<DBRow> hashSet = new HashSet<DBRow>();
        hashSet.add(dbrow);
        return hashSet;
    }
}
