/*
 * Copyright 2014 Gregory Graham.
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
package nz.co.gregs.dbvolution.query;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.columns.*;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
import nz.co.gregs.dbvolution.expressions.*;
import nz.co.gregs.dbvolution.internal.properties.*;

/**
 * Encapsulates the concept of a row that has fields/columns and is part of a
 * table/view on a database.
 *
 * @author Gregory Graham
 */
public class RowDefinition implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final RowDefinitionWrapperFactory wrapperFactory = new RowDefinitionWrapperFactory();
	private transient RowDefinitionInstanceWrapper wrapper = null;
	private transient List<PropertyWrapperDefinition> returnColumns = null;

	/**
	 * Gets a wrapper for the underlying property (field or method) given the
	 * property's object reference.
	 *
	 * <p>
	 * For example the following code snippet will get a property wrapper for the
	 * {@literal name} field:
	 * <pre>
	 * Customer customer = ...;
	 * getPropertyWrapperOf(customer.name);
	 * </pre>
	 *
	 * @param qdt	 qdt	
	 * @return the PropertyWrapper associated with the Object suppled or NULL.
	 */
	public PropertyWrapper getPropertyWrapperOf(Object qdt) {
		List<PropertyWrapper> props = getWrapper().getPropertyWrappers();

		Object qdtOfProp;
		for (PropertyWrapper prop : props) {
			qdtOfProp = prop.rawJavaValue();
			if (qdtOfProp == qdt) {
				return prop;
			}
		}
		return null;
	}

	protected RowDefinitionInstanceWrapper getWrapper() {
		if (wrapper == null) {
			wrapper = wrapperFactory.instanceWrapperFor(this);
		}
		return wrapper;
	}

	/**
	 * Returns the PropertyWrappers used internally to maintain the relationship
	 * between fields and columns
	 *
	 * @return non-null list of property wrappers, empty if none
	 */
	public List<PropertyWrapper> getPropertyWrappers() {
		return getWrapper().getPropertyWrappers();
	}

	/**
	 * Creates a new LargeObjectColumn instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	 fieldOfThisInstance	
	 * @return A LargeObjectColumn representing the supplied field
	 */
	public LargeObjectColumn column(DBLargeObject fieldOfThisInstance) {
		return new LargeObjectColumn(this, fieldOfThisInstance);
	}
	
	public BooleanArrayColumn column(DBBooleanArray fieldOfThisInstance) {
		return new BooleanArrayColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new ColumnProvider instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	 fieldOfThisInstance	
	 * @return A ColumnProvider representing the supplied field
	 */
	public ColumnProvider column(QueryableDatatype fieldOfThisInstance) throws IncorrectRowProviderInstanceSuppliedException {
		ColumnProvider col = null;
		if (DBBoolean.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBBoolean) fieldOfThisInstance);
		} else if (DBDate.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBDate) fieldOfThisInstance);
		} else if (DBLargeObject.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBLargeObject) fieldOfThisInstance);
		} else if (DBInteger.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBInteger) fieldOfThisInstance);
		} else if (DBIntegerEnum.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBIntegerEnum) fieldOfThisInstance);
		} else if (DBNumber.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBNumber) fieldOfThisInstance);
		} else if (DBStringEnum.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBStringEnum) fieldOfThisInstance);
		} else if (DBString.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBString) fieldOfThisInstance);
		} else if (DBBooleanArray.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBBooleanArray) fieldOfThisInstance);
		}
		if (col == null) {
			throw new IncorrectRowProviderInstanceSuppliedException(this, fieldOfThisInstance);
		}
		return col;
	}

	/**
	 * Creates a new ColumnProvider instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	 fieldOfThisInstance	
	 * @return A ColumnProvider representing the supplied field
	 */
	public ColumnProvider column(Object fieldOfThisInstance) throws IncorrectRowProviderInstanceSuppliedException {
		ColumnProvider col = null;
		if (QueryableDatatype.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((QueryableDatatype) fieldOfThisInstance);
		} else if (Date.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((Date) fieldOfThisInstance);
		} else if (Boolean.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((Boolean) fieldOfThisInstance);
		} else if (Integer.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((Integer) fieldOfThisInstance);
		} else if (Number.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((Number) fieldOfThisInstance);
		} else if (String.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((String) fieldOfThisInstance);
		} else {
			throw new UnsupportedOperationException("Object class not supported: " + fieldOfThisInstance.getClass().getName());
		}
		if (col == null) {
			throw new IncorrectRowProviderInstanceSuppliedException(this, fieldOfThisInstance);
		}
		return col;
	}

	/**
	 * Creates a new DBExpression instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	 fieldOfThisInstance	
	 * @return A DBExpression representing the supplied field
	 */
	public DBExpression getDBExpression(QueryableDatatype fieldOfThisInstance) throws IncorrectRowProviderInstanceSuppliedException {
		DBExpression col = null;
		if (DBBoolean.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBBoolean) fieldOfThisInstance);
		} else if (DBDate.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBDate) fieldOfThisInstance);
		} else if (DBLargeObject.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBLargeObject) fieldOfThisInstance);
		} else if (DBInteger.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBInteger) fieldOfThisInstance);
		} else if (DBNumber.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBNumber) fieldOfThisInstance);
		} else if (DBString.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBString) fieldOfThisInstance);
		}
		if (col == null) {
			throw new IncorrectRowProviderInstanceSuppliedException(this, fieldOfThisInstance);
		}
		return col;
	}

	/**
	 * Creates a new BooleanColumn instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	 fieldOfThisInstance	
	 * @return A LargeObjectColumn representing the supplied field
	 */
	public BooleanColumn column(DBBoolean fieldOfThisInstance) {
		return new BooleanColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new BooleanColumn instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	 fieldOfThisInstance	
	 * @return A Column representing the supplied field
	 */
	public BooleanColumn column(Boolean fieldOfThisInstance) {
		return new BooleanColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new StringColumn instance to help create
	 * {@link DBExpression expressions}.
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	 fieldOfThisInstance	
	 * @return A Column representing the supplied field
	 */
	public StringColumn column(DBStringEnum<?> fieldOfThisInstance) {
		return new StringColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new StringColumn instance to help create
	 * {@link DBExpression expressions}.
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	 fieldOfThisInstance	
	 * @return A Column representing the supplied field
	 */
	public StringColumn column(DBString fieldOfThisInstance) {
		return new StringColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new StringColumn instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	 fieldOfThisInstance	
	 * @return A Column representing the supplied field
	 */
	public StringColumn column(String fieldOfThisInstance) {
		return new StringColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new StringExpression for the field with a default value for NULL
	 * entries.
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * @return A StringExpression representing the supplied field with a default
	 * value for NULLs
	 */
	public StringExpression column(DBString fieldOfThisInstance, StringResult valueToUseIfDBNull) {
		return (new StringColumn(this, fieldOfThisInstance)).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new StringExpression for the field with a default value for NULL
	 * entries.
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * @return A StringExpression representing the supplied field with a default
	 * value for NULLs
	 */
	public StringExpression column(DBString fieldOfThisInstance, String valueToUseIfDBNull) {
		return (new StringColumn(this, fieldOfThisInstance)).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new StringExpression for the field with a default value for NULL
	 * entries.
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * @return A StringExpression representing the supplied field with a default
	 * value for NULLs
	 */
	public StringExpression column(String fieldOfThisInstance, StringResult valueToUseIfDBNull) {
		return (new StringColumn(this, fieldOfThisInstance)).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new StringExpression for the field with a default value for NULL
	 * entries.
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * @return A StringExpression representing the supplied field with a default
	 * value for NULLs
	 */
	public StringExpression column(String fieldOfThisInstance, String valueToUseIfDBNull) {
		return (new StringColumn(this, fieldOfThisInstance)).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new NumberColumn instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	 fieldOfThisInstance	
	 * @return A Column representing the supplied field
	 */
	public NumberColumn column(DBNumber fieldOfThisInstance) {
		return new NumberColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new NumberColumn instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	 fieldOfThisInstance	
	 * @return A Column representing the supplied field
	 */
	public NumberColumn column(Number fieldOfThisInstance) {
		return new NumberColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new NumberExpression representing the column and supplying a
	 * default value for NULL entries.
	 *
	 * <p>
	 * This method is an easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * @return A NumberExpression representing the supplied field with a default
	 * for DBNULL values
	 */
	public NumberExpression column(Number fieldOfThisInstance, Number valueToUseIfDBNull) {
		return new NumberColumn(this, fieldOfThisInstance).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new NumberExpression representing the column and supplying a
	 * default value for NULL entries.
	 *
	 * <p>
	 * This method is an easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * @return A NumberExpression representing the supplied field with a default
	 * for DBNULL values
	 */
	public NumberExpression column(DBNumber fieldOfThisInstance, Number valueToUseIfDBNull) {
		return new NumberColumn(this, fieldOfThisInstance).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new NumberExpression representing the column and supplying a
	 * default value for NULL entries.
	 *
	 * <p>
	 * This method is an easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * @return A NumberExpression representing the supplied field with a default
	 * for DBNULL values
	 */
	public NumberExpression column(DBNumber fieldOfThisInstance, NumberResult valueToUseIfDBNull) {
		return new NumberColumn(this, fieldOfThisInstance).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new IntegerColumn instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	 fieldOfThisInstance	
	 * @return A Column representing the supplied field
	 */
	public IntegerColumn column(DBInteger fieldOfThisInstance) {
		return new IntegerColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new IntegerColumn instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	 fieldOfThisInstance	
	 * @return A Column representing the supplied field
	 */
	public IntegerColumn column(DBIntegerEnum<?> fieldOfThisInstance) {
		return new IntegerColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new IntegerColumn instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	 fieldOfThisInstance	
	 * @return A Column representing the supplied field
	 */
	public IntegerColumn column(Long fieldOfThisInstance) {
		return new IntegerColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new IntegerColumn instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	 fieldOfThisInstance	
	 * @return A Column representing the supplied field
	 */
	public IntegerColumn column(Integer fieldOfThisInstance) {
		return new IntegerColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new NumberExpression representing the column and supplying a
	 * default value for NULL entries.
	 *
	 * <p>
	 * This method is an easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * @return A NumberExpression representing the supplied field with a default
	 * for DBNULL values
	 */
	public NumberExpression column(Integer fieldOfThisInstance, Integer valueToUseIfDBNull) {
		return new IntegerColumn(this, fieldOfThisInstance).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new NumberExpression representing the column and supplying a
	 * default value for NULL entries.
	 *
	 * <p>
	 * This method is an easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * @return A NumberExpression representing the supplied field with a default
	 * for DBNULL values
	 */
	public NumberExpression column(Integer fieldOfThisInstance, Long valueToUseIfDBNull) {
		return new IntegerColumn(this, fieldOfThisInstance).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new NumberExpression representing the column and supplying a
	 * default value for NULL entries.
	 *
	 * <p>
	 * This method is an easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * @return A NumberExpression representing the supplied field with a default
	 * for DBNULL values
	 */
	public NumberExpression column(Integer fieldOfThisInstance, NumberResult valueToUseIfDBNull) {
		return new IntegerColumn(this, fieldOfThisInstance).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new NumberExpression representing the column and supplying a
	 * default value for NULL entries.
	 *
	 * <p>
	 * This method is an easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * @return A NumberExpression representing the supplied field with a default
	 * for DBNULL values
	 */
	public NumberExpression column(DBInteger fieldOfThisInstance, Integer valueToUseIfDBNull) {
		return new IntegerColumn(this, fieldOfThisInstance).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new NumberExpression representing the column and supplying a
	 * default value for NULL entries.
	 *
	 * <p>
	 * This method is an easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * @return A NumberExpression representing the supplied field with a default
	 * for DBNULL values
	 */
	public NumberExpression column(DBInteger fieldOfThisInstance, Long valueToUseIfDBNull) {
		return new IntegerColumn(this, fieldOfThisInstance).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new NumberExpression representing the column and supplying a
	 * default value for NULL entries.
	 *
	 * <p>
	 * This method is an easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * @return A NumberExpression representing the supplied field with a default
	 * for DBNULL values
	 */
	public NumberExpression column(DBInteger fieldOfThisInstance, NumberResult valueToUseIfDBNull) {
		return new IntegerColumn(this, fieldOfThisInstance).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new NumberExpression representing the column and supplying a
	 * default value for NULL entries.
	 *
	 * <p>
	 * This method is an easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * @return A NumberExpression representing the supplied field with a default
	 * for DBNULL values
	 */
	public NumberExpression column(Long fieldOfThisInstance, Integer valueToUseIfDBNull) {
		return new IntegerColumn(this, fieldOfThisInstance).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new NumberExpression representing the column and supplying a
	 * default value for NULL entries.
	 *
	 * <p>
	 * This method is an easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * @return A NumberExpression representing the supplied field with a default
	 * for DBNULL values
	 */
	public NumberExpression column(Long fieldOfThisInstance, Long valueToUseIfDBNull) {
		return new IntegerColumn(this, fieldOfThisInstance).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new NumberExpression representing the column and supplying a
	 * default value for NULL entries.
	 *
	 * <p>
	 * This method is an easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * @return A NumberExpression representing the supplied field with a default
	 * for DBNULL values
	 */
	public NumberExpression column(Long fieldOfThisInstance, NumberResult valueToUseIfDBNull) {
		return new IntegerColumn(this, fieldOfThisInstance).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new DateColumn instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	 fieldOfThisInstance	
	 * @return A Column representing the supplied field
	 */
	public DateColumn column(DBDate fieldOfThisInstance) {
		return new DateColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new DateColumn instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	 fieldOfThisInstance	
	 * @return A Column representing the supplied field
	 */
	public DateColumn column(Date fieldOfThisInstance) {
		return new DateColumn(this, fieldOfThisInstance);
	}

	@Override
	public String toString() {
		StringBuilder string = new StringBuilder();
		List<PropertyWrapper> fields = getWrapper().getPropertyWrappers();

		String separator = "" + this.getClass().getSimpleName();

		for (PropertyWrapper field : fields) {
			if (field.isColumn()) {
				string.append(separator);
				string.append(" ");
				string.append(field.javaName());
				string.append(":");
				string.append(field.getQueryableDatatype());
				separator = ",";
			}
		}
		return string.toString();
	}

	public String toSeparatedLine(String separatorToUse) {
		StringBuilder string = new StringBuilder();
		List<PropertyWrapper> fields = getWrapper().getPropertyWrappers();

		String separator = "" + this.getClass().getSimpleName();

		for (PropertyWrapper field : fields) {
			if (field.isColumn()) {
				string.append(separator).append(field.getQueryableDatatype().toString().trim().replaceAll("\"", "\"\""));
				separator = separatorToUse;
			}
		}
		return string.toString();
	}

	public String toSeparatedHeader(String separatorToUse) {
		StringBuilder string = new StringBuilder();
		List<PropertyWrapper> fields = getWrapper().getPropertyWrappers();

		String separator = "" + this.getClass().getSimpleName();

		for (PropertyWrapper field : fields) {
			if (field.isColumn()) {
				string.append(separator).append(field.javaName());
				separator = separatorToUse;
			}
		}
		return string.toString();
	}

	public String toCSVHeader() {
		return toSeparatedHeader(",");
	}

	public String toCSVLine() {
		return toSeparatedLine(",");
	}

	public String toTabbedHeader() {
		return toSeparatedHeader("\t");
	}

	public String toTabbedLine() throws IllegalArgumentException, IllegalAccessException {
		return toSeparatedLine("\t");
	}

	public String getHTMLTableRow(String tableRowCSSClass) {
		StringBuilder string = new StringBuilder();
		List<PropertyWrapper> fields = getWrapper().getPropertyWrappers();

		string.append("<tr class=\"").append(tableRowCSSClass).append("\">");
		for (PropertyWrapper field : fields) {
			if (field.isColumn()) {
				string.append("<td>");
				string.append(field.getQueryableDatatype().stringValue());
				string.append("</td>");
			}
		}
		string.append("</tr>");
		return string.toString();
	}

	public String getHTMLTableHeader(String tableRowCSSClass, String tableHeaderCellCSSClass) {
		StringBuilder string = new StringBuilder();
		List<PropertyWrapper> fields = getWrapper().getPropertyWrappers();

		string.append("<tr class=\"").append(tableRowCSSClass).append("\">");
		for (PropertyWrapper field : fields) {
			if (field.isColumn()) {
				string.append("<th class=\"").append(tableHeaderCellCSSClass).append("\">");
				string.append(field.javaName());
				string.append("</th>");
			}
		}
		string.append("</tr>");
		return string.toString();
	}

	/**
	 * Returns all the fields names from all fields in this DBRow.
	 *
	 * <p>
	 * This is essentially a list of all the columns returned from the database
	 * query.
	 *
	 * <p>
	 * Column data may not have been populated.
	 *
	 * <p>
	 * Please note this is a crude instrument for accessing the data in this
	 * DBRow. You should probably be using the fields and methods of the actual
	 * DBRow class.
	 *
	 * @return a list of field names.
	 
	 */
	public List<String> getFieldNames() {
		List<String> returnList = new ArrayList<String>();
		List<PropertyWrapperDefinition> fieldDefns = this.getReturnColumns();
		for (PropertyWrapperDefinition prop : fieldDefns) {
			returnList.add(prop.javaName());
		}
		return returnList;
	}

	/**
	 * Returns all the fields values from this DBRow.
	 *
	 * <p>
	 * This is essentially a list of all the values returned from the database
	 * query.
	 *
	 * <p>
	 * Please note this is a crude instrument for accessing the data in this
	 * DBRow. You should probably be using the fields and methods of the DBRow
	 * class.
	 *
	 * @param dateFormat	 dateFormat	
	 * @return a list of field names.
	 
	 */
	public Collection<? extends String> getFieldValues(SimpleDateFormat dateFormat) {
		List<String> returnList = new ArrayList<String>();
		for (PropertyWrapperDefinition prop : this.getReturnColumns()) {
			QueryableDatatype qdt = prop.getQueryableDatatype(this);
			if (dateFormat != null && DBDate.class.isAssignableFrom(qdt.getClass())) {
				DBDate dBDate = (DBDate) qdt;
				Date dateValue = dBDate.dateValue();
				if (dateValue != null) {
					returnList.add(dateFormat.format(dateValue));
				} else {
					returnList.add(qdt.stringValue());
				}
			} else {
				returnList.add(qdt.stringValue());
			}
		}
		return returnList;
	}

	protected List<PropertyWrapperDefinition> getReturnColumns() {
		if (returnColumns == null) {
			returnColumns = this.getAllPropertyWrapperDefinitions();
		}
		return returnColumns;
	}

	protected void setReturnColumns(List<PropertyWrapperDefinition> returnColumns) {
		this.returnColumns = returnColumns;
	}

	protected List<PropertyWrapperDefinition> getAllPropertyWrapperDefinitions() {
		List<PropertyWrapperDefinition> columns = new ArrayList<PropertyWrapperDefinition>();
		List<PropertyWrapper> propertyWrappers = this.getPropertyWrappers();
		for (PropertyWrapper propertyWrapper : propertyWrappers) {
			columns.add(propertyWrapper.getDefinition());
		}
		return columns;
	}

}
