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
package nz.co.gregs.dbvolution.datatypes;

/**
 * Syncs between a simple-type external value and a QDT internal value.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Malcolm Lett
 */
public class SimpleValueQueryableDatatypeSyncer extends QueryableDatatypeSyncer {

	private static final long serialVersionUID = 1l;

	/**
	 *
	 * @param propertyName used in error messages
	 * @param internalQdtType internalQdtType
	 * @param internalQdtLiteralType internalQdtLiteralType
	 * @param typeAdaptor typeAdaptor
	 * @param externalSimpleType externalSimpleType externalSimpleType
	 *
	 *
	 *
	 *
	 */
	public SimpleValueQueryableDatatypeSyncer(String propertyName, Class<? extends QueryableDatatype<?>> internalQdtType,
			Class<?> internalQdtLiteralType, Class<?> externalSimpleType, DBTypeAdaptor<Object, Object> typeAdaptor) {
		super(propertyName, internalQdtType, internalQdtLiteralType, externalSimpleType, typeAdaptor);
	}

	/**
	 * Sets the cached internal QDT value from the provided non-QDT external
	 * value.
	 *
	 * @param externalValue may be null
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the updated internal QDT
	 */
	public QueryableDatatype<?> setInternalQDTFromExternalSimpleValue(Object externalValue) {
		Object internalValue = getToInternalSimpleTypeAdaptor().convert(externalValue);
		QueryableDatatype<?> internalQDT = getInternalQueryableDatatype();
		if (internalValue == null) {
			// TODO complete this
			internalQDT.setLiteralValue(null);
			internalQDT.setDefined(false);
			internalQDT.setOperator(null);
			internalQDT.setChanged(false);
			internalQDT.setPreviousValue(null);
		} else {
			setQDTValueUsingDangerousReflection(internalQDT, internalValue);
		}
		return internalQDT;
	}

	/**
	 * Warning: this directly returns the value from the type adaptor, without
	 * casting to the specific type expected by the target java property.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the internal value as a base Java object
	 */
	public Object getExternalSimpleValueFromInternalQDT() {
		return getToExternalSimpleTypeAdaptor().convert(getInternalQueryableDatatype().getValue());
	}
}
