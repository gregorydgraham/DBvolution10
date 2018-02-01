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

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import nz.co.gregs.dbvolution.expressions.DBExpression;

import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.internal.properties.SafeOneWaySimpleTypeAdaptor;
import nz.co.gregs.dbvolution.internal.properties.SafeOneWaySimpleTypeAdaptor.Direction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Allows synchronizations to be done between two QueryableDatatypes, based on a
 * Type Adaptor.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Malcolm Lett
 */
public class QueryableDatatypeSyncer implements Serializable{

	private static final long serialVersionUID = 1l;

	private static final Log LOG = LogFactory.getLog(QueryableDatatypeSyncer.class);

	private final String propertyName;
//	private final DBTypeAdaptor<Object, Object> typeAdaptor;
	private final Class<? extends QueryableDatatype<?>> internalQdtType;
	private QueryableDatatype<?> internalQdt;
	private SafeOneWaySimpleTypeAdaptor toExternalSimpleTypeAdaptor;
	private SafeOneWaySimpleTypeAdaptor toInternalSimpleTypeAdaptor;

	/**
	 *
	 * @param propertyName used in error messages
	 * @param internalQdtType internalQdtType
	 * @param internalQdtLiteralType internalQdtLiteralType
	 * @param externalSimpleType externalSimpleType
	 * @param typeAdaptor typeAdaptor typeAdaptor
	 */
	public QueryableDatatypeSyncer(
			String propertyName,
			Class<? extends QueryableDatatype<?>> internalQdtType,
			Class<?> internalQdtLiteralType,
			Class<?> externalSimpleType,
			DBTypeAdaptor<Object, Object> typeAdaptor) {
		if (typeAdaptor == null) {
			throw new DBRuntimeException("Null typeAdaptor was passed, this is an internal bug");
		}
		this.propertyName = propertyName;
//		this.typeAdaptor = typeAdaptor;
		this.internalQdtType = internalQdtType;
		this.toExternalSimpleTypeAdaptor = new SafeOneWaySimpleTypeAdaptor(propertyName,
				typeAdaptor, Direction.TO_EXTERNAL, internalQdtLiteralType, externalSimpleType);

		this.toInternalSimpleTypeAdaptor = new SafeOneWaySimpleTypeAdaptor(propertyName,
				typeAdaptor, Direction.TO_INTERNAL, externalSimpleType, internalQdtLiteralType);

		try {
			this.internalQdt = internalQdtType.newInstance();
		} catch (InstantiationException e) {
			// TODO produce a better error message that is consistent with how this is handled elsewhere
			throw new DBRuntimeException("Instantiation error creating internal "
					+ internalQdtType.getSimpleName() + " QDT: " + e.getMessage(), e);
		} catch (IllegalAccessException e) {
			// TODO produce a better error message that is consistent with how this is handled elsewhere
			throw new DBRuntimeException("Access error creating internal "
					+ internalQdtType.getSimpleName() + " QDT: " + e.getMessage(), e);
		}
	}

	/**
	 * supplies the QDT used internally, that is the QDT the represents the
	 * database's view of the data.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the internal QDT.
	 */
	public QueryableDatatype<?> getInternalQueryableDatatype() {
		return internalQdt;
	}

	/**
	 * Replaces the internal QDT with the one provided. Validates that the
	 * provided QDT is of the correct type.
	 *
	 * @param internalQdt	internalQdt
	 */
	public void setInternalQueryableDatatype(QueryableDatatype<?> internalQdt) {
		if (internalQdt != null && !internalQdt.getClass().equals(internalQdtType)) {
			//throw new RuntimeException("Don't know what to do here: targetQdtType:"+internalQdt.getClass().getSimpleName()+" != "+internalQdtType+":"+internalQdtType.getSimpleName());
			throw new ClassCastException("Cannot assign " + internalQdt.getClass().getSimpleName()
					+ " to " + internalQdtType.getSimpleName() + " property " + propertyName);
		}
		this.internalQdt = internalQdt;
	}

	/**
	 * Sets the cached internal QDT after adapting the value from the provided
	 * QDT.
	 *
	 * @param externalQdt may be null
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the updated internal QDT
	 */
	public QueryableDatatype<?> setInternalQDTFromExternalQDT(QueryableDatatype<?> externalQdt) {
		if (externalQdt == null) {
			internalQdt = null;
		} else {
			DBSafeInternalQDTAdaptor qdtAdaptor = new DBSafeInternalQDTAdaptor(internalQdtType, getToInternalSimpleTypeAdaptor());
			qdtAdaptor.setTargetQDTFromSourceQDT(getInternalQueryableDatatype(), externalQdt);
		}
		return getInternalQueryableDatatype();
	}

	/**
	 * Sets the provided external QDT from the internal QDT and returns the
	 * updated external QDT.
	 *
	 * @param externalQdt	externalQdt
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the updated external QDT or null if the internal QDT is null
	 */
	public QueryableDatatype<?> setExternalFromInternalQDT(QueryableDatatype<?> externalQdt) {
		if (getInternalQueryableDatatype() == null) {
			return null;
		} else {
			@SuppressWarnings("unchecked")
			DBSafeInternalQDTAdaptor qdtAdaptor = new DBSafeInternalQDTAdaptor((Class<? extends QueryableDatatype<?>>) externalQdt.getClass(), getToExternalSimpleTypeAdaptor());
			qdtAdaptor.setTargetQDTFromSourceQDT(externalQdt, getInternalQueryableDatatype());
		}
		return externalQdt;
	}

	// for DEBUG purposes only
	static String qdtToString(QueryableDatatype<?> qdt) {
		String literalStr;
		if (qdt == null) {
			literalStr = null;
		} else if (qdt.getLiteralValue() == null) {
			literalStr = "null";
		} else {
			literalStr = qdt.getLiteralValue().getClass().getSimpleName() + "[" + qdt.getLiteralValue() + "]";
		}
		StringBuilder buf = new StringBuilder();
		if (qdt == null) {
			buf.append("null");
		} else {
			buf.append(qdt.getClass().getSimpleName());
			buf.append("[");
			buf.append(qdt);
			buf.append(", ");
			buf.append("literal=").append(literalStr);
			if (qdt.getOperator() != null) {
				buf.append(", ");
				buf.append(qdt.getOperator().getClass().getSimpleName());
			}
			buf.append("]");
		}
		return buf.toString();
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the toExternalSimpleTypeAdaptor
	 */
	protected SafeOneWaySimpleTypeAdaptor getToExternalSimpleTypeAdaptor() {
		return toExternalSimpleTypeAdaptor;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the toInternalSimpleTypeAdaptor
	 */
	protected SafeOneWaySimpleTypeAdaptor getToInternalSimpleTypeAdaptor() {
		return toInternalSimpleTypeAdaptor;
	}

	/**
	 *
	 * @param internalQDT
	 * @param internalValue
	 */
	protected static final void setQDTValueUsingDangerousReflection(QueryableDatatype<?> internalQDT, Object internalValue) {
		try {
			// TODO what type checking can/should be done here?
			//internalQDT.setValue(internalValue);
			if (internalValue == null) {
				internalQDT.setToNull();
			} else {
				Method method = internalQDT.getClass().getMethod("setValue", internalValue.getClass());
				method.invoke(internalQDT, internalValue);
			}
		} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
			throw new DBRuntimeException("Synchronisation Failed:" + ex.getMessage(), ex);
		}

	}

	/**
	 * One-shot cycle-aware recursive QDT adaptor. Converts from existing QDT to
	 * brand new one, and copies from one QDT to another.
	 *
	 * <p>
	 * DBOperators can reference the same QDT that own the operator instance, such
	 * as:
	 * <code>QueryableDatatype.setLiteralValue{this.operator = new DBEqualsOperator(this)}</code>.
	 * Cycles are handled by tracking source QDTs observed and returning the
	 * previously mapped target QDT when re-observed.
	 *
	 * <p>
	 * Must be used only once for a given read or write of a field.
	 */
	public static class DBSafeInternalQDTAdaptor {

		private final Class<? extends QueryableDatatype<?>> targetQdtType;
		private final SafeOneWaySimpleTypeAdaptor simpleTypeAdaptor;
		private final List<Map.Entry<QueryableDatatype<?>, QueryableDatatype<?>>> observedSourcesAndTargets
				= new ArrayList<>();

		/**
		 * Constructor
		 *
		 * @param targetQdtType targetQdtType
		 * @param typeAdaptor typeAdaptor
		 */
		public DBSafeInternalQDTAdaptor(
				Class<? extends QueryableDatatype<?>> targetQdtType,
				SafeOneWaySimpleTypeAdaptor typeAdaptor) {
			this.targetQdtType = targetQdtType;
			this.simpleTypeAdaptor = typeAdaptor;
		}

		/**
		 * Creates a brand new QDT of the configured target type, based on converted
		 * values from the given QDT. Recursively traverses the operators and inner
		 * QDT references within the given QDT.
		 *
		 * <p>
		 * If {@code source} is null, returns {@code null}.
		 *
		 * @param source the QDT to convert to the target type, may be null
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 * @return the newly created QDT of the target type, or null if
		 * {@code source} was null
		 */
		public DBExpression convert(DBExpression source) {
			if (!(source instanceof QueryableDatatype<?>)) {
				return source;
			} else {
				QueryableDatatype<?> sourceQDT = (QueryableDatatype<?>) source;
				try {
					// cycle-detection
					// (note: important that it uses reference equality, not object equality)
					for (Map.Entry<QueryableDatatype<?>, QueryableDatatype<?>> sourceAndTarget : observedSourcesAndTargets) {
						if (sourceAndTarget.getKey() == sourceQDT) {
							// re-use existing value
							return sourceAndTarget.getValue();
						}
					}

					QueryableDatatype<?> targetQdt = newTargetQDT();
					setTargetQDTFromSourceQDT(targetQdt, sourceQDT);

					LOG.debug(simpleTypeAdaptor + " converting " + qdtToString(sourceQDT) + " ==> " + qdtToString(targetQdt));
					return targetQdt;
				} catch (RuntimeException e) {
					LOG.debug(simpleTypeAdaptor + " converting " + qdtToString(sourceQDT) + " ==> " + e.getClass().getSimpleName());
					throw e;
				}
			}
		}

		/**
		 * Updates the target QDT with converted values from the source QDT.
		 * Recursively traverses the operations and inner QDT references within the
		 * given source QTD.
		 *
		 * @param targetQdt the QDT to update (must not be null)
		 * @param sourceQdt the QDT with values to convert and copy to the target
		 * (must not be null)
		 */
		@SuppressWarnings("unchecked")
		protected void setTargetQDTFromSourceQDT(QueryableDatatype<?> targetQdt, QueryableDatatype<?> sourceQdt) {
			// sanity checks
			if (!targetQdt.getClass().equals(targetQdtType)) {
				throw new RuntimeException("Don't know what to do here: targetQdtType:"
						+ targetQdt.getClass().getSimpleName() + " != " + targetQdtType + ":" + targetQdtType.getSimpleName());
			}

			// cycle-detection
			// (note: important that it uses reference equality, not object equality)
			for (Map.Entry<QueryableDatatype<?>, QueryableDatatype<?>> soFarEntry : observedSourcesAndTargets) {
				if (soFarEntry.getKey() == sourceQdt) {
					// already observed, so already done.
					return;
				}
			}
			observedSourcesAndTargets.add(new SimpleEntry<QueryableDatatype<?>, QueryableDatatype<?>>(sourceQdt, targetQdt));

			// copy simple fields
			targetQdt.setChanged(sourceQdt.hasChanged());
			if (sourceQdt.isNull()) {
				targetQdt.setToNull();
			}
//			targetQdt.isPrimaryKey = sourceQdt.isPrimaryKey;
			targetQdt.setDefined(sourceQdt.isDefined());
			if (sourceQdt.getSortOrder().equals(QueryableDatatype.SORT_ASCENDING)) {
				targetQdt.setSortOrderAscending();
			} else {
				targetQdt.setSortOrderDescending();
			}
			targetQdt.setColumnExpression(sourceQdt.getColumnExpression());

			// copy literal value with translation
			//targetQdt.setLiteralValue(simpleTypeAdaptor.convert(sourceQdt.getLiteralValue()));
			setQDTValueUsingDangerousReflection(targetQdt, simpleTypeAdaptor.convert(sourceQdt.getLiteralValue()));

			// copy previous value with translation
			targetQdt.setPreviousValue((QueryableDatatype) convert(sourceQdt.getPreviousValue()));

			// copy operator with translation
			if (sourceQdt.getOperator() == null) {
				targetQdt.setOperator(null);
			} else {
				targetQdt.setOperator(sourceQdt.getOperator().copyAndAdapt(this));
			}
		}

		// factory method
		private QueryableDatatype<?> newTargetQDT() {
			try {
				return targetQdtType.newInstance();
			} catch (InstantiationException e) {
				// TODO produce a better error message that is consistent with how this is handled elsewhere
				throw new DBRuntimeException("Instantiation error creating internal "
						+ targetQdtType.getSimpleName() + " QDT: " + e.getMessage(), e);
			} catch (IllegalAccessException e) {
				// TODO produce a better error message that is consistent with how this is handled elsewhere
				throw new DBRuntimeException("Access error creating internal "
						+ targetQdtType.getSimpleName() + " QDT: " + e.getMessage(), e);
			}
		}
	}
}
