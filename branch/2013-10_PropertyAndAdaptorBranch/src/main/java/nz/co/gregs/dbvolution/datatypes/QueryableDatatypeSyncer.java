package nz.co.gregs.dbvolution.datatypes;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.internal.SafeOneWaySimpleTypeAdaptor;
import nz.co.gregs.dbvolution.internal.SafeOneWaySimpleTypeAdaptor.Direction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Allows synchronisations to be done between two QueryableDatatypes,
 * based on a Type Adaptor.
 * @author Malcolm Lett
 */
// TODO come up with a better name
public class QueryableDatatypeSyncer {
	public static final Log log = LogFactory.getLog(QueryableDatatypeSyncer.class);
	
	protected final String propertyName;
	protected final DBTypeAdaptor<Object, Object> typeAdaptor;
	protected final Class<? extends QueryableDatatype> internalQdtType;
	protected QueryableDatatype internalQdt;
	protected SafeOneWaySimpleTypeAdaptor toExternalSimpleTypeAdaptor;
	protected SafeOneWaySimpleTypeAdaptor toInternalSimpleTypeAdaptor;

	/**
	 * 
	 * @param propertyName used in error messages
	 * @param internalQdtType
	 * @param typeAdaptor
	 */
	public QueryableDatatypeSyncer(String propertyName, Class<? extends QueryableDatatype> internalQdtType,
			DBTypeAdaptor<Object, Object> typeAdaptor) {
		if (typeAdaptor == null) {
			throw new DBRuntimeException("Null typeAdaptor was passed, this is an internal bug");
		}
		this.propertyName = propertyName;
		this.typeAdaptor = typeAdaptor;
		this.internalQdtType = internalQdtType;
		this.toExternalSimpleTypeAdaptor = new SafeOneWaySimpleTypeAdaptor(propertyName,
				typeAdaptor, Direction.TO_EXTERNAL, null, null);
		
		this.toInternalSimpleTypeAdaptor = new SafeOneWaySimpleTypeAdaptor(propertyName,
				typeAdaptor, Direction.TO_INTERNAL, null, null);
		
		try {
			this.internalQdt = internalQdtType.newInstance();
		} catch (InstantiationException e) {
			// TODO produce a better error message that is consistent with how this is handled elsewhere
			throw new DBRuntimeException("Instantiation error creating internal "
					+internalQdtType.getSimpleName()+" QDT: "+e.getMessage(), e);
		} catch (IllegalAccessException e) {
			// TODO produce a better error message that is consistent with how this is handled elsewhere
			throw new DBRuntimeException("Access error creating internal "
					+internalQdtType.getSimpleName()+" QDT: "+e.getMessage(), e);
		}
	}
	
	public QueryableDatatype getInternalQueryableDatatype() {
		return internalQdt;
	}

	public void setInternalQueryableDatatype(QueryableDatatype internalQdt) {
		if (internalQdt != null && !internalQdt.getClass().equals(internalQdtType)) {
			throw new RuntimeException("Don't know what to do here: targetQdtType:"+internalQdt.getClass().getSimpleName()+" != "+internalQdtType+":"+internalQdtType.getSimpleName());
		}
		this.internalQdt = internalQdt;
	}
	
	public void setInternalQDTFromExternalQDT(QueryableDatatype externalQdt) {
		//setTargetQDTFromSourceQDT(Direction.TO_INTERNAL, internalQdt, externalQdt);
		DBSafeInternalQDTAdaptor qdtAdaptor = new DBSafeInternalQDTAdaptor(internalQdtType, toInternalSimpleTypeAdaptor);
		qdtAdaptor.setTargetQDTFromSourceQDT(internalQdt, externalQdt);
	}

	public void setExternalFromInternalQDT(QueryableDatatype externalQdt) {
		//setTargetQDTFromSourceQDT(Direction.TO_EXTERNAL, externalQdt, internalQdt);
		DBSafeInternalQDTAdaptor qdtAdaptor = new DBSafeInternalQDTAdaptor(externalQdt.getClass(), toExternalSimpleTypeAdaptor);
		qdtAdaptor.setTargetQDTFromSourceQDT(externalQdt, internalQdt);
	}

	// for DEBUG purposes only
	private static String qdtToString(QueryableDatatype qdt) {
		String literalStr = (qdt == null) ? null : qdt.literalValue.getClass().getSimpleName()+"["+qdt.literalValue+"]";
		StringBuilder buf = new StringBuilder();
		if (qdt == null) {
			buf.append("null");
		}
		else {
			buf.append(qdt.getClass().getSimpleName());
			buf.append("[");
			buf.append(qdt);
			buf.append(", ");
			buf.append("literal=").append(literalStr);
			if (qdt.operator != null) {
				buf.append(", ");
				buf.append(qdt.operator.getClass().getSimpleName());
			}
			buf.append("]");
		}
		return buf.toString();
	}
	
	/**
	 * One-shot cycle-aware recursive QDT adaptor.
	 * Converts from existing QDT to brand new one,
	 * and copies from one QDT to another.
	 * 
	 * <p> DBOperators can reference the same QDT that own the
	 * operator instance, such as:
	 * <code>QueryableDatatype.setValue{this.operator = new DBEqualsOperator(this)}</code>.
	 * Cycles are handled by tracking source QDTs observed and returning
	 * the previously mapped target QDT when re-observed.
	 * 
	 * <p> Must be used only once for a given read or write of a field.
	 */
	public static class DBSafeInternalQDTAdaptor {
		private Class<? extends QueryableDatatype> targetQdtType;
		private SafeOneWaySimpleTypeAdaptor simpleTypeAdaptor;
		private List<Map.Entry<QueryableDatatype, QueryableDatatype>> soFar =
			new ArrayList<Map.Entry<QueryableDatatype,QueryableDatatype>>();
		
		public DBSafeInternalQDTAdaptor(
				Class<? extends QueryableDatatype> targetQdtType,
				SafeOneWaySimpleTypeAdaptor typeAdaptor) {
			this.targetQdtType = targetQdtType;
			this.simpleTypeAdaptor = typeAdaptor;
		}

		/**
		 * Creates a brand new QDT of the configured target type,
		 * based on converted values from the given QDT.
		 * Recursively traverses the operators and inner QDT references
		 * within the given QDT.
		 * 
		 * <p> If {@code sourceQdt} is null, returns {@cod null}.
		 * @param qdt the QDT to convert to the target type, may be null
		 * @return the newly created QDT of the target type, or null if {@code sourceQdt} was null
		 */
		@SuppressWarnings("synthetic-access")
		public QueryableDatatype convert(QueryableDatatype sourceQdt) {
			try {
				// cycle-detection
				// (note: important that it uses reference equality, not object equality)
				for (Map.Entry<QueryableDatatype, QueryableDatatype> soFarEntry: soFar) {
					if (soFarEntry.getKey() == sourceQdt) {
						// re-use existing value
						return soFarEntry.getValue();
					}
				}

				QueryableDatatype targetQdt = null;
				if (sourceQdt != null) {
					targetQdt = newTargetQDT();
					setTargetQDTFromSourceQDT(targetQdt, sourceQdt);
				}
				log.info(simpleTypeAdaptor+" converting "+qdtToString(sourceQdt)+" ==> "+qdtToString(targetQdt));
				return targetQdt;
			} catch (RuntimeException e) {
				log.info(simpleTypeAdaptor+" converting "+qdtToString(sourceQdt)+" ==> "+e.getClass().getSimpleName());
				throw e;
			}
		}

		/**
		 * Updates the target QDT with converted values from the source QDT.
		 * Recursively traverses the operations and inner QDT references within
		 * the given source QTD.
		 * @param targetQdt the QDT to update (must not be null)
		 * @param sourceQdt the QDT with values to convert and copy to the target (must not be null)
		 */
		protected void setTargetQDTFromSourceQDT(QueryableDatatype targetQdt, QueryableDatatype sourceQdt) {
			// sanity checks
			if (!targetQdt.getClass().equals(targetQdtType)) {
				throw new RuntimeException("Don't know what to do here: targetQdtType:"+
						targetQdt.getClass().getSimpleName()+" != "+targetQdtType+":"+targetQdtType.getSimpleName());
			}
			
			// cycle-detection
			// (note: important that it uses reference equality, not object equality)
			for (Map.Entry<QueryableDatatype, QueryableDatatype> soFarEntry: soFar) {
				if (soFarEntry.getKey() == sourceQdt) {
					// already observed, so already done.
					return;
				}
			}
			soFar.add(new SimpleEntry<QueryableDatatype, QueryableDatatype>(sourceQdt, targetQdt));
			
			// copy simple fields
			targetQdt.changed = sourceQdt.changed;
			targetQdt.includingNulls = sourceQdt.includingNulls;
			targetQdt.invertOperator = sourceQdt.invertOperator;
			targetQdt.isDBNull = sourceQdt.isDBNull;
			targetQdt.isPrimaryKey = sourceQdt.isPrimaryKey;
			targetQdt.undefined = sourceQdt.undefined;
			targetQdt.sort = sourceQdt.sort;
			
			// copy literal value with translation
			targetQdt.literalValue = simpleTypeAdaptor.convert(sourceQdt.literalValue);
			
			// copy previous value with translation
			targetQdt.previousValueAsQDT = convert(sourceQdt.previousValueAsQDT);
			
			// copy operator with translation
			if (sourceQdt.operator == null) {
				targetQdt.operator = null;
			}
			else {
				targetQdt.operator = sourceQdt.operator.copyAndAdapt(this);
			}
		}
		
		// factory method
		private QueryableDatatype newTargetQDT() {
			try {
				return targetQdtType.newInstance();
			} catch (InstantiationException e) {
				// TODO produce a better error message that is consistent with how this is handled elsewhere
				throw new DBRuntimeException("Instantiation error creating internal "
						+targetQdtType.getSimpleName()+" QDT: "+e.getMessage(), e);
			} catch (IllegalAccessException e) {
				// TODO produce a better error message that is consistent with how this is handled elsewhere
				throw new DBRuntimeException("Access error creating internal "
						+targetQdtType.getSimpleName()+" QDT: "+e.getMessage(), e);
			}
		}
	}
}
