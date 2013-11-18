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
	
	//protected enum Direction {TO_INTERNAL, TO_EXTERNAL}
	protected final String propertyName;
	protected final DBTypeAdaptor<Object, Object> typeAdaptor;
	protected final Class<? extends QueryableDatatype> internalQdtType;
	protected QueryableDatatype internalQdt;
	protected SafeOneWaySimpleTypeAdaptor toExternalSimpleTypeAdaptor;
	protected SafeOneWaySimpleTypeAdaptor toInternalSimpleTypeAdaptor;
//	private DBSafeInternalQDTAdaptor toExternalQDTAdaptor;
//	private DBSafeInternalQDTAdaptor toInternalQDTAdaptor;

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
		
		//this.toExternalQDTAdaptor = new DBSafeInternalQDTAdaptor(toExternalSimpleTypeAdaptor);
		//this.toInternalQDTAdaptor = new DBSafeInternalQDTAdaptor(toInternalSimpleTypeAdaptor);

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
		this.internalQdt = internalQdt;
	}
	
	public void setInternalQDTFromExternalQDT(QueryableDatatype externalQdt) {
		setTargetQDTFromSourceQDT(Direction.TO_INTERNAL, internalQdt, externalQdt);
	}

	public void setExternalFromInternalQDT(QueryableDatatype externalQdt) {
		setTargetQDTFromSourceQDT(Direction.TO_EXTERNAL, externalQdt, internalQdt);
	}

	// TODO subsume this into the two methods above
	private void setTargetQDTFromSourceQDT(Direction direction, QueryableDatatype targetQdt, QueryableDatatype sourceQdt) {
		DBSafeInternalQDTAdaptor qdtAdaptor;
		if (direction == Direction.TO_INTERNAL) {
			if (!targetQdt.getClass().equals(internalQdtType)) {
				throw new RuntimeException("Don't know what to do here: targetQdtType:"+targetQdt.getClass().getSimpleName()+" != "+internalQdtType+":"+internalQdtType.getSimpleName());
			}
			
			qdtAdaptor = new DBSafeInternalQDTAdaptor(internalQdtType, toInternalSimpleTypeAdaptor);
		}
		else {
			qdtAdaptor = new DBSafeInternalQDTAdaptor(targetQdt.getClass(), toExternalSimpleTypeAdaptor);
		}
		
		qdtAdaptor.setTargetQDTFromSourceQDT(targetQdt, sourceQdt);
		//setTargetQDTFromSourceQDT(null, direction, targetQdt, sourceQdt);
	}
	
//	private void setTargetQDTFromSourceQDT(DBSafeInternalQDTAdaptor qdtAdaptor, Direction direction, QueryableDatatype targetQdt, QueryableDatatype sourceQdt) {
//		targetQdt.changed = sourceQdt.changed;
//		targetQdt.includingNulls = sourceQdt.includingNulls;
//		targetQdt.invertOperator = sourceQdt.invertOperator;
//		targetQdt.isDBNull = sourceQdt.isDBNull;
//		targetQdt.isPrimaryKey = sourceQdt.isPrimaryKey;
//		targetQdt.undefined = sourceQdt.undefined;
//		targetQdt.sort = sourceQdt.sort;
//		
//		// copy literal value with translation
////		targetQdt.literalValue = adaptValue(direction, sourceQdt.literalValue);
//		if (direction == Direction.TO_INTERNAL) {
//			targetQdt.literalValue = toInternalSimpleTypeAdaptor.convert(sourceQdt.literalValue);
//		}
//		else {
//			targetQdt.literalValue = toExternalSimpleTypeAdaptor.convert(sourceQdt.literalValue);
//		}
//		
//		// copy operator with translation
//		// TODO call operator.copyAndTranslate(new DBSafeInternalTypeAdaptor(typeAdaptor))
//		//   class DBSafeInternalTypeAdaptor {
//		//       public QueryableDatatype convert(QueryableDatatype qdt);
//	    //   }
//		// Needs to detect where DBOperator is created as QueryableDatatype.do{new DBOperator(this)};
//		//  (eg: in QueryableDatatype.setValue())
//		//targetQdt.operator = sourceQdt.operator; // TODO: translate
//		if (direction == Direction.TO_INTERNAL) {
//			if (!targetQdt.getClass().equals(internalQdtType)) {
//				throw new RuntimeException("Don't know what to do here: targetQdtType:"+targetQdt.getClass().getSimpleName()+" != "+internalQdtType+":"+internalQdtType.getSimpleName());
//			}
//			
//			DBSafeInternalQDTAdaptor toInternalQDTAdaptor = (qdtAdaptor != null) ? qdtAdaptor :
//					new DBSafeInternalQDTAdaptor(this, direction, internalQdtType, toInternalSimpleTypeAdaptor);
//			targetQdt.operator = sourceQdt.operator.copyAndAdapt(toInternalQDTAdaptor);
//		}
//		else {
//			DBSafeInternalQDTAdaptor toExternalQDTAdaptor = (qdtAdaptor != null) ? qdtAdaptor :
//					new DBSafeInternalQDTAdaptor(this, direction, targetQdt.getClass(), toExternalSimpleTypeAdaptor);
//			targetQdt.operator = sourceQdt.operator.copyAndAdapt(toExternalQDTAdaptor);
//		}
//		
//		// copy previous value with translation
//		//sourceQdt.previousValueAsQDT = // TODO 
//	}

//	protected Object adaptValue(Direction direction, Object sourceLiteralValue) {
//		if (direction == Direction.TO_INTERNAL) {
//			return toInternalSimpleTypeAdaptor.convert(sourceLiteralValue);
//		}
//		else {
//			return toExternalSimpleTypeAdaptor.convert(sourceLiteralValue);
//		}
//	}
	
//	protected Object adaptValueToInternal(Object externalLiteralValue) {
//		return toInternalSimpleTypeAdaptor.convert(externalLiteralValue);
//	}
	
//	protected Object adaptValueToExternal(Object internalLiteralValue) {
//		return toExternalSimpleTypeAdaptor.convert(internalLiteralValue);
//	}
	
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
		 * @param qdt the QDT to convert to the target type
		 * @return the newly created QDT of the target type
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
				
				QueryableDatatype targetQdt = newTargetQDT();
				setTargetQDTFromSourceQDT(targetQdt, sourceQdt);
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
		 * @param targetQdt the QDT to update
		 * @param sourceQdt the QDT with values to convert and copy to the target
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
			
			// copy operator with translation
			// TODO call operator.copyAndTranslate(new DBSafeInternalTypeAdaptor(typeAdaptor))
			//   class DBSafeInternalTypeAdaptor {
			//       public QueryableDatatype convert(QueryableDatatype qdt);
		    //   }
			// Needs to detect where DBOperator is created as QueryableDatatype.do{new DBOperator(this)};
			//  (eg: in QueryableDatatype.setValue())
			if (sourceQdt.operator == null) {
				targetQdt.operator = null;
			}
			else {
				targetQdt.operator = sourceQdt.operator.copyAndAdapt(this);
			}
			
			// copy previous value with translation
			//sourceQdt.previousValueAsQDT = // TODO 
		}
		
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
