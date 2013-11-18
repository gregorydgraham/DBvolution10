package nz.co.gregs.dbvolution.datatypes;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.internal.SafeOneWaySimpleTypeAdaptor;
import nz.co.gregs.dbvolution.internal.SafeOneWaySimpleTypeAdaptor.Direction;

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
//	private DBSafeInternalQDTAdaptor toExternalQDTAdaptor;
//	private DBSafeInternalQDTAdaptor toInternalQDTAdaptor;
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
	
	private void setTargetQDTFromSourceQDT(Direction direction, QueryableDatatype targetQdt, QueryableDatatype sourceQdt) {
		setTargetQDTFromSourceQDT(null, direction, targetQdt, sourceQdt);
	}
	
	private void setTargetQDTFromSourceQDT(DBSafeInternalQDTAdaptor qdtAdaptor, Direction direction, QueryableDatatype targetQdt, QueryableDatatype sourceQdt) {
		targetQdt.changed = sourceQdt.changed;
		targetQdt.includingNulls = sourceQdt.includingNulls;
		targetQdt.invertOperator = sourceQdt.invertOperator;
		targetQdt.isDBNull = sourceQdt.isDBNull;
		targetQdt.isPrimaryKey = sourceQdt.isPrimaryKey;
		targetQdt.undefined = sourceQdt.undefined;
		targetQdt.sort = sourceQdt.sort;
		
		// copy literal value with translation
//		targetQdt.literalValue = adaptValue(direction, sourceQdt.literalValue);
		if (direction == Direction.TO_INTERNAL) {
			targetQdt.literalValue = toInternalSimpleTypeAdaptor.convert(sourceQdt.literalValue);
		}
		else {
			targetQdt.literalValue = toExternalSimpleTypeAdaptor.convert(sourceQdt.literalValue);
		}
		
		// copy operator with translation
		// TODO call operator.copyAndTranslate(new DBSafeInternalTypeAdaptor(typeAdaptor))
		//   class DBSafeInternalTypeAdaptor {
		//       public QueryableDatatype convert(QueryableDatatype qdt);
	    //   }
		// Needs to detect where DBOperator is created as QueryableDatatype.do{new DBOperator(this)};
		//  (eg: in QueryableDatatype.setValue())
		//targetQdt.operator = sourceQdt.operator; // TODO: translate
		if (direction == Direction.TO_INTERNAL) {
			if (!targetQdt.getClass().equals(internalQdtType)) {
				throw new RuntimeException("Don't know what to do here: targetQdtType:"+targetQdt.getClass().getSimpleName()+" != "+internalQdtType+":"+internalQdtType.getSimpleName());
			}
			
			DBSafeInternalQDTAdaptor toInternalQDTAdaptor = (qdtAdaptor != null) ? qdtAdaptor :
					new DBSafeInternalQDTAdaptor(this, direction, internalQdtType, toInternalSimpleTypeAdaptor);
			targetQdt.operator = sourceQdt.operator.copyAndAdapt(toInternalQDTAdaptor);
		}
		else {
			DBSafeInternalQDTAdaptor toExternalQDTAdaptor = (qdtAdaptor != null) ? qdtAdaptor :
					new DBSafeInternalQDTAdaptor(this, direction, targetQdt.getClass(), toExternalSimpleTypeAdaptor);
			targetQdt.operator = sourceQdt.operator.copyAndAdapt(toExternalQDTAdaptor);
		}
		
		// copy previous value with translation
		//sourceQdt.previousValueAsQDT = // TODO 
	}

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
	
	public static class DBSafeInternalQDTAdaptor {
		private QueryableDatatypeSyncer syncer;
		private Direction direction;
		private Class<? extends QueryableDatatype> targetQdtType;
		private SafeOneWaySimpleTypeAdaptor typeAdaptor;
		
		public DBSafeInternalQDTAdaptor(
				QueryableDatatypeSyncer syncer,
				Direction direction,
				Class<? extends QueryableDatatype> targetQdtType,
				SafeOneWaySimpleTypeAdaptor typeAdaptor) {
			this.syncer = syncer;
			this.direction = direction;
			this.targetQdtType = targetQdtType;
			this.typeAdaptor = typeAdaptor;
		}

		@SuppressWarnings("synthetic-access")
		public QueryableDatatype convert(QueryableDatatype qdt) {
			try {
				QueryableDatatype result = convertInternal(qdt);
				log.info(typeAdaptor+" converting "+qdtToString(qdt)+" ==> "+qdtToString(result));
				return result;
			} catch (RuntimeException e) {
				log.info(typeAdaptor+" converting "+qdtToString(qdt)+" ==> "+e.getClass().getSimpleName());
				throw e;
			}
		}
		
		private QueryableDatatype convertInternal(QueryableDatatype sourceQdt) {
//			QueryableDatatype targetQdt = newTargetQDT();
//			syncer.setTargetQDTFromSourceQDT(this, direction, targetQdt, sourceQdt);
//			return targetQdt;
			
//			if (qdt == null) {
//				return null;
//			}
//			else if (qdt instanceof DBInteger) {
//				
//			}
			throw new UnsupportedOperationException(typeAdaptor+" trying to convert from "+qdtToString(sourceQdt));
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
