package nz.co.gregs.dbvolution.datatypes;

import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.exceptions.DBThrownByEndUserCodeException;

/**
 * Allows synchronisations to be done between two QueryableDatatypes,
 * based on a Type Adaptor.
 * @author Malcolm Lett
 */
// TODO come up with a better name
public class QueryableDatatypeSyncer {
	protected enum Direction {TO_INTERNAL, TO_EXTERNAL}
	protected final String propertyName;
	protected final DBTypeAdaptor<Object, Object> typeAdaptor;
	protected QueryableDatatype internalQdt;
	private DBSafeInternalTypeAdaptor internalToExternalTypeAdaptor;
	private DBSafeInternalTypeAdaptor internalToInternalTypeAdaptor;

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
		this.internalToExternalTypeAdaptor = new DBSafeInternalTypeAdaptor(Direction.TO_EXTERNAL, typeAdaptor);
		this.internalToInternalTypeAdaptor = new DBSafeInternalTypeAdaptor(Direction.TO_INTERNAL, typeAdaptor);
		
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

	protected Object adaptValue(Direction direction, Object sourceLiteralValue) {
		if (direction == Direction.TO_INTERNAL) {
			return adaptValueToInternal(sourceLiteralValue);
		}
		else {
			return adaptValueToExternal(sourceLiteralValue);
		}
	}
	
	protected Object adaptValueToInternal(Object externalLiteralValue) {
		try {
			return typeAdaptor.toDatabaseValue(externalLiteralValue);
		} catch (RuntimeException e) {
			String msg = (e.getLocalizedMessage() == null) ? "" : ": "+e.getLocalizedMessage();
            throw new DBThrownByEndUserCodeException("Type adaptor threw " + e.getClass().getSimpleName()
                    + " when setting property " + propertyName + msg, e);
		}
	}
	
	protected Object adaptValueToExternal(Object internalLiteralValue) {
		try {
			return typeAdaptor.fromDatabaseValue(internalLiteralValue);
		} catch (RuntimeException e) {
			String msg = (e.getLocalizedMessage() == null) ? "" : ": "+e.getLocalizedMessage();
	        throw new DBThrownByEndUserCodeException("Type adaptor threw " + e.getClass().getSimpleName()
	                + " when getting property " + propertyName + msg, e);
		}
	}
	
	public void setInternalQDTFromExternalQDT(QueryableDatatype externalQdt) {
		setTargetQDTFromSourceQDT(Direction.TO_INTERNAL, internalQdt, externalQdt);
	}

	public void setExternalFromInternalQDT(QueryableDatatype externalQdt) {
		setTargetQDTFromSourceQDT(Direction.TO_EXTERNAL, externalQdt, internalQdt);
	}
	
	protected void setTargetQDTFromSourceQDT(Direction direction, QueryableDatatype targetQdt, QueryableDatatype sourceQdt) {
		targetQdt.changed = sourceQdt.changed;
		targetQdt.includingNulls = sourceQdt.includingNulls;
		targetQdt.invertOperator = sourceQdt.invertOperator;
		targetQdt.isDBNull = sourceQdt.isDBNull;
		targetQdt.isPrimaryKey = sourceQdt.isPrimaryKey;
		targetQdt.undefined = sourceQdt.undefined;
		targetQdt.sort = sourceQdt.sort;
		
		// copy literal value with translation
		targetQdt.literalValue = adaptValue(direction, sourceQdt.literalValue);
		
		// copy operator with translation
		// TODO call operator.copyAndTranslate(new DBSafeInternalTypeAdaptor(typeAdaptor))
		//   class DBSafeInternalTypeAdaptor {
		//       public QueryableDatatype convert(QueryableDatatype qdt);
	    //   }
		// Needs to detect where DBOperator is created as QueryableDatatype.do{new DBOperator(this)};
		//  (eg: in QueryableDatatype.setValue())
		//targetQdt.operator = sourceQdt.operator; // TODO: translate
		if (direction == Direction.TO_INTERNAL) {
			targetQdt.operator = sourceQdt.operator.copyAndAdapt(internalToInternalTypeAdaptor);
		}
		else {
			targetQdt.operator = sourceQdt.operator.copyAndAdapt(internalToExternalTypeAdaptor);
		}
		
		// copy previous value with translation
		//sourceQdt.previousValueAsQDT = // TODO 
	}
	
	public static class DBSafeInternalTypeAdaptor {
		private Direction direction;
		private DBTypeAdaptor<Object,Object> typeAdaptor;
		
		public DBSafeInternalTypeAdaptor(Direction direction, DBTypeAdaptor<Object,Object> typeAdaptor) {
			this.direction = direction;
			this.typeAdaptor = typeAdaptor;
		}
		
		public QueryableDatatype convert(QueryableDatatype qdt) {
//			if (qdt == null) {
//				return null;
//			}
//			else if (qdt instanceof DBInteger) {
//				
//			}
			throw new UnsupportedOperationException("Trying to convert from "+qdt.getClass().getSimpleName()+"="+qdt);
		}
	}
}
