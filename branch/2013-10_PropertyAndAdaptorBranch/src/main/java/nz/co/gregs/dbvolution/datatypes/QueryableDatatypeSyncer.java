package nz.co.gregs.dbvolution.datatypes;

import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.internal.SafeOneWayTypeAdaptor;
import nz.co.gregs.dbvolution.internal.SafeOneWayTypeAdaptor.Direction;

/**
 * Allows synchronisations to be done between two QueryableDatatypes,
 * based on a Type Adaptor.
 * @author Malcolm Lett
 */
// TODO come up with a better name
public class QueryableDatatypeSyncer {
	//protected enum Direction {TO_INTERNAL, TO_EXTERNAL}
	protected final String propertyName;
	protected final DBTypeAdaptor<Object, Object> typeAdaptor;
	protected QueryableDatatype internalQdt;
	private DBSafeInternalQDTAdaptor internalToExternalQDTAdaptor;
	private DBSafeInternalQDTAdaptor internalToInternalQDTAdaptor;
	private SafeOneWayTypeAdaptor internalToExternalTypeAdaptor;
	private SafeOneWayTypeAdaptor internalToInternalTypeAdaptor;

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
		this.internalToExternalTypeAdaptor = new SafeOneWayTypeAdaptor(propertyName,
				typeAdaptor, Direction.TO_EXTERNAL, null, null);
		
		this.internalToInternalTypeAdaptor = new SafeOneWayTypeAdaptor(propertyName,
				typeAdaptor, Direction.TO_INTERNAL, null, null);
		
		this.internalToExternalQDTAdaptor = new DBSafeInternalQDTAdaptor(internalToExternalTypeAdaptor);
		this.internalToInternalQDTAdaptor = new DBSafeInternalQDTAdaptor(internalToInternalTypeAdaptor);
		
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
			targetQdt.operator = sourceQdt.operator.copyAndAdapt(internalToInternalQDTAdaptor);
		}
		else {
			targetQdt.operator = sourceQdt.operator.copyAndAdapt(internalToExternalQDTAdaptor);
		}
		
		// copy previous value with translation
		//sourceQdt.previousValueAsQDT = // TODO 
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
		return internalToInternalTypeAdaptor.convert(externalLiteralValue);
	}
	
	protected Object adaptValueToExternal(Object internalLiteralValue) {
		return internalToExternalTypeAdaptor.convert(internalLiteralValue);
	}
	
	public static class DBSafeInternalQDTAdaptor {
		private SafeOneWayTypeAdaptor typeAdaptor;
		
		public DBSafeInternalQDTAdaptor(SafeOneWayTypeAdaptor typeAdaptor) {
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
