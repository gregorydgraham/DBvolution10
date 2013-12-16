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
	protected enum Direction {FROM_EXTERNAL, FROM_INTERNAL}
	protected final String propertyName;
	protected final DBTypeAdaptor<Object, Object> typeAdaptor;
	protected QueryableDatatype internalQdt;

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
		if (direction == Direction.FROM_EXTERNAL) {
			return adaptValueFromExternal(sourceLiteralValue);
		}
		else {
			return adaptValueFromInternal(sourceLiteralValue);
		}
	}
	
	protected Object adaptValueFromExternal(Object externalLiteralValue) {
		try {
			return typeAdaptor.toDatabaseValue(externalLiteralValue);
		} catch (RuntimeException e) {
			String msg = (e.getLocalizedMessage() == null) ? "" : e.getLocalizedMessage();
            throw new DBThrownByEndUserCodeException("Type adaptor threw " + e.getClass().getSimpleName()
                    + " when setting property " + propertyName + msg, e);
		}
	}
	
	protected Object adaptValueFromInternal(Object internalLiteralValue) {
		try {
			return typeAdaptor.fromDatabaseValue(internalLiteralValue);
		} catch (RuntimeException e) {
			String msg = (e.getLocalizedMessage() == null) ? "" : e.getLocalizedMessage();
	        throw new DBThrownByEndUserCodeException("Type adaptor threw " + e.getClass().getSimpleName()
	                + " when getting property " + propertyName + msg, e);
		}
	}
	
	public void setInternalFromExternalQDT(QueryableDatatype externalQdt) {
		setTargetQDTFromSourceQDT(Direction.FROM_EXTERNAL, internalQdt, externalQdt);
	}

	public void setExternalQDTFromInternal(QueryableDatatype externalQdt) {
		setTargetQDTFromSourceQDT(Direction.FROM_INTERNAL, externalQdt, internalQdt);
	}
	
	protected void setTargetQDTFromSourceQDT(Direction direction, QueryableDatatype targetQdt, QueryableDatatype sourceQdt) {
		targetQdt.changed = sourceQdt.changed;
		targetQdt.includingNulls = sourceQdt.includingNulls;
//		targetQdt.invertOperator = sourceQdt.invertOperator;
		targetQdt.isDBNull = sourceQdt.isDBNull;
		targetQdt.isPrimaryKey = sourceQdt.isPrimaryKey;
		targetQdt.undefined = sourceQdt.undefined;
		targetQdt.sort = sourceQdt.sort;
		
		// copy literal value with translation
		targetQdt.literalValue = adaptValue(direction, sourceQdt.literalValue);
		
		// copy operator with translation
		targetQdt.operator = sourceQdt.operator; // TODO: translate
		
		// copy previous value with translation
		//sourceQdt.previousValueAsQDT = // TODO 
	}
}
