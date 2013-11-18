package nz.co.gregs.dbvolution.datatypes;

/**
 * Syncs between a simple-type external value
 * and a QDT internal value.
 * @author Malcolm Lett
 */
public class SimpleValueQueryableDatatypeSyncer extends QueryableDatatypeSyncer {

	/**
	 * 
	 * @param propertyName used in error messages
	 * @param internalQdtType
	 * @param typeAdaptor
	 */
	public SimpleValueQueryableDatatypeSyncer(String propertyName, Class<? extends QueryableDatatype> internalQdtType,
			DBTypeAdaptor<Object, Object> typeAdaptor) {
		super(propertyName, internalQdtType, typeAdaptor);
	}

	public void setInternalFromExternalSimpleValue(Object externalValue) {
		Object internalValue = toInternalSimpleTypeAdaptor.convert(externalValue);
		if (internalValue == null) {
			// TODO complete this
			internalQdt.undefined = true;
			internalQdt.operator = null;
			internalQdt.literalValue = null;
			internalQdt.changed = false;
			internalQdt.previousValueAsQDT = null;
		}
		else {
			internalQdt.setValue(internalValue);
		}
	}

	/**
	 * Note: directly returning the value from the type adaptor,
	 * without casting to the specific type expected by the target
	 * java property.
	 * @return
	 */
	public Object getExternalSimpleValueFromInternal() {
		return toExternalSimpleTypeAdaptor.convert(internalQdt.getValue());
	}
}
