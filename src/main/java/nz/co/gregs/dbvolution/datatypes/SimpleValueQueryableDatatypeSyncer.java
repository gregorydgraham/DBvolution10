package nz.co.gregs.dbvolution.datatypes;

/**
 * Syncs between a simple-type external value and a QDT internal value.
 *
 * @author Malcolm Lett
 */
public class SimpleValueQueryableDatatypeSyncer extends QueryableDatatypeSyncer {

    /**
     *
     * @param propertyName used in error messages
     * @param internalQdtType
     * @param internalQdtLiteralType
     * @param externalSimpleType
     * @param typeAdaptor
     */
    public SimpleValueQueryableDatatypeSyncer(String propertyName, Class<? extends QueryableDatatype> internalQdtType,
            Class<?> internalQdtLiteralType, Class<?> externalSimpleType, DBTypeAdaptor<Object, Object> typeAdaptor) {
        super(propertyName, internalQdtType, internalQdtLiteralType, externalSimpleType, typeAdaptor);
    }

    /**
     * Sets the cached internal QDT value from the provided non-QDT external
     * value.
     *
     * @param externalValue may be null
     * @return the updated internal QDT
     */
    public QueryableDatatype setInternalQDTFromExternalSimpleValue(Object externalValue) {
        Object internalValue = toInternalSimpleTypeAdaptor.convert(externalValue);
        if (internalValue == null) {
            // TODO complete this
            internalQdt.undefined = true;
            internalQdt.operator = null;
            internalQdt.literalValue = null;
            internalQdt.changed = false;
            internalQdt.previousValueAsQDT = null;
        } else {
            // TODO what type checking can/should be done here?
            internalQdt.setValue(internalValue);
        }
        return internalQdt;
    }

    /**
     * Warning: this directly returns the value from the type adaptor, without
     * casting to the specific type expected by the target java property.
     *
     * @return
     */
    public Object getExternalSimpleValueFromInternalQDT() {
        return toExternalSimpleTypeAdaptor.convert(internalQdt.getValue());
    }
}
