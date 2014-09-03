package nz.co.gregs.dbvolution.datatypes;

import nz.co.gregs.dbvolution.internal.properties.PropertyWrapperDefinition;

/**
 * Internal class. Do not use.
 */
/*
 * Note: Comments for this class are provided as non-javadoc comments
 * to avoid polluting end-user javadoc with internal class details.
 * 
 * Used internally to bridge between packages.
 * Makes it possible to hide internal methods on the QueryableDatatype
 * so that they don't pollute the API or javadocs, while still providing
 * access to the internal methods from other packages within DBvolution.
 * 
 * For example QueryableDatatype.setPropertyWrapper() is set to
 * package-private, so the only way of calling it from
 * other packages is via this class.
 * If QueryableDatatype.setPropertyWrapper() was public, then this class
 * wouldn't be needed, but it would pollute the public API.
 */
public class InternalQueryableDatatypeProxy {

	private final QueryableDatatype qdt;

	/**
	 * Internal class, do not use.
	 *
	 * @param qdt
	 */
	public InternalQueryableDatatypeProxy(QueryableDatatype qdt) {
		this.qdt = qdt;
	}

	/*
	 * Injects the PropertyWrapper into the QDT.
	 * For use with QDT types that need meta-data only available
	 * via property wrappers.
	 */
	public void setPropertyWrapper(PropertyWrapperDefinition propertyWrapperDefn) {
		qdt.setPropertyWrapper(propertyWrapperDefn);
	}

	/*
	 * Hides the generic setValue(Object) method within QueryableDatatype while allowing it to be used.
	 */
	public void setValue(Object obj) {
		qdt.setValue(obj);
	}
}
