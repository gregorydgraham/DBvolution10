package nz.co.gregs.dbvolution.datatypes;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapperDefinition;

/**
 * Internal class.Do not use.Used internally to bridge between packages.Makes it possible to hide
 internal methods on the QueryableDatatype so that they don't pollute the API
 or JavaDocs, while still providing access to the internal methods from other
 packages within DBvolution. For example QueryableDatatype.setPropertyWrapper() is set to package-private,
 so the only way of calling it from other packages is via this class. If
 QueryableDatatype.setPropertyWrapper() was public, then this class wouldn't
 be needed, but it would pollute the public API.
 * @param <BASETYPE> the type returned by the QDT's getValue method
 */
public class InternalQueryableDatatypeProxy<BASETYPE> {

	private final QueryableDatatype<BASETYPE> qdt;

	/**
	 * Internal class, do not use.
	 *
	 * @param qdt	qdt
	 */
	public InternalQueryableDatatypeProxy(QueryableDatatype<BASETYPE> qdt) {
		this.qdt = qdt;
	}

	/**
	 * Internal class, do not use.
	 * <p>
	 * Injects the PropertyWrapper into the QDT.
	 * <p>
	 * For use with QDT types that need meta-data only available via property
	 * wrappers.
	 *
	 * @param propertyWrapperDefn	propertyWrapperDefn
	 */
	public void setPropertyWrapper(PropertyWrapperDefinition<?,BASETYPE> propertyWrapperDefn) {
		qdt.setPropertyWrapper(propertyWrapperDefn);
	}

	/**
	 * Internal class, do not use.
	 * <p>
	 * Hides the generic setValue(Object) method within QueryableDatatype while
	 * allowing it to be used.
	 *
	 * @param obj	obj
	 */
	public void setValue(Object obj) {
		try {
			if (obj == null) {
				qdt.setToNull();
			} else {
				Method method = qdt.getClass().getMethod("setValue", obj.getClass());
				method.invoke(qdt, obj);
			}
		} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
			throw new DBRuntimeException("Synchronisation Failed:" + ex.getMessage(), ex);
		}

	}

	/**
	 * Internal class, do not use.
	 * <p>
	 * Hides the generic setValue(Object) method within QueryableDatatype while
	 * allowing it to be used.
	 *
	 * @param obj	obj
	 */
	public void setValueFromDatabase(Object obj) {
		try {
			if (obj == null) {
				qdt.setToNull();
			} else {
				Method method = null;
				Class<?> qdtClass = qdt.getClass();
				NoSuchMethodException nsmEx = null;
				while (method == null && !qdtClass.equals(Object.class)) {
					try {
						method = qdtClass.getDeclaredMethod("setValueFromDatabase", obj.getClass());
					} catch (NoSuchMethodException ex) {
						try {
							method = qdtClass.getDeclaredMethod("setValueFromDatabase", Object.class);
						} catch (NoSuchMethodException ex2) {
							nsmEx = ex;
						}
					}
					qdtClass = qdtClass.getSuperclass();
				}
				if (method != null) {
					method.setAccessible(true);
					method.invoke(qdt, obj);
				} else {
					throw nsmEx;
				}
			}
		} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
			throw new DBRuntimeException("Synchronisation Failed:" + ex.getMessage(), ex);
		}

	}
}
