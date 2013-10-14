package nz.co.gregs.dbvolution.internal;

import java.util.HashMap;
import java.util.Map;

import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;

/**
 * Constructs class adaptors for DB table classes and maintains an in-memory cache for re-use.
 * Creating class adaptors is expensive and this class is provided as a convenience for anything that needs to
 * access class adaptors for multiple types and would benefit from the performance improvement
 * of caching their values.
 * 
 * <p> Note that class adaptors are immutable, so this is safe to do.
 * 
 * <p> This class is <i>thread-safe</i>.
 * @author Malcolm Lett
 */
public class ClassAdaptorFactory {
	/** Thread-safety: access to this object must be synchronized on it */
	private Map<Class<?>, ClassAdaptor> classAdaptorsByClass = new HashMap<Class<?>, ClassAdaptor>();
	
	/**
	 * Gets the class adaptor for the given class.
	 * If an adaptor for the given class has not yet been created, one will be created
	 * and added to the internal cache.
	 * @param clazz
	 * @return the class adaptor
	 */
	public ClassAdaptor classAdaptorFor(Class<?> clazz) {
		synchronized (classAdaptorsByClass) {
			ClassAdaptor adaptor = classAdaptorsByClass.get(clazz);
			if (adaptor == null) {
				adaptor = new ClassAdaptor(clazz);
				classAdaptorsByClass.put(clazz, adaptor);
			}
			return adaptor;
		}
	}

	/**
	 * Gets the class adaptor for the given object's class.
	 * If an adaptor for the given class has not yet been created, one will be created
	 * and added to the internal cache.
	 * @param object
	 * @return the class adaptor
	 */
	public ClassAdaptor classAdaptorFor(Object object) {
		return classAdaptorFor(object.getClass());
	}
	
	/**
	 * Gets the object adaptor for the given object.
	 * If an adaptor for the object's class has not yet been created, one will be created
	 * and added to the internal cache.
	 * @param dbDefn the database definition in use
	 * @param object the object to wrap
	 * @return the object adaptor for the given object
	 */
	public ObjectAdaptor objectAdaptorFor(DBDefinition dbDefn, Object object) {
		return classAdaptorFor(object.getClass()).objectAdaptorFor(dbDefn, object);
	}
}
