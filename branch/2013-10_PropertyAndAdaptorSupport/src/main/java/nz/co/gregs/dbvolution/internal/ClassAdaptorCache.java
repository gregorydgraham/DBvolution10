package nz.co.gregs.dbvolution.internal;

import java.util.HashMap;
import java.util.Map;

/**
 * In-memory cache of class adaptors.
 * Creating class adaptors is expensive and this class is provided as a convenience for anything that needs to
 * access class adaptors for multiple types and would benefit from the performance improvement
 * of caching their values.
 * 
 * <p> This class is <i>thread-safe</i>.
 * @author Malcolm Lett
 */
public class ClassAdaptorCache {
	/** Thread-safety: access to this object must be synchronized on it */
	private Map<Class<?>, ClassAdaptor> classAdaptorsByClass = new HashMap<Class<?>, ClassAdaptor>();
	
	/**
	 * Gets the class adaptor for the given class.
	 * If an adaptor for the given class hasn't yet been created, one will be created.
	 * @param clazz
	 * @return
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
}
