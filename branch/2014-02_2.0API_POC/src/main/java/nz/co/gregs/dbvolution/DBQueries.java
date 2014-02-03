package nz.co.gregs.dbvolution;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

public class DBQueries {
	private static ThreadLocal<PropertyWrapper> ongoingExemplarProperty = new ThreadLocal<PropertyWrapper>();
	
	/**
	 * Get a mock of type {@code type} suitable for adding
	 * search criteria to.
	 * 
	 * <p> Thread-safety: objects returned by this method are 
	 * for <i>one-off</i> use in a query and are <i>not</i> thread-safe. 
	 * @param type
	 * @return
	 */
	// TODO: this will have to be replaced by CGLIB to make it work
	public static <T> T exemplarOf(Class<T> type) {
		DBvInvocationHandler handler = new DBvInvocationHandler(type);
		Object proxy = Proxy.newProxyInstance(type.getClassLoader(),
				new Class<?>[]{type}, handler);
		return (T) proxy;
	}
	
	private static class DBvInvocationHandler implements InvocationHandler {
		private Class<?> proxiedType;
		private Map<String, PropertyWrapper> wrappers = new HashMap<String, PropertyWrapper>();
		
		public DBvInvocationHandler(Class<?> type) {
			this.proxiedType = type;
		}
		
		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			String name = null;
			if (method.getName().startsWith("get")) {
				name = method.getName().substring("get".length());
				name = name.substring(0,1).toLowerCase() + name.substring(1);
			}
			else if (method.getName().startsWith("set")) {
				name = method.getName().substring("set".length());
				name = name.substring(0,1).toLowerCase() + name.substring(1);
			}
			
			if (name != null) {
				PropertyWrapper wrapper = propertyWrapperFor(name);
				ongoingExemplarProperty.set(wrapper);
			}
			
			return null;
		}
		
		private PropertyWrapper propertyWrapperFor(String fieldName) {
			PropertyWrapper wrapper = wrappers.get(fieldName);
			if (wrapper != null) {
				wrapper = PropertyWrapper.getInstance(proxiedType, fieldName);
				wrappers.put(fieldName, wrapper);
			}
			return wrapper;
		}
		
//		private Object dummyValueOfType(Class<?> type) {
//			return null;
//		}
	}
	
	/**
	 * Get property for adding criteria to.
	 * @param methodCall ok, so it's not actually a method call, but call the method anyway
	 * @return
	 */
	public static <V> QueryableDatatype where(V methodCall) {
		PropertyWrapper wrapper = ongoingExemplarProperty.get();
		if (wrapper == null) {
			throw new IllegalStateException("woops");
		}
		return wrapper.getQDT();
	}
}
