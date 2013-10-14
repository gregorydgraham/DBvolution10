package nz.co.gregs.dbvolution;

/**
 * Thrown when the end-user's code threw an exception during invocation.
 * For example, this exception will be thrown when property accessor
 * methods throw exceptions.
 * @author Malcolm Lett
 */
public class DBThrownByEndUserCodeException extends DBRuntimeException {
	private static final long serialVersionUID = 1L;

	public DBThrownByEndUserCodeException() {
	}

	public DBThrownByEndUserCodeException(String message) {
		super(message);
	}

	public DBThrownByEndUserCodeException(Throwable cause) {
		super(cause);
	}

	public DBThrownByEndUserCodeException(String message, Throwable cause) {
		super(message, cause);
	}

	public DBThrownByEndUserCodeException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
