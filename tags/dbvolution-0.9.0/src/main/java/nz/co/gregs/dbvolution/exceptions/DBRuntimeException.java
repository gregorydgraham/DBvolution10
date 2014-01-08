package nz.co.gregs.dbvolution.exceptions;

/**
 * Thrown when unexpected errors occur.
 * @author Malcolm Lett
 */
public class DBRuntimeException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public DBRuntimeException() {
	}

	public DBRuntimeException(String message) {
		super(message);
	}

	public DBRuntimeException(Throwable cause) {
		super(cause);
	}

	public DBRuntimeException(String message, Throwable cause) {
		super(message, cause);
	}

//	public DBRuntimeException(String message, Throwable cause,
//			boolean enableSuppression, boolean writableStackTrace) {
//		super(message, cause, enableSuppression, writableStackTrace);
//	}

}
