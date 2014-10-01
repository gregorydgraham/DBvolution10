package nz.co.gregs.dbvolution.exceptions;

/**
 * Thrown when unexpected errors occur.
 *
 * <p>
 * DBvolution can only cope with so much, and when it reaches breaking point it
 * throws a DBRuntime exception.
 *
 * <p>
 * DBRuntimeException should not be thrown directly, please sub-class it and add
 * information to the exception thrown to help developers improve their code.
 *
 * @author Malcolm Lett
 */
public class DBRuntimeException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * Thrown when unexpected errors occur.
	 *
	 */
	public DBRuntimeException() {
	}

	/**
	 * Thrown when unexpected errors occur.
	 *
	 * @param message
	 */
	public DBRuntimeException(String message) {
		super(message);
	}

	/**
	 * Thrown when unexpected errors occur.
	 *
	 * @param cause
	 */
	public DBRuntimeException(Throwable cause) {
		super(cause);
	}

	/**
	 * Thrown when unexpected errors occur.
	 *
	 * @param message
	 * @param cause
	 */
	public DBRuntimeException(String message, Throwable cause) {
		super(message, cause);
	}

//	public DBRuntimeException(String message, Throwable cause,
//			boolean enableSuppression, boolean writableStackTrace) {
//		super(message, cause, enableSuppression, writableStackTrace);
//	}
}
