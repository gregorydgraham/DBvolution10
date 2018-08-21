package nz.co.gregs.dbvolution.exceptions;

/**
 * Thrown when the end-user's code threw an exception during invocation. For
 * example, this exception will be thrown when property accessor methods throw
 * exceptions.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Malcolm Lett
 */
public class DBThrownByEndUserCodeException extends DBRuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * Thrown when the end-user's code threw an exception during invocation. For
	 * example, this exception will be thrown when property accessor methods throw
	 * exceptions.
	 *
	 * @param message message
	 * @param cause cause
	 */
	public DBThrownByEndUserCodeException(String message, Throwable cause) {
		super(message, cause);
	}
}
