package nz.co.gregs.dbvolution.exceptions;

/**
 * Thrown when the end-user has made a mistake.
 * Obviously this is a derogatory name that should not be kept.
 * This exception needs to be re-named when it's better understood under what scenarios it will be used.
 * @author Malcolm Lett
 */
public class DBPebkacException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public DBPebkacException() {
	}

	public DBPebkacException(String message) {
		super(message);
	}

	public DBPebkacException(Throwable cause) {
		super(cause);
	}

	public DBPebkacException(String message, Throwable cause) {
		super(message, cause);
	}
}
