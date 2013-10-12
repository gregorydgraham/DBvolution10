package nz.co.gregs.dbvolution;

/**
 * Usually thrown when unexpected errors occur and sometimes indicate probable DBvolution bugs.
 * Sub-classes of this are used when more specific details are known.
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

	public DBRuntimeException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
