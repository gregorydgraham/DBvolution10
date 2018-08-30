/*
 * Copyright 2018 Gregory Graham.
 *
 * Commercial licenses are available, please contact info@gregs.co.nz for details.
 * 
 * This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. 
 * To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/4.0/ 
 * or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.
 * 
 * You are free to:
 *     Share - copy and redistribute the material in any medium or format
 *     Adapt - remix, transform, and build upon the material
 * 
 *     The licensor cannot revoke these freedoms as long as you follow the license terms.               
 *     Under the following terms:
 *                 
 *         Attribution - 
 *             You must give appropriate credit, provide a link to the license, and indicate if changes were made. 
 *             You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.
 *         NonCommercial - 
 *             You may not use the material for commercial purposes.
 *         ShareAlike - 
 *             If you remix, transform, or build upon the material, 
 *             you must distribute your contributions under the same license as the original.
 *         No additional restrictions - 
 *             You may not apply legal terms or technological measures that legally restrict others from doing anything the 
 *             license permits.
 * 
 * Check the Creative Commons website for any details, legalese, and updates.
 */
package nz.co.gregs.dbvolution.utility;

import com.google.common.base.Stopwatch;
import java.util.concurrent.TimeUnit;
import nz.co.gregs.dbvolution.exceptions.IncorrectPasswordException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mindrot.jbcrypt.BCrypt;

/**
 *
 * @author gregorygraham
 */
public class UpdatingBCrypt {

	static final Log LOG = LogFactory.getLog(UpdatingBCrypt.class);

	private final int logRounds;
	public static final int DEFAULT_ROUNDS = 5;
	public static final long MINIMUM_EFFORT = 100l;
	public static final long MAXIMUM_EFFORT = 1000l;

	public UpdatingBCrypt() {
		this(DEFAULT_ROUNDS);
	}

	/**
	 * Minimum of 4 rounds
	 *
	 * @param logRounds
	 */
	public UpdatingBCrypt(int logRounds) {
		this.logRounds = logRounds >= 4 ? logRounds : DEFAULT_ROUNDS;
	}

	public String hashPassword(String password) {
		return hashPassword(password, logRounds);
	}

	private String hashPassword(String password, int logRounds) {
		return BCrypt.hashpw(password, BCrypt.gensalt(logRounds));
	}

	public static boolean checkPassword(String password, String hash) {
		return BCrypt.checkpw(password, hash);
	}

	/**
	 * Checks the password matches the hash and automatically generates a new one
	 * if the processor speed warrants it.
	 *
	 * <p>
	 * If the password does not match the hash an IncorrectPasswordException is
	 * thrown.</p>
	 *
	 * <p>
	 * If the password successfully matches to the hash, a quick hash is made to
	 * test the processor speed. If the quick hash is faster than the minimum work
	 * requirement, then a hash stronger than the original is made and returned
	 * from the method.</p>
	 *
	 * <p>
	 * Alternatively if the "quick" hash takes more than a second, the work
	 * requirement is reduced and a new hash is returned.</p>
	 *
	 * @param password
	 * @param hash
	 * @return the original hash or a more appropriate hash for your CPU.
	 * @throws IncorrectPasswordException
	 */
	public String checkPasswordAndCreateSecureHash(String password, String hash) throws IncorrectPasswordException {
		if (looksLikeABCryptHash(hash)) {
			if (BCrypt.checkpw(password, hash)) {
				int rounds = getRounds(hash);
				int minRounds = Math.max(DEFAULT_ROUNDS, rounds - 2);
				Stopwatch timer = Stopwatch.createStarted();
				String newHash = hashPassword(password, minRounds);
				timer.stop();
				long elapsed = timer.elapsed(TimeUnit.MILLISECONDS);
				if (elapsed < MINIMUM_EFFORT) {
					int newRounds = rounds + 1;
					LOG.debug("Updating password from " + rounds + " rounds to " + newRounds);
					newHash = hashPassword(password, newRounds);
					return newHash;
				} else if (elapsed >= MAXIMUM_EFFORT) {
					LOG.debug("Updating password from " + rounds + " rounds to " + minRounds);
					return newHash;
				}
			} else {
				throw new IncorrectPasswordException(hash);
			}
		} else {
			if (hash.equals(password)) {
				return hashPassword(password);
			} else {
				throw new IncorrectPasswordException(hash);
			}
		}
		return hash;
	}

	/*
     * Copy pasted from BCrypt internals :(. Ideally a method
     * to exports parts would be public. We only care about rounds
     * currently.
	 */
	private int getRounds(String hash) {
		char minor;
		int off = 0;

		if (hash.charAt(0) != '$' || hash.charAt(1) != '2') {
			throw new IllegalArgumentException("Invalid salt version");
		}
		if (hash.charAt(2) == '$') {
			off = 3;
		} else {
			minor = hash.charAt(2);
			if (minor != 'a' || hash.charAt(3) != '$') {
				throw new IllegalArgumentException("Invalid salt revision");
			}
			off = 4;
		}

		// Extract number of rounds
		if (hash.charAt(off + 2) > '$') {
			throw new IllegalArgumentException("Missing salt rounds");
		}
		return Integer.parseInt(hash.substring(off, off + 2));
	}

	public static boolean looksLikeABCryptHash(String maybeHash) {
		return maybeHash != null && maybeHash.matches("\\$..\\$..\\$.*");
	}
}
