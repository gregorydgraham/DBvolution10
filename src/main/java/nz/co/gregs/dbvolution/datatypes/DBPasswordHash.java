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
package nz.co.gregs.dbvolution.datatypes;

import nz.co.gregs.dbvolution.exceptions.IncorrectPasswordException;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.utility.UpdatingBCrypt;

/**
 * Creates a password column using the secure hashing.
 *
 * <p>
 * Does not contain passwords, only hashes. This is the correct way to protect
 * and "store" your passwords.</p>
 *
 * <p>
 * Use {@link #checkPasswordWithException(java.lang.String)
 * } and {@link #checkPassword(java.lang.String) } to check a password
 * attempt with the hash. Use {@link #checkPasswordAndUpdateHash(java.lang.String)
 * } to check the password against the hash and update the local (not database)
 * hash with a more appropriate hash.</p>
 *
 * <p>
 * Passwords are automatically hashed with BCrypt as the value is set and the
 * actual password is never stored in the object or database.</p>
 *
 * <p>
 * Please note that DBPasswordHash supports generating hashes appropriate to the
 * CPU on which it is running. Use the {@link #checkPasswordAndUpdateHash(java.lang.String)
 * } method to both check the password is correct and generate a new hash that
 * is more appropriate to the CPU. The system assumes that you will call the
 * method occasionally, updating the hash if it has changed. You will need to
 * update the row on the database using {@link DBDatabase#update(nz.co.gregs.dbvolution.DBRow...)
 * } or similar.</p>
 *
 * @author gregorygraham
 */
public class DBPasswordHash extends DBString {

	static final long serialVersionUID = 1l;

	transient UpdatingBCrypt crypt = new UpdatingBCrypt();

	public DBPasswordHash() {
		this(UpdatingBCrypt.DEFAULT_ROUNDS);
	}

	/**
	 * logRounds should be an number between 4 and 30, probably around 10.
	 *
	 * <p>
	 * Use {@link #checkPasswordAndUpdateHash(java.lang.String) } to generate a
	 * hash closer to the optimum for your system.</p>
	 *
	 * @param logRounds
	 */
	public DBPasswordHash(int logRounds) {
		super();
		crypt = new UpdatingBCrypt(logRounds);
	}

	public DBPasswordHash(String password) {
		this(password, UpdatingBCrypt.DEFAULT_ROUNDS);
	}

	/**
	 * logRounds should be an number between 4 and 30, probably around 10.
	 *
	 * <p>
	 * Use {@link #checkPasswordAndUpdateHash(java.lang.String) } to generate a
	 * hash closer to the optimum for your system.</p>
	 *
	 * @param password
	 * @param logRounds
	 */
	public DBPasswordHash(String password, int logRounds) {
		super();
		crypt = new UpdatingBCrypt(logRounds);
		this.setValue(password);
	}

	@Override
	public final void setValue(String password) {
		super.setValue(password == null ? null : crypt.hashPassword(password));
	}

	public final boolean checkPassword(String password) {
		return UpdatingBCrypt.checkPassword(password, getValue());
	}

	public final boolean checkPasswordWithException(String password) throws IncorrectPasswordException {
		if (UpdatingBCrypt.checkPassword(password, getValue())) {
			return true;
		} else {
			throw new IncorrectPasswordException(getValue());
		}
	}

	/**
	 * DBPasswordHash supports generating hashes appropriate to the CPU on which
	 * it is running.
	 *
	 * <p>
	 * Also handles changing plain text passwords to bcrypt hashes.
	 *
	 * <p>
	 * Use the {@link #checkPasswordAndUpdateHash(java.lang.String)
	 * } method to both check the password is correct and generate a new hash that
	 * is more appropriate to the CPU. The system assumes that you will call the
	 * method occasionally, updating the hash if it has changed. You will need to
	 * update the row on the database using {@link DBDatabase#update(nz.co.gregs.dbvolution.DBRow...)
	 * } or similar.</p>
	 *
	 * @param password
	 * @return
	 * @throws IncorrectPasswordException
	 */
	public boolean checkPasswordAndUpdateHash(String password) throws IncorrectPasswordException {
		String newHash = crypt.checkPasswordAndCreateSecureHash(password, getValue());
		if (!newHash.equals(getValue())) {
			super.setValue(newHash);
		}
		return true;
	}

}
