/*
 * Copyright 2019 Gregory Graham.
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

import nz.co.gregs.dbvolution.exceptions.CannotEncryptInputException;
import nz.co.gregs.dbvolution.exceptions.UnableToDecryptInput;
import nz.co.gregs.dbvolution.utility.encryption.Encrypted;

public class DBEncryptedText extends DBLargeText {

	public DBEncryptedText() {
	}

	/**
	 * Returns the ciphertext value of this as an {@link Encrypted} object.
	 *
	 * @return
	 */
	public final Encrypted getEncryptedValue() {
		return Encrypted.fromCipherText(getValue());
	}

	/**
	 * Uses the provided passphrase to decrypt the value and returns the plaintext
	 * result.
	 *
	 * <p>
	 * The passphrase is never stored during any DBvolution processing.</p>
	 *
	 * @param passPhrase
	 * @return
	 * @throws UnableToDecryptInput
	 */
	public final String getDecryptedValue(String passPhrase) throws UnableToDecryptInput {
		return getEncryptedValue().decrypt(passPhrase);
	}

	/**
	 * Synonym for {@link #getDecryptedValue(java.lang.String) }.
	 *
	 * <p>
	 * The passphrase is never stored during any DBvolution processing.</p>
	 *
	 * @param passPhrase
	 * @return
	 * @throws UnableToDecryptInput
	 */
	public final String decryptWith(String passPhrase) throws UnableToDecryptInput {
		return getEncryptedValue().decrypt(passPhrase);
	}

	/**
	 * Uses the provided passphrase to encrypt the value and set the ciphertext
	 * that will be stored.
	 *
	 * <p>
	 * The passphrase is never stored during any DBvolution processing.</p>
	 *
	 * @param passPhrase
	 * @param plainText
	 * @throws nz.co.gregs.dbvolution.exceptions.CannotEncryptInputException
	 */
	public void setValue(String passPhrase, String plainText) throws CannotEncryptInputException {
		this.setValue(Encrypted.encrypt(passPhrase, plainText).toString());
	}

	/**
	 * Uses the provided passphrase to encrypt the value and set the ciphertext
	 * that will be stored.
	 *
	 * <p>
	 * The passphrase is never stored during any DBvolution processing.</p>
	 *
	 * @param encryptedText
	 * @throws nz.co.gregs.dbvolution.exceptions.CannotEncryptInputException
	 */
	public void setValue(Encrypted encryptedText) throws CannotEncryptInputException {
		this.setValue(encryptedText.toString());
	}

}
