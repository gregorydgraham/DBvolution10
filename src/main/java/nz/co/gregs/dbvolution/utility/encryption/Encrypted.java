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
package nz.co.gregs.dbvolution.utility.encryption;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import nz.co.gregs.dbvolution.exceptions.UnableToDecryptInput;
import nz.co.gregs.dbvolution.exceptions.CannotEncryptInputException;

/**
 *
 * @author gregorygraham
 */
public class Encrypted {

	public static Encrypted fromCipherText(String ciphertext) {
		return new Encrypted(ciphertext);
	}

	public static Encrypted fromCipherText(byte[] value) {
		return new Encrypted(new String(value, StandardCharsets.UTF_8));
	}

	private final String cipherText;

	private Encrypted(String cipherText) {
		this.cipherText = cipherText;
	}

	public static Encrypted encrypt(String passPhrase, String plainText) throws CannotEncryptInputException {
		try {
			return new Encrypted(Encryption_BASE64_AES_GCM_NoPadding.encrypt(passPhrase, plainText));
		} catch (CannotEncryptInputException ex) {
			try {
				return new Encrypted(Encryption_BASE64_AES_CBC_PKCS5Padding.encrypt(passPhrase, plainText));
			} catch (CannotEncryptInputException ex2) {
				try {
					return new Encrypted(Encryption_Internal.encrypt(plainText));
				} catch (CannotEncryptInputException ex3) {
					throw ex;
				}
			}
		}
	}

	public String decrypt(String passPhrase) throws UnableToDecryptInput {
		try {
			return Encryption_BASE64_AES_GCM_NoPadding.decrypt(passPhrase, cipherText);
		} catch (UnableToDecryptInput ex) {
			try {
				return Encryption_BASE64_AES_CBC_PKCS5Padding.decrypt(passPhrase, cipherText);
			} catch (UnableToDecryptInput ex2) {
				try {
					return Encryption_Internal.decrypt(cipherText);
				} catch (UnableToDecryptInput ex3) {
					throw ex;
				}
			}
		}
//		return Encryption_BASE64_AES_CBC_PKCS5Padding.decrypt(passPhrase, cipherText);
	}

	public boolean isEmpty() {
		return cipherText.isEmpty();
	}

	@Override
	public String toString() {
		return cipherText;
	}

	public String getCypherText() {
		return cipherText;
	}

	public byte[] getBytes() {
		return cipherText.getBytes(StandardCharsets.UTF_8);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Encrypted) {
			return this.toString().equals(obj.toString());
		}
		return super.equals(obj);
	}

	@Override
	public int hashCode() {
		int hash = 3;
		hash = 59 * hash + Objects.hashCode(this.cipherText);
		return hash;
	}

}
