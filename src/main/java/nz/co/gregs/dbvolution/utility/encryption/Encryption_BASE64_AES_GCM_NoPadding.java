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

import nz.co.gregs.dbvolution.exceptions.UnableToDecryptInput;
import nz.co.gregs.dbvolution.exceptions.CannotEncryptInputException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import javax.crypto.spec.GCMParameterSpec;
import java.util.Base64;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.utility.Random;

/**
 *
 * @author gregorygraham
 */
public class Encryption_BASE64_AES_GCM_NoPadding {

	private static final String ENCRYPTED_PREAMPLE = "BASE64_AES/GCM/NoPadding";
	private final static String ALGORITHM_NAME = "AES/GCM/NoPadding";
	private final static int ALGORITHM_NONCE_SIZE = 12;
	private final static int ALGORITHM_TAG_SIZE = 128;
	private final static int ALGORITHM_KEY_SIZE = 128;
	private final static String PBKDF2_NAME = "PBKDF2WithHmacSHA256";
	private final static int PBKDF2_SALT_SIZE = 16;
	private final static int PBKDF2_ITERATIONS = 32767;

	public static String encrypt(String password, String plaintext) throws CannotEncryptInputException {
		try {
			// make a salt
			byte[] salt = Random.bytes(PBKDF2_SALT_SIZE);

			// Create an instance of PBKDF2 and derive a key.
			PBEKeySpec pwSpec = new PBEKeySpec(password.toCharArray(), salt, PBKDF2_ITERATIONS, ALGORITHM_KEY_SIZE);
			SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(PBKDF2_NAME);
			byte[] key = keyFactory.generateSecret(pwSpec).getEncoded();

			// make a nonce
			byte[] nonce = Random.bytes(ALGORITHM_NONCE_SIZE);

			// Create the cipher instance and initialize.
			Cipher cipher = getEncryptCipher(key, nonce);

			// Encrypt and prepend nonce.
			byte[] ciphertext = cipher.doFinal(getUTF8Bytes(plaintext));

			// Return as base64 string.
			return ENCRYPTED_PREAMPLE + "|" + getBase64String(salt) + "|" + getBase64String(nonce) + "|" + getBase64String(ciphertext);
		} catch (InvalidAlgorithmParameterException | InvalidKeyException | NoSuchPaddingException | IllegalBlockSizeException | BadPaddingException | NoSuchAlgorithmException | InvalidKeySpecException ex) {
			Logger.getLogger(Encryption_BASE64_AES_GCM_NoPadding.class.getName()).log(Level.SEVERE, null, ex);
			throw new CannotEncryptInputException(ex);
		}
	}

	private static SecretKeySpec getSecretKeySpec(byte[] key) {
		return new SecretKeySpec(key, "AES");
	}

	private static GCMParameterSpec getParameterSpec(byte[] nonce) {
		return new GCMParameterSpec(ALGORITHM_TAG_SIZE, nonce);
	}

	public static String getBase64String(byte[] bytes) {
		return new String(Base64.getEncoder().encode(bytes));
	}

	public static byte[] getBytesFromBase64String(String base64String) {
		return Base64.getDecoder().decode(base64String);
	}

	private static byte[] getSecretKey(String passphrase, byte[] salt) throws NoSuchAlgorithmException, InvalidKeySpecException {
		// Create an instance of PBKDF2 and derive the key.
		PBEKeySpec pwSpec = new PBEKeySpec(passphrase.toCharArray(), salt, PBKDF2_ITERATIONS, ALGORITHM_KEY_SIZE);
		SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(PBKDF2_NAME);
		byte[] key = keyFactory.generateSecret(pwSpec).getEncoded();
		return key;
	}

	public static String decrypt(String passphrase, String encryptedString) throws UnableToDecryptInput {
		InterpretedString interpreted = InterpretedString.interpret(encryptedString);
		if (interpreted.isEncryptedString()) {
			try {
				// Retrieve the salt, nonce, and ciphertext.
				byte[] salt = interpreted.salt;
				byte[] nonce = interpreted.nonce;
				String ciphertext = interpreted.encryptedPart;

				// Create an instance of PBKDF2 and derive the key.
				byte[] key = getSecretKey(passphrase, salt);

				Cipher cipher = getDecryptCipher(key, nonce);

				// Decrypt and return result.
				final byte[] finalText = cipher.doFinal(getBytesFromBase64String(ciphertext));

				return new String(finalText, StandardCharsets.UTF_8);
			} catch (NoSuchAlgorithmException | InvalidKeySpecException | InvalidKeyException | InvalidAlgorithmParameterException | IllegalBlockSizeException | BadPaddingException | NoSuchPaddingException ex) {
				Logger.getLogger(Encryption_BASE64_AES_GCM_NoPadding.class.getName()).log(Level.SEVERE, null, ex);
				throw new UnableToDecryptInput(ex);
			}
		} else {
			throw new UnableToDecryptInput();
		}
	}

	private static Cipher getEncryptCipher(byte[] key, byte[] nonce) throws NoSuchAlgorithmException, InvalidKeyException, InvalidAlgorithmParameterException, NoSuchPaddingException {
		return getCipher(Cipher.ENCRYPT_MODE, key, nonce);
	}

	private static Cipher getDecryptCipher(byte[] key, byte[] nonce) throws NoSuchAlgorithmException, InvalidKeyException, InvalidAlgorithmParameterException, NoSuchPaddingException {
		return getCipher(Cipher.DECRYPT_MODE, key, nonce);
	}

	private static Cipher getCipher(int mode, byte[] key, byte[] nonce) throws NoSuchAlgorithmException, InvalidKeyException, InvalidAlgorithmParameterException, NoSuchPaddingException {
		Cipher cipher = Cipher.getInstance(ALGORITHM_NAME);
		cipher.init(mode, getSecretKeySpec(key), getParameterSpec(nonce));
		return cipher;
	}

	private static byte[] getUTF8Bytes(String input) {
		return input.getBytes(StandardCharsets.UTF_8);
	}

	private static String asString(ByteBuffer buffer) {
		final ByteBuffer copy = buffer.duplicate();
		final byte[] bytes = new byte[copy.remaining()];
		copy.get(bytes);
		return new String(bytes, StandardCharsets.UTF_8);
	}

	private Encryption_BASE64_AES_GCM_NoPadding() {
	}

	private static class InterpretedString {

		private final boolean isGood;
		private final String preamble;
		private final byte[] salt;
		private final byte[] nonce;
		private final String encryptedPart;

		public InterpretedString(Boolean bool) {
			this.isGood = false;
			preamble = null;
			nonce = null;
			encryptedPart = null;
			salt = null;
		}

		public InterpretedString(String preamble, byte[] salt, byte[] nonce, String encryptedPart) {
			this.isGood = true;
			this.preamble = preamble;
			this.salt = salt;
			this.nonce = nonce;
			this.encryptedPart = encryptedPart;
		}

		private boolean isEncryptedString() {
			return encryptedPart != null
					&& !encryptedPart.isEmpty()
					&& isGood;
		}

		public static InterpretedString interpret(String encryptedString) {
			if (encryptedString == null || encryptedString.isEmpty()) {
				return new InterpretedString(null, null, null, null);
			} else if (encryptedString.startsWith(ENCRYPTED_PREAMPLE)) {
				String[] split = encryptedString.split("\\|", 4);
				if (split.length == 4) {
					final Base64.Decoder decoder = Base64.getDecoder();
					return new InterpretedString(
							split[0],
							decoder.decode(split[1]),
							decoder.decode(split[2]),
							split[3]
					);
				}
			}
			return new InterpretedString(false);
		}

	}
}
