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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import nz.co.gregs.dbvolution.utility.Random;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.crypto.cipher.CryptoCipher;
import org.apache.commons.crypto.utils.Utils;

/**
 *
 * @author gregorygraham
 */
public class Encryption_BASE64_AES_CBC_PKCS5Padding {

	private static final String TRANSFORM = "AES/CBC/PKCS5Padding";
	private static final String ENCRYPTED_PREAMPLE = "BASE64_AES_CBC_PKCS5Padding";

	public static String encrypt(String passphrase, String inputString) throws CannotEncryptInputException {
		Properties properties = new Properties();
		//Creates a CryptoCipher instance with the transformation and properties.
		final int updateBytes;
		final int finalBytes;
		try (CryptoCipher encipher = Utils.getCipherInstance(TRANSFORM, properties)) {

			final byte[] utF8Bytes = getUTF8Bytes(inputString);
			int bufferSize = utF8Bytes.length;
			ByteBuffer inBuffer = ByteBuffer.allocateDirect(bufferSize);
			ByteBuffer outBuffer = ByteBuffer.allocateDirect(bufferSize*2);
			inBuffer.put(utF8Bytes);

			inBuffer.flip(); // ready for the cipher to read it

			// Initializes the cipher with ENCRYPT_MODE,key and iv.
			byte[] salt = makeSalt();
			final SecretKey secretKeySpec = getSecretKeySpec(passphrase, salt);
			final IvParameterSpec iv = getIV();
			encipher.init(Cipher.ENCRYPT_MODE, secretKeySpec, iv);
			// Continues a multiple-part encryption/decryption operation for byte buffer.
			updateBytes = encipher.update(inBuffer, outBuffer);

			// We should call do final at the end of encryption/decryption.
			finalBytes = encipher.doFinal(inBuffer, outBuffer);

			outBuffer.flip(); // ready for use as decrypt
			byte[] encoded = new byte[updateBytes + finalBytes];
			outBuffer.duplicate().get(encoded);

			final String base64Encoded = new String(Base64.encodeBase64(encoded));
			final String base64Salt = new String(Base64.encodeBase64(salt));
			final String base64IV = new String(Base64.encodeBase64(iv.getIV()));

			return ENCRYPTED_PREAMPLE + "|" + base64Salt + "|" + base64IV + "|" + base64Encoded;
		} catch (IOException | InvalidKeyException | InvalidAlgorithmParameterException | ShortBufferException | IllegalBlockSizeException | BadPaddingException | NoSuchAlgorithmException | InvalidKeySpecException ex) {
			Logger.getLogger(Encryption_BASE64_AES_CBC_PKCS5Padding.class.getName()).log(Level.SEVERE, null, ex);
			throw new CannotEncryptInputException(ex);
		}
	}

	private static IvParameterSpec getIV() {
		return getIV(Random.string(16));
	}

	private static IvParameterSpec getIV(String ivString) {
		byte[] iv = getUTF8Bytes(ivString);
		return getIV(iv);
	}

	private static IvParameterSpec getIV(byte[] iv) {
		final IvParameterSpec ivParameterSpec = new IvParameterSpec(Arrays.copyOf(iv, iv.length));
		return ivParameterSpec;
	}

	private static SecretKey getSecretKeySpec(String passphrase, byte[] salt) throws NoSuchAlgorithmException, InvalidKeySpecException {
		SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
		KeySpec spec = new PBEKeySpec(passphrase.toCharArray(), salt, 65536, 256);
		SecretKey tmp = factory.generateSecret(spec);
		SecretKey secret = new SecretKeySpec(tmp.getEncoded(), "AES");
		return secret;
	}

	private static byte[] makeSalt() {
		return Random.bytes(16);
	}

	public static String decrypt(String passphrase, String encryptedString) throws UnableToDecryptInput {
		InterpretedString interpreted = InterpretedString.interpret(encryptedString);
		if (interpreted.isEncryptedString()) {
			Properties properties = new Properties();
			// decode the base64 encoded input string
			byte[] decodedBytes = Base64.decodeBase64(interpreted.encryptedPart);
			int bufferSize = decodedBytes.length;
			//Creates a CryptoCipher instance with the transformation and properties.
			final ByteBuffer outBuffer = ByteBuffer.allocateDirect(bufferSize);
			// push the decoded input into the buffer
			outBuffer.put(decodedBytes);
			// reverse the buffer to output mode for processing
			outBuffer.flip();

			try (CryptoCipher decipher = Utils.getCipherInstance(TRANSFORM, properties)) {
				final SecretKey secretKeySpec = getSecretKeySpec(passphrase, interpreted.getSalt());
				final IvParameterSpec iv = getIV(interpreted.getIV());
				decipher.init(Cipher.DECRYPT_MODE, secretKeySpec, iv);
				ByteBuffer decoded = ByteBuffer.allocateDirect(bufferSize*2);
				decipher.update(outBuffer, decoded);
				decipher.doFinal(outBuffer, decoded);
				decoded.flip(); // ready for use
				final String decrypted = asString(decoded);
				return decrypted;
			} catch (IOException | ShortBufferException | IllegalBlockSizeException | BadPaddingException | NoSuchAlgorithmException | InvalidKeySpecException | InvalidKeyException | InvalidAlgorithmParameterException ex) {
				Logger.getLogger(Encryption_BASE64_AES_CBC_PKCS5Padding.class.getName()).log(Level.SEVERE, null, ex);
				throw new UnableToDecryptInput(ex);
			}
		} else {
			throw new UnableToDecryptInput();
		}
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

	private Encryption_BASE64_AES_CBC_PKCS5Padding() {
	}

	private static class InterpretedString {

		private final boolean isGood;
		private final byte[] salt;
		private final byte[] iv;
		private final String encryptedPart;

		public InterpretedString(Boolean bool) {
			this.isGood = false;
			iv = null;
			encryptedPart = null;
			salt = null;
		}

		public InterpretedString(String preamble, byte[] salt, byte[] iv, String encryptedPart) {
			this.isGood = true;
			this.salt = salt;
			this.iv = iv;
			this.encryptedPart = encryptedPart;
		}

		private boolean isEncryptedString() {
			return encryptedPart != null
					&& !encryptedPart.isEmpty()
					&& isGood;
		}

		private byte[] getIV() {
			return iv;
		}

		private byte[] getSalt() {
			return salt;
		}

		public static InterpretedString interpret(String encryptedString) {
			if (encryptedString == null || encryptedString.isEmpty()) {
				return new InterpretedString(null, null, null, null);
			} else if (encryptedString.startsWith(ENCRYPTED_PREAMPLE)) {
				String[] split = encryptedString.split("\\|", 4);
				if (split.length == 4) {
					return new InterpretedString(
							split[0],
							Base64.decodeBase64(split[1]),
							Base64.decodeBase64(split[2]),
							split[3]
					);
				}
			}
			return new InterpretedString(false);
		}

	}
}
