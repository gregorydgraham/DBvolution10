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
package nz.co.gregs.dbvolution.utility;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.crypto.cipher.CryptoCipher;
import org.apache.commons.crypto.utils.Utils;

/**
 *
 * @author gregorygraham
 */
public class Encryption {

	private static final int BUFFERSIZE = 1024;
	private static final String TRANSFORM = "AES/CBC/PKCS5Padding";
	private static final IvParameterSpec IV = new IvParameterSpec(getUTF8Bytes("DBVOLUTION IV SE"));
	private static final SecretKeySpec KEY = new SecretKeySpec(getUTF8Bytes("DBVOLUTION KEY S"), "AES");
	private static final String ENCYPTED_PREAMPLE = "BASE64_AES:";

	public static String encrypt(String inputString) throws CannotEncryptInputException {
		Properties properties = new Properties();
		//Creates a CryptoCipher instance with the transformation and properties.
		final ByteBuffer outBuffer;
		final int updateBytes;
		final int finalBytes;
		try (CryptoCipher encipher = Utils.getCipherInstance(TRANSFORM, properties)) {

			ByteBuffer inBuffer = ByteBuffer.allocateDirect(BUFFERSIZE);
			outBuffer = ByteBuffer.allocateDirect(BUFFERSIZE);
			inBuffer.put(getUTF8Bytes(inputString));

			inBuffer.flip(); // ready for the cipher to read it

			// Initializes the cipher with ENCRYPT_MODE,key and iv.
			encipher.init(Cipher.ENCRYPT_MODE, KEY, IV);
			// Continues a multiple-part encryption/decryption operation for byte buffer.
			updateBytes = encipher.update(inBuffer, outBuffer);

			// We should call do final at the end of encryption/decryption.
			finalBytes = encipher.doFinal(inBuffer, outBuffer);

			outBuffer.flip(); // ready for use as decrypt
			byte[] encoded = new byte[updateBytes + finalBytes];
			outBuffer.duplicate().get(encoded);

			final String base64Encoded = new String(Base64.encodeBase64(encoded));

			return ENCYPTED_PREAMPLE + base64Encoded;
		} catch (IOException | InvalidKeyException | InvalidAlgorithmParameterException | ShortBufferException | IllegalBlockSizeException | BadPaddingException ex) {
			Logger.getLogger(Encryption.class.getName()).log(Level.SEVERE, null, ex);
			throw new CannotEncryptInputException(ex);
		}
	}

	public static String decrypt(String encryptedString) throws UnableToDecryptInput {
		if (encryptedString != null && !encryptedString.isEmpty() && encryptedString.startsWith(ENCYPTED_PREAMPLE)) {
			String removedPreample = encryptedString.replaceFirst(ENCYPTED_PREAMPLE, "");
			Properties properties = new Properties();
			//Creates a CryptoCipher instance with the transformation and properties.
			final ByteBuffer outBuffer = ByteBuffer.allocateDirect(BUFFERSIZE);
			// decode the base64 encoded input string
			byte[] decodedBytes = Base64.decodeBase64(removedPreample);
			// push the decoded input into the buffer
			outBuffer.put(decodedBytes);
			// reverse the buffer to output mode for processing
			outBuffer.flip();

			try (CryptoCipher decipher = Utils.getCipherInstance(TRANSFORM, properties)) {
				decipher.init(Cipher.DECRYPT_MODE, KEY, IV);
				ByteBuffer decoded = ByteBuffer.allocateDirect(BUFFERSIZE);
				decipher.update(outBuffer, decoded);
				decipher.doFinal(outBuffer, decoded);
				decoded.flip(); // ready for use
				final String decrypted = asString(decoded);
				return decrypted;
			} catch (IOException | InvalidKeyException | InvalidAlgorithmParameterException | ShortBufferException | IllegalBlockSizeException | BadPaddingException ex) {
				Logger.getLogger(Encryption.class.getName()).log(Level.SEVERE, null, ex);
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

	private Encryption() {
	}

	public static class UnableToDecryptInput extends Exception {

		static final long serialVersionUID = 1L;

		public UnableToDecryptInput() {
			super();
		}

		public UnableToDecryptInput(Throwable ex) {
			super(ex);
		}
	}

	public static class CannotEncryptInputException extends Exception {

		static final long serialVersionUID = 1L;

		public CannotEncryptInputException(Throwable ex) {
			super(ex);
		}
	}
}
