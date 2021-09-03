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

import nz.co.gregs.dbvolution.utility.Toggle;
import java.io.IOException;
import java.nio.BufferOverflowException;
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
import nz.co.gregs.dbvolution.exceptions.CannotEncryptInputException;
import nz.co.gregs.dbvolution.exceptions.UnableToDecryptInput;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.crypto.cipher.CryptoCipher;
import org.apache.commons.crypto.utils.Utils;

/**
 *
 * @author gregorygraham
 */
public class Encryption_Internal {

	private static final int DEFAULT_BUFFER_SIZE = 1024;
	private static final String TRANSFORM = "AES/CBC/PKCS5Padding";
	private static final IvParameterSpec IV = new IvParameterSpec(getUTF8Bytes("DBVOLUTION IV SE"));
	private static final SecretKeySpec KEY = new SecretKeySpec(getUTF8Bytes("DBVOLUTION KEY S"), "AES");
	private static final String ENCYPTED_PREAMPLE = "BASE64_AES:";

	public static String encrypt(String inputString) throws CannotEncryptInputException {
		Properties properties = new Properties();
		//Creates a CryptoCipher instance with the transformation and properties.
		int finalBytes;
		try (CryptoCipher encipher = Utils.getCipherInstance(TRANSFORM, properties)) {

			final byte[] utF8Bytes = getUTF8Bytes(inputString);
			ByteBuffer inBuffer;

			int bufferSize = DEFAULT_BUFFER_SIZE;
			if (utF8Bytes.length > bufferSize) {
				bufferSize = utF8Bytes.length;
			}
			inBuffer = ByteBuffer.allocateDirect(bufferSize);

			inBuffer.put(utF8Bytes);

			inBuffer.flip(); // ready for the cipher to read it

			// Initializes the cipher with ENCRYPT_MODE,key and iv.
			encipher.init(Cipher.ENCRYPT_MODE, KEY, IV);

			ByteBuffer outBuffer;
			int outBufferSize = bufferSize;
			int updateBytes;
			boolean updateCompleted = false;
			while (!updateCompleted) {
				try {
					outBuffer = ByteBuffer.allocateDirect(outBufferSize);
					// Continues a multiple-part encryption/decryption operation for byte buffer.
					updateBytes = encipher.update(inBuffer, outBuffer);
					updateCompleted = true;
					// We should call do final at the end of encryption/decryption.
					finalBytes = encipher.doFinal(inBuffer, outBuffer);

					outBuffer.flip(); // ready for use as decrypt
					byte[] encoded = new byte[updateBytes + finalBytes];
					outBuffer.duplicate().get(encoded);

					final String base64Encoded = new String(Base64.encodeBase64(encoded));

					return ENCYPTED_PREAMPLE + base64Encoded;
				} catch (javax.crypto.ShortBufferException exp) {
					outBufferSize *= 2;
				} catch (IllegalBlockSizeException | BadPaddingException | BufferOverflowException ex) {
					Logger.getLogger(Encryption_Internal.class.getName()).log(Level.SEVERE, null, ex);
					throw new CannotEncryptInputException(ex);
				}
			}
		} catch (IOException | InvalidKeyException | InvalidAlgorithmParameterException ex) {
			Logger.getLogger(Encryption_Internal.class.getName()).log(Level.SEVERE, null, ex);
			throw new CannotEncryptInputException(ex);
		}
		throw new CannotEncryptInputException();
	}

	public static String decrypt(String encodedCipherText) throws UnableToDecryptInput {
		if (encodedCipherText != null && !encodedCipherText.isEmpty() && encodedCipherText.startsWith(ENCYPTED_PREAMPLE)) {
			String encryptedString = encodedCipherText.replaceFirst(ENCYPTED_PREAMPLE, "");
			Properties properties = new Properties();
			// decode the base64 encoded input string
			byte[] encryptedBytes = Base64.decodeBase64(encryptedString);
			int bufferSize = DEFAULT_BUFFER_SIZE;
			if (encryptedBytes.length > bufferSize) {
				bufferSize = encryptedBytes.length;
			}
			//Creates a CryptoCipher instance with the transformation and properties.
			ByteBuffer inBuffer;
			inBuffer = ByteBuffer.allocateDirect(bufferSize);

			// push the decoded input into the buffer
			inBuffer.put(encryptedBytes);
			// reverse the buffer to output mode for processing
			inBuffer.flip();

			try (CryptoCipher decipher = Utils.getCipherInstance(TRANSFORM, properties)) {
				decipher.init(Cipher.DECRYPT_MODE, KEY, IV);
				int outBufferSize = bufferSize;
				Toggle update = new Toggle();
				Toggle doFinal = new Toggle();
				while (update.isNeeded() || doFinal.isNeeded()) {
					ByteBuffer outBuffer = ByteBuffer.allocateDirect(outBufferSize);
					try {
						decipher.update(inBuffer, outBuffer);
						update.done();
						decipher.doFinal(inBuffer, outBuffer);
						doFinal.done();
						outBuffer.flip(); // ready for use
						final String decrypted = asString(outBuffer);
						return decrypted;
					} catch (ShortBufferException ex) {
						outBufferSize *= 2;
					}
				}
			} catch (IOException | InvalidKeyException | InvalidAlgorithmParameterException | IllegalBlockSizeException | BadPaddingException | BufferOverflowException ex) {
				Logger.getLogger(Encryption_Internal.class
						.getName()).log(Level.SEVERE, null, ex);
				throw new UnableToDecryptInput(ex);
			}
		} else {
			throw new UnableToDecryptInput();
		}
		throw new UnableToDecryptInput();
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

	private Encryption_Internal() {
	}
}
