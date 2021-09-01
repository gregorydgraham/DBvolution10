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

import java.util.logging.Level;
import java.util.logging.Logger;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Test;

/**
 *
 * @author gregorygraham
 */
public class EncryptedTest {

	public EncryptedTest() {
	}

	@Test
	public void testEncryption_Internal() {
		String original = "really long and awkward sentence to test that the encryption and decryption works";
		try {
			String encrypt = Encryption_Internal.encrypt(original);
			assertThat(encrypt, not(original));
			assertThat(encrypt, is("BASE64_AES:7OGfgSiNNz4Jd2B3oJBXEkF3odydPiW3fKAkUdx5Xq/6fXFAs7Tip6SMaObseLzQAQFn63yp+XKqjMOXujxc3f/SISVbMfDYn0Ak1KTxyTJJmH4HYnc7Xiemj9h8bA3O"));

			String decrypt = Encryption_Internal.decrypt(encrypt);
			assertThat(decrypt, is(original));

		} catch (Exception ex) {
			Logger.getLogger(EncryptedTest.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	@Test
	public void testDecryptionWithEncryption_Internal() {
		String original = "really long and awkward sentence to test that the encryption and decryption works";
		String password = "The really good password no one can guess";
		try {
			String encrypt = Encryption_Internal.encrypt(original);
			assertThat(encrypt, not(original));
			assertThat(encrypt, is("BASE64_AES:7OGfgSiNNz4Jd2B3oJBXEkF3odydPiW3fKAkUdx5Xq/6fXFAs7Tip6SMaObseLzQAQFn63yp+XKqjMOXujxc3f/SISVbMfDYn0Ak1KTxyTJJmH4HYnc7Xiemj9h8bA3O"));

			String decrypt = Encrypted.fromCipherText(encrypt).decrypt(password);
			assertThat(decrypt, is(original));

		} catch (Exception ex) {
			Logger.getLogger(EncryptedTest.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	@Test
	public void testDecryptionWithCBCAlgorithm() throws Exception {
		String original = "really long and awkward sentence to test that the encryption and decryption works";
		String password = "The really good password no one can guess";
		try {
			String encrypt = Encryption_BASE64_AES_CBC_PKCS5Padding.encrypt(password, original);
			assertThat(encrypt, not(original));

			String decrypt = Encrypted.fromCipherText(encrypt).decrypt(password);
			assertThat(decrypt, is(original));

		} catch (Exception ex) {
			Logger.getLogger(EncryptedTest.class.getName()).log(Level.SEVERE, null, ex);
			throw ex;
		}
	}

	@Test
	public void testDecryptionWithGCMAlgorithm() throws Exception {
		String original = "really long and awkward sentence to test that the encryption and decryption works";
		String password = "The really good password no one can guess";
		try {
			String encrypt = Encryption_BASE64_AES_GCM_NoPadding.encrypt(password, original);
			assertThat(encrypt, not(original));

			String decrypt = Encrypted.fromCipherText(encrypt).decrypt(password);
			assertThat(decrypt, is(original));

		} catch (Exception ex) {
			Logger.getLogger(EncryptedTest.class.getName()).log(Level.SEVERE, null, ex);
			throw ex;
		}
	}

	@Test
	public void testDecryptionWithEncrypted() throws Exception {
		String original = "really long and awkward sentence to test that the encryption and decryption works";
		String password = "The really good password no one can guess";
		try {
			Encrypted encrypt = Encrypted.encrypt(password, original);
			assertThat(encrypt.getCypherText(), not(original));

			String decrypt = encrypt.decrypt(password);
			assertThat(decrypt, is(original));

		} catch (Exception ex) {
			Logger.getLogger(EncryptedTest.class.getName()).log(Level.SEVERE, null, ex);
			throw ex;
		}
	}

}
