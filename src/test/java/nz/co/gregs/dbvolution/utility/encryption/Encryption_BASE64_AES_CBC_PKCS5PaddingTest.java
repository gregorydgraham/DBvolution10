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
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author gregorygraham
 */
public class Encryption_BASE64_AES_CBC_PKCS5PaddingTest {

	public Encryption_BASE64_AES_CBC_PKCS5PaddingTest() {
	}

	@Test
	public void testEncryptionAndDecryption() {
		String original = "really long and awkward sentence to test that the encryption and decryption works";
		try {
			final String passphrase = "secret key of safety";
			String encrypt = Encryption_BASE64_AES_CBC_PKCS5Padding.encrypt(passphrase, original);
			Assert.assertThat(encrypt, not(original));
			Assert.assertThat(encrypt, startsWith("BASE64_AES_CBC_PKCS5Padding|"));

			String decrypt = Encryption_BASE64_AES_CBC_PKCS5Padding.decrypt(passphrase, encrypt);
			Assert.assertThat(decrypt, is(original));

		} catch (Exception ex) {
			Logger.getLogger(Encryption_BASE64_AES_CBC_PKCS5PaddingTest.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

}
