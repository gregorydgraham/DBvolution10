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
public class Encryption_BASE64_AES_GCM_NoPaddingTest {

	public Encryption_BASE64_AES_GCM_NoPaddingTest() {
	}

	@Test
	public void testEncryptionAndDecryption() {
		String original = "really long and awkward sentence to test that the encryption and decryption works";
		try {
			final String passphrase = "secret key of safety";
			String encrypt = Encryption_BASE64_AES_GCM_NoPadding.encrypt(passphrase, original);
			assertThat(encrypt, not(original));

			String decrypt = Encryption_BASE64_AES_GCM_NoPadding.decrypt(passphrase, encrypt);
			assertThat(decrypt, is(original));

		} catch (Exception ex) {
			Logger.getLogger(Encryption_BASE64_AES_GCM_NoPaddingTest.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

}
