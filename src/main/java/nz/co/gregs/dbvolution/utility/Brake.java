/*
 * Copyright 2021 Gregory Graham.
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

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A, probably completely superfluous class, to encapsulate the wait/notifyAll
 * pattern.
 *
 * <p>
 * Use {@link #checkBrake() } to check if waiting is required and wait. Call {@link #release()
 * } in another thread to permit waiting threads to proceed. The brake can be
 * switched off using {@link #release() } and switched on with {@link #brake() }
 * or {@link #apply() }.</p>
 *
 * <p>
 * {@link #brake() } and {@link #apply() } are synonyms.</p>
 * <p>
 * With in the external thread use {@link #release() } to allow the process to
 * continue.</p>
 *
 * @author gregorygraham
 */
public class Brake {

	private boolean brakeRequired = true;
	private final Object monitor = new Object();
	private long timeout = 10000;

	public Brake() {
	}

	public static final Brake withTimeoutInMilliseconds(long milliseconds) {
		var result = new Brake();
		result.timeout = milliseconds;
		return result;
	}

	public static final Brake untilReleased() {
		var result = new Brake();
		result.timeout = 0;
		return result;
	}

	public static final Brake defaultSettings() {
		var result = new Brake();
		return result;
	}

	public void brake() {
		apply();
	}

	public void apply() {
		synchronized (monitor) {
			this.brakeRequired = true;
		}
	}

	public void release() {
		synchronized (monitor) {
			this.brakeRequired = false;
			monitor.notifyAll();
		}
	}

	public void checkBrake() {
		synchronized (monitor) {
			while (brakeRequired) {
				try {
					if (timeout > 0) {
						monitor.wait(timeout);
					} else {
						monitor.wait();
					}
				} catch (InterruptedException ex) {
					Logger.getLogger(Brake.class.getName()).log(Level.SEVERE, null, ex);
				}
			}
		}
	}

}
