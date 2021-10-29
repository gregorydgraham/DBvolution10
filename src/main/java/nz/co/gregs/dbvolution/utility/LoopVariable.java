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

import java.util.function.Supplier;

/**
 * Implements a while/for loop combination.
 * 
 * @author gregorygraham
 */
public class LoopVariable {

	private boolean needed = true;
	private int tries = 0;
	private int maxAttemptsAllowed = 1000;
	private boolean limitMaxAttempts = true;

	public static LoopVariable factory() {
		return new LoopVariable();
	}

	/**
	 * Checks the whether the loop is still needed (that is {@link #done()} has
	 * not been called) and if the loop has exceeded the maximum attempts (if a
	 * max is defined).
	 *
	 * @return true if the loop is still needed.
	 */
	public boolean isNeeded() {
		if (limitMaxAttempts) {
			return needed && attempts() < maxAttemptsAllowed;
		} else {
			return needed;
		}
	}

	/**
	 * Checks the whether the loop is still needed (that is {@link #done()} has
	 * not been called) and if the loop has exceeded the maximum attempts (if a
	 * max is defined).
	 *
	 * @return true if the loop is no longer needed.
	 */
	public boolean isNotNeeded() {
		if (limitMaxAttempts) {
			return !needed || attempts() >= maxAttemptsAllowed;
		} else {
			return !needed;
		}
	}

	/**
	 * Synonym for {@link #isNotNeeded() }.
	 *
	 * @return true if the loop is no longer needed.
	 */
	public boolean hasHappened() {
		return isNotNeeded();
	}

	/**
	 * Synonym for {@link #isNeeded() }.
	 *
	 * @return true if the loop is still needed.
	 */
	public boolean hasNotHappened() {
		return isNeeded();
	}

	/**
	 * Informs the LoopVariable that the loop has been successful and is no longer
	 * needed.
	 * <p>
	 * This method is used to indicate that a loop that takes multiple attempts to
	 * complete one task, has successfully completed that task.</p>
	 */
	public void done() {
		needed = false;
	}

	/**
	 * Indicates that an attempt has been started.
	 *
	 * <p>
	 * This method is used to indicate that a loop that takes multiple attempts to
	 * complete one task, has started an attempt to complete that task. Each cvall
	 * of {@link #attempt() } counts towards the
	 * {@link #setMaxAttemptsAllowed(int) maximum attempts} if a maximum has been
	 * set.</p>
	 *
	 */
	public void attempt() {
		tries++;
	}

	/**
	 * The number of attempts recorded using {@link #attempt() }.
	 *
	 * @return the number of attempts started.
	 */
	public int attempts() {
		return tries;
	}

	/**
	 * Sets the maximum attempts allowed for this loop variable.
	 *
	 * <p>
	 * Maximum attempts will stop a correctly used LoopVariable after the maximum
	 * attempts by changing {@link #isNeeded() } to false</p>
	 *
	 * <p>
	 * Attempts are registered by calling {@link #attempt() } at the start of each
	 * loop.</p>
	 *
	 * @param maxAttemptsAllowed
	 */
	public void setMaxAttemptsAllowed(int maxAttemptsAllowed) {
		if (maxAttemptsAllowed > 0) {
			limitMaxAttempts = true;
			this.maxAttemptsAllowed = maxAttemptsAllowed;
		}
	}

	/**
	 * Removes the default limit from the LoopVariable.
	 *
	 * <p>
	 * By default {@link #isNeeded() } will return false after 1000 attempts. Use
	 * this method to remove the limit and permit infinite loops.</p>
	 *
	 * <p>
	 * Alternatively you can seta higher, or lower, limit with {@link #setMaxAttemptsAllowed(int)
	 * }.</p>
	 */
	public void setInfiniteLoopPermitted() {
		limitMaxAttempts = false;
	}

	/**
	 * Performs action until {@link #done() } has been called.
	 *
	 * <p>
	 * This is a re-implementation of the while loop mechanism. Probably not as
	 * good as the actual while loop but it was fun to do.</p>
	 *
	 * <p>
	 * recommended use:</p>
	 * <pre>
	 *		// Decide on a maximum number of loops
	 *		final int intendedAttempts = 10;
	 *		
	 *		// Create the method to loop over
	 *		final Supplier&lt;Void&gt; action = () -> {
	 *			// do your processing
	 *			// here
	 *
	 *			// check for terminating condition
	 *			if (successfullyCompleted()) {
	 *				// call done() on the LoopVariable to stop the loop
	 *				looper.done();
	 *			}
	 *			// return null as required by the Java spec
	 *			return null;
	 *		};
	 * 
	 *		// Create the LoopVariable
	 *		LoopVariable looper = new LoopVariable();
	 * 
	 *		// set the maximum number of loops (this is optional, but will default to 1000 anyway)
	 *		looper.setMaxAttemptsAllowed(intendedAttempts);
	 * 
	 *		// loop over the action
	 *		looper.loop(action);
	 * </pre>
	 *
	 * @param action
	 */
	public void loop(Supplier<Void> action) {
		while (isNeeded()) {
			attempt();
			action.get();
		}
	}

	/**
	 * Performs action until {@link #done() } has been called, or {@link #attempt()
	 * } has been called maxAttempts times.
	 *
	 * <p>
	 * This is a re-implementation of the while loop mechanism. Probably not as
	 * good as the actual while loop but it was fun to do.</p>
	 *
	 * <pre>
	 *		// Create the LoopVariable
	 *		LoopVariable looper = new LoopVariable();
	 *
	 *		// Call loop() with maximum number of attempts to try
	 *		looper.loop(100, () -> {
	 *
	 *			// Perform your actions here
	 *
	 *			// check for the termination conditions
	 *			if (looper.attempts() >= 10) {
	 *				// call done() to terminate the loop
	 *				looper.done();
	 *			}
	 *
	 *			// return NULL because Java requires us to
	 *			return null;
	 *		});
	 * </pre>
	 *
	 * @param maxAttempts
	 * @param action
	 * @deprecated this is superfluous and, very slightly, slower. Just use {@link #loop(java.util.function.Supplier) }
	 */
	@Deprecated
	public void loop(int maxAttempts, Supplier<Void> action) {
		while (isNeeded() && attempts() < maxAttempts) {
			attempt();
			action.get();
		}
	}

	/**
	 * Performs action until test returns true.
	 *
	 * <p>
	 * This is a re-implementation of the while loop mechanism. Probably not as
	 * good as the actual while loop but it was fun to do.</p>
	 *
	 * <pre>
	 *		LoopVariable looper = new LoopVariable();
	 *		final int intendedAttempts = 10;
	 *		looper.loop(
	 *				() -> {
	 *					// do your processing here
	 *
	 *					// return null as required by Java
	 *					return null;
	 *				},
	 *				() -> {
	 *					// Check for termination conditions here
	 *					return trueIfTaskCompletedOtherwiseFalse();
	 *				}
	 *		);
	 * </pre>
	 *
	 * @param action
	 * @param test
	 */
	public void loop(Supplier<Void> action, Supplier<Boolean> test) {
		while (isNeeded()) {
			attempt();
			action.get();
			if (test.get()) {
				done();
			}
		}
	}
}
