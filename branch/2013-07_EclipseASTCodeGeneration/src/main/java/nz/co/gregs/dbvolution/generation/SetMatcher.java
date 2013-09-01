package nz.co.gregs.dbvolution.generation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Utility class for comparing two sets of things and grouping them into matched and unmatched items.
 * Designed to handle matching between different types by the caller supplying a custom comparator.
 */
final class SetMatcher<A,B> {
	private Collection<A> collectionA;
	private Collection<B> collectionB;
	private Comparator<Object> comparator;
	
	private List<A> onlyInA;
	private List<B> onlyInB;
	private Map<A,B> inBothViaA;
	private Map<B,A> inBothViaB;
	
	/**
	 * 
	 * @param collectionA
	 * @param collectionB
	 * @param comparator must be capable of comparing type A to type B
	 */
	public SetMatcher(Collection<A> collectionA, Collection<B> collectionB, Comparator<Object> comparator) {
		this.collectionA = collectionA;
		this.collectionB = collectionB;
		this.comparator = comparator;
		matchup();
	}
	
	private void matchup() {
		this.onlyInA = new ArrayList<A>();
		this.onlyInB = new ArrayList<B>();
		this.inBothViaA = new HashMap<A,B>();
		this.inBothViaB = new HashMap<B,A>();
		
		List<B> remainingInB = new LinkedList<B>(collectionB);
		for (A a: collectionA) {
			Iterator<B> itrB = remainingInB.iterator();
			while (itrB.hasNext()) {
				B b = itrB.next();
				
				if (comparator.compare(a, b) == 0) {
					inBothViaA.put(a, b);
					inBothViaB.put(b, a);
					itrB.remove();
				}
			}
			
			// A not in B
			onlyInA.add(a);
		}
		
		// B not in A
		onlyInB.addAll(remainingInB);
	}
	
	public List<A> getOnlyInA() {
		return onlyInA;
	}

	public Collection<A> getCollectionA() {
		return collectionA;
	}

	public Collection<B> getCollectionB() {
		return collectionB;
	}

	public Comparator<Object> getComparator() {
		return comparator;
	}

	public List<B> getOnlyInB() {
		return onlyInB;
	}

	public Map<A, B> getInBothViaA() {
		return inBothViaA;
	}

	public Map<B, A> getInBothViaB() {
		return inBothViaB;
	}
	
	
}
