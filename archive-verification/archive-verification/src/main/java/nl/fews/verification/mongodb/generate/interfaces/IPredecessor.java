package nl.fews.verification.mongodb.generate.interfaces;

public interface IPredecessor {
	/**
	 * Retrieves the predecessors of the current object.
	 *
	 * @return an array of strings representing the predecessors
	 */
	String[] getPredecessors();
}
