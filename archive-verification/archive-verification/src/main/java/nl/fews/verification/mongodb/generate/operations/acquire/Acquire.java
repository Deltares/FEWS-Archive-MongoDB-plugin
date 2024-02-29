package nl.fews.verification.mongodb.generate.operations.acquire;

import nl.fews.verification.mongodb.generate.shared.execute.Execute;
import nl.fews.verification.mongodb.generate.shared.graph.Graph;

public final class Acquire {
	private Acquire(){}

	/**
	 * Executes a directed acyclic graph of tasks.
	 *
	 * The execute method retrieves a directed acyclic graph (DAG) of tasks based on the Acquire class. It then iterates over the groups of vertices in the graph and executes each group
	 * of tasks. The execution of each group is performed using a thread pool to execute the tasks concurrently.
	 *
	 * The method uses the following helper methods from the Graph class:
	 * - getDirectedAcyclicGraph: Retrieves a directed acyclic graph (DAG) based on a given class. In this case, the Acquire class is used as the input.
	 * - getDirectedAcyclicGraphGroups: Takes a DirectedAcyclicGraph and returns a list of groups where each group contains vertices that do not have any ancestors in the graph. The
	 * method uses a clone of the input graph to ensure that the original graph is not modified.
	 */
	public static void execute(){
		Graph.getDirectedAcyclicGraphGroups(Graph.getDirectedAcyclicGraph(Acquire.class)).forEach(Execute::execute);
	}
}
