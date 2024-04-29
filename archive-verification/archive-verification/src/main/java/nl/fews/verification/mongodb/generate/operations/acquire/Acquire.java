package nl.fews.verification.mongodb.generate.operations.acquire;

import nl.fews.verification.mongodb.generate.shared.execute.Execute;
import nl.fews.verification.mongodb.generate.shared.graph.Graph;

public final class Acquire {
	private Acquire(){}

	public static void execute(){
		Graph.getDirectedAcyclicGraphGroups(Graph.getDirectedAcyclicGraph(Acquire.class)).forEach(Execute::execute);
	}
}
