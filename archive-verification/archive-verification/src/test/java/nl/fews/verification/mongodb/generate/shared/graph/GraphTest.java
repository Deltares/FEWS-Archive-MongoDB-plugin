package nl.fews.verification.mongodb.generate.shared.graph;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class GraphTest {

	@Test
	void getDirectedAcyclicGraphGroups() {
		DirectedAcyclicGraph<Object, DefaultEdge> directedAcyclicGraph = new DirectedAcyclicGraph<>(DefaultEdge.class);
		Object v1 = new Object();
		Object v2 = new Object();
		Object v3 = new Object();
		directedAcyclicGraph.addVertex(v1);
		directedAcyclicGraph.addVertex(v2);
		directedAcyclicGraph.addVertex(v3);
		directedAcyclicGraph.addEdge(v1, v2);
		directedAcyclicGraph.addEdge(v2, v3);

		var result = Graph.getDirectedAcyclicGraphGroups(directedAcyclicGraph);

		assertTrue(result.get(0).contains(v1));
		assertTrue(result.get(1).contains(v2));
		assertTrue(result.get(2).contains(v3));
	}

	@Test
	void getDirectedAcyclicGraph() {
		DirectedAcyclicGraph<Object, DefaultEdge> result = Graph.getDirectedAcyclicGraph(SampleClass.class);

        assertEquals(1, result.vertexSet().size());
        result.vertexSet().forEach(vertex -> {
			assertInstanceOf(SampleClass.class, vertex);
        });

		result = Graph.getDirectedAcyclicGraph(SampleClass.class, new Object[]{"param"});

        assertEquals(1, result.vertexSet().size());
        result.vertexSet().forEach(vertex -> {
			assertInstanceOf(SampleClass.class, vertex);
        });
	}

	public static class SampleClass implements IPredecessor, IExecute {
		public SampleClass(){}
		public SampleClass(String x){}

		@Override
		public String[] getPredecessors() { return new String[]{}; }

		@Override
		public void execute() {	}
	}
}