package nl.fews.verification.mongodb.generate;

import nl.fews.verification.mongodb.generate.operations.acquire.Acquire;
import nl.fews.verification.mongodb.generate.operations.cube.Cube;
import nl.fews.verification.mongodb.generate.operations.deploy.Deploy;
import nl.fews.verification.mongodb.generate.operations.model.Model;
import nl.fews.verification.mongodb.generate.operations.transform.Transform;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Generate {

	private Generate(){}

	private static final Logger logger = LoggerFactory.getLogger(Generate.class);

	public static void execute() {
		try {
			Acquire.execute();
			Transform.execute();
			Model.execute();
			Cube.execute();
			Deploy.execute();
		}
		catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
			throw ex;
		}
	}
}
