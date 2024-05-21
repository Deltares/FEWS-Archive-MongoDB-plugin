package nl.fews.verification.mongodb.generate;

import nl.fews.verification.mongodb.generate.operations.acquire.Acquire;
import nl.fews.verification.mongodb.generate.operations.cube.Cube;
import nl.fews.verification.mongodb.generate.operations.deploy.Deploy;
import nl.fews.verification.mongodb.generate.operations.missing.Missing;
import nl.fews.verification.mongodb.generate.operations.powerquery.PowerQuery;
import nl.fews.verification.mongodb.generate.operations.drdlyaml.DrdlYaml;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Generate {

	private Generate(){}

	private static final Logger logger = LoggerFactory.getLogger(Generate.class);

	public static void execute() {
		try {
			Acquire.execute();
			Missing.execute();
			DrdlYaml.execute();
			PowerQuery.execute();
			Cube.execute();
			Deploy.execute();
		}
		catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
			throw ex;
		}
	}
}
