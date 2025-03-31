package nl.fews.verification.mongodb.generate;

import nl.fews.verification.mongodb.generate.operations.acquire.Acquire;
import nl.fews.verification.mongodb.generate.operations.cube.Cube;
import nl.fews.verification.mongodb.generate.operations.csv.Csv;
import nl.fews.verification.mongodb.generate.operations.deploy.Deploy;
import nl.fews.verification.mongodb.generate.operations.integrity.Integrity;
import nl.fews.verification.mongodb.generate.operations.missing.Missing;
import nl.fews.verification.mongodb.generate.operations.powerquery.PowerQuery;
import nl.fews.verification.mongodb.generate.operations.drdlyaml.DrdlYaml;

import nl.fews.verification.mongodb.shared.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Generate {

	private Generate(){}

	private static final Logger logger = LoggerFactory.getLogger(Generate.class);

	public static void execute() {
		var acquisitionType = Settings.get("acquisitionType", String.class);
		try {
			Acquire.execute();
			Integrity.execute();
			Missing.execute();
			DrdlYaml.execute();
			PowerQuery.execute();
			Cube.execute();
			if (acquisitionType.equals("csv")){
				Csv.execute();
			}
			Deploy.execute();
		}
		catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
			throw ex;
		}
	}
}
