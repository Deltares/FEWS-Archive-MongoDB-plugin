package nl.fews.verification.mongodb.web.controllers.verification;

import graphql.schema.DataFetchingEnvironment;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.web.shared.conversion.Conversion;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;

import java.util.List;
import java.util.stream.StreamSupport;

@Controller
public class ConfigurationSettings {
	@QueryMapping
	public Document configurationSettingsById(@Argument String _id, DataFetchingEnvironment e){
		return Mongo.findOne("configuration.Settings", new Document("_id", new ObjectId(_id)), Conversion.getProjection(e));
	}

	@QueryMapping
	public Document configurationSettingsByEnvironment(@Argument String environment, DataFetchingEnvironment e){
		return Mongo.findOne("configuration.Settings", new Document("Environment", environment), Conversion.getProjection(e));
	}

	@QueryMapping
	public List<Document> configurationSettingsN(DataFetchingEnvironment e){
		return StreamSupport.stream(Mongo.find("configuration.Settings", new Document(), Conversion.getProjection(e)).sort(new Document("environment", 1)).spliterator(), false).toList();
	}

	@MutationMapping
	public String createConfigurationSettings(
			@Argument String toEmailAddresses,
			@Argument String fromEmailAddress,
			@Argument String drdlYamlPath,
			@Argument String fewsRestApiUri,
			@Argument String environment,
			@Argument String cubeAdmins,
			@Argument String cubeUsers,
			@Argument String bimPath,
			@Argument String databaseConnectionString,
			@Argument String smtpServer,
			@Argument String tabularConnectionString,
			@Argument String drdlYamlServiceRestart,
			@Argument boolean execute,
			@Argument String reprocessCubes,
			@Argument String taskInterval,
			@Argument String fewsArchiveDbAesPassword,
			@Argument String fewsArchiveDbUsername,
			@Argument String drdlYamlConfigPath,
			@Argument String fewsArchiveDbConnection,
			@Argument String drdlYamlServiceRestartSuccess,
			@Argument int dataStaleAfterSeconds,
			@Argument int cubeParallelPartitions,
			@Argument int cubeThreads,
			@Argument int dataParallelPartitions,
			@Argument int dataThreads,
			@Argument String fewsVerificationDbAesPassword,
			@Argument String fewsVerificationDbConnection,
			@Argument String fewsVerificationDbUsername,
			@Argument boolean parallel,
			@Argument boolean processData){
		return Mongo.insertOne("configuration.Settings",
				new Document("toEmailAddresses", toEmailAddresses)
				.append("fromEmailAddress", fromEmailAddress)
				.append("drdlYamlPath", drdlYamlPath)
				.append("fewsRestApiUri", fewsRestApiUri)
				.append("environment", environment)
				.append("cubeAdmins", cubeAdmins)
				.append("cubeUsers", cubeUsers)
				.append("bimPath", bimPath)
				.append("databaseConnectionString", databaseConnectionString)
				.append("smtpServer", smtpServer)
				.append("tabularConnectionString", tabularConnectionString)
				.append("drdlYamlServiceRestart", drdlYamlServiceRestart)
				.append("execute", execute)
				.append("reprocessCubes", reprocessCubes)
				.append("taskInterval", taskInterval)
				.append("fewsArchiveDbAesPassword", fewsArchiveDbAesPassword)
				.append("fewsArchiveDbUsername", fewsArchiveDbUsername)
				.append("drdlYamlConfigPath", drdlYamlConfigPath)
				.append("fewsArchiveDbConnection", fewsArchiveDbConnection)
				.append("drdlYamlServiceRestartSuccess", drdlYamlServiceRestartSuccess)
				.append("dataStaleAfterSeconds", dataStaleAfterSeconds)
				.append("cubeParallelPartitions", cubeParallelPartitions)
				.append("cubeThreads", cubeThreads)
				.append("dataParallelPartitions", dataParallelPartitions)
				.append("dataThreads", dataThreads)
				.append("fewsVerificationDbAesPassword", fewsVerificationDbAesPassword)
				.append("fewsVerificationDbConnection", fewsVerificationDbConnection)
				.append("fewsVerificationDbUsername", fewsVerificationDbUsername)
				.append("parallel", parallel)
				.append("processData", processData)
		).getInsertedId().asObjectId().getValue().toString();
	}

	@MutationMapping
	public Long updateConfigurationSettings(
			@Argument String _id,
			@Argument String toEmailAddresses,
			@Argument String fromEmailAddress,
			@Argument String drdlYamlPath,
			@Argument String fewsRestApiUri,
			@Argument String environment,
			@Argument String cubeAdmins,
			@Argument String cubeUsers,
			@Argument String bimPath,
			@Argument String databaseConnectionString,
			@Argument String smtpServer,
			@Argument String tabularConnectionString,
			@Argument String drdlYamlServiceRestart,
			@Argument boolean execute,
			@Argument String reprocessCubes,
			@Argument String taskInterval,
			@Argument String fewsArchiveDbAesPassword,
			@Argument String fewsArchiveDbUsername,
			@Argument String drdlYamlConfigPath,
			@Argument String fewsArchiveDbConnection,
			@Argument String drdlYamlServiceRestartSuccess,
			@Argument int dataStaleAfterSeconds,
			@Argument int cubeParallelPartitions,
			@Argument int cubeThreads,
			@Argument int dataParallelPartitions,
			@Argument int dataThreads,
			@Argument String fewsVerificationDbAesPassword,
			@Argument String fewsVerificationDbConnection,
			@Argument String fewsVerificationDbUsername,
			@Argument boolean parallel,
			@Argument boolean processData){
		return Mongo.updateOne("configuration.Settings", new Document("_id", new ObjectId(_id)), new Document("$set",
				new Document("toEmailAddresses", toEmailAddresses)
				.append("fromEmailAddress", fromEmailAddress)
				.append("drdlYamlPath", drdlYamlPath)
				.append("fewsRestApiUri", fewsRestApiUri)
				.append("environment", environment)
				.append("cubeAdmins", cubeAdmins)
				.append("cubeUsers", cubeUsers)
				.append("bimPath", bimPath)
				.append("databaseConnectionString", databaseConnectionString)
				.append("smtpServer", smtpServer)
				.append("tabularConnectionString", tabularConnectionString)
				.append("drdlYamlServiceRestart", drdlYamlServiceRestart)
				.append("execute", execute)
				.append("reprocessCubes", reprocessCubes)
				.append("taskInterval", taskInterval)
				.append("fewsArchiveDbAesPassword", fewsArchiveDbAesPassword)
				.append("fewsArchiveDbUsername", fewsArchiveDbUsername)
				.append("drdlYamlConfigPath", drdlYamlConfigPath)
				.append("fewsArchiveDbConnection", fewsArchiveDbConnection)
				.append("drdlYamlServiceRestartSuccess", drdlYamlServiceRestartSuccess)
				.append("dataStaleAfterSeconds", dataStaleAfterSeconds)
				.append("cubeParallelPartitions", cubeParallelPartitions)
				.append("cubeThreads", cubeThreads)
				.append("dataParallelPartitions", dataParallelPartitions)
				.append("dataThreads", dataThreads)
				.append("fewsVerificationDbAesPassword", fewsVerificationDbAesPassword)
				.append("fewsVerificationDbConnection", fewsVerificationDbConnection)
				.append("fewsVerificationDbUsername", fewsVerificationDbUsername)
				.append("parallel", parallel)
				.append("processData", processData)
		)).getModifiedCount();
	}

	@MutationMapping
	public Long deleteConfigurationSettings(@Argument String _id){
		return Mongo.deleteOne("configuration.Settings", new Document("_id", new ObjectId(_id))).getDeletedCount();
	}
}
