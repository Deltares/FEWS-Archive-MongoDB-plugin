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
import java.util.stream.Collectors;
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
		return StreamSupport.stream(Mongo.find("configuration.Settings", new Document(), Conversion.getProjection(e)).spliterator(), false).collect(Collectors.toList());
	}

	@MutationMapping
	public String createConfigurationSettings(
			@Argument String toEmailAddresses,
			@Argument String fromEmailAddress,
			@Argument String drdlYamlPath,
			@Argument String environment,
			@Argument int threads,
			@Argument String cubeAdmins,
			@Argument String cubeUsers,
			@Argument String bimPath,
			@Argument String databaseConnectionString,
			@Argument String smtpServer,
			@Argument String tabularConnectionString,
			@Argument String drdlYamlServiceRestart,
			@Argument boolean execute,
			@Argument String archiveDb,
			@Argument String taskInterval,
			@Argument String databaseConnectionAesPassword,
			@Argument String databaseConnectionUsername,
			@Argument int parallelPartitions){
		return Mongo.insertOne("configuration.Settings",
				new Document("toEmailAddresses", toEmailAddresses)
				.append("fromEmailAddress", fromEmailAddress)
				.append("drdlYamlPath", drdlYamlPath)
				.append("environment", environment)
				.append("threads", threads)
				.append("cubeAdmins", cubeAdmins)
				.append("cubeUsers", cubeUsers)
				.append("bimPath", bimPath)
				.append("databaseConnectionString", databaseConnectionString)
				.append("smtpServer", smtpServer)
				.append("tabularConnectionString", tabularConnectionString)
				.append("drdlYamlServiceRestart", drdlYamlServiceRestart)
				.append("execute", execute)
				.append("archiveDb", archiveDb)
				.append("taskInterval", taskInterval)
				.append("databaseConnectionAesPassword", databaseConnectionAesPassword)
				.append("databaseConnectionUsername", databaseConnectionUsername)
				.append("parallelPartitions", parallelPartitions)
		).getInsertedId().asObjectId().getValue().toString();
	}

	@MutationMapping
	public Long updateConfigurationSettings(
			@Argument String _id,
			@Argument String toEmailAddresses,
			@Argument String fromEmailAddress,
			@Argument String drdlYamlPath,
			@Argument String environment,
			@Argument int threads,
			@Argument String cubeAdmins,
			@Argument String cubeUsers,
			@Argument String bimPath,
			@Argument String databaseConnectionString,
			@Argument String smtpServer,
			@Argument String tabularConnectionString,
			@Argument String drdlYamlServiceRestart,
			@Argument boolean execute,
			@Argument String archiveDb,
			@Argument String taskInterval,
			@Argument String databaseConnectionAesPassword,
			@Argument String databaseConnectionUsername,
			@Argument int parallelPartitions){
		return Mongo.updateOne("configuration.Settings", new Document("_id", new ObjectId(_id)), new Document("$set",
				new Document("toEmailAddresses", toEmailAddresses)
				.append("fromEmailAddress", fromEmailAddress)
				.append("drdlYamlPath", drdlYamlPath)
				.append("environment", environment)
				.append("threads", threads)
				.append("cubeAdmins", cubeAdmins)
				.append("cubeUsers", cubeUsers)
				.append("bimPath", bimPath)
				.append("databaseConnectionString", databaseConnectionString)
				.append("smtpServer", smtpServer)
				.append("tabularConnectionString", tabularConnectionString)
				.append("drdlYamlServiceRestart", drdlYamlServiceRestart)
				.append("execute", execute)
				.append("archiveDb", archiveDb)
				.append("taskInterval", taskInterval)
				.append("databaseConnectionAesPassword", databaseConnectionAesPassword)
				.append("databaseConnectionUsername", databaseConnectionUsername)
				.append("parallelPartitions", parallelPartitions))).getModifiedCount();
	}

	@MutationMapping
	public Long deleteConfigurationSettings(@Argument String _id){
		return Mongo.deleteOne("configuration.Settings", new Document("_id", new ObjectId(_id))).getDeletedCount();
	}
}
