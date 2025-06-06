package nl.fews.verification.mongodb.web;

import nl.fews.verification.mongodb.generate.Generate;
import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.mail.Mail;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.json.JSONObject;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

@Configuration
@EnableScheduling
public class GenerateConfiguration implements SchedulingConfigurer {

	private static final Logger logger = LoggerFactory.getLogger(GenerateConfiguration.class);

	private static final AtomicBoolean running = new AtomicBoolean(false);

	public void generate(){
		JSONObject settings;
		try {
			Path path = Path.of(Settings.get("bimPath"), "Settings.json");
			settings = path.toFile().exists() ? Settings.fromJsonString(IO.readString(path)) : null;
		}
		catch (Exception ex) {
			logger.warn(ex.getMessage(), ex);
			settings = null;
		}

		if ((settings == null || !settings.getBoolean("execute")) && running.compareAndSet(false, true)) {
			try {
				Generate.execute();
			}
			catch (Exception ex) {
				logger.warn(ex.getMessage(), ex);
				try (StringWriter sw = new StringWriter(); PrintWriter pw = new PrintWriter(sw)){
					ex.printStackTrace(pw);
					Mail.send("ERROR - Verification GenerateTimer", sw.toString());
				}
				catch (Exception mex) {
					logger.warn(mex.getMessage(), mex);
				}
			}
			finally {
				running.set(false);
			}
		}
	}

	@Override
	public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
		taskRegistrar.setScheduler(Executors.newScheduledThreadPool(1));
		taskRegistrar.addFixedRateTask(this::generate, Duration.parse(Settings.get("taskInterval")));
	}
}
