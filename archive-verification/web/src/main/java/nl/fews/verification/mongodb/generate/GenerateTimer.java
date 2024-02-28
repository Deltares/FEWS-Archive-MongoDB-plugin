package nl.fews.verification.mongodb.generate;

import nl.fews.verification.mongodb.shared.io.IO;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.json.JSONObject;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GenerateTimer {
	private GenerateTimer(){}

	private static final Logger logger = LoggerFactory.getLogger(GenerateTimer.class);

	private static final Timer timer = new Timer("GenerateTimer");

	private static TimerTask task = null;

	private static boolean running = false;

	private static final long delay = 1000;

	public static void start(){
		if (task != null)
			task.cancel();
		task = new TimerTask() {
			@Override
			public void run(){
				JSONObject settings;
				try {
					Path path = Path.of(Settings.get("bimPath"), "Settings.json");
					settings = path.toFile().exists() ? Settings.fromJsonString(IO.readString(path.toString())) : null;
				}
				catch (Exception ex){
					logger.warn(ex.getMessage(), ex);
					settings = null;
				}

				if (!running && (settings == null || !settings.getBoolean("execute"))) {
					running = true;
					try {
						Generate.execute();
					}
					catch (Exception ex) {
						logger.warn(ex.getMessage(), ex);
					}
					finally {
						running = false;
					}
				}
				else {
					logger.info(String.format("already running: %s; wait for processing: %s", running, settings != null && settings.getBoolean("execute")));
				}
			}
		};
		timer.scheduleAtFixedRate(task, delay, Duration.parse(Settings.get("taskInterval")).toMillis());
	}
}
