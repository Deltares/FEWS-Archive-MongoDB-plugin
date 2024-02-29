package nl.fews.verification.mongodb.web;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.util.Map;

public class JaasConfiguration extends Configuration {
	@Override
	public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
		return new AppConfigurationEntry[]{new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule", AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
			Map.of(
				"useTicketCache", "true",
				"doNotPrompt", "true",
				"renewTGT", "true"))};
	}
}