package nl.fews.verification.mongodb.shared.mail;

import nl.fews.verification.mongodb.shared.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import java.net.InetAddress;

public final class Mail{
	private Mail(){}

	private static final Logger logger = LoggerFactory.getLogger(Mail.class);

	public static void send(String subject, String message){
		try{
			var sender = new JavaMailSenderImpl();
			sender.setHost(Settings.get("smtpServer"));
			var mail = new SimpleMailMessage();
			mail.setSubject(String.format("%s %s", InetAddress.getLocalHost().getHostName().toUpperCase(), subject));
        	mail.setText(message);
        	mail.setTo(Settings.get("toEmailAddresses", String.class).split(","));
        	mail.setFrom(Settings.get("fromEmailAddress"));
        	sender.send(mail);
		}
		catch (Exception ex){
			logger.warn(ex.getMessage(), ex);
		}
	}
}
