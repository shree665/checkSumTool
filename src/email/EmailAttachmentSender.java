/**
 * 
 */
package email;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;

import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import org.apache.log4j.Logger;

import persistence.DatabaseRegion;
import tools.CommonCompareUtil;

/**
 * @author vivek.subedi
 *
 */
public class EmailAttachmentSender {
	
	private static Logger logger = Logger.getLogger(EmailAttachmentSender.class);
	
	public static void sendEmail(File file, DatabaseRegion region) {
		//SMTP INFO
		String host = "smtp.gmail.com";
		String port = "587";
		String mailFrom = "";
		String password = "";
		
		//message info
		String mailTo = "";
		String subject = "Comparison output of ["+region.getDisplay().toString()+"]";
		String message = "Hello All, \nThis email has attachment !!";
		
		//attachments
		String[] attachFiles = new String[1];
		attachFiles[0] = file.getPath();
		try {
            sendEmailWithAttachment(host, port, mailFrom, password, mailTo,
                subject, message, attachFiles);
            logger.info("Email has been sent to ["+mailTo+"]");
        } catch (Exception ex) {
            logger.info("Could not send email. Please check your file in the location you saved.");
            logger.info(CommonCompareUtil.getStackTrace(ex));
        }
				
	}
	
	public static void sendEmailWithAttachment(String host, String port, final String userName, final String password, String toAddress,
            String subject, String message, String[] attachFiles) throws AddressException, MessagingException {
		
		//setting SMTP server properties
		Properties properties = new Properties();
		properties.put("mail.smtp.host", host);
		properties.put("mail.smtp.port", port);
		properties.put("mail.smtp.auth", "true");
        properties.put("mail.smtp.starttls.enable", "true");
        properties.put("mail.user", userName);
        properties.put("mail.password", password);
        
     // creates a new session with an authenticator
        Authenticator auth = new Authenticator() {
            public PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(userName, password);
            }
        };
        Session session = Session.getInstance(properties, auth);
        
        //creates a new email message with multiple receivers
        Message msg = new MimeMessage(session);
        msg.setFrom(new InternetAddress(userName));
        String[] toMailAddress = toAddress.split(";");
        InternetAddress[] toAddresses = new InternetAddress[toMailAddress.length];
        for (int i = 0; i < toMailAddress.length; i++) {
			toAddresses[i] =  new InternetAddress(toMailAddress[i]);
		}
        msg.setRecipients(Message.RecipientType.TO, toAddresses);
        msg.setSubject(subject);
        msg.setSentDate(new Date());
        
        //creates message part
        MimeBodyPart messageBodyPart = new MimeBodyPart();
        messageBodyPart.setContent(message, "text/html");
        
        //creates multi-part
        Multipart multipart = new MimeMultipart();
        multipart.addBodyPart(messageBodyPart);
        
        // adds attachments
        if (attachFiles != null && attachFiles.length > 0) {
            for (String filePath : attachFiles) {
                MimeBodyPart attachPart = new MimeBodyPart();
 
                try {
                    attachPart.attachFile(filePath);
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
 
                multipart.addBodyPart(attachPart);
            }
        }
 
        // sets the multi-part as e-mail's content
        msg.setContent(multipart);
 
        // sends the e-mail
        Transport.send(msg);
	}
}
