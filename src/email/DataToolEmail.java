/**
 * 
 */
package email;


/**
 * @author vivek.subedi
 *
 */
public class DataToolEmail {
	
	/*private static Logger logger = LoggerFactory.getLogger(DataToolEmail.class);
	
	@Autowired
	private ConfigServiceImpl configService;
	private File file;
	private DatabaseRegions region;
	
	public void createEmail(File file, DatabaseRegions region) {
		this.file = file;
		this.region = region;
		
		try {
			EmailNotifierUtil dataUtilEmailer = this.builStatusEmail();
			dataUtilEmailer.sendMail();
		} catch (MessagingException e) {
			logger.error("Unable to build and send an email", e);
		}
	}

	private EmailNotifierUtil builStatusEmail() throws MessagingException {
		logger.info("Building data-util status email");
		EmailNotifierUtil email = new EmailNotifierUtil();
		email.setSubject("Comparison output of ["+region+"]");
		email.setBodyContent(getEmailContaint());
		String[] fileNameStrings = {file.getName()};
		email.setAttachmentNameList(fileNameStrings);
		logEmail(email.getBodyContent());
		
		*//**
		 * Just for testing purpose we are using EDWA_ROLLUP_CATEGORY
		 * We will change application specific once we implement it
		 *//*
		ApplicationContext context = new ClassPathXmlApplicationContext(
                "classpath:/applicationContext.xml");
		configService = (ConfigServiceImpl) context.getBean("email");
		String otherEmailInfo = configService.findValue(ConfigurationItem.EDWA_ROLLUP_CATEGORY, EmailNotifierUtil.EMAIL_TO);
		if (StringUtils.isNotBlank(otherEmailInfo)) {
			String[] toEmailStrings = otherEmailInfo.split(";");
			email.setSendTo(toEmailStrings);
		}
		
		String ccs = configService.findValue(ConfigurationItem.EDWA_ROLLUP_CATEGORY, EmailNotifierUtil.EMAIL_CC);
		if (StringUtils.isNotBlank(ccs)) {
			String[] ccEmails = ccs.split(";");
			email.setCcTo(ccEmails);
		}
		
		email.setSmtpIp(configService.findValue(ConfigurationItem.EDWA_ROLLUP_CATEGORY, EmailNotifierUtil.EMAIL_SMTP_IP));
		email.setMailFrom(configService.findValue(ConfigurationItem.EDWA_ROLLUP_CATEGORY, EmailNotifierUtil.EMAIL_FROM));
		
		return email;
	}


	private List<String> getEmailContaint() {
		List<String>  content = new ArrayList<String>();
		content.add("####################### This is an auto-generated email for data-util users.##############################");
		content.add("");
		content.add("This email contains the comparison run of ["+region+"]\n Please see the attachment.");
		content.add("");
		content.add("From");
		content.add("data-util status emailer");
		content.add("##########################################################################################################");
		
		
		return content;
	}

	*//**
	 * Logs email to the logger.
	 *
	 * @param bodyContent the body content
	 *//*
	private void logEmail(List<String> bodyContent) {
		logger.info("Email body is:");
		int i = 1;
		for(String emailString: bodyContent){
			logger.info("Line {} : {}", (i++), emailString);
		}
		
	}*/
}
