/**
 * 
 */
package security;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;

public class DecryptPassword {
	
	public static final Logger logger = Logger.getLogger(DecryptPassword.class);
	
	private final String propertyFileName;
    private String propertyKey;
 
    /**
     * The constructor does most of the work.
     * It initializes all final variables and invoke two methods
     * for encryption and decryption job. After successful job
     * the constructor puts the decrypted password in variable
     * to be retrieved by calling class.
     * 
	 * 
	 * @param pPropertyFileName /Name of the properties file that contains the password
	 * 
	 * @throws Exception
	 */
    public DecryptPassword(String pPropertyFileName) throws Exception {
        this.propertyFileName = pPropertyFileName;
    }
 
    public String decryptPropertyValue(String pUserPasswordKey) throws ConfigurationException {
    	this.propertyKey = pUserPasswordKey;
    	logger.info("Starting decryption..");
        PropertiesConfiguration config = new PropertiesConfiguration(propertyFileName);
        String encryptedPropertyValue = config.getString(propertyKey);
        logger.debug(encryptedPropertyValue); 
 
        StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
        encryptor.setPassword("jasypt");
        String decryptedPropertyValue = encryptor.decrypt(encryptedPropertyValue); 
        
        logger.info("Decryption is completed..");
        return decryptedPropertyValue;
    }
}
