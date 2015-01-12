/**
 * 
 */
package test;

import java.util.Date;
import java.util.TimeZone;

/**
 * @author vivek.subedi
 *
 */
public class DayLightSavingTest {
	public static void main(String[] args) {
		Boolean dayLightSaving = TimeZone.getDefault().inDaylightTime(new Date());
		System.out.println(dayLightSaving);

	}

}
