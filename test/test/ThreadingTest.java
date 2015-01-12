/**
 * 
 */
package test;

/**
 * @author vivek.subedi
 *
 */
public class ThreadingTest {
	/*private Map<String, LinkedList<String>> primaryKey;
	private LinkedHashSet<String> tables;
	private Map<String, LinkedList<String>> toBeSummedColumns;
	private CoreTAService coreTAService;
	private String username = "accvys";
	private String password = "sarmila7";
	private LinkedList<String> coreTables;

	
	@Before
	public void setUp() throws Exception {
		coreTAService = new CoreTAService(username, password, DB2Region.U2993);
		coreTAService.repTableNames(DB2Region.U2993);
		tables = coreTAService.getTableNameList();
		toBeSummedColumns = coreTAService.getSumColumns(DB2Region.U2993, tables);
		coreTables = new LinkedList<String>();
		Iterator<Entry<String, LinkedList<String>>> iterator = toBeSummedColumns.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<String, LinkedList<String>> entry = iterator.next();
			coreTables.add(entry.getKey());
		}
		primaryKey = coreTAService.getPrimaryKeyTableMap(DB2Region.U2993, tables);
	}

	
	@After
	public void tearDown() throws Exception {
		coreTAService = null;
		toBeSummedColumns = null;
		tables = null;
		primaryKey = null;
	}

	@Test
	public void testProducerThread() {
		LinkedBlockingQueue<String> masterQueue = new LinkedBlockingQueue<String>();
		DelayQueue<DelayObject> taDelayQueue = new DelayQueue<DelayObject>();
		Vector<DelayObject> allQueryResultVector = new Vector<DelayObject>();
		JProgressBar progressBar = new JProgressBar();
		DefaultListModel<String> doneModel = new DefaultListModel<>();
		
		for (int i = 0; i < 10; i++) {
			masterQueue.add(coreTables.get(i));
		}
		
		CountDownLatch latch = new CountDownLatch(masterQueue.size());
		for (String string : coreTables) {
			masterQueue.add(string);
		}
		System.out.println("The size of the master queue: "+ masterQueue.size());
		
		LinkedList<Thread> threadList = new LinkedList<Thread>();
		Long delaytime = (long) 1;
		
		for (int i = 1; i <= 2; i++) {
			CoreObjectProducer coreObjectProducer = new CoreObjectProducer(toBeSummedColumns, primaryKey, taDelayQueue, masterQueue, delaytime,4);
			Thread producerThread = new Thread(coreObjectProducer);
			threadList.add(producerThread);
		}
		
		for (int i = 1; i <= 2; i++) {
			TAObjectConsumer taObjectConsumer = new TAObjectConsumer(taDelayQueue, toBeSummedColumns, primaryKey, allQueryResultVector, latch, progressBar, doneModel);
			Thread consumerThread = new Thread(taObjectConsumer);
			threadList.add(consumerThread);
		}
		

		for (int i=0; i < threadList.size(); i++) {
			threadList.get(i).start();
		}
		
		for (int i=0; i < threadList.size(); i++) {
			try {
				if (threadList.get(i).isAlive()) {
					threadList.get(i).join();
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
*/
}
