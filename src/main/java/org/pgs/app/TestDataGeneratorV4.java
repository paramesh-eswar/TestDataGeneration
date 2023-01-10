package org.pgs.app;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.github.javafaker.CreditCardType;
import com.github.javafaker.Faker;

public class TestDataGeneratorV4 {
	private static String inputFilePath = "";
	private static Long numOfRows = 0l;
	private static String DESCRIPTOR_FILE_PATH = "descriptor.json";

	public static void main(String[] args) throws Exception{
		//validating the program arguments passed
		if(args.length == 0 || args.length != 2) {
			System.out.println("Invalid arguments count!!");
			System.exit(0);
		}
		
		if(args.length == 1 && !args[0].equalsIgnoreCase("--help")) {
			System.out.println("Invalid argument!!");
			System.out.println("Usage format for help: java TestDataGenerate --help");
			System.exit(0);
		}
		
		if(args.length == 1 && args[0].equalsIgnoreCase("--help" )) {
			System.out.println("Usage Format");
			System.out.println("===============================================================================================");
			System.out.println("java TestDataGenerate <complete-file-path> <number-rows-needed>");
			System.out.println("-----------------------------------------------------------------------------------------------");
			System.out.println("complete-file-path should be full path of the metadata file");
			System.out.println("number-rows-needed should always be a positive integer (no decimals or negative values allowed)");
			System.out.println("===============================================================================================");
			System.exit(0);
		}
		
		if(args.length == 2) {
			inputFilePath = args[0];
			numOfRows = Long.parseLong(args[1]);
		}
		
		//invoking test data generation method
		long startTime = System.currentTimeMillis();
		TestDataGeneratorV4 tdg = new TestDataGeneratorV4();
		boolean isDataGenerated = tdg.generateTestData(inputFilePath, numOfRows);
		long endTime = System.currentTimeMillis();
		if(!isDataGenerated)
			System.out.println("Test data genearation failed with errors!!");
		System.out.println("Time taken to generate test data: " + ((endTime-startTime)/1000) + " sec");
	}
	
	//Sequence number generator
	private static class NumberGenerator {
		private Long initialValue = 1L;
		
		public NumberGenerator() {
		}
		
		public NumberGenerator(Long initialValue) {
			this.initialValue = initialValue > 0 ? initialValue : 1;
		}
		
		private AtomicLong getNumberGenerator() {
			return new AtomicLong(this.initialValue);
		}
	}
	
	//Random date generator
	private static class DateGenerator {
		private static long seqDay = 0l;
		private LocalDate startInclusive;
		private LocalDate endExclusive;
		private DateTimeFormatter formatter;
		
		public DateGenerator(LocalDate startInclusive, LocalDate endExclusive, DateTimeFormatter formatter) {
			this.startInclusive = startInclusive;
			this.endExclusive = endExclusive;
			this.formatter = formatter;
		}
		
		private String getRandomDateString() {
		    long startEpochDay = this.startInclusive.toEpochDay();
		    long endEpochDay = this.endExclusive.toEpochDay();
		    long randomDay = ThreadLocalRandom.current().nextLong(startEpochDay, endEpochDay);

		    return String.valueOf(LocalDate.ofEpochDay(randomDay));
		}
		
		private String getRandomDate() {
		    long startEpochDay = this.startInclusive.toEpochDay();
		    long endEpochDay = this.endExclusive.toEpochDay();
		    long randomDay = ThreadLocalRandom.current().nextLong(startEpochDay, endEpochDay);

		    return LocalDate.ofEpochDay(randomDay).format(formatter);
		}
		
		private String getAndIncrement() {
			long startEpochDay = this.startInclusive.toEpochDay();
		    long nextDay = startEpochDay + seqDay;
		    seqDay++;
		    
			return LocalDate.ofEpochDay(nextDay).format(formatter);
		}
	}
	
	/**
	 * Method to generate test data
	 * @param inputFilePath - file path to read metadata information of the test data to be generated
	 * @param numOfRows - total number of rows expected in test data after generation
	 * @return - boolean value to represent whether data generated successfully or not
	 */
	private boolean generateTestData(String inputFilePath, Long numOfRows) {
		String fileName = null, filePath = null, outputFilePath = null;
		File inputFile = null;
		try {
			inputFile = new File(inputFilePath);
			fileName = inputFile.getName();
			filePath = inputFile.getParent();
			outputFilePath = filePath + File.separator + fileName.substring(0, fileName.lastIndexOf(".")) + "_output.csv";
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Map<String, JSONObject> metaData = new LinkedHashMap<String, JSONObject>();
		Map<String, JSONObject> numCols = new LinkedHashMap<String, JSONObject>();
		Map<String, JSONObject> dateCols = new LinkedHashMap<String, JSONObject>();
		JSONParser parser = new JSONParser();
		JSONObject descriptorJson = loadDescriptor(parser);
	    try {
	    	Object obj = parser.parse(new FileReader(new File(inputFilePath)));
	    	JSONArray jsonArray = (JSONArray)obj;
	    	Iterator<JSONObject> iterator = jsonArray.iterator();
	    	while (iterator.hasNext()) {
	    		JSONObject jsonObject = (JSONObject) iterator.next();
	    		if(!metaData.containsKey(jsonObject.get("name"))) {
	    			metaData.put(String.valueOf(jsonObject.get("name")), jsonObject);
	    			if(String.valueOf(jsonObject.get("default_value")).trim().length() == 0) {
    					if(("number").equalsIgnoreCase(String.valueOf(jsonObject.get("datatype")))) {
		    				numCols.put(String.valueOf(jsonObject.get("name")), jsonObject);
		    			} else if(("date").equalsIgnoreCase(String.valueOf(jsonObject.get("datatype"))) || 
		    					("timestamp").equalsIgnoreCase(String.valueOf(jsonObject.get("datatype")))) {
		    				dateCols.put(String.valueOf(jsonObject.get("name")), jsonObject);
		    			}
	    			}
	    		}
			}
	    } catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
			System.out.println("Metadata not in expected format. Please change it and re-run to generate test data");
			return false;
		} catch(Exception e) {
	    	e.printStackTrace();
	    }
		System.out.println("Test data generation is in progress ...");
		
		//validate the input metadata
		String errors = validateSchemaMetaData(descriptorJson, metaData, numOfRows);
		if(errors != null && errors.length() > 0) {
			System.out.println("Metadata not in expected format. Please change below and re-run to generate test data.");
			System.out.println("============================");
			System.out.println(errors);
			System.out.println("============================");
			return false;
		}
		
		//Automatic number sequence generators
		Map<String, AtomicLong> numGenerators = new LinkedHashMap<String, AtomicLong>();
		for(Map.Entry<String, JSONObject> entry : numCols.entrySet()) {
			Long initialValue = Long.valueOf(entry.getValue().get("range").toString().split("~")[0]);
			numGenerators.put(entry.getKey(), new NumberGenerator(initialValue).getNumberGenerator());
		}
		
		//Date generators to get random dates
		Map<String, DateGenerator> dateGenerators = new LinkedHashMap<String, DateGenerator>();
		for(Map.Entry<String, JSONObject> entry : dateCols.entrySet()) {
			DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(entry.getValue().get("date_format").toString());
			String[] minAndMaxDates = entry.getValue().get("range").toString().split("~");
			LocalDate minDate = LocalDate.parse(minAndMaxDates[0], dateFormatter);
			LocalDate maxDate = LocalDate.parse(minAndMaxDates[1], dateFormatter);
			dateGenerators.put(entry.getKey(), new DateGenerator(minDate, maxDate, dateFormatter));
		}
		
		int numOfThreads = 10, counter = 10;
		WriteDataToFile[] threads = new WriteDataToFile[numOfThreads];
		try  {
			for(int t=0;t<numOfThreads; t++) {
				threads[t] = new WriteDataToFile();
				threads[t].start();
			}
			//v4 start
			ArrayList<String> partFileNames = new ArrayList<String>();
			while(true) {
				Iterator<Map.Entry<String, JSONObject>> iterator = metaData.entrySet().iterator();
				while(iterator.hasNext()) {
					Map.Entry<String, JSONObject> entry = iterator.next();
					String partFileName = filePath + File.separator + "file_"+entry.getKey().toString()+".csv";
					partFileNames.add(partFileName);
					for(int i=0;i<numOfThreads; i++) {
						if(!threads[i].isBusy) {
							threads[i].setValues(partFileName, entry, descriptorJson, numGenerators.get(entry.getKey()), dateGenerators.get(entry.getKey()));
							threads[i].isBusy = true;
							iterator.remove();
							break;
						}
					}
				}
				if(metaData.size() == 0)
					break;
			}
			//v4 end
			while(true) {
				for(int t=0;t<numOfThreads; t++) {
					if(!threads[t].isBusy && threads[t].isRunning()) {
						threads[t].setRunning(false);
						counter--;
					}
				}
				if(counter == 0)
					break;
			}
			System.out.println("Wait is over");
		} catch (Exception e) {
        	e.printStackTrace();
        	System.out.println("Un expected error occured while writing the data to file!!");
        }
		
		System.out.println("Test data generation completed successfully!!\nOutput file location: " + outputFilePath);
		return true;
	}

	private static String getCreditCardNumber(Faker fakeDataGenerator, Map.Entry<String, JSONObject> entry) {
		CreditCardType ccType = null;
		String ccnumber = "";
		if(entry.getValue().get("cctype").toString() != "" && 
				entry.getValue().get("cctype").toString().trim() != "" && 
				!("any").equalsIgnoreCase(entry.getValue().get("cctype").toString().trim()))
			ccType = CreditCardType.valueOf(entry.getValue().get("cctype").toString().toUpperCase());
		if(ccType != null)
			ccnumber = fakeDataGenerator.finance().creditCard(ccType);
		else
			ccnumber = fakeDataGenerator.finance().creditCard();
		return ccnumber;
	}
	
	private static String getRandomTimeStamp(String timestampFormat, String startDate, String endDate) {
		long offset = Timestamp.valueOf(startDate + " 00:00:00").getTime();
		long end = Timestamp.valueOf(endDate + " 00:00:00").getTime();
		long diff = end - offset + 1;
		Timestamp timestamp = new Timestamp(offset + (long)(Math.random() * diff));
		return timestampFormat.length() != 0 ? (new SimpleDateFormat(timestampFormat).format(timestamp)) : timestamp.toString();
	}
	
	private static JSONObject loadDescriptor(JSONParser parser) {
		JSONObject jsonObject = null;
		try (InputStreamReader streamReader = new InputStreamReader(TestDataGeneratorV4.class.getClassLoader().getResourceAsStream(DESCRIPTOR_FILE_PATH))){
	    	Object obj = parser.parse(streamReader);
	    	jsonObject = (JSONObject)obj;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
			System.out.println("Error occured while loading the descriptor file");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonObject;
	}

	/**
	 * Method to validate the metadata information received in input file
	 * @param descriptorJson - descriptor file object to load pre-defined formats
	 * @param metaData - metadata of the attributes
	 * @param numOfRows - total number of rows need to be generated
	 * @return errors - consolidated list of validation errors of the metadata given
	 */
	private static String validateSchemaMetaData(JSONObject descriptorJson, Map<String, JSONObject> metaData, Long numOfRows) {
		StringBuilder errorMessages = new StringBuilder();
		for(Map.Entry<String, JSONObject> entry : metaData.entrySet()) {
			switch (entry.getValue().get("datatype").toString()) {
				case "number": {
					if(entry.getValue().get("default_value") == null || entry.getValue().get("default_value").toString().trim() == "") {
						if(entry.getValue().get("duplicates_allowed") == null || entry.getValue().get("duplicates_allowed").toString() == "") {
							errorMessages.append("Invalid value for the property duplicates_allowed for the attribute " + entry.getKey() + "\n");
							continue;
						}
						if(entry.getValue().get("range").toString() != "") {
							String[] range = entry.getValue().get("range").toString().split("~");
							if(range != null && range.length != 2) {
								errorMessages.append("Invalid range value for the attribute " + entry.getKey() + "\n");
								continue;
							}
							if(("no").equalsIgnoreCase(entry.getValue().get("duplicates_allowed").toString().trim())) {
								try {
									Long.valueOf(entry.getValue().get("range").toString().split("~")[0]);
								} catch (NumberFormatException nfe) {
									errorMessages.append("Invalid lower bound value for the attribute " + entry.getKey() + "\n");
									continue;
								}
							} else {
								Long[] numRange = new Long[range.length];
								for(int i=0; i<range.length; i++) {
									try {
										numRange[i] = Long.valueOf(range[i]);
									} catch (NumberFormatException nfe) {
										errorMessages.append("Invalid lower or upper bound value(s) for the attribute " + entry.getKey() + "\n");
										break;
									}
								}
							}
						}
					}
					break;
				}
				case "text": {
					if(entry.getValue().get("default_value") == null || entry.getValue().get("default_value").toString().trim() == "") {
						if(entry.getValue().get("duplicates_allowed") == null || entry.getValue().get("duplicates_allowed").toString() == "") {
							errorMessages.append("Invalid value for the property duplicates_allowed for the attribute " + entry.getKey() + "\n");
							continue;
						}
						JSONArray range = (JSONArray) entry.getValue().get("range");
						if(range.toArray().length == 0) {
							errorMessages.append("Invalid range values for the attribute " + entry.getKey() + "\n");
							continue;
						}
						if(("no").equalsIgnoreCase(entry.getValue().get("duplicates_allowed").toString().trim())) {
							if(range.toArray().length < numOfRows) {
								errorMessages.append("Range is always greater than or equal to the number of rows for the attribute " + entry.getKey() + "\n");
								continue;
							}
						}
					}
					break;
				}
				case "float": {
					if(entry.getValue().get("default_value") == null || entry.getValue().get("default_value").toString().trim() == "") {
						if(entry.getValue().get("duplicates_allowed") == null || entry.getValue().get("duplicates_allowed").toString() == "") {
							errorMessages.append("Invalid value for the property duplicates_allowed for the attribute " + entry.getKey() + "\n");
							continue;
						}
						if(entry.getValue().get("scale").toString() == "") {
							errorMessages.append("Invalid value for the property scale for the attribute " + entry.getKey() + "\n");
							continue;
						}
						if(entry.getValue().get("range").toString() != "") {
							String[] range = entry.getValue().get("range").toString().split("~");
							if(range != null && range.length != 2) {
								errorMessages.append("Invalid range value for the attribute " + entry.getKey() + "\n");
								continue;
							}
							if(("no").equalsIgnoreCase(entry.getValue().get("duplicates_allowed").toString().trim())) {
								try {
									Double.valueOf(entry.getValue().get("range").toString().split("~")[0]);
								} catch (NumberFormatException nfe) {
									errorMessages.append("Invalid lower bound value for the attribute " + entry.getKey() + "\n");
									continue;
								}
							} else {
								Double[] numRange = new Double[range.length];
								for(int i=0; i<range.length; i++) {
									try {
										numRange[i] = Double.valueOf(range[i]);
									} catch (NumberFormatException nfe) {
										errorMessages.append("Invalid lower or upper bound value(s) for the attribute " + entry.getKey() + "\n");
										break;
									}
								}
							}
						}
					}
					break;
				}
				case "date": {
					if(entry.getValue().get("default_value") == null || entry.getValue().get("default_value").toString().trim() == "") {
						DateTimeFormatter dateFormatter = null;
						LocalDate startDate = null, endDate = null;
						if(entry.getValue().get("duplicates_allowed") == null || entry.getValue().get("duplicates_allowed").toString() == "") {
							errorMessages.append("Invalid value for the property duplicates_allowed for the attribute " + entry.getKey() + "\n");
							continue;
						}
						if(entry.getValue().get("range").toString() != "") {
							String[] range = entry.getValue().get("range").toString().split("~");
							if(range != null && range.length != 2) {
								errorMessages.append("Invalid range value for the attribute " + entry.getKey() + "\n");
								continue;
							}
							if(range.length == 2) {
								try {
									dateFormatter = DateTimeFormatter.ofPattern(entry.getValue().get("date_format").toString());
									LocalDate.parse(range[0], dateFormatter);
									LocalDate.parse(range[1], dateFormatter);
								} catch (DateTimeParseException dtpe) {
									errorMessages.append("Invalid date format of the property range for the attribute " + entry.getKey() + "\n");
									continue;
								} catch (IllegalArgumentException ilare) {
									errorMessages.append("Invalid date format of the property date_format for the attribute " + entry.getKey() + "\n");
									continue;
								}
								if(("no").equalsIgnoreCase(entry.getValue().get("duplicates_allowed").toString().trim())) {
									dateFormatter = DateTimeFormatter.ofPattern(entry.getValue().get("date_format").toString());
									startDate = LocalDate.parse(range[0], dateFormatter);
									endDate = LocalDate.parse(range[1], dateFormatter);
									Long diff = endDate.toEpochDay() - startDate.toEpochDay();
									if(diff < numOfRows) {
										errorMessages.append("Range is always greater than or equal to the number of rows for the attribute " + entry.getKey() + "\n");
										continue;
									}
								}
							}
						}
					}
					break;
				}
				case "gender":
				case "boolean": {
					if(entry.getValue().get("default_value") == null || entry.getValue().get("default_value").toString().trim() == "") {
						if(entry.getValue().get("format") == null || entry.getValue().get("format").toString() == "") {
							errorMessages.append("Invalid value for the property format for the attribute " + entry.getKey() + "\n");
							continue;
						}
					}
					break;
				}
				case "ssn": 
				case "email": 
				case "phonenumber": 
				case "aadhar": {
					if(entry.getValue().get("default_value") == null || entry.getValue().get("default_value").toString().trim() == "") {
						if(entry.getValue().get("duplicates_allowed") == null || entry.getValue().get("duplicates_allowed").toString() == "") {
							errorMessages.append("Invalid value for the property duplicates_allowed for the attribute " + entry.getKey() + "\n");
							continue;
						}
					}
					break;
				}
				case "creditcard": {
					if(entry.getValue().get("default_value") == null || entry.getValue().get("default_value").toString().trim() == "") {
						if(entry.getValue().get("duplicates_allowed") == null || entry.getValue().get("duplicates_allowed").toString() == "") {
							errorMessages.append("Invalid value for the property duplicates_allowed for the attribute " + entry.getKey() + "\n");
							continue;
						}
						if(entry.getValue().get("cctype") == null || entry.getValue().get("cctype").toString().trim() == "") {
							errorMessages.append("Invalid value for the property cctype for the attribute " + entry.getKey() + "\n");
							continue;
						} else {
							JSONObject creditcardObj = (JSONObject) descriptorJson.get("creditcard");
							JSONArray cctypes = (JSONArray) creditcardObj.get("cctypes");
							if(!(cctypes.contains(entry.getValue().get("cctype")) || ("any").equalsIgnoreCase(entry.getValue().get("cctype").toString()))) {
								errorMessages.append("Invalid cctype for the attribute " + entry.getKey() + "\n");
								continue;
							}
						}
					}
					break;
				}
				case "ipaddress": {
					if(entry.getValue().get("default_value") == null || entry.getValue().get("default_value").toString().trim() == "") {
						if(entry.getValue().get("ipaddress_type") == null || entry.getValue().get("ipaddress_type").toString().trim() == "") {
							errorMessages.append("Invalid value for the property ipaddress_type for the attribute " + entry.getKey() + "\n");
							continue;
						} else {
							JSONObject ipaddressObj = (JSONObject) descriptorJson.get("ipaddress");
							JSONArray iptypes = (JSONArray) ipaddressObj.get("iptypes");
							if(!(iptypes.contains(entry.getValue().get("ipaddress_type")) || ("any").equalsIgnoreCase(entry.getValue().get("ipaddress_type").toString()))) {
								errorMessages.append("Invalid ipaddress_type for the attribute " + entry.getKey() + "\n");
								continue;
							}
						}
					}
					break;
				}case "timestamp": {
					if(entry.getValue().get("default_value") == null || entry.getValue().get("default_value").toString().trim() == "") {
						DateTimeFormatter dateFormatter = null;
						if(entry.getValue().get("range").toString() != "") {
							String[] range = entry.getValue().get("range").toString().split("~");
							if(range != null && range.length != 2) {
								errorMessages.append("Invalid range value for the attribute " + entry.getKey() + "\n");
								continue;
							}
							if(range.length == 2) {
								try {
									dateFormatter = DateTimeFormatter.ofPattern(entry.getValue().get("date_format").toString());
									LocalDate.parse(range[0], dateFormatter);
									LocalDate.parse(range[1], dateFormatter);
								} catch (DateTimeParseException dtpe) {
									errorMessages.append("Invalid date format of the property range for the attribute " + entry.getKey() + "\n");
									continue;
								} catch (IllegalArgumentException ilare) {
									errorMessages.append("Invalid date format of the property date_format for the attribute " + entry.getKey() + "\n");
									continue;
								}
							}
						}
						if(entry.getValue().get("timestamp_format") != null && entry.getValue().get("timestamp_format").toString() != "") {
							try {
								new SimpleDateFormat(entry.getValue().get("timestamp_format").toString());
							} catch (IllegalArgumentException ilare) {
								errorMessages.append("Invalid timestamp format of the property timestamp_format for the attribute " + entry.getKey() + "\n");
								continue;
							}
						}
					}
					break;
				}
				case "zipcode":
				case "uuid": {
					break;
				}
				default:
					throw new IllegalArgumentException("Unexpected data type: " + entry.getValue().get("datatype").toString());
			}
		}
		return errorMessages.toString();
	}

	//Inner class for threads
	public class WriteDataToFile extends Thread{
		private boolean started;
		private boolean running;
		private boolean isBusy=false;
		
		Map.Entry<String, JSONObject> metaData;
		JSONObject descriptorJson;
		AtomicLong numGenerators;
		DateGenerator dateGenerators;
		Long startRowNum, endRowNum;
		String partFileName;
		
		public WriteDataToFile() {
		}
		
		public boolean isStarted() {
		    return started;
		}

		public boolean isRunning() {
		    return running;
		}
		
		public boolean isCompleted() {
			return startRowNum == endRowNum;
		}
		
		public void setValues(String partFileName, Map.Entry<String, JSONObject> metaData, JSONObject descriptorJson, AtomicLong numGenerators, DateGenerator dateGenerators)
		{
			this.partFileName = partFileName;
			this.metaData = metaData;
			this.descriptorJson = descriptorJson;
			this.numGenerators = numGenerators;
			this.dateGenerators = dateGenerators;
		}

		public void setRunning(boolean running) {
		    this.running = running;
		    if (running)
		        started  = true;
		}
		
		public void run()
		{
			setRunning(true);
			while(running)
			{
				if(isBusy) {
					try {
						writeDataToFile(this.partFileName, this.metaData, this.descriptorJson, this.numGenerators, this.dateGenerators);
					} catch (IOException e) {
						System.out.println("Exception occured while writing the part file!! -- " + partFileName);
						e.printStackTrace();
					}
				}
			}
		}
		
		private void writeDataToFile(String partFileName, Map.Entry<String, JSONObject> entry, JSONObject descriptorJson, AtomicLong numGenerators, DateGenerator dateGenerators) throws IOException {
			StringBuffer dataRow = new StringBuffer();
			Faker fakeDataGenerator = new Faker();
			Random random = new Random();
			Long rowCount = 1l;
			
//			Part file for each column
			FileWriter file;
			BufferedWriter buffer = null;
			try {
				file = new FileWriter(partFileName);
				buffer = new BufferedWriter(file);
			} catch (IOException e) {
				System.out.println("Exception occured while creating the part file!! -- " + partFileName);
				e.printStackTrace();
			}
			
			String headerRow = "sno," + entry.getKey().toString() + "\n";
			buffer.write(headerRow);
			double seq = 0.00;
			ArrayList<String> ssnNumberList = new ArrayList<String>();
			ArrayList<String> creditcardNumberList = new ArrayList<String>();
			ArrayList<String> emailsList = new ArrayList<String>();
			ArrayList<String> phoneNumbersList = new ArrayList<String>();
			ArrayList<String> aadharNumberList = new ArrayList<String>();
			while(rowCount <= numOfRows) {
				dataRow.append(rowCount + ",");
				switch (entry.getValue().get("datatype").toString()) {
				case "number": {
					if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
						dataRow = dataRow.append(entry.getValue().get("default_value").toString() + "\n");
					else if(entry.getValue().get("duplicates_allowed").toString() != "" 
							&& ("no").equalsIgnoreCase(entry.getValue().get("duplicates_allowed").toString().trim()))
						dataRow = dataRow.append(numGenerators.getAndIncrement() + "\n");
					else {
						String[] numRange = entry.getValue().get("range").toString().split("~");
						dataRow = dataRow.append( (Integer.valueOf(numRange[0]) + (random.nextInt(Integer.valueOf(numRange[1])-Integer.valueOf(numRange[0]))) ) + "\n");
					}
					break;
				}
				case "text": {
					if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
						dataRow = dataRow.append(entry.getValue().get("default_value").toString() + "\n");
					else if(entry.getValue().get("duplicates_allowed").toString() != "" 
							&& ("no").equalsIgnoreCase(entry.getValue().get("duplicates_allowed").toString().trim())) {
						JSONArray range = (JSONArray) entry.getValue().get("range");
						if(range.toArray().length > 0) {
							dataRow = dataRow.append(range.get(rowCount.intValue()) + "\n");
						} else {
							dataRow = dataRow.append(entry.getValue().get("name").toString() + "-" + rowCount + "\n");
						}
					} else {
						JSONArray range = (JSONArray) entry.getValue().get("range");
						dataRow = dataRow.append(range.get(random.nextInt(range.toArray().length)) + "\n");
					}
					break;
				}
				case "float": {
					if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
						dataRow = dataRow.append(entry.getValue().get("default_value").toString() + "\n");
					else if(entry.getValue().get("duplicates_allowed").toString() != "" 
							&& ("no").equalsIgnoreCase(entry.getValue().get("duplicates_allowed").toString().trim())) {
						String[] floatRange = entry.getValue().get("range").toString().split("~");
						int scale = Integer.valueOf(entry.getValue().get("scale").toString());
						if(seq == 0.00) {
							seq = Double.valueOf(floatRange[0]);
						} else {
							seq += 1;
						}
						BigDecimal doubleVal = new BigDecimal(seq);
						dataRow = dataRow.append(doubleVal.setScale(scale, RoundingMode.HALF_UP) + "\n");
					} else {
						String[] floatRange = entry.getValue().get("range").toString().split("~");
						int scale = Integer.valueOf(entry.getValue().get("scale").toString());
						Double doubleRandomVal = Double.valueOf(floatRange[0]) + (random.nextFloat() * (Double.valueOf(floatRange[1])-Double.valueOf(floatRange[0])));
						BigDecimal doubleVal = new BigDecimal(doubleRandomVal);
						dataRow = dataRow.append(doubleVal.setScale(scale, RoundingMode.HALF_UP) + "\n");
					}
					break;
				}
				case "date": {
					if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
						dataRow = dataRow.append(entry.getValue().get("default_value").toString() + "\n");
					else if(entry.getValue().get("duplicates_allowed").toString() != "" 
							&& ("no").equalsIgnoreCase(entry.getValue().get("duplicates_allowed").toString().trim()))
						dataRow = dataRow.append(dateGenerators.getAndIncrement() + "\n");
					else
						dataRow = dataRow.append(dateGenerators.getRandomDate() + "\n");
					break;
				}
				case "gender": {
					JSONObject genderObj = (JSONObject) descriptorJson.get("gender");
					if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
						dataRow = dataRow.append(entry.getValue().get("default_value").toString() + "\n");
					else if(entry.getValue().get("format").toString() != "" 
							&& entry.getValue().get("format").toString().trim() != "") {
						JSONArray range = ("long").equalsIgnoreCase(entry.getValue().get("format").toString()) ? 
								(JSONArray) genderObj.get("range") : (JSONArray) genderObj.get("short-range");
						dataRow = dataRow.append(range.get(random.nextInt(range.toArray().length)) + "\n");
					}
					break;
				}
				case "boolean": {
					JSONObject booleanObj = (JSONObject) descriptorJson.get("boolean");
					if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
						dataRow = dataRow.append(entry.getValue().get("default_value").toString() + "\n");
					else if(entry.getValue().get("format").toString() != "" 
							&& entry.getValue().get("format").toString().trim() != "") {
						JSONArray range = ("long").equalsIgnoreCase(entry.getValue().get("format").toString()) ? 
								(JSONArray) booleanObj.get("range") : (JSONArray) booleanObj.get("short-range");
						dataRow = dataRow.append(range.get(random.nextInt(range.toArray().length)) + "\n");
					}
					break;
				}
				case "ssn": {
					if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
						dataRow = dataRow.append(entry.getValue().get("default_value").toString() + "\n");
					else if(entry.getValue().get("duplicates_allowed").toString() != "" 
							&& ("no").equalsIgnoreCase(entry.getValue().get("duplicates_allowed").toString().trim())) {
						String ssnNumber = new String();
						ssnNumber = fakeDataGenerator.idNumber().ssnValid();
						while(ssnNumberList.contains(ssnNumber))
							ssnNumber = fakeDataGenerator.idNumber().ssnValid();
						ssnNumberList.add(ssnNumber);
						dataRow = dataRow.append(ssnNumber + "\n");
					} else {
						dataRow = dataRow.append(fakeDataGenerator.idNumber().ssnValid() + "\n");
					}
					break;
				}
				case "creditcard": {
					if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
						dataRow = dataRow.append(entry.getValue().get("default_value").toString() + "\n");
					else if(entry.getValue().get("duplicates_allowed").toString() != "" 
							&& ("no").equalsIgnoreCase(entry.getValue().get("duplicates_allowed").toString().trim())) {
						String ccnumber = "";
						ccnumber = getCreditCardNumber(fakeDataGenerator, entry);
						while(creditcardNumberList.contains(ccnumber))
							ccnumber = getCreditCardNumber(fakeDataGenerator, entry);
						creditcardNumberList.add(ccnumber);
						dataRow = dataRow.append(ccnumber + "\n");
					} else {
						String ccnumber = getCreditCardNumber(fakeDataGenerator, entry);
						dataRow = dataRow.append(ccnumber + "\n");
					}
					break;
				}
				case "email": {
					if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
						dataRow = dataRow.append(entry.getValue().get("default_value").toString() + "\n");
					else if(entry.getValue().get("duplicates_allowed").toString() != "" 
							&& ("no").equalsIgnoreCase(entry.getValue().get("duplicates_allowed").toString().trim())) {
						String email = "";
						email = fakeDataGenerator.internet().emailAddress();
						while(emailsList.contains(email))
							email = fakeDataGenerator.internet().emailAddress();
						emailsList.add(email);
						dataRow = dataRow.append(email + "\n");
					} else {
						dataRow = dataRow.append(fakeDataGenerator.internet().emailAddress() + "\n");
					}
					break;
				}
				case "phonenumber": {
					if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
						dataRow = dataRow.append(entry.getValue().get("default_value").toString() + "\n");
					else if(entry.getValue().get("duplicates_allowed").toString() != "" 
							&& ("no").equalsIgnoreCase(entry.getValue().get("duplicates_allowed").toString().trim())) {
						String phoneNumber = "";
						phoneNumber = fakeDataGenerator.phoneNumber().cellPhone();
						while(phoneNumbersList.contains(phoneNumber))
							phoneNumber = fakeDataGenerator.phoneNumber().cellPhone();
						phoneNumbersList.add(phoneNumber);
						dataRow = dataRow.append(phoneNumber + "\n");
					} else {
						dataRow = dataRow.append(fakeDataGenerator.phoneNumber().cellPhone() + "\n");
					}
					break;
				}
				case "zipcode": {
					if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
						dataRow = dataRow.append(entry.getValue().get("default_value").toString() + "\n");
					else {
						 dataRow = dataRow.append(fakeDataGenerator.address().zipCode() + "\n");
					}
					break;
				}
				case "uuid": {
					if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
						dataRow = dataRow.append(entry.getValue().get("default_value").toString() + "\n");
					else {
						String regex = ((JSONObject) descriptorJson.get("uuid")).get("uuid").toString();
						dataRow = dataRow.append(fakeDataGenerator.regexify(regex) + "\n");
					}
					break;
				}
				case "ipaddress": {
					JSONObject ipaddressObj = (JSONObject) descriptorJson.get("ipaddress");
					if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
						dataRow = dataRow.append(entry.getValue().get("default_value").toString() + "\n");
					else if(entry.getValue().get("ipaddress_type").toString() != "" && 
							(("ipv4").equalsIgnoreCase(entry.getValue().get("ipaddress_type").toString().trim())) || 
							(("ipv6").equalsIgnoreCase(entry.getValue().get("ipaddress_type").toString().trim()))) {
						String ipaddressRegex = ("ipv4").equalsIgnoreCase(entry.getValue().get("ipaddress_type").toString()) ? 
								ipaddressObj.get("ipv4").toString() : ipaddressObj.get("ipv6").toString();
						dataRow = dataRow.append(fakeDataGenerator.regexify(ipaddressRegex) + "\n");
					} else {
						JSONArray ipTypes = (JSONArray) ipaddressObj.get("iptypes");
						String ipaddressRegex = ipaddressObj.get(ipTypes.get(random.nextInt(ipTypes.toArray().length))).toString();
						dataRow = dataRow.append(fakeDataGenerator.regexify(ipaddressRegex) + "\n");
					}
					break;
				}
				case "timestamp": {
					if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
						dataRow = dataRow.append(entry.getValue().get("default_value").toString() + "\n");
					else if(entry.getValue().get("timestamp_format").toString() != "" && entry.getValue().get("timestamp_format").toString().trim() != ""){
						String timestampFormat = entry.getValue().get("timestamp_format").toString();
						dataRow = dataRow.append(getRandomTimeStamp(timestampFormat, dateGenerators.getRandomDateString(), dateGenerators.getRandomDateString()) + "\n");
					} else {
						dataRow = dataRow.append(getRandomTimeStamp("", dateGenerators.getRandomDateString(), dateGenerators.getRandomDateString()) + "\n");
					}
					break;
				}
				case "aadhar": {
					JSONObject aadharObj = (JSONObject) descriptorJson.get("aadhar");
					String aadharRegex = aadharObj.get("format").toString();
					if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
						dataRow = dataRow.append(entry.getValue().get("default_value").toString() + "\n");
					else if(entry.getValue().get("duplicates_allowed").toString() != "" 
							&& ("no").equalsIgnoreCase(entry.getValue().get("duplicates_allowed").toString().trim())) {
						String aadharNumber = new String();
						aadharNumber = fakeDataGenerator.regexify(aadharRegex);
						while(aadharNumberList.contains(aadharNumber))
							aadharNumber = fakeDataGenerator.regexify(aadharRegex);
						aadharNumberList.add(aadharNumber);
						dataRow = dataRow.append(aadharNumber + "\n");
					} else {
						dataRow = dataRow.append(fakeDataGenerator.regexify(aadharRegex) + "\n");
					}
					break;
				}
				default:
						throw new IllegalArgumentException("Unexpected data type: " + entry.getValue().get("datatype").toString());
				}
				if(rowCount%1000 == 0) {
					buffer.write(dataRow.toString());
					dataRow.setLength(0);
				}
				rowCount++;
			}
			if(rowCount > numOfRows && numOfRows%1000 < 1000)
				buffer.write(dataRow.toString());
			buffer.close();
			this.isBusy = false;
		}
	}
}
