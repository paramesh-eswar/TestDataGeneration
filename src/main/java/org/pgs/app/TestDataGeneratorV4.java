package org.pgs.app;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.github.javafaker.CreditCardType;
import com.github.javafaker.Faker;
import com.opencsv.CSVWriter;

public class TestDataGeneratorV4 {
	private static String inputFilePath = "";
	private static Long numOfRows = 0l;
	private static String DESCRIPTOR_FILE_PATH = "descriptor.json";
	public StringBuilder errorMessage = new StringBuilder();
//	private static CSVWriter writer;

	public static void main(String[] args) throws Exception{
		//validating the program arguments passed
		if(args.length == 0 || args.length != 2) {
			AppLogger.warn("Invalid arguments count!!");
			System.exit(0);
		}
		
		if(args.length == 1 && !args[0].equalsIgnoreCase("--help")) {
			AppLogger.warn("Invalid argument!!");
			AppLogger.info("Usage format for help: java TestDataGenerate --help");
			System.exit(0);
		}
		
		if(args.length == 1 && args[0].equalsIgnoreCase("--help" )) {
			AppLogger.info("Usage Format");
			AppLogger.info("===============================================================================================");
			AppLogger.info("java TestDataGenerate <complete-file-path> <number-rows-needed>");
			AppLogger.info("-----------------------------------------------------------------------------------------------");
			AppLogger.info("complete-file-path should be full path of the metadata file");
			AppLogger.info("number-rows-needed should always be a positive integer (no decimals or negative values allowed)");
			AppLogger.info("===============================================================================================");
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
			AppLogger.error("Test data genearation failed with errors!!");
		AppLogger.info("Time taken to generate test data: " + ((endTime-startTime)/1000) + " sec");
	}
	
	//Sequence number generator
	private static class NumberGenerator {
		private Long initialValue = 1L;
		
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
	public boolean generateTestData(String inputFilePath, Long numOfRows) {
		String fileName = null, filePath = null, outputFilePath = null;
		File inputFile = null;
		try {
			inputFile = new File(inputFilePath);
			fileName = inputFile.getName();
			filePath = inputFile.getParent();
			outputFilePath = filePath + File.separator + fileName.substring(0, fileName.lastIndexOf(".")) + "_output.csv";
			
		} catch (Exception e) {
			AppLogger.error(e.toString());
		}
		
		Map<String, JSONObject> metaData = new LinkedHashMap<String, JSONObject>();
		Map<String, JSONObject> numCols = new LinkedHashMap<String, JSONObject>();
		Map<String, JSONObject> dateCols = new LinkedHashMap<String, JSONObject>();
		JSONParser parser = new JSONParser();
		JSONObject descriptorJson = loadDescriptor(parser);
		StringBuffer headerRow = new StringBuffer();
	    try {
	    	Object obj = parser.parse(new FileReader(new File(inputFilePath)));
	    	JSONArray jsonArray = (JSONArray)obj;
	    	@SuppressWarnings("unchecked")
	    	Iterator<Object> iterator = jsonArray.iterator();
	    	while (iterator.hasNext()) {
	    		JSONObject jsonObject = (JSONObject) iterator.next();
	    		if(!metaData.containsKey(jsonObject.get("name"))) {
	    			metaData.put(String.valueOf(jsonObject.get("name")), jsonObject);
	    			headerRow = headerRow.append(String.valueOf(jsonObject.get("name")) + ",");
					if(Util.isBlank(jsonObject.get("default_value"))) {
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
		    AppLogger.error(e.toString());
	    } catch (ParseException e) {
		    AppLogger.error(e.toString());
		    AppLogger.error("Metadata not in expected format. Please change it and re-run to generate test data");
		    errorMessage.append("Metadata not in expected format. Please change it and re-run to generate test data\n");
		    return false;
	    } catch(Exception e) {
		    AppLogger.error(e.toString());
	    }
	    headerRow.deleteCharAt(headerRow.lastIndexOf(","));
	AppLogger.info("Test data generation is in progress ...");
		
		//validate the input metadata
		String errors = validateSchemaMetaData(descriptorJson, metaData, numOfRows);
		if(errors != null && errors.length() > 0) {
			AppLogger.error("Metadata not in expected format. Please change below and re-run to generate test data.");
			AppLogger.error("============================");
			AppLogger.error(errors);
			AppLogger.error("============================");
			errorMessage.append("Metadata not in expected format. Please change below and re-run to generate test data.\n");
			errorMessage.append("============================\n");
			errorMessage.append(errors);
			errorMessage.append("============================\n");
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
		
//		StringBuffer dataRow = new StringBuffer();
//		Random random = new Random();
		Long rowCount = 1L, endCount = 0L;
		int numOfThreads = 10;
//		Faker fakeDataGenerator = new Faker();
//		CSVWriter writer = null;
		try(CSVWriter writer = new CSVWriter(new FileWriter(outputFilePath), ',', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END))  {
//			writer = new CSVWriter(new FileWriter(outputFilePath), ',', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
			writer.writeNext(headerRow.toString().split(","));
			Map<String, Integer> rangeSequence = new ConcurrentHashMap<String, Integer>();
			Map<String, Double> floatSequence = new ConcurrentHashMap<String, Double>();
			Map<String, CopyOnWriteArrayList<String>> rangeSeq = new ConcurrentHashMap<String, CopyOnWriteArrayList<String>>();

			ExecutorService executor = Executors.newFixedThreadPool(numOfThreads);
			List<Future<?>> futures = new ArrayList<>();
			try {
				while(rowCount<=numOfRows) {
					if(rowCount <= numOfRows-100)
						endCount = rowCount + 100;
					else
						endCount = numOfRows + 1;
					futures.add(executor.submit(new DataWriterTask(rowCount, endCount, metaData, descriptorJson, numGenerators, dateGenerators, rangeSequence, floatSequence, rangeSeq, writer)));
					rowCount = endCount;
				}
			} finally {
				executor.shutdown();
				// Poll in a loop
				/* try {
					// executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
					if(!executor.awaitTermination(1, TimeUnit.HOURS)) {
						executor.shutdownNow();
					}
				} catch (InterruptedException ie) {
					executor.shutdownNow();
					Thread.currentThread().interrupt();
				} */
				// Using futures
				/* for (Future<?> future : futures) {
					try {
						future.get();
					} catch (InterruptedException | ExecutionException e) {
						AppLogger.error(e.toString());
						executor.shutdownNow();
						Thread.currentThread().interrupt();
					}
				} */
				while (!executor.isTerminated()) {
					try {
						executor.awaitTermination(1, TimeUnit.MINUTES);
					} catch (InterruptedException ie) {
						AppLogger.error(ie.toString());
						executor.shutdownNow();
						Thread.currentThread().interrupt();
					}
				}
			}
			AppLogger.debug("Wait is over");
			errorMessage.append("Wait is over\n");
		} catch (Exception e) {
			AppLogger.error(e.toString());
			AppLogger.error("Un expected error occured while writing the data to file!!");
			errorMessage.append("Un expected error occured while writing the data to file!!\n");
		}
        
		AppLogger.info("Test data generation completed successfully!!\nOutput file location: " + outputFilePath);
		errorMessage.append("Test data generation completed successfully!!\nOutput file location: " + outputFilePath);
		return true;
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
			AppLogger.error(e.toString());
		} catch (ParseException e) {
			AppLogger.error(e.toString());
			AppLogger.error("Error occured while loading the descriptor file");
		} catch (Exception e) {
			AppLogger.error(e.toString());
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
					if(Util.isBlank(entry.getValue().get("default_value"))) {
						if(Util.isBlank(entry.getValue().get("duplicates_allowed"))) {
							errorMessages.append("Invalid value for the property duplicates_allowed for the attribute " + entry.getKey() + "\n");
							continue;
						}
						if(!Util.isBlank(entry.getValue().get("range"))) {
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
					if(Util.isBlank(entry.getValue().get("default_value"))) {
						if(Util.isBlank(entry.getValue().get("duplicates_allowed"))) {
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
					if(Util.isBlank(entry.getValue().get("default_value"))) {
						if(Util.isBlank(entry.getValue().get("duplicates_allowed"))) {
							errorMessages.append("Invalid value for the property duplicates_allowed for the attribute " + entry.getKey() + "\n");
							continue;
						}
						if(Util.isBlank(entry.getValue().get("scale"))) {
							errorMessages.append("Invalid value for the property scale for the attribute " + entry.getKey() + "\n");
							continue;
						}
						if(!Util.isBlank(entry.getValue().get("range"))) {
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
					if(Util.isBlank(entry.getValue().get("default_value"))) {
						DateTimeFormatter dateFormatter = null;
						LocalDate startDate = null, endDate = null;
						if(Util.isBlank(entry.getValue().get("duplicates_allowed"))) {
							errorMessages.append("Invalid value for the property duplicates_allowed for the attribute " + entry.getKey() + "\n");
							continue;
						}
						if(!Util.isBlank(entry.getValue().get("range"))) {
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
					if(Util.isBlank(entry.getValue().get("default_value"))) {
						if(Util.isBlank(entry.getValue().get("format"))) {
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
					if(Util.isBlank(entry.getValue().get("default_value"))) {
						if(Util.isBlank(entry.getValue().get("duplicates_allowed"))) {
							errorMessages.append("Invalid value for the property duplicates_allowed for the attribute " + entry.getKey() + "\n");
							continue;
						}
					}
					break;
				}
				case "creditcard": {
					if(Util.isBlank(entry.getValue().get("default_value"))) {
						if(Util.isBlank(entry.getValue().get("duplicates_allowed"))) {
							errorMessages.append("Invalid value for the property duplicates_allowed for the attribute " + entry.getKey() + "\n");
							continue;
						}
						if(Util.isBlank(entry.getValue().get("cctype"))) {
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
					if(Util.isBlank(entry.getValue().get("default_value"))) {
						if(Util.isBlank(entry.getValue().get("ipaddress_type"))) {
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
					if(Util.isBlank(entry.getValue().get("default_value"))) {
						DateTimeFormatter dateFormatter = null;
						if(!Util.isBlank(entry.getValue().get("range"))) {
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
						if(!Util.isBlank(entry.getValue().get("timestamp_format"))) {
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


	// Worker task for writing data (used with ExecutorService)
	public class DataWriterTask implements Runnable{
		Map<String, JSONObject> metaData;
		JSONObject descriptorJson;
		Map<String, AtomicLong> numGenerators;
		Map<String, DateGenerator> dateGenerators;
		Map<String, Integer> rangeSequence;
		Map<String, Double> floatSequence;
		Map<String, CopyOnWriteArrayList<String>> rangeSeq;
		Long startRowNum, endRowNum;
		CSVWriter writer;

		public DataWriterTask(Long startRowNum, Long endRowNum, Map<String, JSONObject> metaData, JSONObject descriptorJson, Map<String, AtomicLong> numGenerators, Map<String, DateGenerator> dateGenerators, Map<String, Integer> rangeSequence, Map<String, Double> floatSequence, Map<String, CopyOnWriteArrayList<String>> rangeSeq, CSVWriter writer) {
			this.startRowNum = startRowNum;
			this.endRowNum = endRowNum;
			this.metaData = metaData;
			this.descriptorJson = descriptorJson;
			this.numGenerators = numGenerators;
			this.dateGenerators = dateGenerators;
			this.rangeSequence = rangeSequence;
			this.floatSequence = floatSequence;
			this.rangeSeq = rangeSeq;
			this.writer = writer;
		}

		@Override
		public void run() {
			writeDataToFile(this.startRowNum, this.endRowNum, this.metaData, this.descriptorJson, this.numGenerators, this.dateGenerators, this.rangeSequence, this.floatSequence, this.rangeSeq);
		}

		private void writeDataToFile(Long rowCount, Long endCount, Map<String, JSONObject> metaData, JSONObject descriptorJson, Map<String, AtomicLong> numGenerators, Map<String, DateGenerator> dateGenerators, Map<String, Integer> rangeSequence, Map<String, Double> floatSequence, Map<String, CopyOnWriteArrayList<String>> rangeSeq) {
			StringBuffer dataRow = new StringBuffer();
			Faker fakeDataGenerator = new Faker();
			Random random = new Random();
			while(rowCount < endCount) {
				for(Map.Entry<String, JSONObject> entry : metaData.entrySet()) {
					switch (entry.getValue().get("datatype").toString()) {
					case "number": {
						String defaultVal = entry.getValue().get("default_value") == null ? "" : entry.getValue().get("default_value").toString();
						String dupAllowed = entry.getValue().get("duplicates_allowed") == null ? "" : entry.getValue().get("duplicates_allowed").toString();
						if(!defaultVal.trim().isEmpty())
							dataRow = dataRow.append(defaultVal + ",");
						else if(!dupAllowed.trim().isEmpty() && ("no").equalsIgnoreCase(dupAllowed.trim()))
							dataRow = dataRow.append(numGenerators.get(entry.getKey()).getAndIncrement() + ",");
						else {
							String[] numRange = entry.getValue().get("range").toString().split("~");
							dataRow = dataRow.append( (Integer.valueOf(numRange[0]) + (random.nextInt(Integer.valueOf(numRange[1])-Integer.valueOf(numRange[0]))) ) + ",");
						}
						break;
					}
					case "text": {
						String defaultVal = entry.getValue().get("default_value") == null ? "" : entry.getValue().get("default_value").toString();
						String dupAllowed = entry.getValue().get("duplicates_allowed") == null ? "" : entry.getValue().get("duplicates_allowed").toString();
						if(!defaultVal.trim().isEmpty())
							dataRow = dataRow.append(defaultVal + ",");
						else if(!dupAllowed.trim().isEmpty() && ("no").equalsIgnoreCase(dupAllowed.trim())) {
							JSONArray range = (JSONArray) entry.getValue().get("range");
							if(range.toArray().length > 0) {
								int seqIndex;
								if(rangeSequence.containsKey(entry.getValue().get("name").toString())) {
									seqIndex = rangeSequence.get(entry.getValue().get("name").toString()) + 1;
								} else {
									seqIndex = 0;
								}
								rangeSequence.put(entry.getValue().get("name").toString(), seqIndex);
								dataRow = dataRow.append(range.get(seqIndex) + ",");
							} else {
								dataRow = dataRow.append(entry.getValue().get("name").toString() + "-" + rowCount + ",");
							}
						} else {
							JSONArray range = (JSONArray) entry.getValue().get("range");
							dataRow = dataRow.append(range.get(random.nextInt(range.toArray().length)) + ",");
						}
						break;
					}
					case "float": {
						String defaultVal = entry.getValue().get("default_value") == null ? "" : entry.getValue().get("default_value").toString();
						String dupAllowed = entry.getValue().get("duplicates_allowed") == null ? "" : entry.getValue().get("duplicates_allowed").toString();
						if(!defaultVal.trim().isEmpty())
							dataRow = dataRow.append(defaultVal + ",");
						else if(!dupAllowed.trim().isEmpty() && ("no").equalsIgnoreCase(dupAllowed.trim())) {
							String[] floatRange = entry.getValue().get("range").toString().split("~");
							int scale = Integer.valueOf(entry.getValue().get("scale").toString());
							double seq;
							if(floatSequence.containsKey(entry.getValue().get("name").toString())) {
								seq = floatSequence.get(entry.getValue().get("name").toString()) + 1;
							} else {
								seq = Double.valueOf(floatRange[0]);
							}
							floatSequence.put(entry.getValue().get("name").toString(), seq);
							BigDecimal doubleVal = new BigDecimal(seq);
							dataRow = dataRow.append(doubleVal.setScale(scale, RoundingMode.HALF_UP) + ",");
						} else {
							String[] floatRange = entry.getValue().get("range").toString().split("~");
							int scale = Integer.valueOf(entry.getValue().get("scale").toString());
							Double doubleRandomVal = Double.valueOf(floatRange[0]) + (random.nextFloat() * (Double.valueOf(floatRange[1])-Double.valueOf(floatRange[0])));
							BigDecimal doubleVal = new BigDecimal(doubleRandomVal);
							dataRow = dataRow.append(doubleVal.setScale(scale, RoundingMode.HALF_UP) + ",");
						}
						break;
					}
					case "date": {
						String defaultVal = entry.getValue().get("default_value") == null ? "" : entry.getValue().get("default_value").toString();
						String dupAllowed = entry.getValue().get("duplicates_allowed") == null ? "" : entry.getValue().get("duplicates_allowed").toString();
						if(!defaultVal.trim().isEmpty())
							dataRow = dataRow.append(defaultVal + ",");
						else if(!dupAllowed.trim().isEmpty() && ("no").equalsIgnoreCase(dupAllowed.trim()))
							dataRow = dataRow.append(dateGenerators.get(entry.getKey()).getAndIncrement() + ",");
						else
							dataRow = dataRow.append(dateGenerators.get(entry.getKey()).getRandomDate() + ",");
						break;
					}
					case "gender": {
						JSONObject genderObj = (JSONObject) descriptorJson.get("gender");
						String defaultVal = entry.getValue().get("default_value") == null ? "" : entry.getValue().get("default_value").toString();
						String format = entry.getValue().get("format") == null ? "" : entry.getValue().get("format").toString();
						if(!defaultVal.trim().isEmpty())
							dataRow = dataRow.append(defaultVal + ",");
						else if(!format.trim().isEmpty()) {
							JSONArray range = ("long").equalsIgnoreCase(format) ? (JSONArray) genderObj.get("range") : (JSONArray) genderObj.get("short-range");
							dataRow = dataRow.append(range.get(random.nextInt(range.toArray().length)) + ",");
						}
						break;
					}
					case "boolean": {
						JSONObject booleanObj = (JSONObject) descriptorJson.get("boolean");
						String defaultVal = entry.getValue().get("default_value") == null ? "" : entry.getValue().get("default_value").toString();
						String format = entry.getValue().get("format") == null ? "" : entry.getValue().get("format").toString();
						if(!defaultVal.trim().isEmpty())
							dataRow = dataRow.append(defaultVal + ",");
						else if(!format.trim().isEmpty()) {
							JSONArray range = ("long").equalsIgnoreCase(format) ? (JSONArray) booleanObj.get("range") : (JSONArray) booleanObj.get("short-range");
							dataRow = dataRow.append(range.get(random.nextInt(range.toArray().length)) + ",");
						}
						break;
					}
					case "ssn": {
						String defaultVal = entry.getValue().get("default_value") == null ? "" : entry.getValue().get("default_value").toString();
						String dupAllowed = entry.getValue().get("duplicates_allowed") == null ? "" : entry.getValue().get("duplicates_allowed").toString();
						if(!defaultVal.trim().isEmpty())
							dataRow = dataRow.append(entry.getValue().get("default_value").toString() + ",");
						else if(!dupAllowed.trim().isEmpty() && ("no").equalsIgnoreCase(dupAllowed.trim())) {
					    CopyOnWriteArrayList<String> ssnNumberList;
							String ssnNumber = new String();
							if(rangeSeq.containsKey(entry.getValue().get("name").toString())) {
								ssnNumberList = rangeSeq.get(entry.getValue().get("name").toString());
								ssnNumber = fakeDataGenerator.idNumber().ssnValid();
								while(ssnNumberList.contains(ssnNumber))
									ssnNumber = fakeDataGenerator.idNumber().ssnValid();
							} else {
								ssnNumberList = new CopyOnWriteArrayList<String>();
								ssnNumber = fakeDataGenerator.idNumber().ssnValid();
							}
							ssnNumberList.add(ssnNumber);
							rangeSeq.put(entry.getValue().get("name").toString(), ssnNumberList);
							dataRow = dataRow.append(ssnNumber + ",");
						} else {
							dataRow = dataRow.append(fakeDataGenerator.idNumber().ssnValid() + ",");
						}
						break;
					}
					case "creditcard": {
						String defaultVal = entry.getValue().get("default_value") == null ? "" : entry.getValue().get("default_value").toString();
						String dupAllowed = entry.getValue().get("duplicates_allowed") == null ? "" : entry.getValue().get("duplicates_allowed").toString();
						if(!defaultVal.trim().isEmpty())
							dataRow = dataRow.append(entry.getValue().get("default_value").toString() + ",");
						else if(!dupAllowed.trim().isEmpty() && ("no").equalsIgnoreCase(dupAllowed.trim())) {
							CopyOnWriteArrayList<String> creditcardNumberList;
							String ccnumber = "";
							if(rangeSeq.containsKey(entry.getValue().get("name").toString())) {
								creditcardNumberList = rangeSeq.get(entry.getValue().get("name").toString());
								ccnumber = getCreditCardNumber(fakeDataGenerator, entry);
								while(creditcardNumberList.contains(ccnumber))
									ccnumber = getCreditCardNumber(fakeDataGenerator, entry);
							} else {
								creditcardNumberList = new CopyOnWriteArrayList<String>();
								ccnumber = getCreditCardNumber(fakeDataGenerator, entry);
							}
							creditcardNumberList.add(ccnumber);
							rangeSeq.put(entry.getValue().get("name").toString(), creditcardNumberList);
							dataRow = dataRow.append(ccnumber + ",");
						} else {
							String ccnumber = getCreditCardNumber(fakeDataGenerator, entry);
							dataRow = dataRow.append(ccnumber + ",");
						}
						break;
					}
					case "email": {
						String defaultVal = entry.getValue().get("default_value") == null ? "" : entry.getValue().get("default_value").toString();
						String dupAllowed = entry.getValue().get("duplicates_allowed") == null ? "" : entry.getValue().get("duplicates_allowed").toString();
						if(!defaultVal.trim().isEmpty())
							dataRow = dataRow.append(entry.getValue().get("default_value").toString() + ",");
						else if(!dupAllowed.trim().isEmpty() && ("no").equalsIgnoreCase(dupAllowed.trim())) {
							CopyOnWriteArrayList<String> emailsList;
							String email = "";
							if(rangeSeq.containsKey(entry.getValue().get("name").toString())) {
								emailsList = rangeSeq.get(entry.getValue().get("name").toString());
								email = fakeDataGenerator.internet().emailAddress();
								while(emailsList.contains(email))
									email = fakeDataGenerator.internet().emailAddress();
							} else {
								emailsList = new CopyOnWriteArrayList<String>();
								email = fakeDataGenerator.internet().emailAddress();
							}
							emailsList.add(email);
							rangeSeq.put(entry.getValue().get("name").toString(), emailsList);
							dataRow = dataRow.append(email + ",");
						} else {
							dataRow = dataRow.append(fakeDataGenerator.internet().emailAddress() + ",");
						}
						break;
					}
					case "phonenumber": {
						String defaultVal = entry.getValue().get("default_value") == null ? "" : entry.getValue().get("default_value").toString();
						String dupAllowed = entry.getValue().get("duplicates_allowed") == null ? "" : entry.getValue().get("duplicates_allowed").toString();
						if(!defaultVal.trim().isEmpty())
							dataRow = dataRow.append(entry.getValue().get("default_value").toString() + ",");
						else if(!dupAllowed.trim().isEmpty() && ("no").equalsIgnoreCase(dupAllowed.trim())) {
							CopyOnWriteArrayList<String> phoneNumbersList;
							String phoneNumber = "";
							if(rangeSeq.containsKey(entry.getValue().get("name").toString())) {
								phoneNumbersList = rangeSeq.get(entry.getValue().get("name").toString());
								phoneNumber = fakeDataGenerator.phoneNumber().cellPhone();
								while(phoneNumbersList.contains(phoneNumber))
									phoneNumber = fakeDataGenerator.phoneNumber().cellPhone();
							} else {
								phoneNumbersList = new CopyOnWriteArrayList<String>();
								phoneNumber = fakeDataGenerator.phoneNumber().cellPhone();
							}
							phoneNumbersList.add(phoneNumber);
							rangeSeq.put(entry.getValue().get("name").toString(), phoneNumbersList);
							dataRow = dataRow.append(phoneNumber + ",");
						} else {
							dataRow = dataRow.append(fakeDataGenerator.phoneNumber().cellPhone() + ",");
						}
						break;
					}
					case "zipcode": {
						String defaultVal = entry.getValue().get("default_value") == null ? "" : entry.getValue().get("default_value").toString();
						if(!defaultVal.trim().isEmpty())
							dataRow = dataRow.append(defaultVal + ",");
						else {
							dataRow = dataRow.append(fakeDataGenerator.address().zipCode() + ",");
						}
						break;
					}
					case "uuid": {
						String defaultVal = entry.getValue().get("default_value") == null ? "" : entry.getValue().get("default_value").toString();
						if(!defaultVal.trim().isEmpty())
							dataRow = dataRow.append(defaultVal + ",");
						else {
							String regex = ((JSONObject) descriptorJson.get("uuid")).get("uuid").toString();
							dataRow = dataRow.append(fakeDataGenerator.regexify(regex) + ",");
						}
						break;
					}
					case "ipaddress": {
						JSONObject ipaddressObj = (JSONObject) descriptorJson.get("ipaddress");
						String defaultVal = entry.getValue().get("default_value") == null ? "" : entry.getValue().get("default_value").toString();
						String ipType = entry.getValue().get("ipaddress_type") == null ? "" : entry.getValue().get("ipaddress_type").toString();
						if(!defaultVal.trim().isEmpty())
							dataRow = dataRow.append(defaultVal + ",");
						else if(!ipType.trim().isEmpty() && (("ipv4").equalsIgnoreCase(ipType.trim()) || ("ipv6").equalsIgnoreCase(ipType.trim()))) {
							String ipaddressRegex = ("ipv4").equalsIgnoreCase(ipType) ? ipaddressObj.get("ipv4").toString() : ipaddressObj.get("ipv6").toString();
							dataRow = dataRow.append(fakeDataGenerator.regexify(ipaddressRegex) + ",");
						} else {
							JSONArray ipTypes = (JSONArray) ipaddressObj.get("iptypes");
							String ipaddressRegex = ipaddressObj.get(ipTypes.get(random.nextInt(ipTypes.toArray().length))).toString();
							dataRow = dataRow.append(fakeDataGenerator.regexify(ipaddressRegex) + ",");
						}
						break;
					}
					case "timestamp": {
	//						JSONObject timestampObj = (JSONObject) descriptorJson.get("timestamp");
						String defaultVal = entry.getValue().get("default_value") == null ? "" : entry.getValue().get("default_value").toString();
						String timestampFormat = entry.getValue().get("timestamp_format") == null ? "" : entry.getValue().get("timestamp_format").toString();
						if(!defaultVal.trim().isEmpty())
							dataRow = dataRow.append(defaultVal + ",");
						else if(!timestampFormat.trim().isEmpty()){
							/*
							 * String timestampRegex = timestampObj.get("format").toString(); dataRow =
							 * dataRow.append(fakeDataGenerator.regexify(timestampRegex) + ",");
							 */
							dataRow = dataRow.append(getRandomTimeStamp(timestampFormat, dateGenerators.get(entry.getKey()).getRandomDateString(), dateGenerators.get(entry.getKey()).getRandomDateString()) + ",");
						} else {
							dataRow = dataRow.append(getRandomTimeStamp("", dateGenerators.get(entry.getKey()).getRandomDateString(), dateGenerators.get(entry.getKey()).getRandomDateString()) + ",");
						}
						break;
					}
					case "aadhar": {
						JSONObject aadharObj = (JSONObject) descriptorJson.get("aadhar");
						String aadharRegex = aadharObj.get("format").toString();
						String defaultVal = entry.getValue().get("default_value") == null ? "" : entry.getValue().get("default_value").toString();
						String dupAllowed = entry.getValue().get("duplicates_allowed") == null ? "" : entry.getValue().get("duplicates_allowed").toString();
						if(!defaultVal.trim().isEmpty())
							dataRow = dataRow.append(defaultVal + ",");
						else if(!dupAllowed.trim().isEmpty() && ("no").equalsIgnoreCase(dupAllowed.trim())) {
							CopyOnWriteArrayList<String> aadharNumberList;
							String aadharNumber = new String();
							if(rangeSeq.containsKey(entry.getValue().get("name").toString())) {
								aadharNumberList = rangeSeq.get(entry.getValue().get("name").toString());
								aadharNumber = fakeDataGenerator.regexify(aadharRegex);
								while(aadharNumberList.contains(aadharNumber))
									aadharNumber = fakeDataGenerator.regexify(aadharRegex);
							} else {
								aadharNumberList = new CopyOnWriteArrayList<String>();
								aadharNumber = fakeDataGenerator.regexify(aadharRegex);
							}
							aadharNumberList.add(aadharNumber);
							rangeSeq.put(entry.getValue().get("name").toString(), aadharNumberList);
							dataRow = dataRow.append(aadharNumber + ",");
						} else {
							dataRow = dataRow.append(fakeDataGenerator.regexify(aadharRegex) + ",");
						}
						break;
					}
					default:
							throw new IllegalArgumentException("Unexpected data type: " + entry.getValue().get("datatype").toString());
					}
				}
				dataRow.deleteCharAt(dataRow.lastIndexOf(","));
				synchronized (writer) {
					writer.writeNext(dataRow.toString().split(","));
					dataRow.setLength(0);
					rowCount++;
				}
				/*
				 * writer.writeNext(dataRow.toString().split(",")); dataRow.setLength(0);
				 * rowCount++;
				 */
			}
			// task completed
		}
	}

	private static String getCreditCardNumber(Faker fakeDataGenerator, Map.Entry<String, JSONObject> entry) {
		CreditCardType ccType = null;
		String ccnumber = "";
		if(!Util.isBlank(entry.getValue().get("cctype")) && 
				!("any").equalsIgnoreCase(entry.getValue().get("cctype").toString().trim()))
			ccType = CreditCardType.valueOf(entry.getValue().get("cctype").toString().toUpperCase());
		if(ccType != null)
			ccnumber = fakeDataGenerator.finance().creditCard(ccType);
		else
			ccnumber = fakeDataGenerator.finance().creditCard();
		return ccnumber;
	}
}
