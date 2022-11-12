package org.pgs.app;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.github.javafaker.CreditCardType;
import com.github.javafaker.Faker;
import com.opencsv.CSVWriter;

public class TestDataGeneratorV2 {
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
		
		Faker faker = new Faker(new Locale("en-US"));
		System.out.println(faker.name().firstName() + " " + faker.name().lastName());
		System.out.println(faker.internet().emailAddress());
		
		//invoking test data generation method
		long startTime = System.currentTimeMillis();
//		boolean isDataGenerated = generateTestData(inputFilePath, numOfRows);
		long endTime = System.currentTimeMillis();
//		if(!isDataGenerated)
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
		
		public DateGenerator(LocalDate startInclusive, LocalDate endExclusive) {
			this.startInclusive = startInclusive;
			this.endExclusive = endExclusive;
		}
		
		private LocalDate getRandomDate() {
		    long startEpochDay = this.startInclusive.toEpochDay();
		    long endEpochDay = this.endExclusive.toEpochDay();
		    long randomDay = ThreadLocalRandom.current().nextLong(startEpochDay, endEpochDay);

		    return LocalDate.ofEpochDay(randomDay);
		}
		
		private LocalDate getAndIncrement() {
			long startEpochDay = this.startInclusive.toEpochDay();
		    long nextDay = startEpochDay + seqDay;
		    seqDay++;
		    
			return LocalDate.ofEpochDay(nextDay);
		}
	}
	
	/**
	 * Method to generate test data
	 * @param inputFilePath - file path to read metadata information of the test data to be generated
	 * @param numOfRows - total number of rows expected in test data after generation
	 * @return - boolean value to represent whether data generated successfully or not
	 */
	private static boolean generateTestData(String inputFilePath, Long numOfRows) {
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
		StringBuffer headerRow = new StringBuffer();
	    try {
	    	Object obj = parser.parse(new FileReader(new File(inputFilePath)));
	    	JSONArray jsonArray = (JSONArray)obj;
	    	Iterator<JSONObject> iterator = jsonArray.iterator();
	    	while (iterator.hasNext()) {
	    		JSONObject jsonObject = (JSONObject) iterator.next();
	    		if(!metaData.containsKey(jsonObject.get("name"))) {
	    			metaData.put(String.valueOf(jsonObject.get("name")), jsonObject);
	    			headerRow = headerRow.append(String.valueOf(jsonObject.get("name")) + ",");
	    			if(String.valueOf(jsonObject.get("default_value")).trim().length() == 0) {
    					if(("number").equalsIgnoreCase(String.valueOf(jsonObject.get("datatype")))) {
		    				numCols.put(String.valueOf(jsonObject.get("name")), jsonObject);
		    			} else if(("date").equalsIgnoreCase(String.valueOf(jsonObject.get("datatype")))) {
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
	    headerRow.deleteCharAt(headerRow.lastIndexOf(","));
		System.out.println("Test data generation is in progress ...");
		
		//validate the input metadata
		if(!validateSchemaMetaData(descriptorJson, metaData, numOfRows)) {
			System.out.println("Metadata not in expected format. Please change it and re-run to generate test data");
			return false;
		}
		
		//Automatic number sequence generators
		Map<String, AtomicLong> numGenerators = new LinkedHashMap<String, AtomicLong>();
		for(Map.Entry<String, JSONObject> entry : numCols.entrySet()) {
			Long initialValue = Long.valueOf(entry.getValue().get("range").toString().split("-")[0]);
			numGenerators.put(entry.getKey(), new NumberGenerator(initialValue).getNumberGenerator());
		}
		
		//Date generators to get random dates
		Map<String, DateGenerator> dateGenerators = new LinkedHashMap<String, DateGenerator>();
		for(Map.Entry<String, JSONObject> entry : dateCols.entrySet()) {
			DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(entry.getValue().get("date_format").toString());
			String[] minAndMaxDates = entry.getValue().get("range").toString().split("-");
			LocalDate minDate = LocalDate.parse(minAndMaxDates[0], dateFormatter);
			LocalDate maxDate = LocalDate.parse(minAndMaxDates[1], dateFormatter);
			dateGenerators.put(entry.getKey(), new DateGenerator(minDate, maxDate));
		}
		
		StringBuffer dataRow = new StringBuffer();
		Long rowCount = 1L;
		Random random = new Random();
		Faker fakeDataGenerator = new Faker();
		try (CSVWriter writer = new CSVWriter(new FileWriter(outputFilePath), ',', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END)) {
			writer.writeNext(headerRow.toString().split(","));
			Map<String, Integer> rangeSequence = new LinkedHashMap<String, Integer>();
			Map<String, Double> floatSequence = new LinkedHashMap<String, Double>();
			Map<String, ArrayList<String>> ssnNumbers = new LinkedHashMap<String, ArrayList<String>>();
			Map<String, ArrayList<String>> creditcardNumbers = new LinkedHashMap<String, ArrayList<String>>();
			while(rowCount<=numOfRows) {
				for(Map.Entry<String, JSONObject> entry : metaData.entrySet()) {
					switch (entry.getValue().get("datatype").toString()) {
					case "number": {
						if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
							dataRow = dataRow.append(entry.getValue().get("default_value").toString() + ",");
						else if(entry.getValue().get("duplicates_allowed").toString() != "" 
								&& ("no").equalsIgnoreCase(entry.getValue().get("duplicates_allowed").toString().trim()))
							dataRow = dataRow.append(numGenerators.get(entry.getKey()).getAndIncrement() + ",");
						else {
							String[] numRange = entry.getValue().get("range").toString().split("-");
							dataRow = dataRow.append( (Integer.valueOf(numRange[0]) + (random.nextInt(Integer.valueOf(numRange[1])-Integer.valueOf(numRange[0]))) ) + ",");
						}
						break;
					}
					case "text": {
						if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
							dataRow = dataRow.append(entry.getValue().get("default_value").toString() + ",");
						else if(entry.getValue().get("duplicates_allowed").toString() != "" 
								&& ("no").equalsIgnoreCase(entry.getValue().get("duplicates_allowed").toString().trim())) {
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
						if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
							dataRow = dataRow.append(entry.getValue().get("default_value").toString() + ",");
						else if(entry.getValue().get("duplicates_allowed").toString() != "" 
								&& ("no").equalsIgnoreCase(entry.getValue().get("duplicates_allowed").toString().trim())) {
							String[] floatRange = entry.getValue().get("range").toString().split("-");
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
							String[] floatRange = entry.getValue().get("range").toString().split("-");
							int scale = Integer.valueOf(entry.getValue().get("scale").toString());
							Double doubleRandomVal = Double.valueOf(floatRange[0]) + (random.nextFloat() * (Double.valueOf(floatRange[1])-Double.valueOf(floatRange[0])));
							BigDecimal doubleVal = new BigDecimal(doubleRandomVal);
							dataRow = dataRow.append(doubleVal.setScale(scale, RoundingMode.HALF_UP) + ",");
						}
						break;
					}
					case "date": {
						if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
							dataRow = dataRow.append(entry.getValue().get("default_value").toString() + ",");
						else if(entry.getValue().get("duplicates_allowed").toString() != "" 
								&& ("no").equalsIgnoreCase(entry.getValue().get("duplicates_allowed").toString().trim()))
							dataRow = dataRow.append(dateGenerators.get(entry.getKey()).getAndIncrement() + ",");
						else
							dataRow = dataRow.append(dateGenerators.get(entry.getKey()).getRandomDate() + ",");
						break;
					}
					case "gender": {
						JSONObject genderObj = (JSONObject) descriptorJson.get("gender");
						if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
							dataRow = dataRow.append(entry.getValue().get("default_value").toString() + ",");
						else if(entry.getValue().get("duplicates_allowed").toString() != "" 
								&& ("no").equalsIgnoreCase(entry.getValue().get("duplicates_allowed").toString().trim())) {
							JSONArray range = ("long").equalsIgnoreCase(entry.getValue().get("format").toString()) ? 
									(JSONArray) genderObj.get("range") : (JSONArray) genderObj.get("short-range");
							int seqIndex;
							if(rangeSequence.containsKey(entry.getValue().get("name").toString())) {
								seqIndex = rangeSequence.get(entry.getValue().get("name").toString()) + 1;
							} else {
								seqIndex = 0;
							}
							rangeSequence.put(entry.getValue().get("name").toString(), seqIndex);
							dataRow = dataRow.append(range.get(seqIndex) + ",");
						} else {
							JSONArray range = ("long").equalsIgnoreCase(entry.getValue().get("format").toString()) ? 
									(JSONArray) genderObj.get("range") : (JSONArray) genderObj.get("short-range");
							dataRow = dataRow.append(range.get(random.nextInt(range.toArray().length)) + ",");
						}
						break;
					}
					case "boolean": {
						JSONObject booleanObj = (JSONObject) descriptorJson.get("boolean");
						if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
							dataRow = dataRow.append(entry.getValue().get("default_value").toString() + ",");
						else if(entry.getValue().get("duplicates_allowed").toString() != "" 
								&& ("no").equalsIgnoreCase(entry.getValue().get("duplicates_allowed").toString().trim())) {
							JSONArray range = ("long").equalsIgnoreCase(entry.getValue().get("format").toString()) ? 
									(JSONArray) booleanObj.get("range") : (JSONArray) booleanObj.get("short-range");
							int seqIndex;
							if(rangeSequence.containsKey(entry.getValue().get("name").toString())) {
								seqIndex = rangeSequence.get(entry.getValue().get("name").toString()) + 1;
							} else {
								seqIndex = 0;
							}
							rangeSequence.put(entry.getValue().get("name").toString(), seqIndex);
							dataRow = dataRow.append(range.get(seqIndex) + ",");
						} else {
							JSONArray range = ("long").equalsIgnoreCase(entry.getValue().get("format").toString()) ? 
									(JSONArray) booleanObj.get("range") : (JSONArray) booleanObj.get("short-range");
							dataRow = dataRow.append(range.get(random.nextInt(range.toArray().length)) + ",");
						}
						break;
					}
					case "ssn": {
						if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
							dataRow = dataRow.append(entry.getValue().get("default_value").toString() + ",");
						else if(entry.getValue().get("duplicates_allowed").toString() != "" 
								&& ("no").equalsIgnoreCase(entry.getValue().get("duplicates_allowed").toString().trim())) {
							ArrayList<String> ssnNumberList;
							String ssnNumber = new String();
							if(ssnNumbers.containsKey(entry.getValue().get("name").toString())) {
								ssnNumberList = ssnNumbers.get(entry.getValue().get("name").toString());
								ssnNumber = fakeDataGenerator.idNumber().ssnValid();
								while(ssnNumberList.contains(ssnNumber))
									ssnNumber = fakeDataGenerator.idNumber().ssnValid();
							} else {
								ssnNumberList = new ArrayList<String>();
								ssnNumber = fakeDataGenerator.idNumber().ssnValid();
							}
							ssnNumberList.add(ssnNumber);
							ssnNumbers.put(entry.getValue().get("name").toString(), ssnNumberList);
							dataRow = dataRow.append(ssnNumber + ",");
						} else {
							dataRow = dataRow.append(fakeDataGenerator.idNumber().ssnValid() + ",");
						}
						break;
					}
					case "creditcard": {
						if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
							dataRow = dataRow.append(entry.getValue().get("default_value").toString() + ",");
						else if(entry.getValue().get("duplicates_allowed").toString() != "" 
								&& ("no").equalsIgnoreCase(entry.getValue().get("duplicates_allowed").toString().trim())) {
							ArrayList<String> creditcardNumberList;
							String ccnumber = "";
							if(creditcardNumbers.containsKey(entry.getValue().get("name").toString())) {
								creditcardNumberList = creditcardNumbers.get(entry.getValue().get("name").toString());
								ccnumber = getCreditCardNumber(fakeDataGenerator, entry);
								while(creditcardNumberList.contains(ccnumber))
									ccnumber = getCreditCardNumber(fakeDataGenerator, entry);
							} else {
								creditcardNumberList = new ArrayList<String>();
								ccnumber = getCreditCardNumber(fakeDataGenerator, entry);
							}
							creditcardNumberList.add(ccnumber);
							creditcardNumbers.put(entry.getValue().get("name").toString(), creditcardNumberList);
							dataRow = dataRow.append(ccnumber + ",");
						} else {
							String ccnumber = getCreditCardNumber(fakeDataGenerator, entry);
							dataRow = dataRow.append(ccnumber + ",");
						}
						break;
					}
					default:
							throw new IllegalArgumentException("Unexpected data type: " + entry.getValue().get("datatype").toString());
					}
				}
				dataRow.deleteCharAt(dataRow.lastIndexOf(","));
				writer.writeNext(dataRow.toString().split(","));
				dataRow.setLength(0);
				rowCount++;
			}
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
	
	private static JSONObject loadDescriptor(JSONParser parser) {
		JSONObject jsonObject = null;
		try (InputStreamReader streamReader = new InputStreamReader(TestDataGeneratorV2.class.getClassLoader().getResourceAsStream(DESCRIPTOR_FILE_PATH))){
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
	 * @param descriptorJson
	 * @param metaData
	 * @param numOfRows
	 * @return
	 */
	private static boolean validateSchemaMetaData(JSONObject descriptorJson, Map<String, JSONObject> metaData, Long numOfRows) {
		/*
		 * JSONObject creditcardObj = (JSONObject) descriptorJson.get("creditcard");
		 * JSONArray cctypes = (JSONArray) creditcardObj.get("cctypes");
		 * if(cctypes.contains(entry.getValue().get("cctype")))
		 * System.out.println("Found");
		 */
		return true;
	}

	/**
	 * Method to validate the metadata information received in input file
	 * @param colDataTypeLengthMap - collection (Map) object holds the attribute and its data type, lengths as key-value pairs
	 * @param duplicatesAllowed - flag to check whether test data should consists of duplicate values or not
	 * @param numOfRows - total number of rows expected to be present in test data
	 * @return - boolean value represent whether metadata is valid or not 
	 */
	private static boolean validateSchemaMetaData(Map<String, String> colDataTypeLengthMap, boolean duplicatesAllowed, Long numOfRows) {
		for(Map.Entry<String, String> entry : colDataTypeLengthMap.entrySet()) {
			switch (entry.getValue().substring(0, entry.getValue().indexOf("|"))) {
			case "number": {
				String numRange = entry.getValue().substring(entry.getValue().indexOf("|") + 1, entry.getValue().length());
				if( numRange == null || numRange.length() < 3 || !numRange.contains("-") )
					return false;
				String[] minAndMaxRange = entry.getValue().substring(entry.getValue().indexOf("|") + 1, entry.getValue().length()).split("-");
				if(!duplicatesAllowed) {
					if( minAndMaxRange.length != 2 || (Long.valueOf(minAndMaxRange[1]) - Long.valueOf(minAndMaxRange[0])) < numOfRows )
						return false;
				} else {
					if( minAndMaxRange.length != 2 || (Long.valueOf(minAndMaxRange[1]) - Long.valueOf(minAndMaxRange[0])) < 1 )
						return false;
				}
				break;
			}
			case "text": {
				String text = entry.getValue().substring(entry.getValue().indexOf("|") + 1, entry.getValue().length());
				if( text == null || text.length() < 1 )
					return false;
				break;
			}
			case "float": {
				String floatRange = entry.getValue().substring(entry.getValue().indexOf("|") + 1, entry.getValue().length());
				if( floatRange == null || floatRange.length() < 3 || !floatRange.contains("-") )
					return false;
				String[] minAndMaxRange = entry.getValue().substring(entry.getValue().indexOf("|") + 1, entry.getValue().length()).split("-");
				if( minAndMaxRange.length != 2 || (Double.valueOf(minAndMaxRange[1]) - Double.valueOf(minAndMaxRange[0])) < 1.0 )
					return false;
				break;
			}
			case "date": {
				String dateRange = entry.getValue().substring(entry.getValue().indexOf("|") + 1, entry.getValue().length());
				if( dateRange == null || dateRange.length() < 21 || !dateRange.contains("-") )
					return false;
				String[] minAndMaxRange = entry.getValue().substring(entry.getValue().indexOf("|") + 1, entry.getValue().length()).split("-");
				DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
				try {
					LocalDate.parse(minAndMaxRange[0], dateFormatter);
					LocalDate.parse(minAndMaxRange[1], dateFormatter);
				} catch (DateTimeParseException e) {
					System.out.println("Invalid date format!!");
					return false;
				}
				break;
			}
			default:
				throw new IllegalArgumentException("Unexpected value: " + entry.getValue().substring(0, entry.getValue().indexOf("|")));
			}
		}
		return true;
	}

	/**
	 * Method to write test data to target file
	 * @param testDataToWrite - test data as collection object to write in file
	 * @param filePath - output file path to store the test data
	 */
	private static void writeTestDataToFile(List<String[]> testDataToWrite, String filePath) {
		try (CSVWriter writer = new CSVWriter(new FileWriter(filePath), ',', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END)) {
	        for (String[] line : testDataToWrite) {
	            writer.writeNext(line);
	        }
		} catch (Exception e) {
        	e.printStackTrace();
        	System.out.println("Un expected error occured while writing the data to file!!");
        }
		System.out.println("Test data generation completed successfully!!\nOutput file location: " + filePath);
	}

}
