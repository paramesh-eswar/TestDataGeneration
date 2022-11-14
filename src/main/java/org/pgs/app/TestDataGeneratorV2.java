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
import java.time.temporal.TemporalField;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
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
		
		//invoking test data generation method
		long startTime = System.currentTimeMillis();
		boolean isDataGenerated = generateTestData(inputFilePath, numOfRows);
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
	    headerRow.deleteCharAt(headerRow.lastIndexOf(","));
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
		
		StringBuffer dataRow = new StringBuffer();
		Long rowCount = 1L;
		Random random = new Random();
		Faker fakeDataGenerator = new Faker();
		try (CSVWriter writer = new CSVWriter(new FileWriter(outputFilePath), ',', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END)) {
			writer.writeNext(headerRow.toString().split(","));
			Map<String, Integer> rangeSequence = new LinkedHashMap<String, Integer>();
			Map<String, Double> floatSequence = new LinkedHashMap<String, Double>();
			Map<String, ArrayList<String>> rangeSeq = new LinkedHashMap<String, ArrayList<String>>();
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
							String[] numRange = entry.getValue().get("range").toString().split("~");
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
						else if(entry.getValue().get("format").toString() != "" 
								&& entry.getValue().get("format").toString().trim() != "") {
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
						else if(entry.getValue().get("format").toString() != "" 
								&& entry.getValue().get("format").toString().trim() != "") {
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
							if(rangeSeq.containsKey(entry.getValue().get("name").toString())) {
								ssnNumberList = rangeSeq.get(entry.getValue().get("name").toString());
								ssnNumber = fakeDataGenerator.idNumber().ssnValid();
								while(ssnNumberList.contains(ssnNumber))
									ssnNumber = fakeDataGenerator.idNumber().ssnValid();
							} else {
								ssnNumberList = new ArrayList<String>();
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
						if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
							dataRow = dataRow.append(entry.getValue().get("default_value").toString() + ",");
						else if(entry.getValue().get("duplicates_allowed").toString() != "" 
								&& ("no").equalsIgnoreCase(entry.getValue().get("duplicates_allowed").toString().trim())) {
							ArrayList<String> creditcardNumberList;
							String ccnumber = "";
							if(rangeSeq.containsKey(entry.getValue().get("name").toString())) {
								creditcardNumberList = rangeSeq.get(entry.getValue().get("name").toString());
								ccnumber = getCreditCardNumber(fakeDataGenerator, entry);
								while(creditcardNumberList.contains(ccnumber))
									ccnumber = getCreditCardNumber(fakeDataGenerator, entry);
							} else {
								creditcardNumberList = new ArrayList<String>();
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
						if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
							dataRow = dataRow.append(entry.getValue().get("default_value").toString() + ",");
						else if(entry.getValue().get("duplicates_allowed").toString() != "" 
								&& ("no").equalsIgnoreCase(entry.getValue().get("duplicates_allowed").toString().trim())) {
							ArrayList<String> emailsList;
							String email = "";
							if(rangeSeq.containsKey(entry.getValue().get("name").toString())) {
								emailsList = rangeSeq.get(entry.getValue().get("name").toString());
								email = fakeDataGenerator.internet().emailAddress();
								while(emailsList.contains(email))
									email = fakeDataGenerator.internet().emailAddress();
							} else {
								emailsList = new ArrayList<String>();
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
						if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
							dataRow = dataRow.append(entry.getValue().get("default_value").toString() + ",");
						else if(entry.getValue().get("duplicates_allowed").toString() != "" 
								&& ("no").equalsIgnoreCase(entry.getValue().get("duplicates_allowed").toString().trim())) {
							ArrayList<String> phoneNumbersList;
							String phoneNumber = "";
							if(rangeSeq.containsKey(entry.getValue().get("name").toString())) {
								phoneNumbersList = rangeSeq.get(entry.getValue().get("name").toString());
								phoneNumber = fakeDataGenerator.phoneNumber().cellPhone();
								while(phoneNumbersList.contains(phoneNumber))
									phoneNumber = fakeDataGenerator.phoneNumber().cellPhone();
							} else {
								phoneNumbersList = new ArrayList<String>();
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
						if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
							dataRow = dataRow.append(entry.getValue().get("default_value").toString() + ",");
						/*
						 * else if(entry.getValue().get("duplicates_allowed").toString() != "" &&
						 * ("no").equalsIgnoreCase(entry.getValue().get("duplicates_allowed").toString()
						 * .trim())) { ArrayList<String> phoneNumbersList; String phoneNumber = "";
						 * if(phoneNumbers.containsKey(entry.getValue().get("name").toString())) {
						 * phoneNumbersList = phoneNumbers.get(entry.getValue().get("name").toString());
						 * phoneNumber = fakeDataGenerator.phoneNumber().cellPhone();
						 * while(phoneNumbersList.contains(phoneNumber)) phoneNumber =
						 * fakeDataGenerator.phoneNumber().cellPhone(); } else { phoneNumbersList = new
						 * ArrayList<String>(); phoneNumber =
						 * fakeDataGenerator.phoneNumber().cellPhone(); }
						 * phoneNumbersList.add(phoneNumber);
						 * emailAddresses.put(entry.getValue().get("name").toString(),
						 * phoneNumbersList); dataRow = dataRow.append(phoneNumber + ","); }
						 */ else {
							 dataRow = dataRow.append(fakeDataGenerator.address().zipCode() + ",");
						}
						break;
					}
					case "uuid": {
						if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
							dataRow = dataRow.append(entry.getValue().get("default_value").toString() + ",");
						else {
							String regex = ((JSONObject) descriptorJson.get("uuid")).get("uuid").toString();
							dataRow = dataRow.append(fakeDataGenerator.regexify(regex) + ",");
						}
						break;
					}
					case "ipaddress": {
						JSONObject ipaddressObj = (JSONObject) descriptorJson.get("ipaddress");
						if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
							dataRow = dataRow.append(entry.getValue().get("default_value").toString() + ",");
						else if(entry.getValue().get("ipaddress_type").toString() != "" && 
								(("ipv4").equalsIgnoreCase(entry.getValue().get("ipaddress_type").toString().trim())) || 
								(("ipv6").equalsIgnoreCase(entry.getValue().get("ipaddress_type").toString().trim()))) {
							String ipaddressRegex = ("ipv4").equalsIgnoreCase(entry.getValue().get("ipaddress_type").toString()) ? 
									ipaddressObj.get("ipv4").toString() : ipaddressObj.get("ipv6").toString();
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
						if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
							dataRow = dataRow.append(entry.getValue().get("default_value").toString() + ",");
						else if(entry.getValue().get("timestamp_format").toString() != "" && entry.getValue().get("timestamp_format").toString().trim() != ""){
							/*
							 * String timestampRegex = timestampObj.get("format").toString(); dataRow =
							 * dataRow.append(fakeDataGenerator.regexify(timestampRegex) + ",");
							 */
							String timestampFormat = entry.getValue().get("timestamp_format").toString();
							dataRow = dataRow.append(getRandomTimeStamp(timestampFormat, dateGenerators.get(entry.getKey()).getRandomDateString(), dateGenerators.get(entry.getKey()).getRandomDateString()) + ",");
						} else {
							dataRow = dataRow.append(getRandomTimeStamp("", dateGenerators.get(entry.getKey()).getRandomDateString(), dateGenerators.get(entry.getKey()).getRandomDateString()) + ",");
						}
						break;
					}
					case "aadhar": {
						JSONObject aadharObj = (JSONObject) descriptorJson.get("aadhar");
						String aadharRegex = aadharObj.get("format").toString();
						if(entry.getValue().get("default_value").toString() != "" && entry.getValue().get("default_value").toString().trim() != "")
							dataRow = dataRow.append(entry.getValue().get("default_value").toString() + ",");
						else if(entry.getValue().get("duplicates_allowed").toString() != "" 
								&& ("no").equalsIgnoreCase(entry.getValue().get("duplicates_allowed").toString().trim())) {
							ArrayList<String> aadharNumberList;
							String aadharNumber = new String();
							if(rangeSeq.containsKey(entry.getValue().get("name").toString())) {
								aadharNumberList = rangeSeq.get(entry.getValue().get("name").toString());
								aadharNumber = fakeDataGenerator.regexify(aadharRegex);
								while(aadharNumberList.contains(aadharNumber))
									aadharNumber = fakeDataGenerator.regexify(aadharRegex);
							} else {
								aadharNumberList = new ArrayList<String>();
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
	
	private static String getRandomTimeStamp(String timestampFormat, String startDate, String endDate) {
		long offset = Timestamp.valueOf(startDate + " 00:00:00").getTime();
		long end = Timestamp.valueOf(endDate + " 00:00:00").getTime();
		long diff = end - offset + 1;
		Timestamp timestamp = new Timestamp(offset + (long)(Math.random() * diff));
		return timestampFormat.length() != 0 ? (new SimpleDateFormat(timestampFormat).format(timestamp)) : timestamp.toString();
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

}
