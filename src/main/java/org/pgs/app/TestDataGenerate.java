package org.pgs.app;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;

public class TestDataGenerate {
	private static String inputFilePath = "";
	private static boolean duplicatesAllowed = false;
	private static Long numOfRows = 0l;

	public static void main(String[] args) throws Exception{
		//validating the program arguments passed
		if(args.length == 0) {
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
			System.out.println("java TestDataGenerate <complete-file-path> <duplicates-allowed> <number-rows-needed>");
			System.out.println("-----------------------------------------------------------------------------------------------");
			System.out.println("duplicates-allowed possible values are: yes no YES NO y n Y N");
			System.out.println("number-rows-needed should always be a positive integer (no decimals or negative values allowed)");
			System.out.println("===============================================================================================");
			System.exit(0);
		}
		
		if(args.length == 3) {
			inputFilePath = args[0];
			if(args[1].equalsIgnoreCase("yes") || args[1].equalsIgnoreCase("y")) {
				duplicatesAllowed = true;
			}
			numOfRows = Long.parseLong(args[2]);
		}
		
		//invoking test data generation method
		long startTime = System.currentTimeMillis();
		boolean isDataGenerated = generateTestData(inputFilePath, duplicatesAllowed, numOfRows);
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
	}
	
	/**
	 * Method to generate test data
	 * @param inputFilePath - file path to read metadata information of the test data to be generated
	 * @param duplicatesAllowed - duplicate flag to inform whether the test data should consists of duplicate data or not
	 * @param numOfRows - total number of rows expected in test data after generation
	 * @return - boolean value to represent whether data generated successfully or not
	 */
	private static boolean generateTestData(String inputFilePath, boolean duplicatesAllowed, Long numOfRows) {
		String metadata[] = null, fileName = null, filePath = null, outputFilePath = null;
		File inputFile = null;
		try {
			inputFile = new File(inputFilePath);
			fileName = inputFile.getName();
			filePath = inputFile.getParent();
			outputFilePath = filePath + File.separator + fileName.substring(0, fileName.lastIndexOf(".")) + "_output." + fileName.substring(fileName.lastIndexOf(".") + 1, fileName.length());
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		//reading metadata file for the attributes information
		try (CSVReader reader = new CSVReader(new FileReader(new File(inputFilePath)))) {
			metadata = reader.readNext();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (CsvValidationException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		//filtering number and date based attributes from the metadata
		Map<String, String> numberCols = new LinkedHashMap<String, String>();
		Map<String, String> dateCols = new LinkedHashMap<String, String>();
		System.out.println("Metadata from file\n=========================");
		for(String colMetadata : metadata) { 
			System.out.println(colMetadata);
			if(colMetadata.contains("|number|")) {
				numberCols.put(colMetadata.substring(0, colMetadata.indexOf("|")), colMetadata.substring(colMetadata.lastIndexOf("|") + 1, colMetadata.length()));
			}
			if(colMetadata.contains("|date|")) {
				dateCols.put(colMetadata.substring(0, colMetadata.indexOf("|")), colMetadata.substring(colMetadata.lastIndexOf("|") + 1, colMetadata.length()));
			}
		}
		System.out.println("=========================");
		System.out.println("Test data generation is in progress ...");
		
		//preparing test data including header row
		List<String[]> testData = new ArrayList<String[]>();
		Map<String, String> colDataTypeLengthMap = new LinkedHashMap<String, String>();
		StringBuffer headerRow = new StringBuffer();
		for(String colMetadata : metadata) {
			headerRow = headerRow.append(colMetadata.substring(0, colMetadata.indexOf("|")) + ",");
			colDataTypeLengthMap.put(colMetadata.substring(0, colMetadata.indexOf("|")), colMetadata.substring(colMetadata.indexOf("|") + 1, colMetadata.length()));
		}
		//System.out.println(colDataTypeLengthMap);
		headerRow.deleteCharAt(headerRow.lastIndexOf(","));
		
		//validate the input metadata
		if(!validateSchemaMetaData(colDataTypeLengthMap, duplicatesAllowed, numOfRows)) {
			System.out.println("Metadata not in expected format. Please change it and re-run to generate test data");
			return false;
		}
		
		//adding header row to the test data to write
		testData.add(headerRow.toString().split(","));
		
		//Automatic number sequence generators
		AtomicLong[] numGenerators = new AtomicLong[numberCols.size()];
		int i=0;
		for(Map.Entry<String, String> entry : numberCols.entrySet()) {
			Long initialValue = Long.valueOf(entry.getValue().split("-")[0]);
			numGenerators[i] = new NumberGenerator(initialValue).getNumberGenerator();
			i++;
		}
		
		//Date generators to get random dates
		DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
		DateGenerator[] dateGenerators = new DateGenerator[dateCols.size()];
		int j=0;
		for(Map.Entry<String, String> entry : dateCols.entrySet()) {
			String[] minAndMaxDates = entry.getValue().substring(entry.getValue().indexOf("|")+1, entry.getValue().length()).split("-");
			LocalDate minDate = LocalDate.parse(minAndMaxDates[0], dateFormatter);
			LocalDate maxDate = LocalDate.parse(minAndMaxDates[1], dateFormatter);
			dateGenerators[j] = new DateGenerator(minDate, maxDate);
			j++;
		}
		
		StringBuffer dataRow = new StringBuffer();
		Long rowCount = 1L;
		Random random = new Random();
		try (CSVWriter writer = new CSVWriter(new FileWriter(outputFilePath), ',', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END)) {
//			List<String[]> testDataToWrite = new LinkedList<String[]>();
//			testDataToWrite.add(headerRow.toString().split(","));
			writer.writeNext(headerRow.toString().split(","));
			if(!duplicatesAllowed) {
				while(rowCount<=numOfRows) {
					i=0;
					j=0;
					for(Map.Entry<String, String> entry : colDataTypeLengthMap.entrySet()) {
						switch (entry.getValue().substring(0, entry.getValue().indexOf("|"))) {
						case "number": {
							dataRow = dataRow.append(numGenerators[i].getAndIncrement()+",");
							i = i<numberCols.size() ? i+1 : 0;
							break;
						}
						case "text": {
							dataRow = dataRow.append(entry.getValue().substring(entry.getValue().indexOf("|")+1, entry.getValue().length()) + "-" + rowCount + ",");
							break;
						}
						case "float": {
							String[] minAndMax = entry.getValue().substring(entry.getValue().indexOf("|")+1, entry.getValue().length()).split("-");
							dataRow = dataRow.append(String.format("%.2f", Double.valueOf(minAndMax[0]) + random.nextDouble()*(Double.valueOf(minAndMax[1]) - Double.valueOf(minAndMax[0])))  + ",");
							break;
						}
						case "date": {
							dataRow = dataRow.append(dateGenerators[j].getRandomDate() + ",");
							j = j<dateCols.size() ? j+1 : 0;
							break;
						}
						default:
							throw new IllegalArgumentException("Unexpected value: " + entry.getValue().substring(0, entry.getValue().indexOf("|")));
						}
					}
					dataRow.deleteCharAt(dataRow.lastIndexOf(","));
					writer.writeNext(dataRow.toString().split(","));
//					testDataToWrite.add(dataRow.toString().split(","));
					dataRow.setLength(0);
					rowCount++;
				}
			} else {
				while(rowCount<=numOfRows) {
					j=0;
					for(Map.Entry<String, String> entry : colDataTypeLengthMap.entrySet()) {
						switch (entry.getValue().substring(0, entry.getValue().indexOf("|"))) {
						case "number": {
							String[] numRange = entry.getValue().substring(entry.getValue().indexOf("|") + 1, entry.getValue().length()).split("-");
							dataRow = dataRow.append( (Integer.valueOf(numRange[0]) + (random.nextInt(Integer.valueOf(numRange[1])-Integer.valueOf(numRange[0]))) ) + ",");
							break;
						}
						case "text": {
							Integer randomVal = 1 + (random.nextInt(numOfRows.intValue() - 1));
							dataRow = dataRow.append(entry.getValue().substring(entry.getValue().indexOf("|")+1, entry.getValue().length()) + "-" + randomVal + ",");
							break;
						}
						case "float": {
							String[] minAndMax = entry.getValue().substring(entry.getValue().indexOf("|")+1, entry.getValue().length()).split("-");
							dataRow = dataRow.append(String.format("%.2f", Double.valueOf(minAndMax[0]) + (random.nextDouble()*(Double.valueOf(minAndMax[1]) - Double.valueOf(minAndMax[0]))))  + ",");
							break;
						}
						case "date": {
							dataRow = dataRow.append(dateGenerators[j].getRandomDate() + ",");
							j = j<dateCols.size() ? j+1 : 0;
							break;
						}
						default:
							throw new IllegalArgumentException("Unexpected value: " + entry.getValue().substring(0, entry.getValue().indexOf("|")));
						}
					}
					dataRow.deleteCharAt(dataRow.lastIndexOf(","));
					writer.writeNext(dataRow.toString().split(","));
//					testDataToWrite.add(dataRow.toString().split(","));
					dataRow.setLength(0);
					rowCount++;
				}
			}
		} catch (Exception e) {
        	e.printStackTrace();
        	System.out.println("Un expected error occured while writing the data to file!!");
        }
//		writeTestDataToFile(testDataToWrite, outputFilePath);
		System.out.println("Test data generation completed successfully!!\nOutput file location: " + filePath);
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
