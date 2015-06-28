/*
* Author : Aditya Borde
* Function : Database functions implementation.
*/
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.SerializationUtils;



public class MyDatabase {

	public enum Options {Select,Insert,Delete,Modify,Count,BulkInsert,Close};
	public enum SelectOptions{PersonId,LastName,State,Email,MainMenu};
	
	// Database field names.
	String firstName = "Firstname";
	String lastName = "Lastname";
	String id ="Id";
	String company = "Company";
	String address = "Address";
	String city = "City";
	String county = "County";
	String state = "State";
	String zip = "Zip";
	String phone1 = "Phone1";
	String phone2 = "Phone2";
	String email = "Email";
	String web = "Web";
	
	public int totalBytes;
	
	BulkData readData = null;
	
	// Tree Map for IDs :
	TreeMap<Integer, String> treeMapIds = new TreeMap<Integer, String>();
	// Tree Map for States:
	TreeMap<String, ArrayList<String>> treeMapStates = new TreeMap<String, ArrayList<String>>();
	// Tree Map for Last names:
	TreeMap<String, ArrayList<String>> treeMap = new TreeMap<String, ArrayList<String>>();
	// Tree Map for Email Id :
	TreeMap<String,String> treeMapEmail = new TreeMap<String, String>();
	
	
	// Hard Coded .csv File path
	String csvFile ="C:\\us-500.csv";
	
	// Index File Paths
	String indexState = "C:\\SQL\\State.ndx";
	String indexLastName = "C:\\SQL\\LastName.ndx";
	String indexId = "C:\\SQL\\Id.ndx";
	String dbFile = "C:\\SQL\\Data.db";
	
	String indexEmail = "C:\\SQL\\Email.ndx";
	
	
	public String matchedRecord = "";
	
	// Flag to detect 'Hard coded' Mode :
	public boolean isHardCord = true;
	
	public static void main(String[] args) {
		try
		{
			MyDatabase myDBObj = new MyDatabase();
			myDBObj.HardcodedOperations();
			System.out.println("\n**My Database**");
			myDBObj.GetUserInput();
		}
		catch(Exception ex)
		{
			System.out.println("Error Occured" + ex.getMessage());
		}
	}
	
	public void HardcodedOperations() {
		System.out.printf("Reading CSV file And creating Database and Index files..");
		
		// Insert .csv File records in DB files
		BulkInsertOperation();
		
		// Select Operation
		ShowSelectOptions();
		
		// Count Operation
		RecordCount();
		
		// Insert OPeration
		InsertNewReord();
		
		// Count Operation
		RecordCount();
		
		//Modify Operation
		ModifyRecord();
		
		// Show records
		System.out.println("\nShowing modified record :\n");
		ShowRecords("505",indexId);
		
		// Delete Operation
		DeleteRecord();
		
		// Count Operation
		RecordCount();
		
		// Set the hard coding program run to off : to start Input Driven Program 
		isHardCord = false;
	}

	public MyDatabase() throws IOException 
	{
		//Fill the Tree maps:
		ArrayList<String> records = new ArrayList<String>();
		ArrayList<String> tempList = null;
		
		String[] split;
		File file; 
		file = new File(indexState);
		
		// if file exists
		if (file.exists()) 
		{
			records = GetList(indexState);
			// Construct Tree Map
			for(String data:records)
			{
				tempList = new ArrayList<String>();
				split = data.split(java.util.regex.Pattern.quote("$"));
				for(int i=1;i< split.length;i++)
					tempList.add(split[i]);
				treeMapStates.put(split[0], tempList);
			}
		}			
		
		
		records.clear();
		file = null;
		file = new File(indexLastName);
		
		// if file exists
		if (file.exists()) 
		{
			records = GetList(indexLastName);
			for(String data:records)
			{
				tempList = new ArrayList<String>();
				split = data.split(java.util.regex.Pattern.quote("$"));
				for(int i=1;i< split.length;i++)
					tempList.add(split[i]);
				
					treeMap.put(split[0], tempList);
			}
		}
		
		
		records.clear();
		file = null;
		file = new File(indexId);
		
		// if file exists
		if (file.exists()) 
		{
			records = GetList(indexId);
			for(String recordId: records)
			{
				split = recordId.split(java.util.regex.Pattern.quote("$"));
				int newIntVal = Integer.parseInt(split[0]);
				this.treeMapIds.put(newIntVal, split[1]);
			}
		}
		
		
		records.clear();
		file = null;
		file = new File(indexEmail);
		
		// if file exists
		if (file.exists()) 
		{
			records = GetList(indexEmail);
			for(String recordId: records)
			{
				split = recordId.split(java.util.regex.Pattern.quote("$"));
				this.treeMapEmail.put(split[0], split[1]);
			}
		}
	}
	
	// Get user input
	public void GetUserInput() 
	{
		int inputOption =0;
		inputOption = ShowOptions();
		PerformAction(inputOption);
	}

	//Show Option List to User:
	public int ShowOptions()
	{
		BufferedReader brReader;
		try
		{
			int inputOption = 0;
			System.out.println("\nSelect the Operation :(Enter number associated to the operation)");
			System.out.println("1." + Options.Select);
			System.out.println("2." + Options.Insert);
			System.out.println("3." + Options.Delete);
			System.out.println("4." + Options.Modify);
			System.out.println("5." + Options.Count);
			System.out.println("6." + Options.BulkInsert);
			System.out.println("7." + Options.Close);
			brReader = new BufferedReader( new InputStreamReader(System.in));
			String str = brReader.readLine();
			if(!str.trim().equals(""))
			inputOption = Integer.parseInt(str);
			if(inputOption <=6 && inputOption>0)
				return inputOption;
			else if (inputOption == 7)
			{
				System.out.println("Closing the program.....");
				System.exit(0);
				return 0;
			}
			else
			{
				System.out.println("Please select options only from list");
				return ShowOptions();
			}
		}
		catch(Exception ex)
		{
			System.out.println("Error Occured" + ex.getMessage());
			return 0;
		}
	}
	
	// Perform action depending on option selected
	public void PerformAction(int inputOption)
	{
		try
		{
			switch(inputOption)
			{
				// Select Operation
				case 1: ShowSelectOptions();
						break;
				
				// Insert Operation
				case 2 : InsertNewReord();
						break;
				
				// Delete Operation
				case 3 : DeleteRecord();
						break;
				
				// Modify Operation
				case 4 : ModifyRecord();
					   	break;
				
				// Count Operation
				case 5:	RecordCount();
						break;
						
				// BulkInsert Operation
				case 6:	BulkInsertOperation();
						GetUserInput();
						break;
						
				default : GetUserInput();
						break;
			}
		}
		catch(Exception ex)
		{
			System.out.println("Error Occured" + ex.getMessage());
		}
	}
	
	public void ModifyRecord() 
	{
		BufferedReader br = null;
		try
		{
			// if input driven
			if(!isHardCord)
			{
				boolean isDataExists = false;
				String OffsetToDel="";
				String fieldNameValue = "";
				String[] line;
				System.out.println("Enter the record id to Modify the data: ");
				br = new BufferedReader( new InputStreamReader(System.in));
				String modRecordId = br.readLine();
				
				if(!modRecordId.equals(""))
				{
					isDataExists = IsDataAlreadyPresent(modRecordId, indexId);
					
					// Entered data is present in the file
					if(isDataExists)
					{
						System.out.println("Modifying record : ");
						line = matchedRecord.split(java.util.regex.Pattern.quote("$"));
						OffsetToDel = line[1];
						long offset = Long.parseLong(OffsetToDel);
						ReadDBRecord(offset);
						System.out.println("Enter the field name to modify and new value with 'Equal To' (=) seperation: ");
						br = new BufferedReader( new InputStreamReader(System.in));
						fieldNameValue = br.readLine();
						ModifyDatabaseRecord(fieldNameValue,offset);
					}
					else
					{
						System.out.println("Data is not present in database.");
					}
				}
				GetUserInput();
			}
			else // Hard coded value function
			{
				long offset = 0;
				System.out.println("\nModifying record : id = 1 will be changed to 505\n");
				ReadDBRecord(offset);
				// Hard coded field values
				String fieldNameHardCoded = "id,505";
				ModifyDatabaseRecord(fieldNameHardCoded,offset);
			}
		}
		catch(Exception ex)
		{
			System.out.println("Error Occured" + ex.getMessage());
		}
	}
	
	// Modify the data entry
	public void ModifyDatabaseRecord(String fieldNameValue,long offset) 
	{
		//String input = fieldNameValue.replace(" ", ""); 
		String[] inputFieldValue;
		try
		{
			boolean notUpdated = true;
			
			// Input driven code
			inputFieldValue = fieldNameValue.trim().split("\\s*=\\s*");
			inputFieldValue[0] = inputFieldValue[0].replace(" ","");
			//***
			// ID
			if(inputFieldValue[0].equalsIgnoreCase(id))
			{
				// Size must be Same or less
				int idValue = Integer.parseInt(inputFieldValue[1]);
				
				int key = readData.getId();
				treeMapIds.remove(key);
				int idVal = Integer.parseInt(inputFieldValue[1]);
				// Write ID field
				int isExist = WriteUQIndexFile(idVal, offset, indexId);
				
				if(isExist > 0)
				{
					readData.setId(idValue);
					byte[] buffer = SerializationUtils.serialize(readData);
					// Write DB file
					WriteDBFile(buffer,offset);
					notUpdated = false;
					System.out.println("Record modified successfully. ");
				}
				else
					System.out.println("Entered id already present in Database.");
			}
			// STATE
			else if(inputFieldValue[0].equalsIgnoreCase(state))
			{
				// size must be Same or less
				if(inputFieldValue[1].length() <= readData.getState().length())
				{
					boolean noKey = true;
					String oldKey = readData.getState();
					Set<String> keys = treeMapStates.keySet();
					ArrayList<String> tempList = new ArrayList<String>();
					String offsetString = Long.toString(offset);
					// Remove the Old Offset:
					for(String key: keys)
					{
						if(oldKey.equalsIgnoreCase(key))
						{
							tempList = treeMapStates.get(key);
							if(tempList!=null)
							{
								tempList.remove(new String(offsetString));
								if(tempList.size()< 1)
									treeMapStates.remove(key);
								
								break;
							}
						}	
					}
					
					Set<String> keySet = treeMapStates.keySet();
					// Add to new offset
					for(String keyinSet: keySet)
					{
						if(inputFieldValue[1].equalsIgnoreCase(keyinSet))
						{
							tempList = treeMapStates.get(keyinSet);
							if(tempList!=null)
							{
								tempList.add(new String(offsetString));
								noKey = false;
								break;
							}
						}
					}
					
					if(noKey)
					{
						ArrayList<String> tData = new ArrayList<String>();
						tData.add(new String(offsetString));
						treeMapStates.put(inputFieldValue[1].toUpperCase(), tData);
					}
					
					// Write the new tree in Index
					ModifyStatesIndex();
					
					readData.setState(inputFieldValue[1].toUpperCase());
					byte[] buffer = SerializationUtils.serialize(readData);
					// Write DB file
					WriteDBFile(buffer,offset);
					notUpdated = false;
					System.out.println("Record modified successfully. ");
				}	
			}
			
			//LAST NAME
			else if(inputFieldValue[0].equalsIgnoreCase(lastName))
			{
				// Size must be Same or less
				if(inputFieldValue[1].length() <= readData.getLastName().length())
				{
					boolean noKey = true;
					String oldKey = readData.getLastName();
					Set<String> keys = treeMap.keySet();
					ArrayList<String> tempList = new ArrayList<String>();
					String offsetString = Long.toString(offset);
					// Remove the Old Offset:
					for(String key: keys)
					{
						if(oldKey.equalsIgnoreCase(key))
						{
							tempList = treeMap.get(key);
							if(tempList!=null)
							{
								tempList.remove(new String(offsetString));
								if(tempList.size()< 1)
									treeMap.remove(key);
								
								break;
							}
						}	
					}
					
					Set<String> keySet = treeMap.keySet();
					// Add to new offset
					for(String keyinSet: keySet)
					{
						if(inputFieldValue[1].equalsIgnoreCase(keyinSet))
						{
							tempList = treeMap.get(keyinSet);
							if(tempList!=null)
							{
								tempList.add(new String(offsetString));
								noKey = false;
								break;
							}
						}
					}
					
					if(noKey)
					{
						ArrayList<String> tData = new ArrayList<String>();
						tData.add(new String(offsetString));
						treeMap.put(inputFieldValue[1], tData);
					}
					
					// Write the new tree in Index
					ModifyLastNameIndex();
					readData.setLastName(inputFieldValue[1]);
					byte[] buffer = SerializationUtils.serialize(readData);
					// Write DB file
					WriteDBFile(buffer,offset);
					notUpdated = false;
					System.out.println("Record modified successfully. ");
				}		
			}
			
			
			// Email	
			else if(inputFieldValue[0].equalsIgnoreCase(email))
			{
				// Size must be Same or less
				if(inputFieldValue[1].length() <= readData.getEmail().length())
				{
					String key = readData.getEmail();
					treeMapEmail.remove(key);
					
					// Write ID field
					int isExist = WriteUQEmailIndexFile(inputFieldValue[1], offset, indexEmail);
					if(isExist > 0)
					{
						readData.setEmail(inputFieldValue[1]);
						byte[] buffer = SerializationUtils.serialize(readData);
						
						// Write DB file
						WriteDBFile(buffer,offset);
						notUpdated = false;
						System.out.println("Record modified successfully. ");
					}
					else
					{
						System.out.println("Entered email already present in Database.");
					}
				}
			}
			
			//*******
			
			// First Name
			else if(inputFieldValue[0].equalsIgnoreCase(firstName))
			{
				// Size must be Same
				if(inputFieldValue[1].length() <= readData.getFirstName().length())
				{
					readData.setFirstName(inputFieldValue[1]);
					byte[] buffer = SerializationUtils.serialize(readData);
					// Write DB file
					WriteDBFile(buffer,offset);
					notUpdated = false;
					System.out.println("Record modified successfully. ");
				}
			}	
			
			// City
			else if(inputFieldValue[0].equalsIgnoreCase(city))
			{
				// Size must be Same
				if(inputFieldValue[1].length() <= readData.getCity().length())
				{
					readData.setCity(inputFieldValue[1]);
					byte[] buffer = SerializationUtils.serialize(readData);
					// Write DB file
					WriteDBFile(buffer,offset);
					notUpdated = false;
					System.out.println("Record modified successfully. ");
				}
			}
			
			// CompanyName
			else if(inputFieldValue[0].equalsIgnoreCase(company))
			{
				// Size must be Same
				if(inputFieldValue[1].length() <= readData.getCompanyName().length())
				{
					readData.setCompanyName(inputFieldValue[1]);
					byte[] buffer = SerializationUtils.serialize(readData);
					// Write DB file
					WriteDBFile(buffer,offset);
					notUpdated = false;
					System.out.println("Record modified successfully. ");
				}
			}
			
			// Address
			else if(inputFieldValue[0].equalsIgnoreCase(address))
			{
				// Size must be Same
				if(inputFieldValue[1].length() <= readData.getAddress().length())
				{
					readData.setAddress(inputFieldValue[1]);
					byte[] buffer = SerializationUtils.serialize(readData);
					// Write DB file
					WriteDBFile(buffer,offset);
					notUpdated = false;
					System.out.println("Record modified successfully. ");
				}
			}
			
			// County
			else if(inputFieldValue[0].equalsIgnoreCase(county))
			{
				// Size must be Same or less
				if(inputFieldValue[1].length() <= readData.getCounty().length())
				{
					readData.setCounty(inputFieldValue[1]);
					byte[] buffer = SerializationUtils.serialize(readData);
					// Write DB file
					WriteDBFile(buffer,offset);
					notUpdated = false;
					System.out.println("Record modified successfully. ");
				}
			}
			
			// zip
			else if(inputFieldValue[0].equalsIgnoreCase(zip))
			{
				// Size must be Same
				if(inputFieldValue[1].length() <= readData.getZip().length())
				{
					readData.setZip(inputFieldValue[1]);
					byte[] buffer = SerializationUtils.serialize(readData);
					// Write DB file
					WriteDBFile(buffer,offset);
					notUpdated = false;
					System.out.println("Record modified successfully. ");
				}
			}
			
			// Phone1
			else if(inputFieldValue[0].equalsIgnoreCase(phone1))
			{	
				// Size must be Same
				if(inputFieldValue[1].length() <= readData.getPhoneNo1().length())
				{
					readData.setPhoneNo1(inputFieldValue[1]);
					byte[] buffer = SerializationUtils.serialize(readData);
					// Write DB file
					WriteDBFile(buffer,offset);
					notUpdated = false;
					System.out.println("Record modified successfully. ");
				}
			}
			
			// Phone2
			else if(inputFieldValue[0].equalsIgnoreCase(phone2))
			{
				// Size must be Same
				if(inputFieldValue[1].length() <= readData.getPhoneNo2().length())
				{
					readData.setPhoneNo2(inputFieldValue[1]);
					byte[] buffer = SerializationUtils.serialize(readData);
					// Write DB file
					WriteDBFile(buffer,offset);
					notUpdated = false;
					System.out.println("Record modified successfully. ");
				}
			}
			
			// Web
			else if(inputFieldValue[0].equalsIgnoreCase(web))
			{
				// Size must be Same or less
				if(inputFieldValue[1].length() <= readData.getWeb().length())
				{
					readData.setWeb(inputFieldValue[1]);
					byte[] buffer = SerializationUtils.serialize(readData);
					// Write DB file
					WriteDBFile(buffer,offset);
					notUpdated = false;
					System.out.println("Record modified successfully. ");
				}
			}
			else
			{
				notUpdated = false;
				System.out.println("Invalid input.");
			}
			if(notUpdated)
				System.out.println("Record can't be modified.Size exceeds.");
		}
		catch(Exception ex)
		{
			System.out.println("Error Occured" + ex.getMessage());
		}
		
	}
	
	// Modify the Last Name Index 
	public void ModifyLastNameIndex() throws IOException 
	{
		try
		{
			Set<String> keySet = treeMap.keySet();
			ArrayList<String> tempList = new ArrayList<String>();
			File file = new File(indexLastName);
			BufferedWriter bw = null;
				
			// File Writer Object
			FileWriter fw = new FileWriter(file.getAbsoluteFile(),false);
			bw = new BufferedWriter(fw);
				
			for(String key:keySet)
			{
				String content = "";
				content = key;
				tempList = treeMap.get(key);
				for(String tempData:tempList)
				{
					content = content + "$"+ tempData;
				}
				bw.write(content);
				bw.newLine();
			}
	
			bw.close();	
		}
		catch(Exception e)
		{
			
		}
	}

	// Modify the States Index 
	public void ModifyStatesIndex() throws IOException 
	{
		try
		{
			Set<String> keySet = treeMapStates.keySet();
			ArrayList<String> tempList = new ArrayList<String>();
			File file = new File(indexState);
			BufferedWriter bw = null;
				
			// File Writer Object
			FileWriter fw = new FileWriter(file.getAbsoluteFile(),false);
			bw = new BufferedWriter(fw);
				
			for(String key:keySet)
			{
				String content = "";
				content = key;
				tempList = treeMapStates.get(key);
				for(String tempData:tempList)
				{
					content = content + "$"+ tempData;
				}
				bw.write(content);
				bw.newLine();
			}
	
			bw.close();	
		}
		catch(Exception e)
		{
			
		}
	}

	// Delete the record from Index files and DB file
	public void DeleteRecord() 
	{
		BufferedReader br = null;
		try
		{
			boolean isDataExists = false;
			String OffsetToDel="";
			String[] line;
			String delRecordId ="";
			// If Input driven program is running
			if(!isHardCord)
			{
				System.out.println("Enter the record id to Delete the data: ");
				br = new BufferedReader( new InputStreamReader(System.in));
				delRecordId = br.readLine();
			}
			else // Hard coded program
			{
				delRecordId = "500";
				System.out.println("\nDeleting The record Id = 500");
			}
			
			if(!delRecordId.equals(""))
			{
				isDataExists = IsDataAlreadyPresent(delRecordId, indexId);
				
				// Entered data is present in the file
				if(isDataExists)
				{
					System.out.println("Deleting The record.");
					line = matchedRecord.split(java.util.regex.Pattern.quote("$"));
					OffsetToDel = line[1];
					long offset = Long.parseLong(OffsetToDel);
					ReadDBRecord(offset);
					boolean isSuccess = DeleteIdIndex(line[0],indexId);
					if(isSuccess)
					{
						// Delete the Non-Unique Index file records :
						DeleteEmailIndex(readData.email,indexEmail);
						DeleteNQIndexStateRecords(OffsetToDel,indexState,readData.state);
						DeleteNQIndexLnameRecords(OffsetToDel,indexLastName,readData.lastName);
						RemoveDBEntry(OffsetToDel);
					}
					System.out.println("Record of Id = "+line[0]+" deleted. ");
				}
				else
				{
					System.out.println("Data is not present in database.Failed to delete.");
				}
			}
			
			// If the program is not running hard coded values
			if(!isHardCord)
				GetUserInput();
		}
		catch(Exception ex)
		{
			System.out.println("Error Occured" + ex.getMessage());
		}
	}
	
	
	// Remove Data link from Data.db
	public void RemoveDBEntry(String offsetToDel) 
	{	
		try
		{
			int totalBytesData = 0 ;
			long offset = Long.parseLong(offsetToDel);
			// Open RAF file : 
			RandomAccessFile file = new RandomAccessFile(dbFile, "rw");
			file.seek(offset);
			totalBytesData = file.readInt();
			file.seek(offset);
			byte[] byteData = new byte[totalBytesData + Integer.SIZE/8];

			// Remove the actual record in "Data.db" file
			file.write(byteData);
			
			file.close();
		}
		catch(Exception ex)
		{
			System.out.println("Error Occured" + ex.getMessage());
		}
	}

	// Delete the Non-Unique Index records - State
	public void DeleteNQIndexStateRecords(String delRecordOffset,String path, String deletedKey) 
	{
		try
		{

			ArrayList<String> tempList = new ArrayList<String>();
			Set<String> keys = treeMapStates.keySet();
			
			for(String key: keys)
			{
				if(deletedKey.equals(key))
				{
					tempList = treeMapStates.get(key);
					if(tempList!=null)
					{
						if(tempList.size()<=1)
							treeMapStates.remove(key);
						else
							tempList.remove(new String(delRecordOffset));
						break;
					}
				}
			}
			
			Set<String> keySet = treeMapStates.keySet();
			
			File file = new File(path);
			BufferedWriter bw = null;
			
			// File Writer Object
			FileWriter fw = new FileWriter(file.getAbsoluteFile(),false);
			bw = new BufferedWriter(fw);
			
			for(String key:keySet)
			{
				String content = "";
				content = key;
				tempList = treeMapStates.get(key);
				for(String tempData:tempList)
				{
					content = content + "$"+ tempData;
				}
				bw.write(content);
				bw.newLine();
			}
			System.out.println("Deleting State from Index file..");
			bw.close();			
			
		}
		catch(Exception ex)
		{
			System.out.println("Error Occured" + ex.getMessage());
		}
	}
	
	
	// Delete the Non-Unique Index records - Last name
	public void DeleteNQIndexLnameRecords(String delRecordOffset,String path, String deletedKey) 
	{
		try
		{
			ArrayList<String> tempList;
			Set<String> keys = treeMap.keySet();
				
			for(String key: keys)
			{
				if(deletedKey.equals(key))
				{
					tempList = treeMap.get(key);
					if(tempList!=null)
					{
						if(tempList.size()<=1)
							treeMap.remove(key);
						else
							tempList.remove(new String(delRecordOffset));
						break;
						}
					}
			}
				
			Set<String> keySet = treeMap.keySet();
				
			File file = new File(path);
			BufferedWriter bw = null;
				
			// File Writer Object
			FileWriter fw = new FileWriter(file.getAbsoluteFile(),false);
			bw = new BufferedWriter(fw);
				
			for(String key:keySet)
			{
				String content = "";
				content = key;
				tempList = treeMap.get(key);
				for(String tempData:tempList)
				{
					content = content + "$"+ tempData;
				}
				bw.write(content);
				bw.newLine();
			}
			System.out.println("Deleting Last Name from Index file..");
			bw.close();	
		}
		catch(Exception ex)
		{
			System.out.println("Error Occured" + ex.getMessage());
		}
	}


	// Insert New record in database : "Insert"
	public  void InsertNewReord() 
	{
		BufferedReader brReader = null;
		try
		{
			String insertString = "";
			String temp="",recordSeperator ="\',\'";
			String doubleQuote ="\"";
			String[] input;
			BulkData dataItem = null;
			// If Input Driven is running
			if(!isHardCord)
			{
				System.out.println("Enter the record (in Single quote - Comma seperated) to Insert in database: ");
				brReader = new BufferedReader( new InputStreamReader(System.in));
				insertString = brReader.readLine();
			}
			else // Hard coded is running
			{
				System.out.println("\nInserting New Record in database file : Id = 502");
				insertString = "'502','James','Butt','Benton, John B Jr','6649 N Blue Gum St','New Orleans','Orleans','LA','70116','504-621-8927','504-845-1427','jamesbutt@gmail.com','http://www.bentonjohnbjr.com'";
			}
			if(!insertString.equals(""))
			{
				temp = insertString.replace(doubleQuote,"");
				insertString = temp.substring(1, temp.length()-1);
				input = insertString.split(recordSeperator);
				if(input.length == 13)
				{
					dataItem = new BulkData();
					int idVal = Integer.parseInt(input[0]);
					dataItem.setId(idVal);
					dataItem.setFirstName(input[1]);
					dataItem.setLastName(input[2]);
					dataItem.setCompanyName(input[3]);
					dataItem.setAddress(input[4]);
					dataItem.setCity(input[5]);
					dataItem.setCounty(input[6]);
					dataItem.setState(input[7]);
					dataItem.setZip(input[8]);
					dataItem.setPhoneNo1(input[9]);
					dataItem.setPhoneNo2(input[10]);
					dataItem.setEmail(input[11]);
					dataItem.setWeb(input[12]);
					WriteDBIndexFiles(dataItem);
				}
				else
					System.out.println("Insertion Failed.Input string is not in correct Format.");
				
				// If program is not running in Hard coded mode
				if(!isHardCord)
					GetUserInput();
			}
		}
		catch(Exception ex)
		{
			System.out.println("Error Occured" + ex.getMessage());
		}
	}

	// Get the total Number of Records from DB : "Count"
	public void RecordCount() 
	{
		ArrayList<String> totalRecords = new ArrayList<String>();
		try
		{
			totalRecords = GetList(indexId);
			System.out.println("Total Number Of Records in Database : " + totalRecords.size());
			// Check if the program is running in Hard coded mode
			if(!isHardCord)
				GetUserInput();
		}
		catch(Exception ex)
		{
			System.out.println("Error Occured" + ex.getMessage());
		}
	}
	
	// Show Select Options  

	// Show "Select" operation.
	//Show Select record Options:
	public void ShowSelectOptions()
	{
		try
		{
			// If Input Driven is running
			if(!isHardCord)
			{
				BufferedReader br = null;
				int inputSelectOption = 0;
				System.out.println("Select the Operation :");
				System.out.println("1." + SelectOptions.PersonId);
				System.out.println("2." + SelectOptions.LastName);
				System.out.println("3." + SelectOptions.State);
				System.out.println("4." + SelectOptions.Email);
				System.out.println("5." + SelectOptions.MainMenu);
				br = new BufferedReader( new InputStreamReader(System.in));
				String str = br.readLine();
				if(!str.trim().equals(""))
					inputSelectOption = Integer.parseInt(str);
				
				if(inputSelectOption <=5 && inputSelectOption>=1)
				{
					switch(inputSelectOption)
					{
						case 1: System.out.println("Enter person Id to get the Record:");
								br = new BufferedReader( new InputStreamReader(System.in));
								String id = br.readLine();
								if(!id.equals(""))
									ShowRecords(id,indexId);
								GetUserInput();
								break;
								
						case 2: System.out.println("Enter Last Name to get the Record:");
								br = new BufferedReader( new InputStreamReader(System.in));
								String lastName= br.readLine();
								if(!lastName.equals(""))
									ShowRecords(lastName,indexLastName);
								GetUserInput();
								break;
								
						case 3:System.out.println("Enter state to get the Records:(Short Form)");
							   br = new BufferedReader( new InputStreamReader(System.in));
							   String state = br.readLine();
							   if(!state.equals(""))
								   ShowRecords(state,indexState);
							   GetUserInput();
							   break;
						
						case 4: System.out.println("Enter person Email Id to get the Record:");
								br = new BufferedReader( new InputStreamReader(System.in));
								String emailid = br.readLine();
								if(!emailid.equals(""))
									ShowRecords(emailid,indexEmail);
								GetUserInput();
								break;
								
						default : ShowSelectOptions();
					}
				}
				else if (inputSelectOption == 4)
				{
					System.out.println("Closing the Select operation.....");
					ShowOptions();
				}
				else
				{
					System.out.println("Please select options only from list");
					ShowSelectOptions();
				}
			}
			else //If Hard coded is running
			{
				System.out.printf("\nShowing Record of ID = 50\n");
				ShowRecords("50",indexId);

				System.out.printf("\nShowing Record of Last Name = Butt\n");
				ShowRecords("Butt",indexLastName);

				System.out.printf("\nShowing Record of States = WA\n");
				ShowRecords("WA",indexState);
				
				System.out.printf("\nShowing Record of Email = maurine_yglesias@yglesias.com\n");
				ShowRecords("maurine_yglesias@yglesias.com",indexEmail);
			}
		}
		catch(Exception ex)
		{
			System.out.println("Error Occured" + ex.getMessage());
		}
	}
	
	// Show Records Form Database : "Select"
	// Show Records from DB file
	
	
	// Show Records
	public void ShowRecords(String matchString,String path) 
	{
		ArrayList<String> tempRecords = new ArrayList<String>();
		try
		{
			tempRecords = GetList(path);
			boolean recordPresent = false;
			for(String record : tempRecords)
			{
				String[] collection = record.split(java.util.regex.Pattern.quote("$"));
				if(matchString.equalsIgnoreCase(collection[0]))
				{
					for(String offsetString : collection)
					{
						// Skip First Record
						if (offsetString == collection[0]) continue;
						else
						{
							recordPresent = true;
							long offset = Long.parseLong(offsetString);
							ReadDBRecord(offset);
						}
					}
				}
			}
			if(!recordPresent)
				System.out.println("Record not present in database.");
		}
		catch(Exception ex)
		{
			System.out.println("Error Occured" + ex.getMessage());
		}
	}

	// File Record Insertion in Database.
	// Get the data From file and collect in a single object.
	
	
	// Insert multiple records from file at a time in DB file.
	public void BulkInsertOperation()
	{
		BufferedReader br = null;
		String line = "";
		String temData ="";
		String delimiter1 =",\"";
		String delimiter2 ="\",";
		String cvsSplitBy = "\",\"";
		String replacement = "$";
		try
		{
			BulkData dataItem = null;
			String path ="";
			ArrayList<BulkData> fileData = new ArrayList<BulkData>();
			// Input Driven Program
			if(!isHardCord)
			{
				System.out.println("Enter the file path to BulkInsert:");
				BufferedReader brReader = new BufferedReader( new InputStreamReader(System.in));
				path = brReader.readLine();
			}
			else // If hard coded values
			{
				System.out.println("Static path to BulkInsert:");
				path = csvFile;
			}
			
			// File path is entered.
			if(!path.trim().equals(""))
			{
				System.out.println("Reading the Input file:");
				br = new BufferedReader(new FileReader(path));
				line = br.readLine();
				line = "";
				while ((line = br.readLine()) != null) 
				{ 
					dataItem = new BulkData();
					temData = line.replace(cvsSplitBy, replacement);
					temData = temData.replace(delimiter1, replacement);
					line = temData.replace(delimiter2, replacement);
					line = line.substring(1, line.length()-1);
					String[] singleRecord = line.split(java.util.regex.Pattern.quote("$"));
					int idVal = Integer.parseInt(singleRecord[0]);
					dataItem.setId(idVal);
					dataItem.setFirstName(singleRecord[1]);
					dataItem.setLastName(singleRecord[2]);
					dataItem.setCompanyName(singleRecord[3]);
					dataItem.setAddress(singleRecord[4]);
					dataItem.setCity(singleRecord[5]);
					dataItem.setCounty(singleRecord[6]);
					dataItem.setState(singleRecord[7]);
					dataItem.setZip(singleRecord[8]);
					dataItem.setPhoneNo1(singleRecord[9]);
					dataItem.setPhoneNo2(singleRecord[10]);
					dataItem.setEmail(singleRecord[11]);
					dataItem.setWeb(singleRecord[12]);
					fileData.add(dataItem);
				}
			}
			
			// Sort the Data
			Collections.sort(fileData, BulkData.IdComparator);
			
			// Insert Data into DB and Index files
			for(BulkData bulkDataItem : fileData)
			{
				WriteDBIndexFiles(bulkDataItem);
			}
			System.out.println("Size =" + fileData.size()+"\n***EOF***");
		}
		catch(Exception ex)
		{
			System.out.println("Error Occured" + ex.getMessage());
		}
	}
	
	// Write DB and Index Files
	// Write data in Data.db file
	public void WriteDBIndexFiles(BulkData bulkDataItem)
	{
		try
		{
			byte[] buffer = null;
			int result = 1,emailResult = 1;
			// Get the Byte array of Object
			buffer = SerializationUtils.serialize(bulkDataItem);
			int bufferSize = buffer.length;
			

			// Open RAF file : 
			RandomAccessFile file = new RandomAccessFile(dbFile, "rw");
			file.seek(file.length());
			long offset = file.getFilePointer();
			
			// Write the record in Index "Data.db" file
			result = WriteUQIndexFile(bulkDataItem.id,offset,indexId);
			
			// If Record is not present in DB index file- Insert the record
			if(result > 0)
			{
				// Write the record in Index "Email.ndx" file
				emailResult = WriteUQEmailIndexFile(bulkDataItem.email,offset,indexEmail);
				
				// Check - duplicate Email Id not allowed 
				if(emailResult > 0)
				{
					// Write the record in Index Last name,State files
					WriteNQLastNameIndexFile(bulkDataItem.lastName,offset,indexLastName);
					WriteNQStatesIndexFile(bulkDataItem.state,offset,indexState);
					
					System.out.println("Writing data into Data files..");
					// Write the record bytes in Database "Data.db" file
					file.writeInt(bufferSize);
					// Write the actual record in "Data.db" file
					file.write(buffer);
					//System.out.println("Offset:" + offset);
				}
				else
				{
					System.out.println("Record Email Id Already Exists.Failed to Insert.");
					// Inserted Id in Index Id field should be removed
					treeMapIds.remove(bulkDataItem.id);
					// Writing new tree in Index.ndx file  
					WriteIdIndexFile(indexId);
				}
			}
			else // Record Present in Database file.
			{
				System.out.println("Record Id Already Exists.Failed to Insert.");
			}
			file.close();
			//ReadDBRecord(offset);
		}
		catch(Exception ex)
		{
			System.out.println("Error Occured" + ex.getMessage());
		}
	}
	
	// write/Modify DB file 
	public void WriteDBFile(byte[] buffer,long offset)
	{
		try
		{
			// Open RAF file : 
			RandomAccessFile file = new RandomAccessFile(dbFile, "rw");
			file.seek(offset);
			
			// Write the record bytes in Database "Data.db" file
			file.writeInt(buffer.length);
			// Write the actual record in "Data.db" file
			file.write(buffer);
			file.close();
		}
		catch(Exception ex)
		{
			System.out.println("Error Occured" + ex.getMessage());
		}
	}
	
	// Get the file Records.
	// Get the Array list of File Data
	public ArrayList<String> GetList(String filePath) throws IOException
	{
		ArrayList<String> tempData = null;
		try 
		{
			tempData = new ArrayList<String>();
			
			// If File Exists, Read the file and check the duplicate State
			BufferedReader reader = new BufferedReader(new FileReader(filePath));
			String line = null;
			while ((line = reader.readLine()) != null) 
			{
				tempData.add(line);
			}
			reader.close();
		} 
		catch (Exception e) 
		{
			System.out.println("Error Occured" + e.getMessage());
		}
		return tempData;
	}
	
	
	// Delete the Item from List
	public boolean DeleteIdIndex(String deleteItemId,String filePath) throws IOException
	{
		try
		{
			boolean isPresent = false;
			// Constructing Tree Map
			String tempString = "";
			
			// Check the tree map contains the Key
			Set<Integer> keys = this.treeMapIds.keySet();
			int newIntVal = Integer.parseInt(deleteItemId);
			for(Integer key: keys)
			{
				if(newIntVal == key)
				{
					this.treeMapIds.remove(key);
					isPresent = true;
					break;
				}
			}
			if(isPresent)
			{
				// Write new tree in Index file
				// File Writer Object
				File file = new File(filePath);
				
				// if file doesn't exists, then create it
				if (!file.exists()) {
					file.createNewFile();
				}			
				
				FileWriter fw = new FileWriter(file.getAbsoluteFile(),false);
				BufferedWriter bw = new BufferedWriter(fw);
				Set<Integer> keySet = this.treeMapIds.keySet();			
				for(Integer key:keySet)
				{
					tempString ="";
					String keyString = key.toString();
					String content = "";
					content = keyString;
					tempString = this.treeMapIds.get(key);
					content = content + "$"+ tempString;
					bw.write(content);
					bw.newLine();
				}
				
				bw.close();
				System.out.println("Deleting Id from Index file..");
			}
			return isPresent;		
		}
		catch(Exception ex)
		{
			return false;
		}
	}
	
	// Delete the Item from List
	public boolean DeleteEmailIndex(String deleteItemId,String filePath) throws IOException
	{
		try
		{
			boolean isPresent = false;
			// Constructing Tree Map
			String tempString = "";
			
			// Check the tree map contains the Key
			Set<String> keys = this.treeMapEmail.keySet();
			for(String key: keys)
			{
				if(deleteItemId.equalsIgnoreCase(key))
				{
					this.treeMapEmail.remove(key);
					isPresent = true;
					break;
				}
			}
			if(isPresent)
			{
				// Write new tree in Index file
				// File Writer Object
				File file = new File(filePath);
				
				// if file doesn't exists, then create it
				if (!file.exists()) {
					file.createNewFile();
				}			
				
				FileWriter fw = new FileWriter(file.getAbsoluteFile(),false);
				BufferedWriter bw = new BufferedWriter(fw);
				Set<String> keySet = this.treeMapEmail.keySet();			
				for(String key:keySet)
				{
					tempString ="";
					String keyString = key.toString();
					String content = "";
					content = keyString;
					tempString = this.treeMapEmail.get(key);
					content = content + "$"+ tempString;
					bw.write(content);
					bw.newLine();
				}
				
				bw.close();
				System.out.println("Deleting Email from Index file..");
			}
			return isPresent;		
		}
		catch(Exception ex)
		{
			return false;
		}
	}
		
	
	
	//Write to Non-Unique Index File - LastName Index 
	public  void WriteNQLastNameIndexFile(String newValue, long offset,String path) 
	{
		try
		{
			System.out.println("Writing in LastName Index file..");
			// Constructing Tree Map
			ArrayList<String> tempList = new ArrayList<String>();
			
			// Check the tree map contains the Key
			Set<String> keys = treeMap.keySet();
			String offsetString = Long.toString(offset);
			boolean isKeyAvailable = false;
			for(String key: keys)
			{
				if(newValue.equals(key))
				{
					tempList = treeMap.get(key);
					if(tempList!= null)
					{
						isKeyAvailable = true;
						tempList.add(new String(offsetString));
						break;
					}
				}
			}
			
			if(!isKeyAvailable)
			{
				tempList.add(offsetString);
				treeMap.put(newValue, tempList);
			}
			
			// Write new tree in Index file
			// File Writer Object
			File file = new File(path);
			
			// if file doesn't exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}			
			
			FileWriter fw = new FileWriter(file.getAbsoluteFile(),false);
			BufferedWriter bw = new BufferedWriter(fw);
			Set<String> keySet = treeMap.keySet();			
			for(String key:keySet)
			{
				String content = "";
				content = key;
				tempList = treeMap.get(key);
				for(String tempData:tempList)
				{
					content = content + "$"+ tempData;
				}
				bw.write(content);
				bw.newLine();
			}

			bw.close();			
		}
		catch(Exception ex)
		{
			System.out.println("Error Occured :" + ex.getMessage());
		}
	}
	
	//Write to Non-Unique Index File - States Index 
	public  void WriteNQStatesIndexFile(String newValue, long offset,String path) 
	{
		try
		{
			System.out.println("Writing in States Index file..");
			// Constructing Tree Map
			ArrayList<String> tempList = new ArrayList<String>();
			
			// Check the tree map contains the Key
			Set<String> keys = treeMapStates.keySet();
			String offsetString = Long.toString(offset);
			boolean isKeyAvailable = false;
			for(String key: keys)
			{
				if(newValue.equals(key))
				{
					tempList = treeMapStates.get(key);
					if(tempList!= null)
					{
						isKeyAvailable = true;
						tempList.add(new String(offsetString));
						break;
					}
				}
			}
			
			if(!isKeyAvailable)
			{
				tempList.add(offsetString);
				treeMapStates.put(newValue, tempList);
			}
			
			// Write new tree in Index file
			// File Writer Object
			File file = new File(path);
			
			// if file doesn't exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}			
			
			FileWriter fw = new FileWriter(file.getAbsoluteFile(),false);
			BufferedWriter bw = new BufferedWriter(fw);
			Set<String> keySet = treeMapStates.keySet();			
			for(String key:keySet)
			{
				String content = "";
				content = key;
				tempList = treeMapStates.get(key);
				for(String tempData:tempList)
				{
					content = content + "$"+ tempData;
				}
				bw.write(content);
				bw.newLine();
			}

			bw.close();			
		}
		catch(Exception ex)
		{
			System.out.println("Error Occured :" + ex.getMessage());
		}
	}
	
	// Write to Unique ID Index File
	public int WriteUQIndexFile(int newValue, long offset,String path)
	{
		try
		{
			// Check the tree map contains the Key
			Set<Integer> keys = this.treeMapIds.keySet();
			String offsetString = Long.toString(offset);
			boolean isKeyAvailable = false;
			int newIntVal = newValue;
			for(Integer key: keys)
			{
				if(newIntVal == key)
				{
					return 0;
				}
			}
			
			if(!isKeyAvailable)
			{
				this.treeMapIds.put(newIntVal, offsetString);
			}
			
			System.out.println("Writing in Id Index file..");
			// Write the tree in Id.ndx file
			WriteIdIndexFile(path);
			
			return 1;
		}
		catch(Exception ex)
		{
			System.out.println("Error Occured :" + ex.getMessage());
			return -1;
		}
	}
	
	
	// Writing records in ID Index Files
	public void WriteIdIndexFile(String path) 
	{
		try
		{
			// Constructing Tree Map
			String tempString = "";
			
			// Write new tree in Index file
			// File Writer Object
			File file = new File(path);
			
			// if file doesn't exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}			
			
			FileWriter fw = new FileWriter(file.getAbsoluteFile(),false);
			BufferedWriter bw = new BufferedWriter(fw);
			Set<Integer> keySet = this.treeMapIds.keySet();			
			for(Integer key:keySet)
			{
				tempString ="";
				String keyString = key.toString();
				String content = "";
				content = keyString;
				tempString = this.treeMapIds.get(key);
				content = content + "$"+ tempString;
				bw.write(content);
				bw.newLine();
			}

			bw.close();	
		}
		catch(Exception ex)
		{
			System.out.println("Error Occured :" + ex.getMessage());
		}
	}

	// Write to Unique Email Index File
	public int WriteUQEmailIndexFile(String newValue, long offset,String path)
	{
		try
		{
			// Constructing Tree Map
			String tempString = "";
			
			// Check the tree map contains the Key
			Set<String> keys = this.treeMapEmail.keySet();
			String offsetString = Long.toString(offset);
			boolean isKeyAvailable = false;
			for(String key: keys)
			{
				if(newValue.equalsIgnoreCase(key))
				{
					return 0;
				}
			}
			
			if(!isKeyAvailable)
			{
				this.treeMapEmail.put(newValue, offsetString);
			}
			
			// Write new tree in Index file
			// File Writer Object
			File file = new File(path);
			
			// if file doesn't exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}			
			
			FileWriter fw = new FileWriter(file.getAbsoluteFile(),false);
			BufferedWriter bw = new BufferedWriter(fw);
			Set<String> keySet = this.treeMapEmail.keySet();			
			for(String key:keySet)
			{
				tempString ="";
				String keyString = key.toString();
				String content = "";
				content = keyString;
				tempString = this.treeMapEmail.get(key);
				content = content + "$"+ tempString;
				bw.write(content);
				bw.newLine();
			}

			bw.close();	
			System.out.println("Writing in Email Index file..");
			return 1;
		}
		catch(Exception ex)
		{
			System.out.println("Error Occured :" + ex.getMessage());
			return -1;
		}
	}

	
	// Get the Data is present in file
	public boolean IsDataAlreadyPresent(String fileDataItem,String path)
	{
		boolean alreadyExist = false;
		try
		{
			// Get the file data
			ArrayList<String> fileData = new ArrayList<String>();
			fileData = GetList(path);
			String [] splittedString;
			for(String dataItem: fileData)
			{
				splittedString = dataItem.split(java.util.regex.Pattern.quote("$"));
				if(fileDataItem.equalsIgnoreCase(splittedString[0]))
				{
					matchedRecord = dataItem;
					alreadyExist = true;
					break;
				}
			}
		}
		catch(Exception ex)
		{
			System.out.println("Error Occured :" + ex.getMessage());
			return true;
		}
		return alreadyExist;
	}
	
	// Select the Database record
	public void ReadDBRecord(long Offset)
	{
		try
		{
			readData = null;
			readData = new BulkData();
			int totalBytesData = 0;
			RandomAccessFile file = new RandomAccessFile(dbFile,"r");
			file.seek(Offset);
			totalBytesData = file.readInt();
			byte[] byteData = new byte[totalBytesData];
			file.read(byteData);
			BulkData fileData = new BulkData();
			fileData = (BulkData)SerializationUtils.deserialize(byteData);
			System.out.println("****Reading Database New Record :**** ");
			readData = fileData;
			System.out.println(" Id: " + fileData.id + " ;First Name: " + fileData.firstName + " ;Last Name: "+ fileData.lastName+ " ;Address: " +fileData.address 
			+" ;Company: "+fileData.companyName +" ;City: "+fileData.city + " ;County: "+ fileData.county +" ;State: "+fileData.state +" ;Zip: "+ fileData.zip +" ;phone1: "+fileData.phoneNo1
			+" ;phone2: "+fileData.phoneNo2 +" ;Email : "+ fileData.email +" ;Web: "+ fileData.web);
			
			file.close();
		}
		catch(Exception ex)
		{
			System.out.println("Error Occured :" + ex.getMessage());
		}
	}	
}
