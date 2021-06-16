package com.company;
import  com.dexcom.bulk_data.parser.BulkDataParser;
import com.dexcom.models.private_data.EnhancedDexcomSyncData;
import com.dexcom.models.private_data.JsonUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) throws IOException {

        System.out.println("Data Parser");

        String originalFolderPath = "/Users/bm1120/Documents/Projects/UDP/ParsedFilesFolders/71";
        String outputFolderPath = "/Users/bm1120/Documents/Projects/UDP/ParsedFilesFolders/71BD";
        String[] files = {"G6Android013Confirmed 2-22-2018 2.txt", "071Confirmed398 7-9-2018.txt", "ParsingError 2-22-2018.txt", "G6071Confirmed 6-14-2017.txt", "ConfirmExpand 3-28-2018.txt", "71NotConfirmed 6-28-2016.txt", "71Confirmed 4-22-2016.txt", "G60iOS071Confirmed 2-16-2018.txt", "G5ReceiverApp042Undetermined_10_27_2017.txt", "071Confirmed458 5-8-2018.txt", "013NotConfirmed170 3-20-2018.txt", "NullError11-22-2018.txt", "PL2287 20190111_902.txt"};


        for (int j = 0; j < files.length; j++) {

            String inputJsonPath = originalFolderPath + "/" + files[j];
            String outputJsonPath = outputFolderPath + "/" + files[j];
            System.out.println("Parsing file -> " + files[j]);

            //Get text from file
            String jsonText = "";
            try {
                File myObj = new File(inputJsonPath);
                Scanner myReader = new Scanner(myObj);
                while (myReader.hasNextLine()) {
                    jsonText += myReader.nextLine();
                }
                myReader.close();
            } catch (FileNotFoundException e) {
                System.out.println("An error occurred.");
                e.printStackTrace();
            }

            //Get array from file
            jsonText = jsonText.replace('[', ' ').replace(']', ' ');//Remove the brackets on the file
            String[] objArray = jsonText.split("},");

            //Verify that last one have information
            String lastValue = objArray[objArray.length - 1];
            if (lastValue.length() < 3) {
                objArray = Arrays.copyOf(objArray, objArray.length - 1);
            }


            System.out.println("Parsing bulk data -> " + objArray.length + " objects");

            //Parse each object on the array
            String finalJsonText = "";
            for (int i = 0; i < objArray.length; i++) {
                String objToCast = objArray[i];
                //if (i != objArray.length-1) {
                    objToCast += "}"; //Add missing bracket to each object
                //}

                //Parse object
                //Parse Data to object

                System.out.println("Parsing object number -> " + i + "");
                EnhancedDexcomSyncData eData = (EnhancedDexcomSyncData) JsonUtils.tryToParse(objToCast).get();
                //Parse Object to
                String jsonTextObject = (String) BulkDataParser.getEnhancedDexcomSyncDataAsJson(eData).get();

                //Append json text to final variable
                finalJsonText += jsonTextObject;
                if (i != objArray.length - 1) {
                    finalJsonText += ","; //Add a coma for separation
                }
            }


            //Add [] string because it is an array of objects
            String finalArray = "[" + finalJsonText + "]";

            //Save it to a new file
            FileWriter myWriter = new FileWriter(outputJsonPath);
            myWriter.write(finalArray);
            myWriter.close();

        }
    }


}
