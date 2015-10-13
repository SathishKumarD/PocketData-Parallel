/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.ub.tbd;

import edu.ub.tbd.constants.AppConstants;
import edu.ub.tbd.parser.Parallelize;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
/**
 * This is the driver class of the Phone Data Parser when invoked using jar or command line
 * 
 * @author sathish
 */
public class Main {

    /**
     * @param args the command line arguments [-v|V] [-help] [-p "mode"]
     */
    public static void main(String[] args) {
        
        for(int i = 0; i < args.length; i++){
            if (args[i].equals("--src")) {
            	String sourDir = args[i+1];
                AppConstants.SRC_DIR = sourDir;
            }
            if (args[i].equals("--dest")) {
            	String destFolder = args[i+1];
                AppConstants.DEST_FOLDER = destFolder;
            }
        }
        
        Parallelize parallelizeParser = null;
        try {
            if(startUp()){
                String srcDIR = AppConstants.SRC_DIR; //Leave this. It helps in reducing Class unload cache. Talk to Sankar before u remove this
                parallelizeParser = new Parallelize(srcDIR, AppConstants.SRC_LOG_FILE_EXT);
                parallelizeParser.RunParserParallely();
                
                try {
                    shutDown(parallelizeParser);
                } catch (Exception e) {
                    System.out.println("Improper shutdown of the application");
                    e.printStackTrace();
                }
            } else {
                System.out.println("Start up of the application failed");
            }
            
        } catch (Throwable e) {
            e.printStackTrace();
            if(parallelizeParser != null){
                try {
                    parallelizeParser.shutDown();
                } catch (Exception ex) {
                    System.out.println("Unable to shutDown LogParser");
                    ex.printStackTrace();
                }
            }
            
        }
    }
    
    /**
     * Makes sure the required folder setup for the source and destination file exist. If destination file absent, this creates it
     * 
     * @return true  - if start up is successful <br> 
     *         false - if start up fails. Application should terminate if false
     * 
     * @throws java.lang.Exception 
     */
    public static boolean startUp() throws Exception{
        boolean out = false;
        File destinationFolder = new File(AppConstants.DEST_FOLDER);
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        try {
            if(destinationFolder.exists()){
                int destFileCount = 0;
                for(File file : destinationFolder.listFiles()) {
                	if(file.getName().endsWith(AppConstants.OUTPUT_FILE_EXT)) {
                		destFileCount++;
                	}
                }
                
                if(destFileCount > 0){
                    System.out.println("Destination folder \"" + AppConstants.DEST_FOLDER + "\" already has prior run files. \nWould you like to append the new run? \nY --> Continue\nN --> Abort");
                    String input = null;
                    while((input = br.readLine()) != null){
                        if("Y".equalsIgnoreCase(input)){
                            out = true;
                            break;
                        } else if ("N".equalsIgnoreCase(input)){
                            out = false;
                            break;
                        } else {
                            System.out.println("What did you say??? Enter (Y/N)");
                        }
                    }
                } else {
                    out = true;
                }
            } else {
                destinationFolder.mkdirs();
                out = true;
            }
        } catch (Exception e) {
            throw e;
        } finally {
            br.close();
        }
        return out;
    }
    
     public static void shutDown(Parallelize _paralleliser) throws Exception{
        _paralleliser.shutDown();
    }
    
    
    /*
     * ##############################################################################
     *   All the below are back-up methods to are TO-BE deleted when decided unfit
     * ##############################################################################
     */
    
    /**
     * @deprecated 
     * @param files list of files
     */
    public static void showFiles(File[] files) {
        for (File file : files) {
            if (file.isDirectory()) {
                System.out.println("Directory: " + file.getName());
                showFiles(file.listFiles()); // Calls same method again.
            } else {
                System.out.println("File: " + file.getName());
            }
        }
    }
    
}