package org.directmemory.misc;

public class Format {
	
	public static String it(String string, Object ... args) {
		java.util.Formatter formatter = new java.util.Formatter();
		return formatter.format(string, args).toString();
	}
	
	public static String logo() {
        return
"         ____  _                 __  __  ___\r\n" +                                
"        / __ \\(_)________  _____/ /_/  |/  /___  ____ ___  ____  _______  __\r\n" + 
"       / / / / // ___/ _ \\/ ___/ __/ /|_/ // _ \\/ __ `__ \\/ __ \\/ ___/ / / /\r\n" +
"      / /_/ / // /  /  __/ /__/ /_/ /  / //  __/ / / / / / /_/ / /  / /_/ / \r\n" +
"     /_____/_//_/   \\___/\\___/\\__/_/  /_/ \\___/_/ /_/ /_/\\____/_/   \\__, /\r\n" +
"                                                                   /____/   ";
	                                                               
//          return
//		  "  ___                  _ _ _\r\n" +                        
//		  " ( / \\ o           _/_( / ) )\r\n" +                       
//		  "  /  /,  _   _  _, /   / / / _  _ _ _   __ _   __  ,\r\n" + 
//		  "(/\\_/ (_/ (_(/_(__(__ / / (_(/_/ / / /_(_)/ (_/ (_/_\r\n" + 
//		  "                                                 /\r\n" +  
//		  "                                                '";   
	}

}
