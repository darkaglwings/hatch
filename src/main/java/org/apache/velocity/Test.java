package org.apache.velocity;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.velocity.app.VelocityEngine;

public class Test {
	
	public void init() {
		VelocityEngine ve = new VelocityEngine();
        ve.init();
        
        Map<String, Integer> map =  new HashMap<String, Integer>();
        map.put("id", 2);
        
        VelocityContext context = new VelocityContext();
        context.put("map", map);
 
        /*
         *   get the Template  
         */
 
        Template t = ve.getTemplate( "./src/main/resources/org/apache/velocity/test.xml" );
        
        String info = "#set($result=$map.get(\"id\") == 2)";
        
        /*
         *  now render the template into a Writer, here 
         *  a StringWriter 
         */
 
        StringWriter writer = new StringWriter();

        t.merge(context, writer);
        //ve.evaluate(context, writer, "", info);
 
        /*
         *  use the output in the body of your emails
         */
 
        System.out.println( writer.toString() );
        System.out.println(context.get("result"));
	}
	
	public static void main(String[] args) {
		new Test().init();
	}

}
