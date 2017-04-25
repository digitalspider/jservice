package hello;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class GreetingController {
	private static Logger LOG = Logger.getLogger(GreetingController.class);
	
	private static final String PROPERTIES_FILENAME = "/app.properties";
	private static final String PROP_FILENAME = "filename";
	private static final String DEFAULT_CSV_APP_FILENAME = "/app.csv";
    
	private static final String TEMPLATE = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();
    private static final Properties properties = new Properties();

    @RequestMapping("/greeting")
    public Greeting greeting(@RequestParam(value="name", defaultValue="World") String name) {
        return new Greeting(counter.incrementAndGet(), String.format(TEMPLATE, name));
    }
    
    @RequestMapping("/greetall")
    public List<Greeting> greetAll() throws FileNotFoundException, IOException, Exception {
    	init();
    	String csvFilename = properties.getProperty(PROP_FILENAME, DEFAULT_CSV_APP_FILENAME);
    	
    	List<Greeting> result = new ArrayList<>();
    	DataProcessor dataProcessor = new DataProcessor().withBatchSize(5);

    	Reader reader = new InputStreamReader(getClass().getResourceAsStream(csvFilename));
    	List<Person> people = dataProcessor.readCsv(reader);
    	while (!people.isEmpty()) {
	    	for (Person person : people) {
	    		result.add(new Greeting(counter.incrementAndGet(), String.format(TEMPLATE, person.getFullName())));
	    	}
	    	reader = new InputStreamReader(getClass().getResourceAsStream(csvFilename));
	    	people = dataProcessor.readCsv(reader);
    	}
        return result;
    }

	private void init() throws IOException, FileNotFoundException, Exception {
		if (properties.isEmpty()) {
			InputStream is = getClass().getResourceAsStream(PROPERTIES_FILENAME);
			properties.load(new InputStreamReader(is));
	    	if (properties.isEmpty()) {
	    		throw new Exception("Could not read input file "+PROPERTIES_FILENAME);
	    	}
	    	LOG.info("props="+properties);
		}
	}
}
