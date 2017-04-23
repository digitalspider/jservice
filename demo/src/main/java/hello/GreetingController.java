package hello;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import net.sf.jsefa.Deserializer;
import net.sf.jsefa.Serializer;
import net.sf.jsefa.csv.CsvIOFactory;

@RestController
public class GreetingController {
	private Logger LOG = Logger.getLogger(GreetingController.class);

    private static final String PROPERTIES_FILENAME = "/app.properties";
	private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();
    private static final Properties properties = new Properties();

    @RequestMapping("/greeting")
    public Greeting greeting(@RequestParam(value="name", defaultValue="World") String name) {
        return new Greeting(counter.incrementAndGet(),
                            String.format(template, name));
    }
    
    @RequestMapping("/greetall")
    public List<Greeting> greetAll() throws FileNotFoundException, IOException, Exception {
    	init();
    	String csvFilename = properties.getProperty("filename", "app.properties2");
    	
    	List<Greeting> result = new ArrayList<>();
    	List<Person> people = readCsv(csvFilename);
    	for (Person person : people) {
    		result.add(new Greeting(counter.incrementAndGet(),
                    String.format(template, person.getFullName())));
    	}

        return result;
    }

	private List<Person> readCsv(String csvFilename) throws FileNotFoundException {
		List<Person> people = new ArrayList<>();
    	Deserializer deserializer = null;
    	try {
	    	deserializer = CsvIOFactory.createFactory(Person.class).createDeserializer();
	    	InputStreamReader reader = new InputStreamReader(getClass().getResourceAsStream(csvFilename));
	    	deserializer.open(reader);
	    	while (deserializer.hasNext()) {
	    	    Person person = deserializer.next();
	    	    people.add(person);
	    	}
    	} finally {
    		if (deserializer!=null) {
    			deserializer.close(true);
    		}
    	}
    	return people;
	}

	private void writeCsv(Writer writer) {
		Serializer serializer = null;
		try {
			serializer = CsvIOFactory.createFactory(Person.class).createSerializer();
	    	serializer.open(writer);
		} finally {
			if (serializer!=null) {
				serializer.close(true);
			}
		}
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
