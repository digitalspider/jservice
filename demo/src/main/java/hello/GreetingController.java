package hello;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.opencsv.CSVReader;

import net.sf.jsefa.Deserializer;
import net.sf.jsefa.Serializer;
import net.sf.jsefa.csv.CsvIOFactory;

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
    	Reader reader = new InputStreamReader(getClass().getResourceAsStream(csvFilename));
    	List<Person> people = readCsv(reader);
    	for (Person person : people) {
    		result.add(new Greeting(counter.incrementAndGet(), String.format(TEMPLATE, person.getFullName())));
    	}
        return result;
    }

    /**
     * Read CSV files using OpenCSV
     * 
     * @param reader
     * @return
     * @throws FileNotFoundException
     */
    public static List<Person> readCsv(Reader reader) throws FileNotFoundException {
		List<Person> people = new ArrayList<>();
    	CSVReader csvReader = null;
        try {
            csvReader = new CSVReader(reader);
            String[] line;
            while ((line = csvReader.readNext()) != null) {
            	Person person = new Person();
            	if (line.length>1) {
	            	person.setName(line[0]);
	            	person.setSurname(line[1]);
	            	people.add(person);
            	} else{
            		LOG.warn("Input line invalid: "+line);
            	}
            }
        } catch (IOException e) {
            LOG.error(e,e);
        }
        return people;
    }
    
    /**
     * Read CSV files using JSEFA
     * 
     * @param reader
     * @return
     * @throws FileNotFoundException
     */
	public static List<Person> readCsv2(Reader reader) throws FileNotFoundException {
		List<Person> people = new ArrayList<>();
    	Deserializer deserializer = null;
    	try {
	    	deserializer = CsvIOFactory.createFactory(Person.class).createDeserializer();
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

	public static void writeCsv(Writer writer) {
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
