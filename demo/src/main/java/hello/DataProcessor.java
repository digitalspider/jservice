package hello;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;

import net.sf.jsefa.Deserializer;
import net.sf.jsefa.Serializer;
import net.sf.jsefa.csv.CsvIOFactory;

public class DataProcessor {
	private static Logger LOG = Logger.getLogger(DataProcessor.class);
	
	private int batchSize = 50;
	private AtomicInteger batchReadCounter = new AtomicInteger();
	
	public DataProcessor withBatchSize(int batchSize) {
		setBatchSize(batchSize);
		return this;
	}
	
	 /**
     * Read CSV files using OpenCSV
     * 
     * @param reader
     * @return
	 * @throws IOException 
     */
    public List<Person> readCsv(Reader reader) throws IOException {
		List<Person> people = new ArrayList<>();
		CSVReader csvReader = null;
        try {
        	csvReader = new CSVReader(reader, CSVParser.DEFAULT_SEPARATOR, CSVParser.DEFAULT_QUOTE_CHARACTER, batchSize*batchReadCounter.getAndIncrement());
            String[] line;
            while ((line = csvReader.readNext()) != null) {
            	if (csvReader.getRecordsRead()>batchSize) {
            		break;
            	}
            	Person person = new Person();
            	if (line.length>1) {
	            	person.setName(line[0]);
	            	person.setSurname(line[1]);
	            	people.add(person);
            	} else{
            		LOG.warn("Input line invalid: "+line);
            	}

            }
        } finally {
        	if (csvReader!=null) {
        		csvReader.close();
        	}
        }
        return people;
    }
    
    public static int countLines(Reader reader) throws IOException {
    	LineNumberReader lnr = new LineNumberReader(reader);
    	int result = 0;
    	try {
			lnr.skip(Long.MAX_VALUE);
	    	result = lnr.getLineNumber() + 1; //Add 1 because line index starts at 0
	    	
		} finally {
			lnr.close();
		}
    	return result;
    }
    
    /**
     * Read CSV files using JSEFA
     * 
     * @param reader
     * @return
     * @throws FileNotFoundException
     */
	public synchronized List<Person> readCsv2(Reader reader) throws FileNotFoundException {
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

	public synchronized void writeCsv(Writer writer, List<Person> people) {
		Serializer serializer = null;
		try {
			serializer = CsvIOFactory.createFactory(Person.class).createSerializer();
	    	serializer.open(writer);
	    	for (Person person : people) {
	    		serializer.write(person);
	    	}
		} finally {
			if (serializer!=null) {
				serializer.close(true);
			}
		}
	}
	
	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}
}
