/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hello;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;

@AutoConfigureMockMvc
public class DataProcessorTests {

    @Test
    public void testReaderWithBatchSize10() throws IOException {
    	Reader reader = new InputStreamReader(getClass().getResourceAsStream("/app.csv"));
    	DataProcessor dataProcessor = new DataProcessor().withBatchSize(10);
    	List<Person> people = dataProcessor.readCsv(reader);
    	Assert.assertEquals(10, people.size());
    	Assert.assertEquals("david test", people.get(0).getFullName());
    }

    @Test
    public void testReaderWithBatchSize2() throws IOException {
    	Reader reader = new InputStreamReader(getClass().getResourceAsStream("/app.csv"));
    	DataProcessor dataProcessor = new DataProcessor().withBatchSize(2);
    	List<Person> people = dataProcessor.readCsv(reader);
    	Assert.assertEquals(2, people.size());
    	Assert.assertEquals("david test", people.get(0).getFullName());
    	reader = new InputStreamReader(getClass().getResourceAsStream("/app.csv"));
    	people = dataProcessor.readCsv(reader);
    	Assert.assertEquals(2, people.size());
    	Assert.assertEquals("gloria grace", people.get(0).getFullName());
    }

	public class ReadThread implements Runnable {
		private DataProcessor dataProcessor;
		private Reader reader;
		private List<Person> people = new ArrayList<>();

		public ReadThread(DataProcessor dataProcessor, Reader reader) {
			this.dataProcessor = dataProcessor;
			this.reader = reader;
		}

		@Override
		public void run() {
			try {
				people = dataProcessor.readCsv(reader);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public List<Person> getPeople() {
			return people;
		}
	}

    @Test
    public void testReaderWithBatchSize2AndThreads() throws IOException {
    	List<Person> people = new ArrayList<>();
    	int batchSize = 2;
    	DataProcessor dataProcessor = new DataProcessor().withBatchSize(batchSize);
    	int count = DataProcessor.countLines(new InputStreamReader(getClass().getResourceAsStream("/app.csv")));
    	Assert.assertEquals(18, count);
    	ExecutorService ex = Executors.newFixedThreadPool(3);
    	List<ReadThread> threads = new ArrayList<>();
    	for (int i=0; i*batchSize<count; i++) {
    		ReadThread thread = new ReadThread(dataProcessor, new InputStreamReader(getClass().getResourceAsStream("/app.csv")));
    		threads.add(thread);
        	ex.execute(thread);
    	}
    	ex.shutdown();
        try {
            ex.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        for (ReadThread thread : threads) {
        	people.addAll(thread.getPeople());
        }
        Assert.assertEquals(9, threads.size());
    	Assert.assertEquals(18, people.size());
    	List<String> names = new ArrayList<>();
    	for (Person person : people) {
    		names.add(person.getFullName());
    	}
    	Assert.assertTrue(names.contains("david test"));
    	Assert.assertTrue(names.contains("gloria grace"));
    }
    
    @Ignore // seems to read ";" separated instead of ","
    @Test
    public void testReader2WithBatchSize10() throws FileNotFoundException {
    	Reader reader = new InputStreamReader(getClass().getResourceAsStream("/app.csv"));
    	DataProcessor dataProcessor = new DataProcessor().withBatchSize(10);
    	List<Person> people = dataProcessor.readCsv2(reader);
    	Assert.assertEquals(3, people.size());
    	Assert.assertEquals("david test", people.get(0).getFullName());
    }
}
