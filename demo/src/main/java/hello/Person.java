package hello;

import net.sf.jsefa.csv.annotation.CsvDataType;
import net.sf.jsefa.csv.annotation.CsvField;
import net.sf.jsefa.xml.annotation.XmlDataType;
import net.sf.jsefa.xml.annotation.XmlElement;

@CsvDataType()
@XmlDataType(defaultElementName = "person")
public class Person {
	@CsvField(pos = 1)
	@XmlElement(pos = 1)
	String name;
	
	@CsvField(pos = 2)
	@XmlElement(name = "last-name", pos = 2)
	String surname;

	public String getFullName() {
		return name+" "+surname;
	}

	@Override
	public String toString() {
		return "Person [name=" + name + ", surname=" + surname + "]";
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSurname() {
		return surname;
	}

	public void setSurname(String surname) {
		this.surname = surname;
	}
}
