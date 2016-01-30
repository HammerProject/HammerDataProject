package org.hammer.isabella.query;


import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.SortedMap;


/**
 * A name/value pair
 * 
 * 
 * @author Mauro Pelucchi mauro.pelucchi@gmail.com
 *
 */
public class AttributeValue implements IDataType {

	/**
	 * Formatter
	 */
	@SuppressWarnings("unused")
	private static SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");

	
	/**
	 * Name
	 */
	private String name;
	
	/**
	 * String value
	 */
	private String literalValue;
	
	/**
	 * Number value
	 */
	private Integer numberValue;
	
	/**
	 * Float value
	 */
	private Float floatValue;
	
	/**
	 * Date value
	 */
	private Date dateValue;
	
	/**
	 * @return the dateValue
	 */
	public Date getDateValue() {
		return dateValue;
	}

	/**
	 * @param dateValue the dateValue to set
	 */
	public void setDateValue(Date dateValue) {
		this.dateValue = dateValue;
	}

	/**
	 * @return the literalValue
	 */
	public String getLiteralValue() {
		return literalValue;
	}

	/**
	 * @param literalValue the literalValue to set
	 */
	public void setLiteralValue(String literalValue) {
		this.literalValue = literalValue;
	}

	/**
	 * @return the numberValue
	 */
	public Integer getNumberValue() {
		return numberValue;
	}

	/**
	 * @param numberValue the numberValue to set
	 */
	public void setNumberValue(Integer numberValue) {
		this.numberValue = numberValue;
	}

	/**
	 * @return the floatValue
	 */
	public Float getFloatValue() {
		return floatValue;
	}

	/**
	 * @param floatValue the floatValue to set
	 */
	public void setFloatValue(Float floatValue) {
		this.floatValue = floatValue;
	}


	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = ((name!=null) ? name.toLowerCase() : "");
	}



	/*
	 * (non-Javadoc)
	 * @see biz.jsdi.model.IDataType#eval()
	 */
	@Override
	public void eval(SortedMap<Integer, IsabellaError> errorList, int line, int column) {
		if(name == null || name.trim().length() <= 0) {
			errorList.put((line * 1000) + 1, new IsabellaError(line, column, "Name = null or not set"));
		}
		if(name != null && name.equals("label1") && ((literalValue == null) || (literalValue.trim().length() <= 0) )) {
			errorList.put((line * 1000) + 1, new IsabellaError(line, column, "Label1 is null or blank; es. \"label1\" : \"xxxxx\""));
		}
		if(name != null && name.equals("label2") && ((literalValue == null) || (literalValue.trim().length() <= 0) )) {
			errorList.put((line * 1000) + 1, new IsabellaError(line, column, "Label2 is null or blank; es. \"label2\" : \"xxxxx\""));
		}
			
		if(name != null && name.equals("instance1") && ((literalValue == null) || (literalValue.trim().length() <= 0) )) {
			errorList.put((line * 1000) + 1, new IsabellaError(line, column, "Instance1 is null or blank; es. \"instance1\" : \"xxxxx\""));
		}
		if(name != null && name.equals("instance2") && ((literalValue == null) || (literalValue.trim().length() <= 0) )) {
			errorList.put((line * 1000) + 1, new IsabellaError(line, column, "Instance2 is null or blank; es. \"instance2\" : \"xxxxx\""));
		}
		if(name != null && name.equals("operator") && ((literalValue == null) || (literalValue.trim().length() <= 0) )) {
			errorList.put((line * 1000) + 1, new IsabellaError(line, column, "Operator is null or blank; es. \"operator\" : \"gt|ge|eq|lt|le|ne\""));
		}
		if(name != null && name.equals("operator") && ((literalValue != null) || (literalValue.trim().length() > 0) )) {
			try {
				RelationalOperator.valueOf(literalValue);
			} catch (IllegalArgumentException ex) {
				errorList.put((line * 1000) + 1, new IsabellaError(line, column, "Operator " + literalValue + " not defined!!! Use \"operator\" : \"gt|ge|eq|lt|le|ne\""));				
			}
		}
		if(name != null && name.equals("logicalOperator") && ((literalValue == null) || (literalValue.trim().length() <= 0) )) {
			errorList.put((line * 1000) + 1, new IsabellaError(line, column, "Logical Operator is null or blank; es. \"logicalOperator\" : \"or|and\""));
		}
		if(name != null && name.equals("logicalOperator") && ((literalValue != null) || (literalValue.trim().length() > 0) )) {
			try {
				LogicalOperator.valueOf(literalValue);
			} catch (IllegalArgumentException ex) {
				errorList.put((line * 1000) + 1, new IsabellaError(line, column, "Logical Operator " + literalValue + " not defined!!! Use \"logicalOperator\" : \"or|and\""));				
			}
		}

		if(name != null && name.equals("value") && (
				((literalValue == null) || (literalValue.trim().length() <= 0) )) && 
				(numberValue == null) && 
				(floatValue == null) && 
				(dateValue == null)
				) {
			errorList.put((line * 1000) + 1, new IsabellaError(line, column, "Value is null or blank; es. \"value\" : \"xxxxx\""));
		}
		
	}
	 
}
