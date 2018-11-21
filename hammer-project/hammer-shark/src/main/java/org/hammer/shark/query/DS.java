package org.hammer.shark.query;

import java.io.Serializable;

public class DS implements Serializable {

	private String _id;
	
	private String url;
	
	private String dataset_type;
	
	private String datainput_type;
	
	private String id;
	
	private Float rm;
	
	private Float krm;
	
	private String keywords;

	public String get_id() {
		return _id;
	}

	public void set_id(String _id) {
		this._id = _id;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getDataset_type() {
		return dataset_type;
	}

	public void setDataset_type(String dataset_type) {
		this.dataset_type = dataset_type;
	}

	public String getDatainput_type() {
		return datainput_type;
	}

	public void setDatainput_type(String datainput_type) {
		this.datainput_type = datainput_type;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Float getRm() {
		return rm;
	}

	public void setRm(Float rm) {
		this.rm = rm;
	}

	public Float getKrm() {
		return krm;
	}

	public void setKrm(Float krm) {
		this.krm = krm;
	}

	public String getKeywords() {
		return keywords;
	}

	public void setKeywords(String keywords) {
		this.keywords = keywords;
	}
}
