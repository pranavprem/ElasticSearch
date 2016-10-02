package beans;

import java.util.HashMap;
import java.util.Map;

public class JustABean {

	private Map<String, Object> metaData;
	
	public void addToMap(String key, Object value){
		this.metaData.put(key, value);
		
	}
	public void removeFromMap(String key){
		this.metaData.remove(key);
	}
	
	
	public JustABean(){
		this.metaData=new HashMap<String, Object>();
	}
	public Map<String, Object> getMetaData() {
		return metaData;
	}
	public void setMetaData(Map<String, Object> metaData) {
		this.metaData = metaData;
	}
	
	
	@Override
	public String toString() {
		String returnString="";
		if(this.metaData !=null){
			for(Map.Entry<String, Object> entry : this.metaData.entrySet()){
				returnString+= entry.getKey()+" : "+entry.getValue()+"\n";
				
			}
		}
		return returnString;
	}
	
	
	
}
