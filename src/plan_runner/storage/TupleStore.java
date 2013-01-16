package plan_runner.storage;

import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.map.hash.TObjectLongHashMap;

import java.io.Serializable;
import java.util.LinkedList;


/*
 * store which also keeps the multiplicity and a reverse storage
 * for finding tuples based on tuple string, not id
 */

public class TupleStore implements Serializable {

	private static final long serialVersionUID = 1L;

	private TIntObjectHashMap<String> _storage;
	private int _lastId;
	
	//needed for tuple deletions
	private TObjectIntHashMap<String> _reverseStorage;
	private TObjectLongHashMap<String> _multiplicityStorage;
	private LinkedList<Integer> _availableIds;

	
	public TupleStore()
	{
		_storage = new TIntObjectHashMap<String>();
		_lastId = -1;
		_reverseStorage = new TObjectIntHashMap<String>();
		_multiplicityStorage = new TObjectLongHashMap<String>();
		_availableIds = new LinkedList<Integer>();
	}
	
	public void copy(TupleStore t){
		_storage.putAll(t._storage);
		_reverseStorage.putAll(t._reverseStorage);
		_multiplicityStorage.putAll(t._multiplicityStorage);
		for (Integer i : t._availableIds)
			if (!_availableIds.contains(i))
				_availableIds.add(i);
		_lastId = t._lastId;
	}
	
	
	public int insert(String tuple)
	{
		if (_availableIds.size() != 0) {
			int id = _availableIds.remove();
			_storage.put(id, tuple);
			return id;
		}
		_lastId ++;
		_storage.put(_lastId, tuple);
		return _lastId;
	}
	
	public int insert(String tuple, Long tupleMultiplicity)
	{
		Integer id = _reverseStorage.get(tuple);
		String tupleFromStorage = _storage.get(id);
		if ((id == 0 && tupleFromStorage != null && tupleFromStorage.equals(tuple)) || id != 0) {
			Long multiplicity = _multiplicityStorage.get(tuple) + tupleMultiplicity; 
			//System.out.println("here" + tuple + " "+ multiplicity + " " + tupleMultiplicity);
			if (multiplicity != 0) 
				_multiplicityStorage.put(tuple, multiplicity);
			else { 
				removeTuple(tuple);
				return -id - 1;
			}
			return id;
		}
		if (_availableIds.size() != 0) {
		
			id = _availableIds.remove();
			_storage.put(id, tuple);
			_reverseStorage.put(tuple, id);
			_multiplicityStorage.put(tuple, tupleMultiplicity);
			return id;
		}
		_lastId ++;
		_storage.put(_lastId, tuple);
		_reverseStorage.put(tuple, _lastId);
		_multiplicityStorage.put(tuple, tupleMultiplicity);
		return _lastId;
	}
	
/*	public int updateMultiplicity(String tuple, Long tupleMultiplicity) {
		Integer id = _reverseStorage.get(tuple);
		if (id == null)
			return insert(tuple, tupleMultiplicity);
		Long multiplicity = _multiplicityStorage.get(tuple); 
		_multiplicityStorage.put(tuple, tupleMultiplicity + multiplicity);
		return id;
	} */
	
	public Long getMultiplicityForTuple(String tuple) {
		return _multiplicityStorage.get(tuple);
	}
	
	public Long getMultiplicity(int id) {
		String tuple = _storage.get(id);
		return _multiplicityStorage.get(tuple);
	}
	
	public String get(int id)
	{
		return _storage.get(id);
	}
	
	public int getId(String tuple){
		
		return _reverseStorage.get(tuple);
	}
	
	public int removeTuple(String tuple) {
		int id = _reverseStorage.remove(tuple);
		_availableIds.add(id);
		_storage.remove(id);
		_multiplicityStorage.remove(tuple);
		return id;
	}
	
	public String removeId(int id){
		String tuple = _storage.remove(id);
		_reverseStorage.remove(tuple);
		_multiplicityStorage.remove(tuple);
		_availableIds.add(id);
		return tuple;
	}
	
	public long size()
	{
		return _storage.size();
	}
	
	public void clear()
	{
		_lastId = -1;
		_storage.clear();
		_availableIds.clear();
		_reverseStorage.clear();
		_multiplicityStorage.clear();
	}
	
	public String toString(){
		return _storage.toString();
	}
	public String toStringReverse(){
		return _reverseStorage.toString();
	}
	
	public String toStringMultiplicity(){
		return _multiplicityStorage.toString();
	}
	
	public int getLastId() {
		return _lastId;
	}
}


