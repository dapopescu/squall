package plan_runner.storm_components;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import plan_runner.components.Component;
import plan_runner.components.ComponentProperties;
import plan_runner.conversion.TypeConversion;
import plan_runner.expressions.ValueExpression;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.ChainOperator;
import plan_runner.operators.Operator;
import plan_runner.operators.ProjectOperator;
import plan_runner.storage.AggregationStorage;
import plan_runner.storage.BasicStore;
import plan_runner.storage.KeyValueStore;
import plan_runner.storm_components.synchronization.TopologyKiller;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.PeriodicBatchSend;
import plan_runner.utilities.SystemParameters;
import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class StormDstJoin extends BaseRichBolt implements StormJoin, StormComponent {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(StormDstJoin.class);

	private int _hierarchyPosition=INTERMEDIATE;

	private StormEmitter _firstEmitter, _secondEmitter;
	private BasicStore<ArrayList<String>> _firstSquallStorage, _secondSquallStorage;
	private ProjectOperator _firstPreAggProj, _secondPreAggProj;
	private String _ID;
	private List<String> _compIds; // a sorted list of all the components
	private String _componentIndex; //a unique index in a list of all the components
	//used as a shorter name, to save some network traffic
	//it's of type int, but we use String to save more space
	private String _firstEmitterIndex, _secondEmitterIndex;

	private int _numSentTuples=0;
	private boolean _printOut;

	private ChainOperator _operatorChain;
	private OutputCollector _collector;
	private Map _conf;

	//output has hash formed out of these indexes
	private List<Integer> _hashIndexes;
	private List<ValueExpression> _hashExpressions;
	private List<Integer> _rightHashIndexes; //hash indexes from the right parent

	//for load-balancing
	private List<String> _fullHashList;

	//for No ACK: the total number of tasks of all the parent components
	private int _numRemainingParents;

	//for batch sending
	private final Semaphore _semAgg = new Semaphore(1, true);
	private boolean _firstTime = true;
	private PeriodicBatchSend _periodicBatch;
	private long _batchOutputMillis;
	
	//keep current state for result of nested query
	private AggregationStorage _firstNestedQueryStorage;
	private AggregationStorage _secondNestedQueryStorage;
	//keep pair tuple - current state for nested query
	private KeyValueStore<String, String> _firstCorrespondenceStorage;
	private KeyValueStore<String, String> _secondCorrespondenceStorage;
	private boolean _isFirstEmitterNestedQuery = false;
	private boolean _isSecondEmitterNestedQuery = false;
	private boolean _firstNestedQueryHasGroupBy = false;
	private boolean _secondNestedQueryHasGroupBy = false;
	private BasicStore<ArrayList<List<String>>> _previousAggResult; 
		
	public StormDstJoin(StormEmitter firstEmitter,
			StormEmitter secondEmitter,
			ComponentProperties cp,
			List<String> allCompNames,
			BasicStore<ArrayList<String>> firstPreAggStorage,
			BasicStore<ArrayList<String>> secondPreAggStorage,
			ProjectOperator firstPreAggProj,
			ProjectOperator secondPreAggProj,
			int hierarchyPosition,
			TopologyBuilder builder,
			TopologyKiller killer,
			Config conf) {
		_conf = conf;
		_firstEmitter = firstEmitter;
		_secondEmitter = secondEmitter;
		_ID = cp.getName();
		_componentIndex = String.valueOf(allCompNames.indexOf(_ID));
		_firstEmitterIndex = String.valueOf(allCompNames.indexOf(_firstEmitter.getName()));
		_secondEmitterIndex = String.valueOf(allCompNames.indexOf(_secondEmitter.getName()));
		_batchOutputMillis = cp.getBatchOutputMillis();

		int parallelism = SystemParameters.getInt(conf, _ID+"_PAR");

		//            if(parallelism > 1 && distinct != null){
		//                throw new RuntimeException(_componentName + ": Distinct operator cannot be specified for multiThreaded bolts!");
		//            }

		_operatorChain = cp.getChainOperator();

		_hashIndexes = cp.getHashIndexes();
		_hashExpressions = cp.getHashExpressions();
		_rightHashIndexes = cp.getParents()[1].getHashIndexes();

		_hierarchyPosition = hierarchyPosition;

		_fullHashList = cp.getFullHashList();
		
		_previousAggResult = new KeyValueStore<String, List<String>>(_conf);
		
		InputDeclarer currentBolt = builder.setBolt(_ID, this, parallelism);
		
		// Grouping and initialize necessary storages
		Component[] parents = cp.getParents();
		AggregateOperator aggOpFirstEmitter = parents[0].getChainOperator().getAggregation();
		AggregateOperator aggOpSecondEmitter = parents[1].getChainOperator().getAggregation();
		if (aggOpFirstEmitter != null) {
			if (aggOpFirstEmitter.hasGroupBy()) {
				currentBolt = MyUtilities.attachEmitterCustom(conf, null, currentBolt, firstEmitter, secondEmitter);
				_firstNestedQueryHasGroupBy = true;
			}
			else {
				currentBolt = MyUtilities.attachEmitterAllGrouping(conf, currentBolt, firstEmitter);
				currentBolt = MyUtilities.attachEmitterCustom(conf, _fullHashList, currentBolt, secondEmitter);
			}
		
			_firstNestedQueryStorage = MyUtilities.createAggregationStorage(parents[0].getChainOperator().getAggregation(), conf); 
			_firstCorrespondenceStorage = new KeyValueStore<String, String>(_conf);
			_isFirstEmitterNestedQuery = true;
		}
		else if (aggOpSecondEmitter != null) {
			if (aggOpSecondEmitter.hasGroupBy()) {
				currentBolt = MyUtilities.attachEmitterCustom(conf, null, currentBolt, firstEmitter, secondEmitter);
				_secondNestedQueryHasGroupBy = true;
			}
			else {
				currentBolt = MyUtilities.attachEmitterAllGrouping(conf, currentBolt, secondEmitter);
				currentBolt = MyUtilities.attachEmitterCustom(conf, _fullHashList, currentBolt, firstEmitter);
			}
			
			_secondNestedQueryStorage = MyUtilities.createAggregationStorage(parents[1].getChainOperator().getAggregation(), conf); 
			_secondCorrespondenceStorage = new KeyValueStore<String, String>(_conf);
			_isSecondEmitterNestedQuery = true;
		}
		else {
			currentBolt = MyUtilities.attachEmitterCustom(conf, _fullHashList, currentBolt, firstEmitter, secondEmitter);
		}

		if( _hierarchyPosition == FINAL_COMPONENT && (!MyUtilities.isAckEveryTuple(conf))){
			killer.registerComponent(this, parallelism);
		}

		_printOut = cp.getPrintOut();
		if (_printOut && _operatorChain.isBlocking()){
			currentBolt.allGrouping(killer.getID(), SystemParameters.DUMP_RESULTS_STREAM);
		}

		_firstSquallStorage = firstPreAggStorage;
		_secondSquallStorage = secondPreAggStorage;

		_firstPreAggProj = firstPreAggProj;
		_secondPreAggProj = secondPreAggProj;
	}

	@Override
		public void execute(Tuple stormTupleRcv) {
			if(_firstTime && MyUtilities.isBatchOutputMode(_batchOutputMillis)){
				_periodicBatch = new PeriodicBatchSend(_batchOutputMillis, this);
				_firstTime = false;
			}

			if (receivedDumpSignal(stormTupleRcv)) {
				MyUtilities.dumpSignal(this, stormTupleRcv, _collector);
				return;
			}
			
			String inputComponentIndex=stormTupleRcv.getString(0);
			List<String> tuple = (List<String>) stormTupleRcv.getValue(1);
			String inputTupleHash=stormTupleRcv.getString(2);
			Long inputTupleMultiplicity = stormTupleRcv.getLongByField("Multiplicity");
			String inputTupleString = MyUtilities.tupleToString(tuple, _conf);
			
			if(MyUtilities.isFinalAck(tuple, _conf)){
				_numRemainingParents--;
				MyUtilities.processFinalAck(_numRemainingParents, _hierarchyPosition, stormTupleRcv, _collector, _periodicBatch);
				return;
			}

			boolean isFromFirstEmitter = false;
			BasicStore<ArrayList<String>> affectedStorage, oppositeStorage;
			AggregationStorage affectedNestedQueryStorage;
			KeyValueStore<String, String> affectedCorrespondenceStorage;
			ProjectOperator projPreAgg;
			if(_firstEmitterIndex.equals(inputComponentIndex)){
				//R update
				isFromFirstEmitter = true;
				affectedStorage = _firstSquallStorage;
				oppositeStorage = _secondSquallStorage;
				affectedNestedQueryStorage = _firstNestedQueryStorage;
				affectedCorrespondenceStorage = _firstCorrespondenceStorage;
				projPreAgg = _secondPreAggProj;
			}else if(_secondEmitterIndex.equals(inputComponentIndex)){
				//S update
				isFromFirstEmitter = false;
				affectedStorage = _secondSquallStorage;
				oppositeStorage = _firstSquallStorage;
				affectedNestedQueryStorage = _secondNestedQueryStorage;
				affectedCorrespondenceStorage = _secondCorrespondenceStorage;
				projPreAgg = _firstPreAggProj;
			}else{
				throw new RuntimeException("InputComponentName " + inputComponentIndex +
						" doesn't match neither " + _firstEmitterIndex + " nor " + _secondEmitterIndex + ".");
			}
		
			if (_isFirstEmitterNestedQuery && isFromFirstEmitter) {
				tuple = addTupleToStorageNestedQuery(affectedStorage,
						affectedNestedQueryStorage, 
						affectedCorrespondenceStorage,
						tuple, 
						inputTupleString,
						inputTupleMultiplicity,
						inputTupleHash);
				if (!_firstNestedQueryHasGroupBy)
					inputTupleHash = tuple.get(tuple.size() - 1);
			}
			else if (_isSecondEmitterNestedQuery && !isFromFirstEmitter) {
				tuple = addTupleToStorageNestedQuery(affectedStorage,
						affectedNestedQueryStorage, 
						affectedCorrespondenceStorage,
						tuple, 
						inputTupleString,
						inputTupleMultiplicity,
						inputTupleHash);
				if (!_secondNestedQueryHasGroupBy)
					inputTupleHash = tuple.get(tuple.size() - 1);
				
			}
			else { 
				addTupleToStorage(affectedStorage, inputTupleHash, inputTupleString, inputTupleMultiplicity);
			}
			
			performJoin(stormTupleRcv,
					tuple,
					inputTupleHash,
					isFromFirstEmitter,
					oppositeStorage,
					projPreAgg);
			
			_collector.ack(stormTupleRcv);
		}

	protected void applyOperatorsAndSend(Tuple stormTupleRcv, List<String> tuple){
		if(MyUtilities.isBatchOutputMode(_batchOutputMillis)){
			try {
				_semAgg.acquire();
			} catch (InterruptedException ex) {}
		}
		tuple = _operatorChain.process(tuple);
		if(MyUtilities.isBatchOutputMode(_batchOutputMillis)){
			_semAgg.release();
		}

		if(tuple == null){
			return;
		}
		_numSentTuples++;
		printTuple(tuple);

		if(MyUtilities.isSending(_hierarchyPosition, _batchOutputMillis)){
			tupleSend(tuple, stormTupleRcv);
		}
	}

	@Override
		public void tupleSend(List<String> tuple, Tuple stormTupleRcv) {
			Values stormTupleSnd = MyUtilities.createTupleValues(tuple, _componentIndex,
					_hashIndexes, _hashExpressions, _conf);
			MyUtilities.sendTuple(stormTupleSnd, stormTupleRcv, _collector, _conf);
		}

	@Override
		public void batchSend(){
			if(MyUtilities.isBatchOutputMode(_batchOutputMillis)){
				if (_operatorChain != null){
					Operator lastOperator = _operatorChain.getLastOperator();
					if(lastOperator instanceof AggregateOperator){
						try {
							_semAgg.acquire();
						} catch (InterruptedException ex) {}

						//sending
						AggregateOperator agg = (AggregateOperator) lastOperator;
						List<String> tuples = agg.getContent();
						if (tuples != null) {
                            String columnDelimiter = MyUtilities.getColumnDelimiter(_conf);
							for(String tuple: tuples){
								String tupleHash = tuple.split(" = ")[0];
								tuple = tuple.replaceAll(" = ", columnDelimiter);
								//if we have nested query, send the previous aggregation result and update it for next time
								if (_hierarchyPosition != FINAL_COMPONENT && _operatorChain.getAggregation() != null){
									ArrayList<List<String>> values = _previousAggResult.access(tupleHash);
									if (values != null) {
										List<String> previousAggResult = values.get(0);
										tupleSend(previousAggResult, null, -1L);	
										_previousAggResult.update(tupleHash, previousAggResult, MyUtilities.stringToTuple(tuple, _conf));
									}
									else _previousAggResult.insert(tupleHash, MyUtilities.stringToTuple(tuple, _conf));
								} 
								//send the current aggregate
								tupleSend(MyUtilities.stringToTuple(tuple, _conf), null, 1L);
							}
						}
						
						//clearing only if we don't have nested query
						if (!(_hierarchyPosition != FINAL_COMPONENT && _operatorChain.getAggregation() != null))
							agg.clearStorage();

						_semAgg.release();
					}
				}
			}
		}

	// from IRichBolt
	@Override
		public void cleanup() {

		}

	@Override
		public Map<String,Object> getComponentConfiguration(){
			return _conf;
		}

	@Override
		public void prepare(Map map, TopologyContext tc, OutputCollector collector) {
			_collector=collector;
			_numRemainingParents = MyUtilities.getNumParentTasks(tc, _firstEmitter, _secondEmitter);
		}

	@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			if(_hierarchyPosition!=FINAL_COMPONENT){ // then its an intermediate stage not the final one
				declarer.declareStream(SystemParameters.DATA_STREAM, new Fields(MyUtilities.createDeclarerOutputFields ()));
			
			}else{
				if(!MyUtilities.isAckEveryTuple(_conf)){
					declarer.declareStream(SystemParameters.EOF_STREAM, new Fields(SystemParameters.EOF));
				}
			}
		}

	@Override
		public void printTuple(List<String> tuple){
			if(_printOut){
				if((_operatorChain == null) || !_operatorChain.isBlocking()){
					StringBuilder sb = new StringBuilder();
					sb.append("\nComponent ").append(_ID);
					sb.append("\nReceived tuples: ").append(_numSentTuples);
					sb.append(" Tuple: ").append(MyUtilities.tupleToString(tuple, _conf));
					LOG.info(sb.toString());
				}
			}
		}

	@Override
		public void printContent() {
			if(_printOut){
				if((_operatorChain!=null) && _operatorChain.isBlocking()){
					Operator lastOperator = _operatorChain.getLastOperator();
					if (lastOperator instanceof AggregateOperator){
						MyUtilities.printBlockingResult(_ID,
								(AggregateOperator) lastOperator,
								_hierarchyPosition,
								_conf,
								LOG);
					}else{
						MyUtilities.printBlockingResult(_ID,
								lastOperator.getNumTuplesProcessed(),
								lastOperator.printContent(),
								_hierarchyPosition,
								_conf,
								LOG);
					}
				}
			}
		}

	private boolean receivedDumpSignal(Tuple stormTuple) {
		return stormTuple.getSourceStreamId().equalsIgnoreCase(SystemParameters.DUMP_RESULTS_STREAM);
	}

	// from StormComponent interface
	@Override
		public String getID() {
			return _ID;
		}

	// from StormEmitter interface
	@Override
		public String[] getEmitterIDs() {
			return new String[]{_ID};
		}

	@Override
		public String getName() {
			return _ID;
		}

	@Override
		public String getInfoID() {
			String str = "DestinationStorage " + _ID + " has ID: " + _ID;
			return str;
		}
	
	protected void performJoin(Tuple stormTupleRcv,
			List<String> tuple,
			String inputTupleHash,
			boolean isFromFirstEmitter,
			BasicStore<ArrayList<String>> oppositeStorage,
			ProjectOperator projPreAgg){

		Long tupleMultiplicity = stormTupleRcv.getLongByField("Multiplicity");
		List<String> oppositeStringTupleList = oppositeStorage.access(inputTupleHash);
	
		if(oppositeStringTupleList!=null)
			for (int i = 0; i < oppositeStringTupleList.size(); i++) {
				String oppositeStringTuple= oppositeStringTupleList.get(i);
				Long oppositeTupleMultiplicity = MyUtilities.getMultiplicityFromTupleString(oppositeStringTuple);
				oppositeStringTuple = MyUtilities.getTupleString(oppositeStringTuple);
				List<String> oppositeTuple= MyUtilities.stringToTuple(oppositeStringTuple, getComponentConfiguration());
				
				List<String> firstTuple, secondTuple;
				if(isFromFirstEmitter){
					firstTuple = tuple;
					secondTuple = oppositeTuple;
				}else{
					firstTuple = oppositeTuple;
					secondTuple = tuple;
				}

				List<String> outputTuple;
				if(oppositeStorage instanceof BasicStore){
					outputTuple = MyUtilities.createOutputTuple(firstTuple, secondTuple, _rightHashIndexes);
				}else{
					outputTuple = MyUtilities.createOutputTuple(firstTuple, secondTuple);
				}

				if(projPreAgg != null){
					outputTuple = projPreAgg.process(outputTuple);
				}
				
				//multiplicity for output tuple
				Long multiplicityForOutputTuple = tupleMultiplicity * oppositeTupleMultiplicity;
				
				applyOperatorsAndSend(stormTupleRcv, outputTuple, multiplicityForOutputTuple);					
			}
	}
			
		
	protected void applyOperatorsAndSend(Tuple stormTupleRcv, List<String> tuple, Object... tupleInfo){
		if(MyUtilities.isBatchOutputMode(_batchOutputMillis)){
			try {
				_semAgg.acquire();
			} catch (InterruptedException ex) {}
		}
		
		tuple = _operatorChain.process(tuple, tupleInfo);

		if(MyUtilities.isBatchOutputMode(_batchOutputMillis)){
			_semAgg.release();
		}

		if(tuple == null){
			return;
		}

		_numSentTuples++;
		printTuple(tuple);
	        
		
		if(MyUtilities.isSending(_hierarchyPosition, _batchOutputMillis)){
			//if we have nested query, send the previous aggregation result and update it for next time
			if (_hierarchyPosition != FINAL_COMPONENT && _operatorChain.getAggregation() != null) {
        		String tupleHash = MyUtilities.tupleToString(tuple.subList(0, tuple.size() - 1), _conf);
    			ArrayList<List<String>> values = _previousAggResult.access(tupleHash);
    			if (values != null) {
    				List<String> previousAggResult = values.get(0);
    				tupleSend(previousAggResult, null, -1L);
    				_previousAggResult.update(tupleHash, previousAggResult, tuple);
    			}
    			else _previousAggResult.insert(tupleHash, tuple);
        	}
        		
			if (_operatorChain.getAggregation() != null || _operatorChain.getDistinct() != null)
				tupleInfo[0] = 1L;
			tupleSend(tuple, stormTupleRcv, tupleInfo);
		}
	}

	@Override
		public void tupleSend(List<String> tuple, Tuple stormTupleRcv, Object... tupleInfo) {
			Values stormTupleSnd = MyUtilities.createTupleValues(tuple, _componentIndex,
					_hashIndexes, _hashExpressions, _conf, tupleInfo);
			MyUtilities.sendTuple(stormTupleSnd, stormTupleRcv, _collector, _conf);
		}
	
	/*
	 * used in nested queries only
	 */
	
	private List<String> addTupleToStorageNestedQuery(BasicStore<ArrayList<String>> affectedStorage,
			AggregationStorage affectedNestedQueryStorage,
			KeyValueStore<String, String> affectedCorrespondenceStorage,
			List<String> tuple,
			String inputTupleString,
			Long inputTupleMultiplicity,
			String inputTupleHash) {

		String inputTupleAggHash = MyUtilities.tupleToString(tuple.subList(0, tuple.size() - 1), _conf);
		//add received value to current state
		affectedNestedQueryStorage.update(tuple, inputTupleAggHash, inputTupleMultiplicity);
		TypeConversion typeStorage = affectedNestedQueryStorage.getType();
		ArrayList tuplesFromStorage = affectedNestedQueryStorage.access(inputTupleAggHash);
		List<String> newTuple = new ArrayList<String>(tuple);
		
		if (tuplesFromStorage != null) {
			if (inputTupleMultiplicity > 0) {
				String currentAggValue = typeStorage.toString(tuplesFromStorage.get(0)); 
				affectedCorrespondenceStorage.insert(inputTupleString, currentAggValue);
				//modify tuple with current value for join
				newTuple.set(newTuple.size() - 1, currentAggValue);
				//add the new value to affected storage
				affectedStorage.remove(inputTupleHash);
				String tupleString = MyUtilities.tupleToString(newTuple, _conf);
				tupleString = MyUtilities.addMultiplicityToTupleString(tupleString, inputTupleMultiplicity);//mult == 1
				affectedStorage.insert(inputTupleHash,  tupleString);
			}
			else {
				ArrayList<String> values = affectedCorrespondenceStorage.access(inputTupleString);
				if (values == null)
					throw new RuntimeException("We should have a correspondence for this tuple " + inputTupleString);
				String removedValue = values.get(0);
				affectedCorrespondenceStorage.remove(inputTupleString, removedValue);
				//we need to modify the tuple for join
				newTuple.set(newTuple.size() - 1, removedValue);
				affectedStorage.remove(inputTupleHash);
			} 	
		}
		
		return newTuple;
	}
	
	/*
	 * add the newly arrived tuple to storage taking into account its multiplicity value
	 */
	
	private void addTupleToStorage(BasicStore<ArrayList<String>> affectedStorage,
			String inputTupleHash, String inputTupleString, Long inputTupleMultiplicity) {
			
		//add tuple to storage only if its multiplicity is positive
		if (inputTupleMultiplicity > 0)
		{
			ArrayList<String> tuplesFromStorage = affectedStorage.access(inputTupleHash);
			boolean found = false;
			if (tuplesFromStorage != null) {
				for (String tuple : tuplesFromStorage) {
					Long multiplicityForTupleFromStorage = MyUtilities.getMultiplicityFromTupleString(tuple);
					String tupleStringForTupleFromStorage = MyUtilities.getTupleString(tuple);
					
					if (tupleStringForTupleFromStorage.equals(inputTupleString)) {
						tupleStringForTupleFromStorage = MyUtilities.addMultiplicityToTupleString(tupleStringForTupleFromStorage,
								multiplicityForTupleFromStorage + inputTupleMultiplicity);
						affectedStorage.update(inputTupleHash, tuple, tupleStringForTupleFromStorage);
						found = true;
						break;
					}
				}
				if (!found){
					inputTupleString = MyUtilities.addMultiplicityToTupleString(inputTupleString, inputTupleMultiplicity);
					affectedStorage.insert(inputTupleHash, inputTupleString);
				}
			}
			else {
				inputTupleString = MyUtilities.addMultiplicityToTupleString(inputTupleString, inputTupleMultiplicity);
				affectedStorage.insert(inputTupleHash, inputTupleString);
				
			}
		} 
		else {
			ArrayList<String> tuplesFromStorage = affectedStorage.access(inputTupleHash);
			if (tuplesFromStorage != null) {
				for (String tuple : tuplesFromStorage) {
					Long multiplicityForTupleFromStorage = MyUtilities.getMultiplicityFromTupleString(tuple);
					String tupleStringForTupleFromStorage = MyUtilities.getTupleString(tuple);
					
					if (tupleStringForTupleFromStorage.equals(inputTupleString)) {
						if (Math.abs(inputTupleMultiplicity) == multiplicityForTupleFromStorage) 
							affectedStorage.remove(inputTupleHash, tuple);
						else if (Math.abs(inputTupleMultiplicity) < multiplicityForTupleFromStorage) {
								tupleStringForTupleFromStorage = MyUtilities.addMultiplicityToTupleString(tupleStringForTupleFromStorage,
									multiplicityForTupleFromStorage + inputTupleMultiplicity);
								affectedStorage.update(inputTupleHash, tuple, tupleStringForTupleFromStorage);
							}
							else 
								throw new RuntimeException("Received negative multiplicity for " +
									"tuple larger than the positive one in the storage!!!");
						break;
					}
				}		
			}
		}
	}

}
