package plan_runner.operators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import plan_runner.conversion.NumericConversion;
import plan_runner.conversion.SumCount;
import plan_runner.conversion.SumCountConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.ValueExpression;
import plan_runner.storage.AggregationStorage;
import plan_runner.storage.BasicStore;
import plan_runner.utilities.MyUtilities;
import plan_runner.visitors.OperatorVisitor;

public class AggregateAvgOperator implements AggregateOperator<SumCount> {
        private static final long serialVersionUID = 1L;
        private static Logger LOG = Logger.getLogger(AggregateAvgOperator.class);

        //the GroupBy type
        private static final int GB_UNSET = -1;
        private static final int GB_COLUMNS = 0;
        private static final int GB_PROJECTION = 1;

        private DistinctOperator _distinct;
        private int _groupByType = GB_UNSET;
        private List<Integer> _groupByColumns = new ArrayList<Integer>();
        private ProjectOperator _groupByProjection;
        private int _numTuplesProcessed = 0;
        
        private SumCountConversion _wrapper = new SumCountConversion();
        private ValueExpression _ve;
        private AggregationStorage<SumCount> _storage;

        private Map _map;

        public AggregateAvgOperator(ValueExpression ve, Map map){
            _ve=ve;
            _map=map;
            _storage = new AggregationStorage<SumCount>(this, _wrapper, _map, true);
        }

        //from AgregateOperator
        @Override
        public AggregateAvgOperator setGroupByColumns(List<Integer> groupByColumns) {
             if(!alreadySetOther(GB_COLUMNS)){
                _groupByType = GB_COLUMNS;
                _groupByColumns = groupByColumns;
                _storage.setSingleEntry(false);
                return this;
            }else{
                throw new RuntimeException("Aggragation already has groupBy set!");
            }
        }

        @Override
        public AggregateAvgOperator setGroupByProjection(ProjectOperator groupByProjection){
             if(!alreadySetOther(GB_PROJECTION)){
                _groupByType = GB_PROJECTION;
                _groupByProjection = groupByProjection;
                _storage.setSingleEntry(false);
                return this;
            }else{
                throw new RuntimeException("Aggragation already has groupBy set!");
            }
        }

        @Override
        public AggregateAvgOperator setDistinct(DistinctOperator distinct){
            _distinct = distinct;
            return this;
        }

        @Override
        public List<Integer> getGroupByColumns() {
            return _groupByColumns;
        }

        @Override
        public ProjectOperator getGroupByProjection(){
            return _groupByProjection;
        }

        @Override
        public DistinctOperator getDistinct() {
            return _distinct;
        }

        @Override
        public List<ValueExpression> getExpressions() {
            List<ValueExpression> result = new ArrayList<ValueExpression>();
            result.add(_ve);
            return result;
        }
        
        @Override
        public TypeConversion getType(){
            return _wrapper;
        }
        
        @Override
        public boolean hasGroupBy(){
            return _groupByType != GB_UNSET;
        }

        //from Operator
        @Override
        public List<String> process(List<String> tuple, Object... tupleInfo){
            _numTuplesProcessed++;
            if(_distinct != null){
                tuple = _distinct.process(tuple);
                if(tuple == null){
                    return null;
                }
            }
            String tupleHash;
            if(_groupByType == GB_PROJECTION){
                tupleHash = MyUtilities.createHashString(tuple, _groupByColumns, _groupByProjection.getExpressions(), _map);
            }else{
                tupleHash = MyUtilities.createHashString(tuple, _groupByColumns, _map);
            }
          
            SumCount sumCount = null;
            if (tupleInfo.length > 0) {
            	sumCount = _storage.update(tuple, tupleHash, (Long)tupleInfo[0]);
            }
            else sumCount = _storage.update(tuple, tupleHash);
            String strValue = _wrapper.toString(sumCount);

            // propagate further the affected tupleHash-tupleValue pair
            List<String> affectedTuple = new ArrayList<String>();
            affectedTuple.add(tupleHash);
            affectedTuple.add(strValue);

            return affectedTuple;
        }

        //actual operator implementation
        @Override
        public SumCount runAggregateFunction(SumCount value, List<String> tuple) {
            Double sumDelta;
            Long countDelta;
            
            TypeConversion veType = _ve.getType();
            if(veType instanceof SumCountConversion){
                //when merging results from multiple Components which have SumCount as the output
                SumCount sc = (SumCount) _ve.eval(tuple);
                sumDelta = sc.getSum();
                countDelta = sc.getCount();
            }else{
                NumericConversion nc = (NumericConversion) veType;
                sumDelta = nc.toDouble(_ve.eval(tuple));
                countDelta = 1L;
            }
            
            Double sumNew = sumDelta + value.getSum();
            Long countNew = countDelta + value.getCount();
            
            return new SumCount(sumNew, countNew);
        }

        @Override
        public SumCount runAggregateFunction(SumCount value1, SumCount value2) {
            Double sumNew = value1.getSum() + value2.getSum();
            Long countNew = value1.getCount() + value2.getCount();
            return new SumCount(sumNew, countNew);
        }

        @Override
        public boolean isBlocking() {
            return true;
        }

        @Override
        public BasicStore getStorage(){
            return _storage;
        }

        @Override
        public void clearStorage(){
            _storage.reset();
        }

        @Override
        public int getNumTuplesProcessed(){
            return _numTuplesProcessed;
        }

        @Override
	public String printContent(){
            return _storage.getContent();
        }

        @Override
        public List<String> getContent() {
        	 String str = _storage.getContent(); 
        	 if (str != null) {
        		 List<String> tuples = Arrays.asList(str.split("\\r?\\n"));
        		 for (int i = 0; i < tuples.size(); i ++) {
        			 String tuple = tuples.get(i);
        			 tuple = tuple.replace("[", "");
        			 tuple = tuple.replace("]", "");
        			 tuples.set(i, tuple);
        		 }
        		 return tuples;
        	 }
             return null;
        }


        @Override
        public String toString(){
            StringBuilder sb = new StringBuilder();
            sb.append("AggregateAvgOperator with VE: ");
            sb.append(_ve.toString());
            if(_groupByColumns.isEmpty() && _groupByProjection == null){
                sb.append("\n  No groupBy!");
            }else if (!_groupByColumns.isEmpty()){
                sb.append("\n  GroupByColumns are ").append(getGroupByStr()).append(".");
            }else if (_groupByProjection != null){
                sb.append("\n  GroupByProjection is ").append(_groupByProjection.toString()).append(".");
            }
            if(_distinct!=null){
                sb.append("\n  It also has distinct ").append(_distinct.toString());
            }
            return sb.toString();
        }

        private String getGroupByStr(){
            StringBuilder sb = new StringBuilder();
            sb.append("(");
            for(int i=0; i<_groupByColumns.size(); i++){
                sb.append(_groupByColumns.get(i));
                if(i==_groupByColumns.size()-1){
                    sb.append(")");
                }else{
                    sb.append(", ");
                }
            }
            return sb.toString();
        }

        private boolean alreadySetOther(int GB_COLUMNS) {
            return (_groupByType != GB_COLUMNS && _groupByType != GB_UNSET);
        }

        @Override
        public void accept(OperatorVisitor ov){
            ov.visit(this);
        }


		@Override
		public SumCount runAggregateFunction(SumCount value,
				List<String> tuple, Long tupleMultiplicity) {
			Double sumDelta;
            Long countDelta;
            
            TypeConversion veType = _ve.getType();
            if(veType instanceof SumCountConversion){
                //when merging results from multiple Components which have SumCount as the output
                SumCount sc = (SumCount) _ve.eval(tuple);
                sumDelta = sc.getSum() * tupleMultiplicity;
                countDelta = sc.getCount() * tupleMultiplicity;
            }else{
                NumericConversion nc = (NumericConversion) veType;
                sumDelta = nc.toDouble(_ve.eval(tuple)) * tupleMultiplicity;
                countDelta = 1L * tupleMultiplicity;
            }
            
            Double sumNew = sumDelta + value.getSum();
            Long countNew = countDelta + value.getCount();
          
            return new SumCount(sumNew, countNew);
		}

		@Override
		public SumCount runAggregateFunction(SumCount value1, SumCount value2,
				Long multiplicity) {
			Double sumNew = value1.getSum() + value2.getSum() * multiplicity;
            Long countNew = value1.getCount() + value2.getCount() * multiplicity;
            return new SumCount(sumNew, countNew);
		}

		@Override
		public AggregateOperator createInstance() {
			return new AggregateAvgOperator(new ColumnReference<SumCount>(_wrapper, 1), _map);
		}
		
		@Override
		public List<String> getAggregateValue(List<String> tuple) {
			String tupleHash;
	        if(_groupByType == GB_PROJECTION){
	            tupleHash = MyUtilities.createHashString(tuple, _groupByColumns, _groupByProjection.getExpressions(), _map);
	        }else{
	            tupleHash = MyUtilities.createHashString(tuple, _groupByColumns, _map);
	        }
	        List<SumCount> values = _storage.access(tupleHash);
	      
	        if (values != null) {
	        	SumCount value = values.get(0);
	        
	        	String strValue = _wrapper.toString(value);

	        	// propagate further the affected tupleHash-tupleValue pair
	        	List<String> aggTuple = new ArrayList<String>();
	        	aggTuple.add(tupleHash);
	        	aggTuple.add(strValue);
	        
	        	return aggTuple;
	        }
	        return null;
		}
}
