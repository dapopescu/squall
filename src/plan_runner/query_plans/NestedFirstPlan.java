package plan_runner.query_plans;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.EquiJoinComponent;
import plan_runner.components.OperatorComponent;
import plan_runner.conversion.DoubleConversion;
import plan_runner.conversion.IntegerConversion;
import plan_runner.conversion.NumericConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.operators.AggregateAvgOperator;
import plan_runner.operators.AggregateCountOperator;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.AggregateSumOperator;
import plan_runner.operators.ProjectOperator;

public class NestedFirstPlan {
	private static Logger LOG = Logger.getLogger(NestedFirstPlan.class);

    private QueryPlan _queryPlan = new QueryPlan();
    private static final NumericConversion<Integer> _intConv = new IntegerConversion();
    private static final NumericConversion<Integer> _intConv2 = new IntegerConversion();
    private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
    
    public NestedFirstPlan(String dataPath, String extension, Map conf){
            //-------------------------------------------------------------------------------------
           
    	
    	
    	//	ProjectOperator projectionR = new ProjectOperator(new int[]{0});
            List<Integer> hashR = Arrays.asList(1);
            DataSourceComponent relationR = new DataSourceComponent(
                                            "R",
                                            dataPath + "r" + extension,
                                            _queryPlan).setHashIndexes(hashR);
                                            			

            //-------------------------------------------------------------------------------------
            ProjectOperator projectionS = new ProjectOperator(new int[]{1});
            List<Integer> hashS = Arrays.asList(0);
         
            DataSourceComponent relationS = new DataSourceComponent(
                                            "S",
                                            dataPath + "s" + extension,
                                            _queryPlan).setHashIndexes(hashS)
                                            		   .addOperator(projectionS);
            
            AggregateSumOperator aggOp = new AggregateSumOperator(new ColumnReference(_intConv, 0), conf);
          //  AggregateAvgOperator aggAvg = new AggregateAvgOperator(new ColumnReference(_doubleConv, 0), conf);
            
            List<Integer> hashN = Arrays.asList(1);
            OperatorComponent nestedQueryComponent = new OperatorComponent(
                    relationS,
                    "NESTED_RESULT",
                    _queryPlan).setHashIndexes(hashN)          
                    .addOperator(aggOp);
            
            AggregateCountOperator aggCount = new AggregateCountOperator(conf);
            AggregateSumOperator aggSum = new AggregateSumOperator(new ColumnReference(_intConv2, 0), conf);
            
           
            
            EquiJoinComponent join = new EquiJoinComponent(
                    relationR,
                    nestedQueryComponent,
                    _queryPlan).setHashIndexes(Arrays.asList(0))
                    		  // .addOperator(new ProjectOperator(new int[]{0}))
                    		   .addOperator(aggSum);
                               ; 
                                                       
            //-------------------------------------------------------------------------------------

           

        

    }

    public QueryPlan getQueryPlan() {
        return _queryPlan;
    }
    
}
