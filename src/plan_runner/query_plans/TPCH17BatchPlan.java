package plan_runner.query_plans;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.components.DataSourceComponent;
import plan_runner.components.EquiJoinComponent;
import plan_runner.components.OperatorComponent;
import plan_runner.conversion.AverageConversion;
import plan_runner.conversion.DateConversion;
import plan_runner.conversion.DoubleConversion;
import plan_runner.conversion.NumericConversion;
import plan_runner.conversion.StringConversion;
import plan_runner.conversion.SumCount;
import plan_runner.conversion.SumCountConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.DateSum;
import plan_runner.expressions.Division;
import plan_runner.expressions.Multiplication;
import plan_runner.expressions.Subtraction;
import plan_runner.expressions.ValueExpression;
import plan_runner.expressions.ValueSpecification;
import plan_runner.operators.AggregateAvgOperator;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.AggregateSumOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.operators.SelectOperator;
import plan_runner.predicates.AndPredicate;
import plan_runner.predicates.BetweenPredicate;
import plan_runner.predicates.ComparisonPredicate;


/* Uses an additional component for aggregation in nested query
 * select sum(l_extendedprice) / 7.0 as avg_yearly
	from
		lineitem,
		part
	where
		p_partkey = l_partkey
		and p_brand = '[BRAND]'
		and p_container = '[CONTAINER]'
		and l_quantity < (
				select 0.2 * avg(l_quantity) from lineitem
					where l_partkey = p_partkey);

 */

public class TPCH17BatchPlan {

	 private static Logger LOG = Logger.getLogger(TPCH17BatchPlan.class);

	    private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	    private static final TypeConversion<Double> _scount = new AverageConversion();
	    private static final StringConversion _sc = new StringConversion();
	    private QueryPlan _queryPlan = new QueryPlan();

	    private static final String _brand="Brand#34";
	    private static final String _container="MED JAR";
	    

	    public TPCH17BatchPlan(String dataPath, String extension, Map conf){
	      

	        //-------------------------------------------------------------------------------------
	        
	    	List<Integer> hashPart = Arrays.asList(0);

		    ProjectOperator projectionPart = new ProjectOperator(new int[]{0});
	    	
	        SelectOperator selectionPart = new SelectOperator(
	        		new AndPredicate(
	        				new ComparisonPredicate(
	        						new ColumnReference(_sc, 3),
	   	                         	new ValueSpecification(_sc, _brand)
	   	                     ),
	   	                    new ComparisonPredicate(
	   	                    		new ColumnReference(_sc, 6),
	   	                    		new ValueSpecification(_sc, _container))
	                     	 ));
	        		
	        DataSourceComponent relationPart = new DataSourceComponent(
	                "PART",
	                dataPath + "part" + extension,
	                _queryPlan).setHashIndexes(hashPart)
	                		   .addOperator(selectionPart)
	                           .addOperator(projectionPart); 
	        
	       

	        //-------------------------------------------------------------------------------------
	        List<Integer> hashLineitem = Arrays.asList(0);
	        
	  
	        ColumnReference partKey1 = new ColumnReference(_sc, 1);
	        ColumnReference quantity = new ColumnReference(_doubleConv, 4);
	        
	        ValueExpression<Double> division = new Division(
					new ColumnReference(_doubleConv, 5),
					new ValueSpecification(_doubleConv, 7.0));
	        
	        ProjectOperator projectionLineitem = new ProjectOperator(partKey1, division, quantity);
	        
	        DataSourceComponent relationLineitem = new DataSourceComponent(
	        		"LINEITEM",
	                dataPath + "lineitem" + extension,
	                _queryPlan).setHashIndexes(hashLineitem)
	                           .addOperator(projectionLineitem);

	        //-------------------------------------------------------------------------------------
	        EquiJoinComponent P_Ljoin = new EquiJoinComponent(
					relationPart,
					relationLineitem,
					_queryPlan).setHashIndexes(Arrays.asList(0));
	        //-------------------------------------------------------------------------------------
	        List<Integer> hashLineitem2 = Arrays.asList(0);
	        
	        //0.2 * quantity
			ValueExpression<Double> product = new Multiplication(
					new ValueSpecification(_doubleConv, 0.2),
					new ColumnReference(_doubleConv, 4));
			
			ColumnReference partKey = new ColumnReference(_sc, 1);
	        
	        ProjectOperator projectionLineitem2 = new ProjectOperator(partKey, product);
	        
	        //avg(quantity)
			
	        AggregateOperator aggLineitem = new AggregateAvgOperator(new ColumnReference(_doubleConv, 1), conf)
				.setGroupByColumns(Arrays.asList(0));
	        
	        DataSourceComponent relationLineitem2 = new DataSourceComponent(
	        		"LINEITEM2",
	                dataPath + "lineitem" + extension,
	                _queryPlan).setHashIndexes(hashLineitem2)
	                           .addOperator(projectionLineitem2);
	       
	        OperatorComponent avgComponent = new OperatorComponent(
	                relationLineitem2,
	                "AVG_LINEITEM",
	                _queryPlan).setHashIndexes(hashLineitem2)
	                .addOperator(aggLineitem).setBatchOutputMillis(200);

	        AggregateOperator aggLineitemAll = new AggregateAvgOperator(new ColumnReference(_scount, 1), conf)
			.setGroupByColumns(Arrays.asList(0));
	        
	        OperatorComponent avgComponentAll = new OperatorComponent(
	                avgComponent,
	                "AVG_LINEITEM_ALL",
	                _queryPlan).setHashIndexes(hashLineitem2)
	                .addOperator(aggLineitemAll);

	        //-------------------------------------------------------------------------------------
	        
	        AggregateOperator aggSum = new AggregateSumOperator(new ColumnReference(_doubleConv, 0), conf);
	        
	        ProjectOperator projectP_L_Ljoin = new ProjectOperator(new int[] {1});
	        
	        SelectOperator selectP_L_Ljoin = new SelectOperator(new ComparisonPredicate(
	        		ComparisonPredicate.LESS_OP,
					new ColumnReference(_doubleConv, 2),
					new ColumnReference(_scount, 3)
					));
	        
            EquiJoinComponent P_L_Ljoin = new EquiJoinComponent(
	        		P_Ljoin,
					avgComponentAll,
					_queryPlan).setHashIndexes(Arrays.asList(0))
								.addOperator(selectP_L_Ljoin)
								.addOperator(projectP_L_Ljoin)
								.addOperator(aggSum);
							
	                                           
	        //-------------------------------------------------------------------------------------
	    }

	    public QueryPlan getQueryPlan() {
	        return _queryPlan;
	    }
}
