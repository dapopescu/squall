package plan_runner.query_plans;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.components.DataSourceComponent;
import plan_runner.components.EquiJoinComponent;
import plan_runner.conversion.AverageConversion;
import plan_runner.conversion.DoubleConversion;
import plan_runner.conversion.NumericConversion;
import plan_runner.conversion.StringConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.Division;
import plan_runner.expressions.ValueExpression;
import plan_runner.expressions.ValueSpecification;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.AggregateSumOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.operators.SelectOperator;
import plan_runner.predicates.AndPredicate;
import plan_runner.predicates.ComparisonPredicate;

public class TPCH17ConstantPlan {
	 private static Logger LOG = Logger.getLogger(TPCH17ConstantPlan.class);

	    private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	    private static final TypeConversion<Double> _scount = new AverageConversion();
	    private static final StringConversion _sc = new StringConversion();
	    private QueryPlan _queryPlan = new QueryPlan();

	    private static final String _brand="Brand#34";
	    private static final String _container="MED JAR";
	    private static final double _avg_quantity=25;
	    

	    public TPCH17ConstantPlan(String dataPath, String extension, Map conf){
	      

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
	        
	        
	        SelectOperator selectionLineitem = new SelectOperator(new ComparisonPredicate(
	        		ComparisonPredicate.LESS_OP,
					quantity,
					new ValueSpecification(_doubleConv, _avg_quantity)
					));
	        
	        DataSourceComponent relationLineitem = new DataSourceComponent(
	        		"LINEITEM",
	                dataPath + "lineitem" + extension,
	                _queryPlan).setHashIndexes(hashLineitem)
	                			.addOperator(selectionLineitem)
	                           .addOperator(projectionLineitem);

	        //-------------------------------------------------------------------------------------
	        
	        AggregateOperator aggSum = new AggregateSumOperator(new ColumnReference(_doubleConv, 0), conf);
	        
	        EquiJoinComponent P_Ljoin = new EquiJoinComponent(
					relationPart,
					relationLineitem,
					_queryPlan).setHashIndexes(Arrays.asList(0))
					.addOperator(new ProjectOperator(new int[]{1}))
					.addOperator(aggSum);
	       

	        //-------------------------------------------------------------------------------------
	        
	    }

	    public QueryPlan getQueryPlan() {
	        return _queryPlan;
	    }
}
