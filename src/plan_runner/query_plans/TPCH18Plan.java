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
import plan_runner.conversion.DoubleConversion;
import plan_runner.conversion.NumericConversion;
import plan_runner.conversion.StringConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.Division;
import plan_runner.expressions.Multiplication;
import plan_runner.expressions.ValueExpression;
import plan_runner.expressions.ValueSpecification;
import plan_runner.operators.AggregateAvgOperator;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.AggregateSumOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.operators.SelectOperator;
import plan_runner.predicates.AndPredicate;
import plan_runner.predicates.ComparisonPredicate;


/*
 * select
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice,
	sum(l_quantity)
	from
		customer,
		orders,
		lineitem
	where
		o_orderkey in (
			select
				l_orderkey
			from
				lineitem
			group by
				l_orderkey having
					sum(l_quantity) > [QUANTITY]
			)
	and c_custkey = o_custkey
	and o_orderkey = l_orderkey
	group by
		c_name,
		c_custkey,
		o_orderkey,
		o_orderdate,
		o_totalprice

 */

public class TPCH18Plan {

	 private static Logger LOG = Logger.getLogger(TPCH10Plan.class);

	    private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	    private static final StringConversion _sc = new StringConversion();
	    private QueryPlan _queryPlan = new QueryPlan();

	    private static final double _quantity=300;
	    

	    public TPCH18Plan(String dataPath, String extension, Map conf){
	      

	        //-------------------------------------------------------------------------------------
	     
	    	List<Integer> hashCustomers = Arrays.asList(1);

		    ProjectOperator projectionCustomers = new ProjectOperator(new int[]{1, 0});
	        		
	        DataSourceComponent relationCustomers = new DataSourceComponent(
	                "CUSTOMER",
	                dataPath + "customer" + extension,
	                _queryPlan).setHashIndexes(hashCustomers)
	                           .addOperator(projectionCustomers); 

	        //-------------------------------------------------------------------------------------
	        List<Integer> hashOrders = Arrays.asList(1);
	        
	        ProjectOperator projectionOrders = new ProjectOperator(new int[]{0, 1, 4, 3});
	        
	        DataSourceComponent relationOrders = new DataSourceComponent(
	        		"ORDERS",
	                dataPath + "orders" + extension,
	                _queryPlan).setHashIndexes(hashOrders)
	                           .addOperator(projectionOrders);

	        //-------------------------------------------------------------------------------------
	        EquiJoinComponent C_Ojoin = new EquiJoinComponent(
					relationCustomers,
					relationOrders,
					_queryPlan).setHashIndexes(Arrays.asList(2));
	        //-------------------------------------------------------------------------------------
	        //sum(quantity)
	        List<Integer> hashLineitem = Arrays.asList(0);
	        
	        ProjectOperator projectionLineitem = new ProjectOperator(new int[]{0, 4});
			
	      
	        DataSourceComponent relationLineitem = new DataSourceComponent(
	        		"LINEITEM",
	                dataPath + "lineitem" + extension,
	                _queryPlan).setHashIndexes(hashLineitem)
	                           .addOperator(projectionLineitem);
	        //-------------------------------------------------------------------------------------
	       
	        AggregateOperator aggLineitem = new AggregateSumOperator(new ColumnReference(_doubleConv, 1), conf)
											.setGroupByColumns(Arrays.asList(0));
	        
	        SelectOperator selectAggLineitem = new SelectOperator(new ComparisonPredicate(
	        		ComparisonPredicate.GREATER_OP,
					new ColumnReference(_doubleConv, 1),
					new ValueSpecification(_doubleConv, _quantity)
					));
        
	        OperatorComponent sumComponent = new OperatorComponent(
	                relationLineitem,
	                "SUM_LINEITEM",
	                _queryPlan).setHashIndexes(hashLineitem)
	                .addOperator(aggLineitem)
	                .addOperator(selectAggLineitem);

	        //-------------------------------------------------------------------------------------
	        
	      //  ProjectOperator projectC_O_Ljoin = new ProjectOperator(new int[] {0, 1, 2, 3, 4});
	        
            EquiJoinComponent C_O_Ljoin = new EquiJoinComponent(
	        		C_Ojoin,
					sumComponent,
					_queryPlan).setHashIndexes(Arrays.asList(2))
							//	.addOperator(projectC_O_Ljoin)
								;
	                                           
	        //-------------------------------------------------------------------------------------
            
     /*       List<Integer> hashLineitem2 = Arrays.asList(0);
	        
	        ProjectOperator projectionLineitem2 = new ProjectOperator(new int[]{0, 4});
	      
	        DataSourceComponent relationLineitem2 = new DataSourceComponent(
	        		"LINEITEM2",
	                dataPath + "lineitem" + extension,
	                _queryPlan).setHashIndexes(hashLineitem2)
	                           .addOperator(projectionLineitem2); 
	        
	        //-------------------------------------------------------------------------------------
		       
	        AggregateOperator aggLineitem2 = new AggregateSumOperator(new ColumnReference(_doubleConv, 1), conf)
											.setGroupByColumns(Arrays.asList(0));
        
	        OperatorComponent sumComponent2 = new OperatorComponent(
	                relationLineitem2,
	                "SUM_LINEITEM2",
	                _queryPlan).setHashIndexes(hashLineitem2)
	                .addOperator(aggLineitem2);

	        //-------------------------------------------------------------------------------------
	        
	        EquiJoinComponent C_O_L_Ljoin = new EquiJoinComponent(
	        		C_O_Ljoin,
					sumComponent2,
					_queryPlan).setHashIndexes(Arrays.asList(2)); */
	        
	        //-------------------------------------------------------------------------------------
	  
	    }

	    public QueryPlan getQueryPlan() {
	        return _queryPlan;
	    }
}
