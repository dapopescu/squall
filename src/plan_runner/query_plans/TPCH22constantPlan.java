package plan_runner.query_plans;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import plan_runner.components.DataSourceComponent;
import plan_runner.components.EquiJoinComponent;
import plan_runner.conversion.DoubleConversion;
import plan_runner.conversion.NumericConversion;
import plan_runner.conversion.StringConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.Substring;
import plan_runner.expressions.ValueSpecification;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.AggregateSumOperator;
import plan_runner.operators.DistinctOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.operators.SelectOperator;
import plan_runner.predicates.ComparisonPredicate;

/* Not the exact query TPCH22
 * 
			select
				distinct
				substring(c_phone from 1 for 2) as cntrycode,
				sum(c_acctbal)
			from
				customer, orders, avg_customer
			where
			
			 	c_acctbal > avg and
			 	substring(c_phone from 1 for 2) = cntcode
				and o_custkey = c_custkey
			group by
					cntrycode
 */
public class TPCH22constantPlan {
	
	private QueryPlan _queryPlan = new QueryPlan();
	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	 private static final StringConversion _sc = new StringConversion();
	private static final double valavg = 1000;
    
	public TPCH22constantPlan(String dataPath, String extension, Map conf){
		
        //-------------------------------------------------------------------------------------
        List<Integer> hashCustomer2 = Arrays.asList(0);

        SelectOperator selectionCustomer = new SelectOperator(
                new ComparisonPredicate(
                	ComparisonPredicate.GREATER_OP,
                    new ColumnReference(_doubleConv, 5),
                    new ValueSpecification(_doubleConv, valavg)
                ));
        Substring code2 = new Substring(new ColumnReference<String>(_sc, 4), 0, 2);
        ColumnReference acc2 = new ColumnReference(_sc, 5);
        ColumnReference custkey = new ColumnReference(_sc, 0);
        ProjectOperator projectionCustomer2 = new ProjectOperator(custkey, code2, acc2);

        DataSourceComponent relationCustomer2 = new DataSourceComponent(
                "CUSTOMER2",
                dataPath + "customer" + extension,
                _queryPlan).setHashIndexes(hashCustomer2)
                			.addOperator(selectionCustomer)
                           .addOperator(projectionCustomer2);

        //-------------------------------------------------------------------------------------
        
        DataSourceComponent relationOrders = new DataSourceComponent(
                "ORDERS",
                dataPath + "orders" + extension,
                _queryPlan).addOperator(new ProjectOperator(new int[]{1}))
                           .setHashIndexes(Arrays.asList(0));
      //-------------------------------------------------------------------------------------
        
        AggregateOperator aggSum = new AggregateSumOperator(new ColumnReference(_doubleConv, 1), conf)
        .setGroupByColumns(Arrays.asList(0));
        
        EquiJoinComponent C_Ojoin = new EquiJoinComponent(
        		relationCustomer2,
        		relationOrders,
                _queryPlan).addOperator(new ProjectOperator(new int[]{1, 2}))
                			.addOperator(new DistinctOperator(conf, new int[]{0, 1}))
                			.addOperator(aggSum)
                           .setHashIndexes(Arrays.asList(0)); 
        
        //-------------------------------------------------------------------------------------
        
	}
	
	public QueryPlan getQueryPlan() {
        return _queryPlan;
    }

}
