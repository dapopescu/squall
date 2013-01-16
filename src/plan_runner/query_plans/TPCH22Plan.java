package plan_runner.query_plans;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import plan_runner.components.DataSourceComponent;
import plan_runner.components.EquiJoinComponent;
import plan_runner.components.OperatorComponent;
import plan_runner.conversion.AverageConversion;
import plan_runner.conversion.DoubleConversion;
import plan_runner.conversion.NumericConversion;
import plan_runner.conversion.StringConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.Substring;
import plan_runner.expressions.ValueSpecification;
import plan_runner.operators.AggregateAvgOperator;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.AggregateSumOperator;
import plan_runner.operators.DistinctOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.operators.SelectOperator;
import plan_runner.predicates.ComparisonPredicate;

/* Not the exact query TPCH22, but it is based on it
 * 
			create view avg_customer as 
					select
						substring(c_phone from 1 for 2) as cntcode
						avg(c_acctbal)
									
						from
								customer
						where
								c_acctbal > 0.00
									
						group by cntcode
			
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
public class TPCH22Plan {
	
	private QueryPlan _queryPlan = new QueryPlan();
	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	 private static final TypeConversion<Double> _avgConv = new AverageConversion();
    private static final StringConversion _sc = new StringConversion();
	private static final double zero = 0;
    
	public TPCH22Plan(String dataPath, String extension, Map conf){
		  //-------------------------------------------------------------------------------------
        List<Integer> hashCustomer = Arrays.asList(0);

        SelectOperator selectionCustomer = new SelectOperator(
                new ComparisonPredicate(
                	ComparisonPredicate.GREATER_OP,
                    new ColumnReference(_doubleConv, 5),
                    new ValueSpecification(_doubleConv, zero)
                ));

        Substring code = new Substring(new ColumnReference<String>(_sc, 4), 0, 2);
        ColumnReference acc = new ColumnReference(_sc, 5);
        ProjectOperator projectionCustomer = new ProjectOperator(code, acc);

        DataSourceComponent relationCustomer = new DataSourceComponent(
                "CUSTOMER",
                dataPath + "customer" + extension,
                _queryPlan).setHashIndexes(hashCustomer)
                           .addOperator(selectionCustomer)
                           .addOperator(projectionCustomer);

        //-------------------------------------------------------------------------------------
        
        AggregateOperator aggCustomer = new AggregateAvgOperator(new ColumnReference(_doubleConv, 1), conf)
		.setGroupByColumns(Arrays.asList(0));
        
        OperatorComponent avgComponent = new OperatorComponent(
                relationCustomer,
                "AVG_CUSTOMER",
                _queryPlan).setHashIndexes(Arrays.asList(0))
                .addOperator(aggCustomer);
        //-------------------------------------------------------------------------------------
        List<Integer> hashCustomer2 = Arrays.asList(0);

        Substring code2 = new Substring(new ColumnReference<String>(_sc, 4), 0, 2);
        ColumnReference acc2 = new ColumnReference(_sc, 5);
        ColumnReference custkey = new ColumnReference(_sc, 0);
        ProjectOperator projectionCustomer2 = new ProjectOperator(custkey, code2, acc2);

        DataSourceComponent relationCustomer2 = new DataSourceComponent(
                "CUSTOMER2",
                dataPath + "customer" + extension,
                _queryPlan).setHashIndexes(hashCustomer2)
                           .addOperator(projectionCustomer2);

        //-------------------------------------------------------------------------------------
        
        DataSourceComponent relationOrders = new DataSourceComponent(
                "ORDERS",
                dataPath + "orders" + extension,
                _queryPlan).addOperator(new ProjectOperator(new int[]{1}))
                           .setHashIndexes(Arrays.asList(0));
      //-------------------------------------------------------------------------------------
        
        EquiJoinComponent C_Ojoin = new EquiJoinComponent(
        		relationCustomer2,
        		relationOrders,
                _queryPlan).addOperator(new ProjectOperator(new int[]{1, 2}))
                			.addOperator(new DistinctOperator(conf, new int[]{0, 1}))
                           .setHashIndexes(Arrays.asList(0)); 
        
        //-------------------------------------------------------------------------------------
        
        AggregateOperator aggSum = new AggregateSumOperator(new ColumnReference(_doubleConv, 1), conf)
        .setGroupByColumns(Arrays.asList(0));
        
        SelectOperator selectC_C_Ojoin = new SelectOperator(new ComparisonPredicate(
        		ComparisonPredicate.GREATER_OP,
				new ColumnReference(_doubleConv, 1),
				new ColumnReference(_avgConv, 2)
				));
        
        EquiJoinComponent C_C_Ojoin = new EquiJoinComponent(
        		C_Ojoin,
        		avgComponent,
                _queryPlan).addOperator(selectC_C_Ojoin)
        					.addOperator(new ProjectOperator(new int[]{0, 1}))
        					.addOperator(aggSum)
                           .setHashIndexes(Arrays.asList(0)); 
        
        //-------------------------------------------------------------------------------------
        
        
	}
	
	public QueryPlan getQueryPlan() {
        return _queryPlan;
    }

}
