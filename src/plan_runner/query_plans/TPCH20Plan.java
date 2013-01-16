package plan_runner.query_plans;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import plan_runner.components.DataSourceComponent;
import plan_runner.components.EquiJoinComponent;
import plan_runner.components.OperatorComponent;
import plan_runner.conversion.DateConversion;
import plan_runner.conversion.DoubleConversion;
import plan_runner.conversion.NumericConversion;
import plan_runner.conversion.StringConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.DateSum;
import plan_runner.expressions.Multiplication;
import plan_runner.expressions.ValueExpression;
import plan_runner.expressions.ValueSpecification;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.AggregateSumOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.operators.SelectOperator;
import plan_runner.predicates.BetweenPredicate;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.predicates.LikePredicate;
import plan_runner.predicates.LikePredicateGeneral;


/*
 * this query does not have an aggregate at the end,
 * if run it will output the correct result as tuples,
 * but in LocalMergeResults it will be an exception
 * due to the fact that it doesn't find an aggregate in the result
 * file and the result itself
 */

public class TPCH20Plan {

	private static final TypeConversion<Date> _dc = new DateConversion();
	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
    private static final StringConversion _sc = new StringConversion();
	private static final String COLOR = "forest%";
	private static final String  _countryName = "CANADA";
	
	private static Date _date1, _date2;
	
	private QueryPlan _queryPlan = new QueryPlan();
	
	private static void computeDates(){
        // date2 = date1 + 1 year
        String date1Str = "1994-01-01";
        int interval = 1;
        int unit = Calendar.YEAR;

        //setting _date1
        _date1 = _dc.fromString(date1Str);

        //setting _date2
        ValueExpression<Date> date1Ve, date2Ve;
        date1Ve = new ValueSpecification<Date>(_dc, _date1);
        date2Ve = new DateSum(date1Ve, unit, interval);
        _date2 = date2Ve.eval(null);
        // tuple is set to null since we are computing based on constants
    }
	
	public TPCH20Plan(String dataPath, String extension, Map conf){
		computeDates();
		
		//-------------------------------------------------------------------------------------
        List<Integer> hashLineitem = Arrays.asList(0);
        
        SelectOperator selectionLineitem = new SelectOperator(
                new BetweenPredicate(
                    new ColumnReference(_dc, 10),
                    true, new ValueSpecification(_dc, _date1),
                    false, new ValueSpecification(_dc, _date2)
                ));
        
        ValueExpression<Double> product = new Multiplication(
				new ValueSpecification(_doubleConv, 0.5),
				new ColumnReference(_doubleConv, 4));
		
		ColumnReference partKey = new ColumnReference(_sc, 1);
		ColumnReference suppKey = new ColumnReference(_sc, 2);
		
        ProjectOperator projectionLineitem = new ProjectOperator(partKey, suppKey, product);

        DataSourceComponent relationLineitem = new DataSourceComponent(
                "LINEITEM",
                dataPath + "lineitem" + extension,
                _queryPlan).setHashIndexes(hashLineitem)
                			.addOperator(selectionLineitem)
                			.addOperator(projectionLineitem);
        
        AggregateOperator aggLineitem = new AggregateSumOperator(new ColumnReference(_doubleConv, 2), conf)
					.setGroupByColumns(Arrays.asList(0, 1));
        
        OperatorComponent sumComponent = new OperatorComponent(
                relationLineitem,
                "SUM_LINEITEM",
                _queryPlan).setHashIndexes(hashLineitem)
                .addOperator(aggLineitem);
   
        //-------------------------------------------------------------------------------------
        List<Integer> hashPart = Arrays.asList(0);

        SelectOperator selectionPart = new SelectOperator(
                new LikePredicateGeneral(
                    new ColumnReference(_sc, 1),
                    new ValueSpecification(_sc, COLOR)
                ));

        ProjectOperator projectionPart = new ProjectOperator(new int[]{0});

        DataSourceComponent relationPart = new DataSourceComponent(
                "PART",
                dataPath + "part" + extension,
                _queryPlan).setHashIndexes(hashPart)
                           .addOperator(selectionPart)
                           .addOperator(projectionPart);

        //-------------------------------------------------------------------------------------
        
        List<Integer> hashPartsupp = Arrays.asList(0);

        ProjectOperator projectionPartsupp = new ProjectOperator(new int[]{0, 1, 2});

        DataSourceComponent relationPartsupp = new DataSourceComponent(
                "PARTSUPP",
                dataPath + "partsupp" + extension,
                _queryPlan).setHashIndexes(hashPartsupp)
                           .addOperator(projectionPartsupp);

        //-------------------------------------------------------------------------------------
        EquiJoinComponent PS_Pjoin = new EquiJoinComponent(
				relationPartsupp,
				relationPart,
				_queryPlan).setHashIndexes(Arrays.asList(0, 1));
        //-------------------------------------------------------------------------------------
        
        SelectOperator selectPS_P_Ljoin = new SelectOperator(new ComparisonPredicate(
        		ComparisonPredicate.GREATER_OP,
				new ColumnReference(_doubleConv, 2),
				new ColumnReference(_doubleConv, 3)
				));
        
        EquiJoinComponent PS_P_Ljoin = new EquiJoinComponent(
        		PS_Pjoin,
				sumComponent,
				_queryPlan).setHashIndexes(Arrays.asList(1))
							.addOperator(selectPS_P_Ljoin);
      //-------------------------------------------------------------------------------------

       List<Integer> hashSupplier = Arrays.asList(3);

        ProjectOperator projectionSupplier = new ProjectOperator(new int[]{0, 1, 2, 3});

        DataSourceComponent relationSupplier = new DataSourceComponent(
                "SUPPLIER",
                dataPath + "supplier" + extension,
                _queryPlan).setHashIndexes(hashSupplier)
                           .addOperator(projectionSupplier); 
        
      //-------------------------------------------------------------------------------------
        List<Integer> hashNation = Arrays.asList(0);

        ProjectOperator projectionNation = new ProjectOperator(new int[]{0, 1});

        SelectOperator selectionNation = new SelectOperator( new ComparisonPredicate(
                new ColumnReference(_sc, 1),
                new ValueSpecification(_sc, _countryName)
            ));
        
        DataSourceComponent relationNation = new DataSourceComponent(
                "NATION",
                dataPath + "nation" + extension,
                _queryPlan).setHashIndexes(hashNation)
                		   .addOperator(selectionNation)
                           .addOperator(projectionNation);

        //-------------------------------------------------------------------------------------
        EquiJoinComponent S_Njoin = new EquiJoinComponent(
        		relationSupplier,
                relationNation,
                _queryPlan).addOperator(new ProjectOperator(new int[]{0, 1, 2}))
                           .setHashIndexes(Arrays.asList(0));

        //-------------------------------------------------------------------------------------
        
        EquiJoinComponent S_N_PS_P_Ljoin = new EquiJoinComponent(
        		S_Njoin,
        		PS_P_Ljoin,
                _queryPlan).addOperator(new ProjectOperator(new int[]{1, 2}))
                           .setHashIndexes(Arrays.asList(0)); 

        //-------------------------------------------------------------------------------------
        
        
        
	}
	
	public QueryPlan getQueryPlan() {
        return _queryPlan;
    }
	
}
