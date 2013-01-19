package plan_runner.predicates;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import plan_runner.conversion.StringConversion;
import plan_runner.expressions.ValueExpression;
import plan_runner.expressions.ValueSpecification;
import plan_runner.visitors.PredicateVisitor;

/*
 * It works for any expression which contains % or _
 */

public class LikePredicateGeneral implements Predicate {
    /**
	 * 
	 */
	private static final long serialVersionUID = 8872111292198209593L;
	private ValueExpression<String> _ve1, _ve2;

    public LikePredicateGeneral(ValueExpression<String> ve1, ValueExpression<String> ve2){
      _ve1 = ve1;
      _ve2 = ve2;
      
      if(_ve2 instanceof ValueSpecification){
          String value = _ve2.eval(null);
          value = value.replace("_", ".*");
          value = value.replace("%", ".*");
          _ve2 = new ValueSpecification<String>(new StringConversion(), value);
      }
    }

    public List<ValueExpression> getExpressions(){
        List<ValueExpression> result = new ArrayList<ValueExpression>();
        result.add(_ve1);
        result.add(_ve2);
        return result;
    }

    @Override
    public List<Predicate> getInnerPredicates() {
        return new ArrayList<Predicate>();
    }

    @Override
    public boolean test(List<String> tupleValues){
        String val1 = _ve1.eval(tupleValues);
        String val2 = _ve2.eval(tupleValues);
        return Pattern.matches(val2, val1);
    }

    @Override
    public boolean test(List<String> firstTupleValues, List<String> secondTupleValues){
        String val1 = _ve1.eval(firstTupleValues);
        String val2 = _ve2.eval(firstTupleValues);
        return Pattern.matches(val2, val1);
    }

    @Override
    public void accept(PredicateVisitor pv) {
        pv.visit(this);
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append(_ve1.toString());
        sb.append(" LIKE GENERAL ");
        sb.append(_ve2.toString());
        return sb.toString();
    }
}

