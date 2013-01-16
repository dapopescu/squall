package plan_runner.expressions;

import java.util.Arrays;
import java.util.List;

import plan_runner.conversion.StringConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.utilities.MyUtilities;
import plan_runner.visitors.ValueExpressionVisitor;

public class Substring implements ValueExpression<String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	ValueExpression<String> _str;
	int _startIndex, _endIndex;
	
	public Substring(ValueExpression<String> str, int start, int end) {
		_str = str;
		_startIndex = start;
		_endIndex = end;
	}
	
	@Override
	public String eval(List<String> tuple) {
		return _str.eval(tuple).substring(_startIndex, _endIndex);
	}

	@Override
	public String eval(List<String> tuple, Long tupleMultiplicity) {
		return eval(tuple);
	}

	@Override
	public String evalString(List<String> tuple) {
		return eval(tuple);
	}

	@Override
	public TypeConversion getType() {
		return new StringConversion();
	}

	@Override
	public void accept(ValueExpressionVisitor vev) {
		vev.visit(this);
		
	}

	@Override
	public List<ValueExpression> getInnerExpressions() {
		return MyUtilities.listTypeErasure(Arrays.asList(_str));
	}

	@Override
	public void changeValues(int i, ValueExpression<String> newExpr) {
		
	}

	@Override
	public void inverseNumber() {
		
	}

	@Override
	public boolean isNegative() {
		return false;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
	    sb.append("(").append(_str).append(")");
	    return sb.toString();
	}

}
