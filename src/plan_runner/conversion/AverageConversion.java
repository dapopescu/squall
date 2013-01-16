package plan_runner.conversion;

public class AverageConversion implements NumericConversion<Double>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 5078129348968840772L;

	@Override
    public Double fromString(String str) {
		String parts[] = str.split("\\:");
	    Double sum = Double.valueOf(parts[0]);
	    Long count = Long.valueOf(parts[1]);
        return new SumCount(sum, count).getAvg();
    }

    @Override
    public String toString(Double obj) {
        return obj.toString();
    }

    @Override
    public Double fromDouble(double d) {
        return d;
    }

    @Override
    public double toDouble(Object obj) {
        return (Double) obj;
    }

    @Override
    public Double getInitialValue() {
        return new Double(0.0);
    }

    @Override
    public double getDistance(Double bigger, Double smaller) {
        return bigger - smaller;
    }
    
    //for printing(debugging) purposes
    @Override
    public String toString(){
        return  "Double";
    }    
    
}
