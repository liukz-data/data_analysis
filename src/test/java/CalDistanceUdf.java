import org.apache.hadoop.hive.ql.exec.UDF;

import static java.lang.Math.*;

public class CalDistanceUdf extends UDF {

    private final double PI = 3.14159265358979323;
    private final double TWO_P_DIS_LIMIT = 0.1;
    private final double RadFactor = PI/180;
    //    val RevRadFactor = 180/PI
    private final double eR = 6378.245;

    public double evaluate(double sclng,double sclat,double nclng,double nclat ){
        double BiasE = abs(sclng-nclng);
        double BiasN = abs(sclat-nclat);
        double distance = 0.0;
        //当经纬度都小于门限值时，采用两点间直线距离
        if((BiasE<TWO_P_DIS_LIMIT)&&(BiasN<TWO_P_DIS_LIMIT))
        {
            double t2=sqrt(BiasN*BiasN+BiasE*BiasE)*RadFactor;
            distance=t2*eR;
        }
        else  //当经纬度都大于门限值时，采用大圆距离
        {
            double a0=(90-nclat)*RadFactor;
            double b0=(90-sclat)*RadFactor;
            double C0=abs(sclng-nclng)*RadFactor;
            double t1 = cos(a0)*cos(b0)+sin(a0)*sin(b0)*cos(C0);
            double t2 = acos(t1);
            distance=t2*eR;
        }
        return distance;
    }

    public String evaluate(String sclng) {

        return sclng;
    }
}
