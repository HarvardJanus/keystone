package pipelines.astronomy

class Sepobj(var thresh: Double, var npix: Int, var tnpix: Int, var xmin: Int, var xmax: Int,
             var ymin: Int, var ymax: Int, var x: Double, var y: Double, var x2: Double, var y2: Double,
             var xy: Double, var a: Float, var b: Float, var theta: Float, var cxx: Float, var cyy: Float,
             var cxy: Float, var cflux: Float, var flux: Float, var cpeak: Float, var peak: Float,
             var xpeak: Float, var ypeak: Float, var xcpeak: Float, var ycpeak: Float, var flag: Short, var pix: Int){
  def toDoubleArray:Array[Double] = Array(thresh.toDouble, npix.toDouble, tnpix.toDouble, xmin.toDouble, xmax.toDouble,
    ymin.toDouble, ymax.toDouble, x.toDouble, y.toDouble, x2.toDouble, y2.toDouble,
    xy.toDouble, a.toDouble, b.toDouble, theta.toDouble, cxx.toDouble, cyy.toDouble,
    cxy.toDouble, cflux.toDouble, flux.toDouble, cpeak.toDouble, peak.toDouble,
    xpeak.toDouble, ypeak.toDouble, xcpeak.toDouble, ycpeak.toDouble, flag.toDouble, pix.toDouble)
}