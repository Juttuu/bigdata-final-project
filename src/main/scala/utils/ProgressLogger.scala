package utils

object ProgressLogger {
  final class Tracker(private val startTimeMillis: Long) {
    def step(percent: Int, message: String): Unit = {
      val safePercent = math.max(0, math.min(100, percent))
      println(f"[$safePercent%3d%%] $message (${elapsedSeconds()}%ds elapsed)")
    }

    def done(message: String): Unit = {
      step(100, message)
    }

    private def elapsedSeconds(): Long = {
      (System.currentTimeMillis() - startTimeMillis) / 1000
    }
  }

  def start(): Tracker = {
    new Tracker(System.currentTimeMillis())
  }
}
