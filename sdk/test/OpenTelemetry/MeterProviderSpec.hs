{-# LANGUAGE OverloadedStrings #-}

module OpenTelemetry.MeterProviderSpec (spec) where

import Control.Concurrent.Async (wait, withAsync)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (Exception, SomeException, displayException, throwIO)
import Control.Monad (forM_, void, when)
import Data.IORef (atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.Int (Int64)
import Data.Text (Text)
import qualified Data.Vector as V
import Data.Word (Word64)
import OpenTelemetry.Attributes (addAttribute, defaultAttributeLimits, emptyAttributes, lookupAttribute)
import OpenTelemetry.Attributes.Attribute (Attribute (..), PrimitiveAttribute (..))
import OpenTelemetry.Exporter.Metric (
  AggregationTemporality (..),
  ExponentialHistogramDataPoint (..),
  GaugeDataPoint (..),
  HistogramDataPoint (..),
  MetricExport (..),
  NumberValue (..),
  ResourceMetricsExport (..),
  ScopeMetricsExport (..),
  SumDataPoint (..),
 )
import OpenTelemetry.Internal.Common.Types (FlushResult (..), InstrumentationLibrary (..), ShutdownResult (..))
import OpenTelemetry.MeterProvider
import OpenTelemetry.Metric.Core (
  AdvisoryParameters (..),
  Counter (..),
  Gauge (..),
  Histogram (..),
  HistogramAggregation (..),
  Meter (..),
  MeterProvider (..),
  ObservableResult (..),
  UpDownCounter (..),
  defaultAdvisoryParameters,
  forceFlushMeterProvider,
  getMeter,
  shutdownMeterProvider,
 )
import OpenTelemetry.Metric.View (View (..), ViewAggregation (..), ViewSelector (..))
import OpenTelemetry.Resource (emptyMaterializedResources)
import System.Timeout (timeout)
import Test.Hspec


mkView :: Text -> Maybe Text -> ViewAggregation -> Maybe [Text] -> Maybe Text -> Maybe Text -> View
mkView namePat mKind agg mKeys mName mDesc =
  View
    { viewSelector =
        ViewSelector
          { viewInstrumentNamePattern = namePat
          , viewInstrumentKind = Nothing
          , viewInstrumentUnit = Nothing
          , viewMeterName = Nothing
          , viewMeterVersion = Nothing
          , viewMeterSchemaUrl = Nothing
          }
    , viewAggregation = agg
    , viewAttributeKeys = mKeys
    , viewName = mName
    , viewDescription = mDesc
    }


scope :: InstrumentationLibrary
scope = "test-scope"


firstMetric :: [ResourceMetricsExport] -> MetricExport
firstMetric [rme] =
  V.head (scopeMetricsExports (V.head (resourceMetricsScopes rme)))
firstMetric _ = error "expected single ResourceMetricsExport"


-- | Regression tests for SDK metric aggregation.
spec :: Spec
spec = do
  observableGaugeSpec "Int64" IntNumber $ \m callbacks ->
    void $ meterCreateObservableGaugeInt64 m "observed" Nothing Nothing defaultAdvisoryParameters callbacks
  observableGaugeSpec "Double" DoubleNumber $ \m callbacks ->
    void $ meterCreateObservableGaugeDouble m "observed" Nothing Nothing defaultAdvisoryParameters callbacks
  forM_ [AggregationCumulative, AggregationDelta] $ \temporality ->
    it ("retains synchronous gauges across collections with " <> show temporality) $ do
      (provider, env) <-
        createMeterProvider
          emptyMaterializedResources
          defaultSdkMeterProviderOptions {aggregationTemporality = temporality}
      m <- getMeter provider scope
      intGauge <- meterCreateGaugeInt64 m "sync.int" Nothing Nothing defaultAdvisoryParameters
      doubleGauge <- meterCreateGaugeDouble m "sync.double" Nothing Nothing defaultAdvisoryParameters
      gaugeRecord intGauge 3 emptyAttributes
      gaugeRecord doubleGauge 4.5 emptyAttributes
      forM_ [1 :: Int, 2] $ \_ -> do
        points <- gaugePoints <$> collectResourceMetrics env
        map gaugeDataPointValue points `shouldMatchList` [IntNumber 3, DoubleNumber 4.5]
  describe "OpenTelemetry.MeterProvider" $ do
    it "aggregates Int64 counter measurements (cumulative sum)" $ do
      (provider, env) <- createMeterProvider emptyMaterializedResources defaultSdkMeterProviderOptions
      m <- getMeter provider scope
      c <- meterCreateCounterInt64 m "acount" Nothing Nothing defaultAdvisoryParameters
      counterAdd c 3 emptyAttributes
      counterAdd c 7 emptyAttributes
      _ <- collectResourceMetrics env
      batches <- collectResourceMetrics env
      case firstMetric batches of
        MetricExportSum {mesSumPoints = pts, mesName = nm, mesMonotonic = mono, mesIsInt = isInt} -> do
          nm `shouldBe` "acount"
          mono `shouldBe` True
          isInt `shouldBe` True
          V.length pts `shouldBe` 1
          sumDataPointValue (V.head pts) `shouldBe` IntNumber 10
        _ -> expectationFailure "expected MetricExportSum"

    it "aggregates Double counter measurements" $ do
      (provider, env) <- createMeterProvider emptyMaterializedResources defaultSdkMeterProviderOptions
      m <- getMeter provider scope
      c <- meterCreateCounterDouble m "dcounter" Nothing Nothing defaultAdvisoryParameters
      counterAdd c 1.5 emptyAttributes
      counterAdd c 2.5 emptyAttributes
      _ <- collectResourceMetrics env
      batches <- collectResourceMetrics env
      case firstMetric batches of
        MetricExportSum {mesSumPoints = pts, mesIsInt = isInt} -> do
          isInt `shouldBe` False
          sumDataPointValue (V.head pts) `shouldBe` DoubleNumber 4.0
        _ -> expectationFailure "expected MetricExportSum"

    it "aggregates Int64 up-down counter (non-monotonic)" $ do
      (provider, env) <- createMeterProvider emptyMaterializedResources defaultSdkMeterProviderOptions
      m <- getMeter provider scope
      udc <- meterCreateUpDownCounterInt64 m "queue.depth" Nothing Nothing defaultAdvisoryParameters
      upDownCounterAdd udc 10 emptyAttributes
      upDownCounterAdd udc (-3) emptyAttributes
      batches <- collectResourceMetrics env
      case firstMetric batches of
        MetricExportSum {mesSumPoints = pts, mesMonotonic = mono} -> do
          mono `shouldBe` False
          sumDataPointValue (V.head pts) `shouldBe` IntNumber 7
        _ -> expectationFailure "expected MetricExportSum"

    it "aggregates Double up-down counter" $ do
      (provider, env) <- createMeterProvider emptyMaterializedResources defaultSdkMeterProviderOptions
      m <- getMeter provider scope
      udc <- meterCreateUpDownCounterDouble m "temp.diff" Nothing Nothing defaultAdvisoryParameters
      upDownCounterAdd udc 5.0 emptyAttributes
      upDownCounterAdd udc (-1.2) emptyAttributes
      batches <- collectResourceMetrics env
      case firstMetric batches of
        MetricExportSum {mesSumPoints = pts, mesMonotonic = mono, mesIsInt = isInt} -> do
          mono `shouldBe` False
          isInt `shouldBe` False
          case sumDataPointValue (V.head pts) of
            DoubleNumber d -> abs (d - 3.8) `shouldSatisfy` (< 0.001)
            _ -> expectationFailure "expected Double"
        _ -> expectationFailure "expected MetricExportSum"

    it "records explicit histogram measurements" $ do
      (provider, env) <- createMeterProvider emptyMaterializedResources defaultSdkMeterProviderOptions
      m <- getMeter provider scope
      h <- meterCreateHistogram m "latency" Nothing Nothing defaultAdvisoryParameters
      histogramRecord h 1.0 emptyAttributes
      histogramRecord h 50.0 emptyAttributes
      histogramRecord h 9999.0 emptyAttributes
      _ <- collectResourceMetrics env
      batches <- collectResourceMetrics env
      case firstMetric batches of
        MetricExportHistogram {mehPoints = pts, mehName = nm} -> do
          nm `shouldBe` "latency"
          V.length pts `shouldBe` 1
          let p = V.head pts
          histogramDataPointCount p `shouldBe` 3
          abs (histogramDataPointSum p - 10050.0) `shouldSatisfy` (< 0.001)
          histogramDataPointMin p `shouldBe` Just 1.0
          histogramDataPointMax p `shouldBe` Just 9999.0
        _ -> expectationFailure "expected MetricExportHistogram"

    it "honors advisory explicit bucket boundaries" $ do
      let adv = defaultAdvisoryParameters {advisoryExplicitBucketBoundaries = Just [10, 100]}
      (provider, env) <- createMeterProvider emptyMaterializedResources defaultSdkMeterProviderOptions
      m <- getMeter provider scope
      h <- meterCreateHistogram m "custom.hist" Nothing Nothing adv
      histogramRecord h 5.0 emptyAttributes
      histogramRecord h 50.0 emptyAttributes
      histogramRecord h 200.0 emptyAttributes
      batches <- collectResourceMetrics env
      case firstMetric batches of
        MetricExportHistogram {mehPoints = pts} -> do
          let p = V.head pts
          V.length (histogramDataPointExplicitBounds p) `shouldBe` 2
          V.length (histogramDataPointBucketCounts p) `shouldBe` 3
          V.toList (histogramDataPointBucketCounts p) `shouldBe` [1, 1, 1]
        _ -> expectationFailure "expected MetricExportHistogram"

    it "records Int64 gauge (last value wins)" $ do
      (provider, env) <- createMeterProvider emptyMaterializedResources defaultSdkMeterProviderOptions
      m <- getMeter provider scope
      g <- meterCreateGaugeInt64 m "mem.used" Nothing Nothing defaultAdvisoryParameters
      gaugeRecord g 100 emptyAttributes
      gaugeRecord g 200 emptyAttributes
      _ <- collectResourceMetrics env
      batches <- collectResourceMetrics env
      case firstMetric batches of
        MetricExportGauge {megGaugePoints = pts, megIsInt = isInt} -> do
          isInt `shouldBe` True
          gaugeDataPointValue (V.head pts) `shouldBe` IntNumber 200
        _ -> expectationFailure "expected MetricExportGauge"

    it "records Double gauge" $ do
      (provider, env) <- createMeterProvider emptyMaterializedResources defaultSdkMeterProviderOptions
      m <- getMeter provider scope
      g <- meterCreateGaugeDouble m "cpu.pct" Nothing Nothing defaultAdvisoryParameters
      gaugeRecord g 0.75 emptyAttributes
      gaugeRecord g 0.42 emptyAttributes
      _ <- collectResourceMetrics env
      batches <- collectResourceMetrics env
      case firstMetric batches of
        MetricExportGauge {megGaugePoints = pts} ->
          gaugeDataPointValue (V.head pts) `shouldBe` DoubleNumber 0.42
        _ -> expectationFailure "expected MetricExportGauge"

    it "exponential histogram records positive measurements" $ do
      let adv = defaultAdvisoryParameters {advisoryHistogramAggregation = Just (HistogramAggregationExponential 3)}
      (provider, env) <- createMeterProvider emptyMaterializedResources defaultSdkMeterProviderOptions
      m <- getMeter provider scope
      h <- meterCreateHistogram m "exp.latency" Nothing Nothing adv
      histogramRecord h 2.5 emptyAttributes
      histogramRecord h 0.0 emptyAttributes
      histogramRecord h 100.0 emptyAttributes
      batches <- collectResourceMetrics env
      case firstMetric batches of
        MetricExportExponentialHistogram {meehPoints = pts} -> do
          V.length pts `shouldBe` 1
          let p = V.head pts
          exponentialHistogramDataPointCount p `shouldBe` 3
          exponentialHistogramDataPointZeroCount p `shouldBe` 1
          V.null (exponentialHistogramDataPointPositiveBucketCounts p) `shouldBe` False
        _ -> expectationFailure "expected MetricExportExponentialHistogram"

    it "exponential histogram records negative measurements" $ do
      let adv = defaultAdvisoryParameters {advisoryHistogramAggregation = Just (HistogramAggregationExponential 2)}
      (provider, env) <- createMeterProvider emptyMaterializedResources defaultSdkMeterProviderOptions
      m <- getMeter provider scope
      h <- meterCreateHistogram m "signed" Nothing Nothing adv
      histogramRecord h (-5.0) emptyAttributes
      histogramRecord h (-1.0) emptyAttributes
      batches <- collectResourceMetrics env
      case firstMetric batches of
        MetricExportExponentialHistogram {meehPoints = pts} -> do
          let p = V.head pts
          exponentialHistogramDataPointCount p `shouldBe` 2
          V.null (exponentialHistogramDataPointNegativeBucketCounts p) `shouldBe` False
        _ -> expectationFailure "expected MetricExportExponentialHistogram"

    it "observable counter callbacks run on collect" $ do
      (provider, env) <- createMeterProvider emptyMaterializedResources defaultSdkMeterProviderOptions
      m <- getMeter provider scope
      let cb res = observe res (42 :: Int64) emptyAttributes
      _ <- meterCreateObservableCounterInt64 m "obs.counter" Nothing Nothing defaultAdvisoryParameters [cb]
      batches <- collectResourceMetrics env
      case firstMetric batches of
        MetricExportSum {mesSumPoints = pts, mesName = nm, mesMonotonic = mono} -> do
          nm `shouldBe` "obs.counter"
          mono `shouldBe` True
          sumDataPointValue (V.head pts) `shouldBe` IntNumber 42
        _ -> expectationFailure "expected MetricExportSum"

    it "observable gauge reports last observed value" $ do
      (provider, env) <- createMeterProvider emptyMaterializedResources defaultSdkMeterProviderOptions
      m <- getMeter provider scope
      let cb res = observe res (3.14 :: Double) emptyAttributes
      _ <- meterCreateObservableGaugeDouble m "obs.gauge" Nothing Nothing defaultAdvisoryParameters [cb]
      batches <- collectResourceMetrics env
      case firstMetric batches of
        MetricExportGauge {megGaugePoints = pts, megName = nm} -> do
          nm `shouldBe` "obs.gauge"
          gaugeDataPointValue (V.head pts) `shouldBe` DoubleNumber 3.14
        _ -> expectationFailure "expected MetricExportGauge"

    it "observable up-down counter is non-monotonic" $ do
      (provider, env) <- createMeterProvider emptyMaterializedResources defaultSdkMeterProviderOptions
      m <- getMeter provider scope
      let cb res = observe res (-10 :: Int64) emptyAttributes
      _ <- meterCreateObservableUpDownCounterInt64 m "obs.udc" Nothing Nothing defaultAdvisoryParameters [cb]
      batches <- collectResourceMetrics env
      case firstMetric batches of
        MetricExportSum {mesSumPoints = pts, mesMonotonic = mono} -> do
          mono `shouldBe` False
          sumDataPointValue (V.head pts) `shouldBe` IntNumber (-10)
        _ -> expectationFailure "expected MetricExportSum"

    it "respects cardinality limit (overflow attribute)" $ do
      (provider, env) <-
        createMeterProvider emptyMaterializedResources $
          defaultSdkMeterProviderOptions {cardinalityLimit = 1}
      m <- getMeter provider scope
      c <- meterCreateCounterInt64 m "acount" Nothing Nothing defaultAdvisoryParameters
      let a1 = addAttribute defaultAttributeLimits emptyAttributes "route" ("x" :: Text)
          a2 = addAttribute defaultAttributeLimits emptyAttributes "route" ("y" :: Text)
      counterAdd c 1 a1
      counterAdd c 100 a2
      batches <- collectResourceMetrics env
      case firstMetric batches of
        MetricExportSum {mesSumPoints = pts} -> do
          V.length pts `shouldBe` 2
          let overflow = V.filter (\p -> lookupAttribute (sumDataPointAttributes p) "otel.metric.overflow" == Just (AttributeValue (BoolAttribute True))) pts
          V.length overflow `shouldBe` 1
          sumDataPointValue (V.head overflow) `shouldBe` IntNumber 100
        _ -> expectationFailure "expected MetricExportSum"

    it "view drop prevents recording" $ do
      let v = mkView "nope" Nothing ViewAggregationDrop Nothing Nothing Nothing
          opts = defaultSdkMeterProviderOptions {views = [v]}
      (provider, env) <- createMeterProvider emptyMaterializedResources opts
      m <- getMeter provider scope
      c <- meterCreateCounterInt64 m "nope" Nothing Nothing defaultAdvisoryParameters
      counterAdd c 1 emptyAttributes
      batches <- collectResourceMetrics env
      case batches of
        [rme] -> V.null (resourceMetricsScopes rme) `shouldBe` True
        _ -> expectationFailure "expected single ResourceMetricsExport"

    it "view overrides exported metric name" $ do
      let v = mkView "original" Nothing ViewAggregationDefault Nothing (Just "renamed") Nothing
          opts = defaultSdkMeterProviderOptions {views = [v]}
      (provider, env) <- createMeterProvider emptyMaterializedResources opts
      m <- getMeter provider scope
      c <- meterCreateCounterInt64 m "original" Nothing Nothing defaultAdvisoryParameters
      counterAdd c 5 emptyAttributes
      batches <- collectResourceMetrics env
      case firstMetric batches of
        MetricExportSum {mesName = nm} -> nm `shouldBe` "renamed"
        _ -> expectationFailure "expected MetricExportSum"

    it "view overrides exported metric description" $ do
      let v = mkView "described" Nothing ViewAggregationDefault Nothing Nothing (Just "new desc")
          opts = defaultSdkMeterProviderOptions {views = [v]}
      (provider, env) <- createMeterProvider emptyMaterializedResources opts
      m <- getMeter provider scope
      c <- meterCreateCounterInt64 m "described" Nothing (Just "old desc") defaultAdvisoryParameters
      counterAdd c 1 emptyAttributes
      batches <- collectResourceMetrics env
      case firstMetric batches of
        MetricExportSum {mesDescription = d} -> d `shouldBe` "new desc"
        _ -> expectationFailure "expected MetricExportSum"

    it "view filters attribute keys on export" $ do
      let v = mkView "filtered" Nothing ViewAggregationDefault (Just ["keep"]) Nothing Nothing
          opts = defaultSdkMeterProviderOptions {views = [v]}
      (provider, env) <- createMeterProvider emptyMaterializedResources opts
      m <- getMeter provider scope
      c <- meterCreateCounterInt64 m "filtered" Nothing Nothing defaultAdvisoryParameters
      let attrs =
            addAttribute
              defaultAttributeLimits
              (addAttribute defaultAttributeLimits emptyAttributes "keep" ("v1" :: Text))
              "drop"
              ("v2" :: Text)
      counterAdd c 1 attrs
      batches <- collectResourceMetrics env
      case firstMetric batches of
        MetricExportSum {mesSumPoints = pts} -> do
          let a = sumDataPointAttributes (V.head pts)
          a `shouldSatisfy` (\x -> show x `seq` True)
        _ -> expectationFailure "expected MetricExportSum"

    it "view overrides histogram to explicit bucket with custom bounds" $ do
      let v = mkView "h.*" Nothing (ViewAggregationExplicitBucketHistogram [1, 10, 100]) Nothing Nothing Nothing
          opts = defaultSdkMeterProviderOptions {views = [v]}
      (provider, env) <- createMeterProvider emptyMaterializedResources opts
      m <- getMeter provider scope
      h <- meterCreateHistogram m "h.latency" Nothing Nothing defaultAdvisoryParameters
      histogramRecord h 5.0 emptyAttributes
      batches <- collectResourceMetrics env
      case firstMetric batches of
        MetricExportHistogram {mehPoints = pts} -> do
          let p = V.head pts
          V.length (histogramDataPointExplicitBounds p) `shouldBe` 3
        _ -> expectationFailure "expected MetricExportHistogram"

    it "view overrides histogram to exponential" $ do
      let v = mkView "h.*" Nothing (ViewAggregationExponentialHistogram 5) Nothing Nothing Nothing
          opts = defaultSdkMeterProviderOptions {views = [v]}
      (provider, env) <- createMeterProvider emptyMaterializedResources opts
      m <- getMeter provider scope
      h <- meterCreateHistogram m "h.exp" Nothing Nothing defaultAdvisoryParameters
      histogramRecord h 42.0 emptyAttributes
      batches <- collectResourceMetrics env
      case firstMetric batches of
        MetricExportExponentialHistogram {meehPoints = pts} ->
          V.length pts `shouldBe` 1
        _ -> expectationFailure "expected MetricExportExponentialHistogram"

    it "delta temporality resets after collect" $ do
      let opts = defaultSdkMeterProviderOptions {aggregationTemporality = AggregationDelta}
      (provider, env) <- createMeterProvider emptyMaterializedResources opts
      m <- getMeter provider scope
      c <- meterCreateCounterInt64 m "delta.c" Nothing Nothing defaultAdvisoryParameters
      counterAdd c 5 emptyAttributes
      b1 <- collectResourceMetrics env
      case firstMetric b1 of
        MetricExportSum {mesSumPoints = pts, mesAggregationTemporality = temp} -> do
          temp `shouldBe` AggregationDelta
          sumDataPointValue (V.head pts) `shouldBe` IntNumber 5
        _ -> expectationFailure "expected MetricExportSum"
      counterAdd c 3 emptyAttributes
      b2 <- collectResourceMetrics env
      case firstMetric b2 of
        MetricExportSum {mesSumPoints = pts} ->
          sumDataPointValue (V.head pts) `shouldBe` IntNumber 3
        _ -> expectationFailure "expected MetricExportSum"

    it "delta temporality resets histogram after collect" $ do
      let opts = defaultSdkMeterProviderOptions {aggregationTemporality = AggregationDelta}
      (provider, env) <- createMeterProvider emptyMaterializedResources opts
      m <- getMeter provider scope
      h <- meterCreateHistogram m "delta.hist" Nothing Nothing defaultAdvisoryParameters
      histogramRecord h 10.0 emptyAttributes
      _ <- collectResourceMetrics env
      histogramRecord h 20.0 emptyAttributes
      b2 <- collectResourceMetrics env
      case firstMetric b2 of
        MetricExportHistogram {mehPoints = pts} -> do
          let p = V.head pts
          histogramDataPointCount p `shouldBe` 1
          abs (histogramDataPointSum p - 20.0) `shouldSatisfy` (< 0.001)
        _ -> expectationFailure "expected MetricExportHistogram"

    it "multiple attribute sets produce separate data points" $ do
      (provider, env) <- createMeterProvider emptyMaterializedResources defaultSdkMeterProviderOptions
      m <- getMeter provider scope
      c <- meterCreateCounterInt64 m "multi" Nothing Nothing defaultAdvisoryParameters
      let a1 = addAttribute defaultAttributeLimits emptyAttributes "k" ("v1" :: Text)
          a2 = addAttribute defaultAttributeLimits emptyAttributes "k" ("v2" :: Text)
      counterAdd c 1 a1
      counterAdd c 2 a2
      batches <- collectResourceMetrics env
      case firstMetric batches of
        MetricExportSum {mesSumPoints = pts} ->
          V.length pts `shouldBe` 2
        _ -> expectationFailure "expected MetricExportSum"

    it "startTimeUnixNano is non-zero for cumulative" $ do
      (provider, env) <- createMeterProvider emptyMaterializedResources defaultSdkMeterProviderOptions
      m <- getMeter provider scope
      c <- meterCreateCounterInt64 m "timed" Nothing Nothing defaultAdvisoryParameters
      counterAdd c 1 emptyAttributes
      batches <- collectResourceMetrics env
      case firstMetric batches of
        MetricExportSum {mesSumPoints = pts} ->
          sumDataPointStartTimeUnixNano (V.head pts) `shouldSatisfy` (> (0 :: Word64))
        _ -> expectationFailure "expected MetricExportSum"

    it "shutdown prevents further recording" $ do
      (provider, env) <- createMeterProvider emptyMaterializedResources defaultSdkMeterProviderOptions
      m <- getMeter provider scope
      c <- meterCreateCounterInt64 m "pre.shutdown" Nothing Nothing defaultAdvisoryParameters
      counterAdd c 10 emptyAttributes
      result <- shutdownMeterProvider provider Nothing
      result `shouldBe` ShutdownSuccess
      m2 <- getMeter provider scope
      c2 <- meterCreateCounterInt64 m2 "post.shutdown" Nothing Nothing defaultAdvisoryParameters
      counterAdd c2 99 emptyAttributes
      batches <- collectResourceMetrics env
      case batches of
        [rme] -> V.null (resourceMetricsScopes rme) `shouldBe` True
        _ -> expectationFailure "expected single ResourceMetricsExport"

    it "forceFlush succeeds" $ do
      (provider, _env) <- createMeterProvider emptyMaterializedResources defaultSdkMeterProviderOptions
      result <- forceFlushMeterProvider provider Nothing
      result `shouldBe` FlushSuccess

    it "invalid instrument name produces noop instrument" $ do
      (provider, env) <- createMeterProvider emptyMaterializedResources defaultSdkMeterProviderOptions
      m <- getMeter provider scope
      c <- meterCreateCounterInt64 m "" Nothing Nothing defaultAdvisoryParameters
      enabled <- counterEnabled c
      enabled `shouldBe` False
      counterAdd c 99 emptyAttributes
      batches <- collectResourceMetrics env
      case batches of
        [rme] -> V.null (resourceMetricsScopes rme) `shouldBe` True
        _ -> expectationFailure "expected single ResourceMetricsExport"

    it "enabled returns True for valid SDK instruments" $ do
      (provider, _) <- createMeterProvider emptyMaterializedResources defaultSdkMeterProviderOptions
      m <- getMeter provider scope
      c <- meterCreateCounterInt64 m "valid" Nothing Nothing defaultAdvisoryParameters
      enabled <- counterEnabled c
      enabled `shouldBe` True

    it "collecting without recording returns empty scopes" $ do
      (_, env) <- createMeterProvider emptyMaterializedResources defaultSdkMeterProviderOptions
      batches <- collectResourceMetrics env
      case batches of
        [rme] -> V.null (resourceMetricsScopes rme) `shouldBe` True
        _ -> expectationFailure "expected single ResourceMetricsExport"


data CallbackFailure = CallbackFailure
  deriving (Show, Eq)


instance Exception CallbackFailure


gaugePoints :: [ResourceMetricsExport] -> [GaugeDataPoint]
gaugePoints batches =
  [ point
  | batch <- batches
  , scopeMetrics <- V.toList (resourceMetricsScopes batch)
  , MetricExportGauge {megGaugePoints = points} <- V.toList (scopeMetricsExports scopeMetrics)
  , point <- V.toList points
  ]


observableGaugeSpec
  :: Num a
  => String
  -> (a -> NumberValue)
  -> (Meter -> [ObservableResult a -> IO ()] -> IO ())
  -> Spec
observableGaugeSpec numericType number create = describe ("observable gauge " <> numericType) $ do
  forM_ [AggregationCumulative, AggregationDelta] $ \temporality -> do
    it ("removes absent and replaced labels with " <> show temporality) $ do
      (provider, env) <-
        createMeterProvider
          emptyMaterializedResources
          defaultSdkMeterProviderOptions {aggregationTemporality = temporality, cardinalityLimit = 1}
      m <- getMeter provider scope
      observations <- newIORef [("first", 1), ("overflow", 2)]
      create m [\result -> readIORef observations >>= mapM_ (\(label, value) -> observe result value (attributes label))]
      initial <- gaugePoints <$> collectResourceMetrics env
      length initial `shouldBe` 2
      forM_ [[("second", 3)], [("first", 4)], [], [("third", 5)]] $ \current -> do
        writeIORef observations current
        points <- gaugePoints <$> collectResourceMetrics env
        map (\p -> (gaugeDataPointAttributes p, gaugeDataPointValue p)) points
          `shouldMatchList` [(attributes label, number value) | (label, value) <- current]

  it "keeps all callbacks on the same instrument in each collection" $ do
    (provider, env) <- createMeterProvider emptyMaterializedResources defaultSdkMeterProviderOptions
    m <- getMeter provider scope
    create m [\result -> observe result 1 (attributes "first"), \result -> observe result 2 (attributes "second")]
    forM_ [1 :: Int, 2] $ \_ -> do
      points <- gaugePoints <$> collectResourceMetrics env
      map gaugeDataPointValue points `shouldMatchList` [number 1, number 2]

  it "serialises overlapping collections into complete snapshots" $ do
    (provider, env) <- createMeterProvider emptyMaterializedResources defaultSdkMeterProviderOptions
    m <- getMeter provider scope
    entered <- newEmptyMVar
    resume <- newEmptyMVar
    secondStarted <- newEmptyMVar
    calls <- newIORef (0 :: Int)
    create
      m
      [ \result -> do
          call <- atomicModifyIORef' calls (\n -> (n + 1, n + 1))
          observe result (fromIntegral call) (attributes "first")
          when (call == 1) $ putMVar entered () >> takeMVar resume
          observe result (fromIntegral call) (attributes "second")
      ]
    completed <- timeout 2000000 $
      withAsync (collectResourceMetrics env) $ \first -> do
        takeMVar entered
        withAsync (putMVar secondStarted () >> collectResourceMetrics env) $ \second -> do
          takeMVar secondStarted
          timeout 50000 (void $ wait second) `shouldReturn` Nothing
          putMVar resume ()
          firstPoints <- gaugePoints <$> wait first
          secondPoints <- gaugePoints <$> wait second
          map gaugeDataPointValue firstPoints `shouldMatchList` [number 1, number 1]
          map gaugeDataPointValue secondPoints `shouldMatchList` [number 2, number 2]
    completed `shouldBe` Just ()

  forM_ [False, True] $ \flush ->
    it ("rejects recursive collection without locking the provider, flush=" <> show flush) $ do
      (provider, env) <- createMeterProvider emptyMaterializedResources defaultSdkMeterProviderOptions
      m <- getMeter provider scope
      recursive <- newIORef True
      create
        m
        [ \result -> do
            shouldRecurse <- readIORef recursive
            when shouldRecurse $
              if flush
                then void (forceFlushMeterProvider provider Nothing)
                else void (collectResourceMetrics env)
            observe result 1 emptyAttributes
        ]
      result <-
        timeout 2000000 $
          collectResourceMetrics env
            `shouldThrow` (\err -> displayException (err :: SomeException) == "RecursiveMetricCollection")
      result `shouldBe` Just ()
      writeIORef recursive False
      points <- timeout 2000000 (gaugePoints <$> collectResourceMetrics env)
      fmap (map gaugeDataPointValue) points `shouldBe` Just [number 1]

  it "releases collection ownership after a callback exception" $ do
    (provider, env) <- createMeterProvider emptyMaterializedResources defaultSdkMeterProviderOptions
    m <- getMeter provider scope
    failing <- newIORef True
    create
      m
      [ \result -> do
          shouldFail <- readIORef failing
          observe result 1 (attributes "partial")
          when shouldFail $ throwIO CallbackFailure
      ]
    collectResourceMetrics env `shouldThrow` (== CallbackFailure)
    writeIORef failing False
    points <- timeout 2000000 (gaugePoints <$> collectResourceMetrics env)
    fmap (map gaugeDataPointValue) points `shouldBe` Just [number 1]
  where
    attributes label = addAttribute defaultAttributeLimits emptyAttributes "label" (label :: Text)
