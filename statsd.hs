import Control.Concurrent
import Control.Concurrent.Chan
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Control.Monad.State
import Data.Maybe
import Data.Map
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Network.Socket
import System.IO
import System.IO.Error
import Text.ParserCombinators.Parsec
import Text.ParserCombinators.Parsec.Number

-- TODO: 
-- 1) fix non-tail recursive function to use map/fold/anything else really
-- 2) implement timers support.
-- 3) implement outbound tcp support.
-- 4) fix gauges, shouldn't clear when we send data onwards.

data AggregationFunction = Counter | SampledCounter Float | Timing | Gauge | Sets deriving Show
data Metric = Metric {name :: String, value :: Float} deriving Show
data Datapoint = Datapoint AggregationFunction Metric deriving Show

parseCount :: Parser AggregationFunction
parseCount = do
    char 'c'
    value <- optionMaybe (string "|@" >> floating)
    let func = case value of
                Nothing -> Counter
                Just s -> SampledCounter s
    return func

parseTiming :: Parser AggregationFunction
parseTiming = string "ms" >> return Timing

parseGauges :: Parser AggregationFunction
parseGauges = char 'g' >> return Gauge

parseSets :: Parser AggregationFunction
parseSets = char 's' >> return Sets

parseDatapoint :: Parser Datapoint
parseDatapoint = do
    metricName <- many (noneOf " :")
    char ':'
    metricVal <- floating
    char '|'
    function <- parseCount <|> parseTiming <|> parseGauges <|> parseSets
    optional (string "\r" <|> string "\n")
    return $ Datapoint function $ Metric metricName metricVal

makeAggregators :: IO (String -> IO ())
makeAggregators = do
    counters <- atomically $ newTVar empty
    gauges <- atomically $ newTVar empty
    sets <- atomically $ newTVar empty
    -- counters
    cch <- newChan
    forkIO $ aggregator cch (+) Counter counters
    forkIO $ outputToGraphite 30000000 counters
    -- gauges
    gch <- newChan
    forkIO $ aggregator gch (\ x y -> x) Gauge gauges
    forkIO $ outputToGraphite 30000000 gauges
    -- sets
    sch <- newChan
    forkIO $ aggregator sch (\x y -> y + 1) Sets sets
    forkIO $ outputToGraphite 30000000 sets
    return $ parseAndForward cch gch sch

main :: IO ()
main = do
    let metricMap = empty
    let flushInterval = 30
    parser <- makeAggregators
    tcpSock <- initSocket Stream 4242
    udpSock <- initSocket Datagram 4242
    forkIO $ udpServer udpSock parser
    listen tcpSock 2
    mainLoop tcpSock parser
     where
    mainLoop sock parser = do
        conn <- accept sock
        forkIO $ tcpServer conn parser
        mainLoop sock parser

    initSocket st port = do
        sock <- socket AF_INET st defaultProtocol
        setSocketOption sock ReuseAddr 1
        bindSocket sock (SockAddrInet port iNADDR_ANY)
        return sock 

udpServer :: Socket -> (String -> IO()) -> IO ()
udpServer sock parser = do
    dataread <- tryJust (guard . isEOFError) $ recvFrom sock 4096
    case dataread of
        Right (msg, bytesread, addr) -> parser msg
        Left s -> print "Bah"
    udpServer sock parser

tcpServer :: (Socket, SockAddr) -> (String -> IO()) -> IO ()
tcpServer (sock, sockAddr) parser = do
    msg <- tryJust (guard . isEOFError) $ recv sock 4096
    case msg of
        Right msg -> do 
            parser msg
            tcpServer (sock, sockAddr) parser
        Left _ -> putStrLn "Exiting"

parseAndForward ch gch sch msg = do
    let parsedMsg = parse parseDatapoint " " msg
    case parsedMsg of
        Right (Datapoint (SampledCounter f) m) -> writeChan ch $ Datapoint Counter m 
        Right (Datapoint Gauge m) -> writeChan gch $ Datapoint Gauge m
        Right (Datapoint Sets m) -> writeChan sch $ Datapoint Sets (Metric (name m) 1)
        Right (Datapoint Counter m) -> writeChan ch $ Datapoint Counter m
        Left s -> print "Bah humbug"

aggregator ch aggFunc metricFunc aggs = do
    datapoint <- readChan ch
    aggregate datapoint aggFunc
    aggregator ch aggFunc metricFunc aggs
   where
    aggregate (Datapoint _ m) funk =
        atomically $ readTVar aggs >>= writeTVar aggs . (insertWith funk (name m) (value m))


-- TODO: make this actually calculate stuff. Also, it should be timers, not sets.
calculateSetsValues :: Map String [a] -> Map String [a]
calculateSetsValues m = loop m (keys m)
    where
        loop m (k:[]) = m
        loop m (k:ks) = loop m ks

outputToGraphite sleepInterval counters = do
    threadDelay sleepInterval
    counterSnapshot <- emptyMetrics counters
    sock <- socket AF_INET Datagram defaultProtocol
    loopAndLog sock counterSnapshot (keys counterSnapshot)
    print "Flushed metrics"
    outputToGraphite sleepInterval counters

   where
    loopAndLog sock cs counterKeys = do
        curTime <- round `fmap` getPOSIXTime
        if length counterKeys == 0 then
            return ()
        else
            let firstKey = head counterKeys in
            let metricStr = (firstKey ++ " " ++ (show $ Data.Map.findWithDefault 0.0 firstKey cs) ++ " " ++ (show curTime) ++ "\n") in
            sendTo sock metricStr (SockAddrInet 4243 0) >>
            loopAndLog sock cs (tail counterKeys)

emptyMetrics metrics = atomically $ do
    currentMetrics <- readTVar metrics
    writeTVar metrics empty
    return currentMetrics
