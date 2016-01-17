package fi.aalto.dmg.statistics;

import org.apache.spark.streaming.scheduler.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jun on 15/01/16.
 */
public class PerformanceStreamingListener implements StreamingListener {

    private static Logger logger = LoggerFactory.getLogger(PerformanceStreamingListener.class);



    /** Called when a receiver has been started */
    @Override
    public void onReceiverStarted(StreamingListenerReceiverStarted receiverStarted) { }

    /** Called when a receiver has reported an error */
    @Override
    public void onReceiverError(StreamingListenerReceiverError receiverError) { }

    /** Called when a receiver has been stopped */
    @Override
    public void onReceiverStopped(StreamingListenerReceiverStopped receiverStopped) { }

    /** Called when a batch of jobs has been submitted for processing. */
    @Override
    public void onBatchSubmitted(StreamingListenerBatchSubmitted batchSubmitted) { }

    /** Called when processing of a batch of jobs has started.  */
    @Override
    public void onBatchStarted(StreamingListenerBatchStarted batchStarted) { }

    /** Called when processing of a batch of jobs has completed. */
    @Override
    public void onBatchCompleted(StreamingListenerBatchCompleted batchCompleted) {
        BatchInfo batchInfo = batchCompleted.batchInfo();
        logger.warn( batchInfo.batchTime().milliseconds() + "\t"
                + batchInfo.schedulingDelay().get()+ "\t"
                + batchInfo.processingDelay().get() + "\t"
                + batchInfo.totalDelay().get() + "\t"
                + batchInfo.numRecords());
    }

}
