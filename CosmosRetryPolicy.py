import logging
import re
from time import sleep
import warnings

from cassandra.policies import RetryPolicy

from cassandra import ConsistencyLevel, WriteType

log = logging.getLogger(__name__)


class CosmosRetryPolicy(RetryPolicy):
    """ generated source for class CosmosRetryPolicy """

    fixedBackOffTimeInMillis = None
    growingBackOffTimeInMillis = None
    maxRetryCount = None
    readTimeoutRetriesEnabled = None
    writeTimeoutRetriesEnabled = None

    # TODO: document func after testing


    def get_fixed_backoff_time_in_millis(self):
        return self.fixedBackOffTimeInMillis
        
    
    def get_growing_backoff_time_in_millis(self):
        return self.growingBackOffTimeInMillis

    
    def get_max_retry_count(self):
        return self.maxRetryCount

    def is_read_timeout_retries_enabled(self):
        return self.readTimeoutRetriesEnabled

    def is_write_timeout_retries_enabled(self):
        return self.writeTimeoutRetriesEnabled



    def on_read_timeout(self, query, consistency, required_responses,
                        received_responses, data_retrieved, retryCount):
                        
                        if retryCount != 0:
                            return RetryPolicy.RETHROW, None
                        elif received_responses >= required_responses and not data_retrieved:
                            return self.maxRetryCount, consistency==ConsistencyLevel
                        else:
                            return RetryPolicy.RETHROW, None 


    def on_write_timeout(self, writeTimeoutRetriesEnabled, retryCount, write_type, consistency ):
        if writeTimeoutRetriesEnabled == True:
            return RetryPolicy.RETHROW, None
        elif retryCount==0 and write_type==WriteType.BATCH_LOG:
            RetryPolicy.RETRY, consistency==ConsistencyLevel
        else:
            return RetryPolicy.RETHROW

    def retry_many_times_or_throw(self, retryCount):
        """ generated source for method retryManyTimesOrThrow """
        return RetryPolicy.RETRY if self.maxRetryCount == -1 or retryCount < self.maxRetryCount else RetryPolicy.RETHROW

    def on_unavailable(self,consistency,retryCount):
        """
        """
        # TODO to be added after testing read and writetout
        return (self.RETRY_NEXT_HOST, None) if retryCount == 0 else (RetryPolicy.RETHROW, None)

    def get_retry_after_millis():
        # TODO 
        return