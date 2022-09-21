"""
TODO Will update after testing
"""
import logging
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

    def get_fixed_backoff_time_in_millis(self):
        """
        TODO Will update after testing
        """
        return self.fixedBackOffTimeInMillis

    def get_growing_backoff_time_in_millis(self):
        """
        TODO Will update after testing
        """
        return self.growingBackOffTimeInMillis

    def get_max_retry_count(self):
        """
        TODO Will update after testing
        """
        return self.maxRetryCount

    def is_read_timeout_retries_enabled(self):
        """
        TODO Will update after testing
        """
        return self.readTimeoutRetriesEnabled

    def is_write_timeout_retries_enabled(self):
        """
        TODO Will update after testing
        """
        return self.writeTimeoutRetriesEnabled

    def on_read_timeout(self, query, consistency, required_responses,received_responses, data_retrieved, retry_count):
        """
        TODO Will update after testing
        """
        if retry_count != 0:
            return RetryPolicy.RETHROW, None
        elif received_responses >= required_responses and not data_retrieved:
            return self.maxRetryCount, consistency==ConsistencyLevel
        else:
            return RetryPolicy.RETHROW, None

    def on_write_timeout(self,writeTimeoutRetriesEnabled,retry_count,write_type,consistency):
        """
        TODO Will update after testing
        """
        if writeTimeoutRetriesEnabled is False:
            return RetryPolicy.RETHROW, None
        elif retry_count==0 and write_type==WriteType.BATCH_LOG:
            RetryPolicy.RETRY, consistency==ConsistencyLevel

    def retry_many_times_or_throw(self, retry_count):
        """ generated source for method retryManyTimesOrThrow """
        return RetryPolicy.RETRY if self.maxRetryCount == -1 or retry_count < self.maxRetryCount else RetryPolicy.RETHROW

    # def on_unavailable(self,consistency,retryCount):
    #     """
    #     """
    #     return (self.RETRY_NEXT_HOST, None) if retryCount == 0 else (RetryPolicy.RETHROW, None)

    # def get_retry_after_millis():
    #     """
    #     """
    #     return None
