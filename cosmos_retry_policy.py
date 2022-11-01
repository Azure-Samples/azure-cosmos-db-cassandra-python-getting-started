"""
This builds on Azure Cosmos Cassandra Extensions
https://github.com/Azure/azure-cosmos-cassandra-extensions/blob/1c116639948317347b04e2c624b2e08d4436be78/driver-4/src/main/java/com/azure/cosmos/cassandra/CosmosRetryPolicy.java
"""
import logging
from cassandra.policies import RetryPolicy

from cassandra import ConsistencyLevel, WriteType

log = logging.getLogger(__name__)

DEFAULT_FIXED_BACKOFF_TIME_MILLIS = 5000
DEFAULT_GROWING_BACK_OFF_TIME_MILLIS = 1000
DEFAULT_MAX_RETRY_COUNT = 5
DEFAULT_READ_TIMEOUT_RETRIES_ENABLED = True
DEFAULT_WRITE_TIMEOUT_RETRIES_ENABLED = True
GROWING_BACKOFF_SALT_IN_MILLIS = 1000


class CosmosRetryPolicy(RetryPolicy):
    """ generated source for class CosmosRetryPolicy """
    def __init__(self,
        max_retry_count=DEFAULT_MAX_RETRY_COUNT,
        fixed_back_off_time_in_millis=DEFAULT_FIXED_BACKOFF_TIME_MILLIS, 
        growing_back_off_time_in_millis=DEFAULT_GROWING_BACK_OFF_TIME_MILLIS,
        write_timeout_retries_enabled=DEFAULT_WRITE_TIMEOUT_RETRIES_ENABLED, 
        read_timeout_retries_enabled=DEFAULT_READ_TIMEOUT_RETRIES_ENABLED):

        self.max_retry_count=max_retry_count
        self.read_timeout_retries_enabled=read_timeout_retries_enabled
        self.write_timeout_retries_enabled=write_timeout_retries_enabled
        self.fixed_back_off_time_in_millis=fixed_back_off_time_in_millis
        self.growing_back_off_time_in_millis=growing_back_off_time_in_millis



    def get_fixed_backoff_time_in_millis(self):
        """
        Fixed backoff time in millisecond
        """
        return self.fixed_back_off_time_in_millis

    def get_growing_backoff_time_in_millis(self):
        """
        Growing backoff time in milliseconds
        """
        return self.growing_back_off_time_in_millis

    def get_max_retry_count(self):
        """
        Maximum number of retries
        """
        return self.max_retry_count

    def is_read_timeout_retries_enabled(self):
        """
        Whether retries on read timeouts are enabled.
        Disabling read timeouts may be desirable
        when Cosmos Cassandra API server-side retries are enable
        """
        return self.read_timeout_retries_enabled

    def is_write_timeout_retries_enabled(self):
        """
        Whether retries on write timeouts are enabled. Disabling write timeouts may be
        desirable when Cosmos Cassandra API server-side retries are enabled.
        """
        return self.write_timeout_retries_enabled

    def on_read_timeout(self, request, consistency_level, required_responses, received_responses, data_present, retry_count):
        """
        Request The request that failed.
        consistency_level The consistency level of the request that failed.
        required_responses The number of replica responses required to perform the operation.
        data_present Whether the actual data was amongst the received replica responses
        retry_count the current retry count

        """
        if self.read_timeout_retries_enabled:
            if retry_count == 0 and received_responses >= required_responses and not data_present:
                return RetryPolicy.RETRY, None
            return RetryPolicy.RETHROW, None
        else:
            return RetryPolicy.RETHROW, None


    def on_write_timeout(self,write_timeout_retries_enabled,retry_count,write_type,consistency_level):
        """
        Request The request that failed.
        consistency_level The consistency level of the request that failed.
        required_responses The number of replica responses required to perform the operation.
        data_present Whether the actual data was amongst the received replica responses
        retry_count the current retry count
        """
        if write_timeout_retries_enabled is False:
            return RetryPolicy.RETHROW, None
        elif retry_count==0 and write_type==WriteType.BATCH_LOG:
            RetryPolicy.RETRY, consistency_level==ConsistencyLevel
        else:
            return RetryPolicy.RETHROW, None

    def retry_many_times_or_throw(self, retry_count):
        """ generated source for method retryManyTimesOrThrow """
        return RetryPolicy.RETRY if self.max_retry_count == -1 or retry_count < self.max_retry_count else RetryPolicy.RETHROW
