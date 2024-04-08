import logging
import threading
from abc import abstractmethod, ABC
from datetime import datetime
from typing import Any, List, Dict, Tuple

from robusta.core.model.k8s_operation_type import K8sOperationType
from robusta.core.reporting.base import Finding
from robusta.core.sinks.sink_base_params import ActivityInterval, ActivityParams, SinkBaseParams
from robusta.core.sinks.timing import TimeSlice, TimeSliceAlways


class SinkBase(ABC):
    # The tuples in the types below holds all the attributes we are aggregating on.
    finding_group_start_ts: Dict[Tuple, datetime] = {}
    finding_group_n_ignored: Dict[Tuple, int] = {}
    finding_group_heads: Dict[Tuple, str] = {}  # a mapping from a set of parameters to the head of a thread

    # Summary groups
    finding_sgroup_header: List = []
    finding_sgroup_start_ts: Dict[Tuple, datetime] = {}
    finding_sgroup_counts: Dict[Tuple, int] = {}
    finding_sgroup_heads: Dict[Tuple. str]

    finding_group_lock: threading.Lock = threading.Lock()

    def __init__(self, sink_params: SinkBaseParams, registry):
        self.sink_name = sink_params.name
        self.params = sink_params
        self.default = sink_params.default
        self.registry = registry
        global_config = self.registry.get_global_config()

        self.account_id: str = global_config.get("account_id", "")
        self.cluster_name: str = global_config.get("cluster_name", "")
        self.signing_key = global_config.get("signing_key", "")

        self.time_slices = self._build_time_slices_from_params(self.params.activity)

        # TODO move this out of the ctor, it can be computed on startup
        self.finding_sgroup_header = []
        if sink_params.grouping and sink_params.grouping.notification_mode.summary:
            if sink_params.grouping.notification_mode.summary.by:
                for attr in sink_params.grouping.notification_mode.summary.by:
                    if isinstance(attr, str):
                        self.finding_sgroup_header.append(attr)
                    elif isinstance(attr, dict):
                        keys = list(attr.keys())
                        if len(keys) > 1:
                            logging.error(
                                "Invalid sink configuration: multiple values for one of the elements in"
                                "grouping.notification_mode.summary.by"
                            )
                            raise ValueError()
                        key = keys[0]
                        if key not in ["labels", "attributes"]:
                            logging.error(
                                "Invalid sink configuration: grouping.notification_mode.summary.by.{key} is invalid "
                                "(only labels/attributes allowed)"
                            )
                        for label_or_attr_name in attr[key]:
                            self.finding_sgroup_header.append(f"{key[:-1]}:{label_or_attr_name}")


    def _build_time_slices_from_params(self, params: ActivityParams):
        if params is None:
            return [TimeSliceAlways()]
        else:
            timezone = params.timezone
            return [self._interval_to_time_slice(timezone, interval) for interval in params.intervals]

    def _interval_to_time_slice(self, timezone: str, interval: ActivityInterval):
        return TimeSlice(interval.days, [(time.start, time.end) for time in interval.hours], timezone)

    def is_global_config_changed(self):
        # registry global config can be updated without these stored values being changed
        global_config = self.registry.get_global_config()
        account_id = global_config.get("account_id", "")
        cluster_name = global_config.get("cluster_name", "")
        signing_key = global_config.get("signing_key", "")
        return self.account_id != account_id or self.cluster_name != cluster_name or self.signing_key != signing_key

    def stop(self):
        pass

    def accepts(self, finding: Finding) -> bool:
        return (
            finding.matches(self.params.match, self.params.scope)
            and any(time_slice.is_active_now for time_slice in self.time_slices)
        )

    @abstractmethod
    def write_finding(self, finding: Finding, platform_enabled: bool):
        raise NotImplementedError(f"write_finding not implemented for sink {self.sink_name}")

    def is_healthy(self) -> bool:
        """
        Sink health check. Concrete sinks can implement real health checks
        """
        return True

    def handle_service_diff(self, new_obj: Any, operation: K8sOperationType):
        pass

    def set_cluster_active(self, active: bool):
        pass
