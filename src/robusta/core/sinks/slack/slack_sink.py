import logging
import time
from typing import Optional, Tuple, Dict, List

from robusta.core.reporting.base import Finding
from robusta.core.sinks.sink_base import SinkBase
from robusta.core.sinks.slack.slack_sink_params import SlackSinkConfigWrapper
from robusta.integrations import slack as slack_module


class SlackSink(SinkBase):
    def __init__(self, sink_config: SlackSinkConfigWrapper, registry):
        super().__init__(sink_config.slack_sink, registry)
        self.slack_channel = sink_config.slack_sink.slack_channel
        self.api_key = sink_config.slack_sink.api_key
        self.slack_sender = slack_module.SlackSender(self.api_key, self.account_id, self.cluster_name, self.signing_key)

    def write_finding(self, finding: Finding, platform_enabled: bool) -> None:
        if self.grouping_enabled:
            self.handle_notification_grouping(finding, platform_enabled)
        else:
            self.slack_sender.send_finding_to_slack(finding, self.params, platform_enabled)

    def handle_notification_grouping(self, finding: Finding, platform_enabled: bool) -> None:
        # TODO support notification_mode.regular.ignore_first
        timestamp = time.time()
        finding_data = finding.attribute_map
        # The following will be e.g. Deployment, Job, etc. Sometimes it's undefined.
        finding_data["workload"] = finding.service.resource_type if finding.service else None
        summary_classification = self.classify_finding(
            finding_data, self.params.grouping.notification_mode.summary.by
        )
        group_by_classification = self.classify_finding(finding_data, self.params.grouping.group_by)
        logging.warning(f"****** {group_by_classification=}")
        with self.finding_group_lock:
            if (
                summary_classification not in self.finding_group_start_ts
                or self.finding_group_start_ts[group_by_classification] - timestamp > self.params.grouping.interval
            ):
                # Create a new group/thread
                self.finding_group_start_ts[group_by_classification] = timestamp
                slack_thread_ts = None
            else:
                slack_thread_ts = self.finding_group_heads[group_by_classification]

        if self.grouping_summary_mode:
            logging.warning(f"****** {summary_classification=}")
            slack_thread_ts = None

        if slack_thread_ts is not None:
            # Continue emitting findings in an already existing Slack thread
            if not self.grouping_summary_mode or self.params.grouping.notification_mode.summary.threaded:
                logging.warning(f"Appending to Slack thread {slack_thread_ts}")
                self.slack_sender.send_finding_to_slack(
                    finding, self.params, platform_enabled, thread_ts=slack_thread_ts
                )
            if self.grouping_summary_mode:
                pass
                # TODO update totals in the summary message
        else:
            # Create the first Slack message
            if self.grouping_summary_mode:
                # TODO
                pass
            else:
                slack_thread_ts = self.slack_sender.send_finding_to_slack(finding, self.params, platform_enabled)
                self.finding_group_heads[summary_classification] = slack_thread_ts
                logging.warning(f"Created new Slack thread {slack_thread_ts}")

    def classify_finding(self, finding_data: Dict, attributes: List) -> Tuple:
        values = ()
        for attr in attributes:
            if isinstance(attr, str):
                if attr not in finding_data:
                    logging.warning(f"Notification grouping: tried to group on non-existent attribute {attr}")
                    continue
                values += (finding_data.get(attr), )
            elif isinstance(attr, dict):
                if list(attr.keys()) not in [["labels"], ["attributes"]]:
                    logging.warning(f"Notification grouping: tried to group on non-existent attribute(s) {attr}")
                top_level_attr_name = list(attr.keys())[0]
                values += tuple(
                    finding_data.get(top_level_attr_name, {}).get(subitem_name)
                    for subitem_name in attr[top_level_attr_name]
                )
        return values
