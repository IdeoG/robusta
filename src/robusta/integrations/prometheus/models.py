import html
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from urllib.parse import parse_qs, unquote, urlparse

from hikaru.model.rel_1_26 import DaemonSet, HorizontalPodAutoscaler, Node, StatefulSet
from pydantic import BaseModel

from robusta.core.reporting import Finding, FindingSeverity, FindingSource, FindingSubject, FindingSubjectType
from robusta.integrations.kubernetes.autogenerated.events import (
    DaemonSetEvent,
    DeploymentEvent,
    HorizontalPodAutoscalerEvent,
    JobEvent,
    NodeEvent,
    PodEvent,
    StatefulSetEvent,
)
from robusta.integrations.kubernetes.custom_models import RobustaDeployment, RobustaJob, RobustaPod

SEVERITY_MAP = {
    "critical": FindingSeverity.HIGH,
    "error": FindingSeverity.MEDIUM,
    "warning": FindingSeverity.LOW,
    "info": FindingSeverity.INFO,
}


# for parsing incoming data
class PrometheusAlert(BaseModel):
    endsAt: datetime
    generatorURL: str
    startsAt: datetime
    fingerprint: Optional[str] = ""
    status: str
    labels: Dict[Any, Any]
    annotations: Dict[Any, Any]


# for parsing incoming data
class AlertManagerEvent(BaseModel):
    alerts: List[PrometheusAlert] = []
    externalURL: str
    groupKey: str
    version: str
    commonAnnotations: Optional[Dict[Any, Any]] = None
    commonLabels: Optional[Dict[Any, Any]] = None
    groupLabels: Optional[Dict[Any, Any]] = None
    receiver: str
    status: str


# everything here needs to be optional due to annoying subtleties regarding dataclass inheritance
# see explanation in the code for BaseEvent
@dataclass
class PrometheusKubernetesAlert(
    PodEvent, NodeEvent, DeploymentEvent, JobEvent, DaemonSetEvent, StatefulSetEvent, HorizontalPodAutoscalerEvent
):
    alert: Optional[PrometheusAlert] = None
    alert_name: Optional[str] = None
    alert_severity: Optional[str] = None
    label_namespace: Optional[str] = None
    node: Optional[Node] = None
    pod: Optional[RobustaPod] = None
    deployment: Optional[RobustaDeployment] = None
    job: Optional[RobustaJob] = None
    daemonset: Optional[DaemonSet] = None
    statefulset: Optional[StatefulSet] = None
    hpa: Optional[HorizontalPodAutoscaler] = None

    def get_node(self) -> Optional[Node]:
        return self.node

    def get_pod(self) -> Optional[RobustaPod]:
        return self.pod

    def get_deployment(self) -> Optional[RobustaDeployment]:
        return self.deployment

    def get_job(self) -> Optional[RobustaJob]:
        return self.job

    def get_alert_label(self, label: str) -> Optional[str]:
        return self.alert.labels.get(label, None)

    def get_daemonset(self) -> Optional[DaemonSet]:
        return self.daemonset

    def get_hpa(self) -> Optional[HorizontalPodAutoscaler]:
        return self.hpa

    def get_title(self) -> str:
        annotations = self.alert.annotations
        if annotations.get("summary"):
            return f'{annotations["summary"]}'
        else:
            return self.alert.labels.get("alertname", "")

    def get_prometheus_query(self) -> str:
        """
        Gets the prometheus query that defines this alert.
        """
        url = self.alert.generatorURL
        if not url:
            return ""

        try:
            # decode HTML entities to convert &#43; like representations to characters
            url = html.unescape(url)
            parsed_url = urlparse(url)
            query_params = parse_qs(parsed_url.query)

            q_expr = query_params.get("g0.expr", [])
            if len(q_expr) < 1 or not q_expr[0]:
                return ""

            return unquote(q_expr[0])

        except Exception as e:
            logging.error(f"An error occured in 'get_prometheus_query' | URL: {url} | Error: {e}")
            return ""

    def get_description(self) -> str:
        annotations = self.alert.annotations
        clean_description = ""
        runbook_url = ""
        if annotations.get("description"):
            # remove "LABELS = map[...]" from the description as we already add a TableBlock with labels
            clean_description = re.sub(r"LABELS = map\[.*\]$", "", annotations["description"])
            if annotations.get("runbook_url"):
                runbook_url = f"\n Runbook: <{annotations.get('runbook_url')}|Value>"
        return f"{clean_description} {runbook_url}"

    def get_alert_subject(self) -> FindingSubject:
        subject_type: FindingSubjectType = FindingSubjectType.TYPE_NONE
        name: Optional[str] = "Unresolved"
        namespace: Optional[str] = self.label_namespace
        node_name: Optional[str] = None
        container: Optional[str] = self.alert.labels.get("container")
        labels = {}
        annotations = {}
        if self.deployment:
            subject_type = FindingSubjectType.TYPE_DEPLOYMENT
            name = self.deployment.metadata.name
            namespace = self.deployment.metadata.namespace
            labels = self.deployment.metadata.labels
            annotations = self.deployment.metadata.annotations
        elif self.daemonset:
            subject_type = FindingSubjectType.TYPE_DAEMONSET
            name = self.daemonset.metadata.name
            namespace = self.daemonset.metadata.namespace
            labels = self.daemonset.metadata.labels
            annotations = self.daemonset.metadata.annotations
        elif self.statefulset:
            subject_type = FindingSubjectType.TYPE_STATEFULSET
            name = self.statefulset.metadata.name
            namespace = self.statefulset.metadata.namespace
            labels = self.statefulset.metadata.labels
            annotations = self.statefulset.metadata.annotations
        elif self.node:
            subject_type = FindingSubjectType.TYPE_NODE
            name = self.node.metadata.name
            node_name = self.node.metadata.name
            labels = self.node.metadata.labels
            annotations = self.node.metadata.annotations
        elif self.pod:
            subject_type = FindingSubjectType.TYPE_POD
            name = self.pod.metadata.name
            namespace = self.pod.metadata.namespace
            node_name = self.pod.spec.nodeName
            labels = self.pod.metadata.labels
            annotations = self.pod.metadata.annotations
        elif self.job:
            subject_type = FindingSubjectType.TYPE_JOB
            name = self.job.metadata.name
            namespace = self.job.metadata.namespace
            labels = self.job.metadata.labels
            annotations = self.job.metadata.annotations
        elif self.hpa:
            subject_type = FindingSubjectType.TYPE_HPA
            name = self.hpa.metadata.name
            labels = self.hpa.metadata.labels
            annotations = self.hpa.metadata.annotations

        # Add alert labels and annotations. On duplicates, alert labels/annotations are taken
        labels = {**labels, **self.alert.labels}
        annotations = {**annotations, **self.alert.annotations}

        return FindingSubject(name, subject_type, namespace, node_name, container, labels, annotations)

    def create_default_finding(self) -> Finding:
        alert_subject = self.get_alert_subject()
        status_message = "[RESOLVED] " if self.alert.status.lower() == "resolved" else ""
        title = f"{status_message}{self.get_title()}"
        # AlertManager sends 0001-01-01T00:00:00Z when there's no end date
        ends_at = self.alert.endsAt if self.alert.endsAt.timestamp() > 0 else None
        return Finding(
            title=title,
            description=self.get_description(),
            source=FindingSource.PROMETHEUS,
            aggregation_key=self.alert_name,
            severity=SEVERITY_MAP.get(self.alert.labels.get("severity"), FindingSeverity.INFO),
            subject=alert_subject,
            fingerprint=self.alert.fingerprint,
            starts_at=self.alert.startsAt,
            ends_at=ends_at,
            add_silence_url=True,
            silence_labels=self.alert.labels,
        )

    def get_subject(self) -> FindingSubject:
        return self.get_alert_subject()

    @classmethod
    def get_source(cls) -> FindingSource:
        return FindingSource.PROMETHEUS

    def get_resource(self) -> Optional[Union[RobustaPod, DaemonSet, RobustaDeployment, StatefulSet, Node, RobustaJob]]:
        kind = self.get_subject().subject_type
        if kind == FindingSubjectType.TYPE_DEPLOYMENT:
            return self.deployment
        elif kind == FindingSubjectType.TYPE_DAEMONSET:
            return self.daemonset
        elif kind == FindingSubjectType.TYPE_STATEFULSET:
            return self.statefulset
        elif kind == FindingSubjectType.TYPE_NODE:
            return self.node
        elif kind == FindingSubjectType.TYPE_POD:
            return self.pod
        elif kind == FindingSubjectType.TYPE_JOB:
            return self.job
        elif kind == FindingSubjectType.TYPE_HPA:
            return self.hpa
        return None
