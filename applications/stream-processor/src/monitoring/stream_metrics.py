"""
Streaming Pipeline Monitoring and Metrics
Provides comprehensive monitoring for the insurance claims streaming pipeline
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import json
import time


class MetricType(Enum):
    """Types of metrics to track"""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


class AlertSeverity(Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class StreamMetric:
    """Individual metric data point"""
    name: str
    value: float
    metric_type: MetricType
    timestamp: datetime
    tags: Dict[str, str] = field(default_factory=dict)
    unit: Optional[str] = None


@dataclass
class Alert:
    """Alert for metric threshold breach"""
    metric_name: str
    severity: AlertSeverity
    message: str
    current_value: float
    threshold: float
    timestamp: datetime
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class SLATarget:
    """SLA target definition"""
    metric_name: str
    target_value: float
    comparison: str  # "less_than", "greater_than", "equal"
    window_minutes: int = 5
    breach_severity: AlertSeverity = AlertSeverity.WARNING


class StreamingMetricsCollector:
    """Collects and manages streaming pipeline metrics"""
    
    def __init__(self):
        self.metrics: List[StreamMetric] = []
        self.alerts: List[Alert] = []
        self.sla_targets: List[SLATarget] = []
        self._initialize_sla_targets()
    
    def _initialize_sla_targets(self):
        """Initialize default SLA targets"""
        self.sla_targets = [
            SLATarget("processing_latency_ms", 200, "less_than", 5, AlertSeverity.ERROR),
            SLATarget("processing_latency_p95_ms", 500, "less_than", 5, AlertSeverity.WARNING),
            SLATarget("error_rate", 0.01, "less_than", 5, AlertSeverity.ERROR),
            SLATarget("throughput_per_second", 100, "greater_than", 5, AlertSeverity.WARNING),
            SLATarget("auto_approval_rate", 0.75, "greater_than", 15, AlertSeverity.INFO),
            SLATarget("fraud_detection_rate", 0.035, "equal", 30, AlertSeverity.INFO),
            SLATarget("data_quality_score", 0.9, "greater_than", 10, AlertSeverity.WARNING),
        ]
    
    def record_metric(
        self,
        name: str,
        value: float,
        metric_type: MetricType = MetricType.GAUGE,
        tags: Optional[Dict[str, str]] = None,
        unit: Optional[str] = None
    ):
        """Record a metric value"""
        metric = StreamMetric(
            name=name,
            value=value,
            metric_type=metric_type,
            timestamp=datetime.utcnow(),
            tags=tags or {},
            unit=unit
        )
        self.metrics.append(metric)
        
        # Check SLA targets
        self._check_sla_breach(metric)
    
    def _check_sla_breach(self, metric: StreamMetric):
        """Check if metric breaches any SLA targets"""
        for sla in self.sla_targets:
            if sla.metric_name == metric.name:
                breach = False
                
                if sla.comparison == "less_than" and metric.value >= sla.target_value:
                    breach = True
                elif sla.comparison == "greater_than" and metric.value <= sla.target_value:
                    breach = True
                elif sla.comparison == "equal":
                    tolerance = sla.target_value * 0.1  # 10% tolerance
                    if abs(metric.value - sla.target_value) > tolerance:
                        breach = True
                
                if breach:
                    self._create_alert(metric, sla)
    
    def _create_alert(self, metric: StreamMetric, sla: SLATarget):
        """Create an alert for SLA breach"""
        alert = Alert(
            metric_name=metric.name,
            severity=sla.breach_severity,
            message=f"SLA breach: {metric.name} is {metric.value:.2f} (target: {sla.comparison} {sla.target_value})",
            current_value=metric.value,
            threshold=sla.target_value,
            timestamp=datetime.utcnow(),
            tags=metric.tags
        )
        self.alerts.append(alert)
    
    def get_recent_metrics(
        self,
        metric_name: Optional[str] = None,
        window_minutes: int = 5
    ) -> List[StreamMetric]:
        """Get recent metrics within time window"""
        cutoff_time = datetime.utcnow() - timedelta(minutes=window_minutes)
        
        recent_metrics = [
            m for m in self.metrics
            if m.timestamp >= cutoff_time
        ]
        
        if metric_name:
            recent_metrics = [m for m in recent_metrics if m.name == metric_name]
        
        return recent_metrics
    
    def calculate_aggregates(
        self,
        metric_name: str,
        window_minutes: int = 5
    ) -> Dict[str, float]:
        """Calculate aggregate statistics for a metric"""
        metrics = self.get_recent_metrics(metric_name, window_minutes)
        
        if not metrics:
            return {}
        
        values = [m.value for m in metrics]
        
        return {
            "count": len(values),
            "sum": sum(values),
            "avg": sum(values) / len(values),
            "min": min(values),
            "max": max(values),
            "latest": values[-1] if values else None
        }
    
    def get_active_alerts(
        self,
        severity: Optional[AlertSeverity] = None
    ) -> List[Alert]:
        """Get active alerts, optionally filtered by severity"""
        # Alerts are considered active for 15 minutes
        cutoff_time = datetime.utcnow() - timedelta(minutes=15)
        
        active_alerts = [
            a for a in self.alerts
            if a.timestamp >= cutoff_time
        ]
        
        if severity:
            active_alerts = [a for a in active_alerts if a.severity == severity]
        
        return active_alerts


class PipelineHealthMonitor:
    """Monitors overall pipeline health"""
    
    def __init__(self, metrics_collector: StreamingMetricsCollector):
        self.metrics = metrics_collector
        self.health_checks = []
    
    def check_bronze_layer_health(self) -> Dict[str, Any]:
        """Check Bronze layer health status"""
        health = {
            "layer": "bronze",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "healthy",
            "checks": {}
        }
        
        # Check ingestion rate
        ingestion_metrics = self.metrics.calculate_aggregates("bronze_ingestion_rate", 5)
        if ingestion_metrics:
            health["checks"]["ingestion_rate"] = {
                "value": ingestion_metrics.get("avg", 0),
                "status": "healthy" if ingestion_metrics.get("avg", 0) > 10 else "degraded"
            }
        
        # Check data quality
        quality_metrics = self.metrics.calculate_aggregates("bronze_data_quality", 5)
        if quality_metrics:
            health["checks"]["data_quality"] = {
                "value": quality_metrics.get("avg", 0),
                "status": "healthy" if quality_metrics.get("avg", 0) > 0.8 else "unhealthy"
            }
        
        # Check dead letter queue
        dlq_metrics = self.metrics.calculate_aggregates("bronze_dlq_count", 5)
        if dlq_metrics:
            health["checks"]["dead_letter_queue"] = {
                "value": dlq_metrics.get("sum", 0),
                "status": "healthy" if dlq_metrics.get("sum", 0) < 100 else "warning"
            }
        
        # Determine overall status
        statuses = [check["status"] for check in health["checks"].values()]
        if "unhealthy" in statuses:
            health["status"] = "unhealthy"
        elif "degraded" in statuses or "warning" in statuses:
            health["status"] = "degraded"
        
        return health
    
    def check_silver_layer_health(self) -> Dict[str, Any]:
        """Check Silver layer health status"""
        health = {
            "layer": "silver",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "healthy",
            "checks": {}
        }
        
        # Check processing latency
        latency_metrics = self.metrics.calculate_aggregates("silver_processing_latency", 5)
        if latency_metrics:
            health["checks"]["processing_latency"] = {
                "value": latency_metrics.get("avg", 0),
                "status": "healthy" if latency_metrics.get("avg", 0) < 200 else "degraded"
            }
        
        # Check validation pass rate
        validation_metrics = self.metrics.calculate_aggregates("silver_validation_rate", 5)
        if validation_metrics:
            health["checks"]["validation_rate"] = {
                "value": validation_metrics.get("avg", 0),
                "status": "healthy" if validation_metrics.get("avg", 0) > 0.95 else "warning"
            }
        
        # Check enrichment success
        enrichment_metrics = self.metrics.calculate_aggregates("silver_enrichment_success", 5)
        if enrichment_metrics:
            health["checks"]["enrichment_success"] = {
                "value": enrichment_metrics.get("avg", 0),
                "status": "healthy" if enrichment_metrics.get("avg", 0) > 0.98 else "degraded"
            }
        
        # Determine overall status
        statuses = [check["status"] for check in health["checks"].values()]
        if "unhealthy" in statuses:
            health["status"] = "unhealthy"
        elif "degraded" in statuses or "warning" in statuses:
            health["status"] = "degraded"
        
        return health
    
    def check_gold_layer_health(self) -> Dict[str, Any]:
        """Check Gold layer health status"""
        health = {
            "layer": "gold",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "healthy",
            "checks": {}
        }
        
        # Check aggregation lag
        lag_metrics = self.metrics.calculate_aggregates("gold_aggregation_lag", 5)
        if lag_metrics:
            health["checks"]["aggregation_lag"] = {
                "value": lag_metrics.get("avg", 0),
                "status": "healthy" if lag_metrics.get("avg", 0) < 60 else "warning"
            }
        
        # Check KPI calculation
        kpi_metrics = self.metrics.calculate_aggregates("gold_kpi_calculation_time", 5)
        if kpi_metrics:
            health["checks"]["kpi_calculation"] = {
                "value": kpi_metrics.get("avg", 0),
                "status": "healthy" if kpi_metrics.get("avg", 0) < 30 else "degraded"
            }
        
        # Determine overall status
        statuses = [check["status"] for check in health["checks"].values()]
        if "unhealthy" in statuses:
            health["status"] = "unhealthy"
        elif "degraded" in statuses or "warning" in statuses:
            health["status"] = "degraded"
        
        return health
    
    def get_overall_health(self) -> Dict[str, Any]:
        """Get overall pipeline health status"""
        bronze_health = self.check_bronze_layer_health()
        silver_health = self.check_silver_layer_health()
        gold_health = self.check_gold_layer_health()
        
        layer_statuses = [
            bronze_health["status"],
            silver_health["status"],
            gold_health["status"]
        ]
        
        if "unhealthy" in layer_statuses:
            overall_status = "unhealthy"
        elif "degraded" in layer_statuses:
            overall_status = "degraded"
        else:
            overall_status = "healthy"
        
        # Get active alerts
        critical_alerts = self.metrics.get_active_alerts(AlertSeverity.CRITICAL)
        error_alerts = self.metrics.get_active_alerts(AlertSeverity.ERROR)
        warning_alerts = self.metrics.get_active_alerts(AlertSeverity.WARNING)
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "overall_status": overall_status,
            "layers": {
                "bronze": bronze_health,
                "silver": silver_health,
                "gold": gold_health
            },
            "alerts": {
                "critical": len(critical_alerts),
                "error": len(error_alerts),
                "warning": len(warning_alerts)
            },
            "metrics_summary": self._get_key_metrics_summary()
        }
    
    def _get_key_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of key metrics"""
        return {
            "throughput": self.metrics.calculate_aggregates("throughput_per_second", 5),
            "latency": self.metrics.calculate_aggregates("processing_latency_ms", 5),
            "error_rate": self.metrics.calculate_aggregates("error_rate", 5),
            "fraud_detection": self.metrics.calculate_aggregates("fraud_detection_rate", 15),
            "auto_approval": self.metrics.calculate_aggregates("auto_approval_rate", 15)
        }


class CostOptimizationMonitor:
    """Monitor and optimize streaming costs"""
    
    def __init__(self):
        self.cost_metrics = {}
        self.optimization_recommendations = []
    
    def calculate_streaming_costs(
        self,
        events_processed: int,
        compute_hours: float,
        storage_gb: float,
        network_gb: float
    ) -> Dict[str, float]:
        """Calculate estimated streaming costs"""
        
        # Azure pricing estimates (simplified)
        costs = {
            "event_hub": events_processed * 0.000001,  # Per event
            "databricks_compute": compute_hours * 0.5,  # Per DBU hour
            "storage": storage_gb * 0.02,  # Per GB/month
            "network": network_gb * 0.01,  # Per GB egress
        }
        
        costs["total"] = sum(costs.values())
        
        return costs
    
    def get_optimization_recommendations(
        self,
        metrics: StreamingMetricsCollector
    ) -> List[str]:
        """Get cost optimization recommendations"""
        
        recommendations = []
        
        # Check for over-provisioning
        throughput = metrics.calculate_aggregates("throughput_per_second", 60)
        if throughput and throughput.get("avg", 0) < 50:
            recommendations.append(
                "Consider reducing Event Hub throughput units - current usage is below 50 events/sec"
            )
        
        # Check for inefficient processing
        latency = metrics.calculate_aggregates("processing_latency_ms", 60)
        if latency and latency.get("avg", 0) > 500:
            recommendations.append(
                "High processing latency detected - consider optimizing transformations or increasing compute"
            )
        
        # Check for data skew
        partition_metrics = metrics.calculate_aggregates("partition_skew", 30)
        if partition_metrics and partition_metrics.get("max", 0) > 2.0:
            recommendations.append(
                "Data skew detected - consider repartitioning strategy for better load distribution"
            )
        
        # Check retention policies
        storage_metrics = metrics.calculate_aggregates("storage_gb_used", 60)
        if storage_metrics and storage_metrics.get("latest", 0) > 1000:
            recommendations.append(
                "High storage usage - review data retention policies and archival strategy"
            )
        
        return recommendations


def format_metrics_dashboard(
    health_monitor: PipelineHealthMonitor,
    cost_monitor: CostOptimizationMonitor,
    metrics: StreamingMetricsCollector
) -> str:
    """Format metrics for dashboard display"""
    
    health = health_monitor.get_overall_health()
    
    dashboard = f"""
    ═══════════════════════════════════════════════════════════════
    INSURANCE CLAIMS STREAMING PIPELINE - OPERATIONAL DASHBOARD
    ═══════════════════════════════════════════════════════════════
    
    OVERALL HEALTH: {health['overall_status'].upper()}
    Last Updated: {health['timestamp']}
    
    ┌─────────────────────────────────────────────────────────────┐
    │ LAYER STATUS                                                │
    ├─────────────────────────────────────────────────────────────┤
    │ Bronze: {health['layers']['bronze']['status']:10} │ Silver: {health['layers']['silver']['status']:10} │ Gold: {health['layers']['gold']['status']:10} │
    └─────────────────────────────────────────────────────────────┘
    
    ┌─────────────────────────────────────────────────────────────┐
    │ ACTIVE ALERTS                                               │
    ├─────────────────────────────────────────────────────────────┤
    │ 🔴 Critical: {health['alerts']['critical']:3} │ 🟠 Error: {health['alerts']['error']:3} │ 🟡 Warning: {health['alerts']['warning']:3} │
    └─────────────────────────────────────────────────────────────┘
    
    ┌─────────────────────────────────────────────────────────────┐
    │ KEY METRICS (5 min avg)                                     │
    ├─────────────────────────────────────────────────────────────┤
    """
    
    if health['metrics_summary']['throughput']:
        throughput = health['metrics_summary']['throughput']['avg']
        dashboard += f"│ Throughput: {throughput:.1f} events/sec                           │\n"
    
    if health['metrics_summary']['latency']:
        latency = health['metrics_summary']['latency']['avg']
        dashboard += f"│ Latency: {latency:.0f}ms                                           │\n"
    
    if health['metrics_summary']['error_rate']:
        error_rate = health['metrics_summary']['error_rate']['avg'] * 100
        dashboard += f"│ Error Rate: {error_rate:.2f}%                                      │\n"
    
    if health['metrics_summary']['fraud_detection']:
        fraud_rate = health['metrics_summary']['fraud_detection']['avg'] * 100
        dashboard += f"│ Fraud Detection: {fraud_rate:.2f}%                                 │\n"
    
    if health['metrics_summary']['auto_approval']:
        auto_approval = health['metrics_summary']['auto_approval']['avg'] * 100
        dashboard += f"│ Auto Approval: {auto_approval:.1f}%                                │\n"
    
    dashboard += """└─────────────────────────────────────────────────────────────┘
    
    ┌─────────────────────────────────────────────────────────────┐
    │ COST OPTIMIZATION RECOMMENDATIONS                           │
    ├─────────────────────────────────────────────────────────────┤
    """
    
    recommendations = cost_monitor.get_optimization_recommendations(metrics)
    if recommendations:
        for rec in recommendations[:3]:  # Show top 3
            dashboard += f"│ • {rec[:57]:<57} │\n"
    else:
        dashboard += "│ ✓ No optimization recommendations at this time             │\n"
    
    dashboard += """└─────────────────────────────────────────────────────────────┘
    ═══════════════════════════════════════════════════════════════
    """
    
    return dashboard