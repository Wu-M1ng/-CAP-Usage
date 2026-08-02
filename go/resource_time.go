package main

import (
	"strings"
	"time"
)

// Resource timestamps are presentation values. Keep their instant unchanged
// while serializing them with the dashboard's fixed China Standard Time zone.
func dashboardResourceTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.In(dashboardLocation).Format(time.RFC3339Nano)
}

func dashboardResourceTimeString(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	t, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return value
	}
	return dashboardResourceTime(t)
}

func localizeRequestDetailForResource(detail *RequestDetail) {
	if detail == nil || detail.Timestamp.IsZero() {
		return
	}
	detail.Timestamp = detail.Timestamp.In(dashboardLocation)
}

func localizeRequestDetailsForResource(details []RequestDetail) {
	for i := range details {
		localizeRequestDetailForResource(&details[i])
	}
}

func localizeStatisticsSnapshotForResource(snapshot *StatisticsSnapshot) {
	if snapshot == nil {
		return
	}
	for apiName, apiSnapshot := range snapshot.APIs {
		for modelName, modelSnapshot := range apiSnapshot.Models {
			localizeRequestDetailsForResource(modelSnapshot.Details)
			apiSnapshot.Models[modelName] = modelSnapshot
		}
		snapshot.APIs[apiName] = apiSnapshot
	}
}

func localizeDashboardSummaryForResource(summary *DashboardSummary) {
	if summary == nil {
		return
	}
	summary.GeneratedAt = dashboardResourceTimeString(summary.GeneratedAt)
	summary.Meta.LastRecordedAt = dashboardResourceTimeString(summary.Meta.LastRecordedAt)
	localizeStorageStatusForResource(&summary.Meta.Storage)
	for i := range summary.HealthGrid {
		summary.HealthGrid[i].Start = dashboardResourceTimeString(summary.HealthGrid[i].Start)
		summary.HealthGrid[i].End = dashboardResourceTimeString(summary.HealthGrid[i].End)
	}
}

func localizeEventsResultForResource(result *EventsResult) {
	if result == nil {
		return
	}
	result.GeneratedAt = dashboardResourceTimeString(result.GeneratedAt)
	localizeRequestDetailsForResource(result.Events)
}

func localizeAPIDetailForResource(result *APIDetailResponse) {
	if result == nil {
		return
	}
	result.GeneratedAt = dashboardResourceTimeString(result.GeneratedAt)
	localizeRequestDetailsForResource(result.RecentEvents)
}

func localizeStorageStatusForResource(status *StorageStatus) {
	if status == nil {
		return
	}
	status.LastWriteAt = dashboardResourceTimeString(status.LastWriteAt)
}

func localizeRuntimeStatusForResource(status *RuntimeStatus) {
	if status == nil {
		return
	}
	status.StartedAt = dashboardResourceTimeString(status.StartedAt)
	status.LastRecordedAt = dashboardResourceTimeString(status.LastRecordedAt)
}

func localizeModelPricesResponseForResource(response *ModelPricesResponse) {
	if response == nil {
		return
	}
	response.UpdatedAt = dashboardResourceTimeString(response.UpdatedAt)
	response.ModelsDev.LastAttemptAt = dashboardResourceTimeString(response.ModelsDev.LastAttemptAt)
	response.ModelsDev.LastSuccessAt = dashboardResourceTimeString(response.ModelsDev.LastSuccessAt)
	response.ModelsDev.UpdatedAt = dashboardResourceTimeString(response.ModelsDev.UpdatedAt)
}
