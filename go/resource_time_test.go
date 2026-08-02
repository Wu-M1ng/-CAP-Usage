package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestDashboardResourceTimeUsesChinaOffset(t *testing.T) {
	when := time.Date(2026, 8, 1, 15, 18, 27, 745217974, time.UTC)
	if got, want := dashboardResourceTime(when), "2026-08-01T23:18:27.745217974+08:00"; got != want {
		t.Fatalf("resource time = %q, want %q", got, want)
	}
	if got, want := dashboardResourceTimeString("2026-08-01T15:59:16Z"), "2026-08-01T23:59:16+08:00"; got != want {
		t.Fatalf("resource time string = %q, want %q", got, want)
	}
}

func TestDashboardResourcePayloadTimesUseChinaOffset(t *testing.T) {
	summary := DashboardSummary{
		GeneratedAt: "2026-08-01T15:59:16Z",
		Meta:        DashboardMeta{LastRecordedAt: "2026-08-01T15:18:27Z"},
		HealthGrid: []HealthGridSlot{{
			Start: "2026-08-01T15:00:00Z",
			End:   "2026-08-01T15:15:00Z",
		}},
	}
	localizeDashboardSummaryForResource(&summary)
	if summary.GeneratedAt != "2026-08-01T23:59:16+08:00" || summary.Meta.LastRecordedAt != "2026-08-01T23:18:27+08:00" {
		t.Fatalf("summary resource times = %#v", summary)
	}
	if summary.HealthGrid[0].Start != "2026-08-01T23:00:00+08:00" || summary.HealthGrid[0].End != "2026-08-01T23:15:00+08:00" {
		t.Fatalf("health grid resource times = %#v", summary.HealthGrid[0])
	}

	snapshot := StatisticsSnapshot{
		APIs: map[string]APISnapshot{
			"openai": {Models: map[string]ModelSnapshot{
				"gpt-4.1": {Details: []RequestDetail{{
					Timestamp: time.Date(2026, 8, 1, 15, 18, 27, 0, time.UTC),
				}}},
			}},
		},
	}
	localizeStatisticsSnapshotForResource(&snapshot)
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal localized snapshot: %v", err)
	}
	if !strings.Contains(string(raw), "2026-08-01T23:18:27+08:00") {
		t.Fatalf("localized snapshot JSON = %s", raw)
	}
}
