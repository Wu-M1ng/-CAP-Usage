package main

func sanitizeRequestDetailAPIKeyForOutput(detail RequestDetail) RequestDetail {
	detail.APIKey = maskAPIKey(detail.APIKey)
	detail.UpstreamAPI = sanitizeSensitiveTextForOutput(detail.UpstreamAPI)
	detail.Source = sanitizeSensitiveTextForOutput(detail.Source)
	detail.Provider = sanitizeSensitiveTextForOutput(detail.Provider)
	detail.AuthID = sanitizeSensitiveTextForOutput(detail.AuthID)
	detail.AuthIndex = sanitizeSensitiveTextForOutput(detail.AuthIndex)
	detail.Endpoint = sanitizeSensitiveTextForOutput(detail.Endpoint)
	detail.BaseURL = sanitizeSensitiveTextForOutput(detail.BaseURL)
	detail.Failure = sanitizeSensitiveTextForOutput(detail.Failure)
	if len(detail.Headers) > 0 {
		headers := make(map[string][]string, len(detail.Headers))
		for name, values := range detail.Headers {
			cleaned := append([]string(nil), values...)
			for i := range cleaned {
				cleaned[i] = sanitizeSensitiveTextForOutput(cleaned[i])
			}
			headers[name] = cleaned
		}
		detail.Headers = headers
	}
	return detail
}

func sanitizeRequestDetailsAPIKeysForOutput(details []RequestDetail) []RequestDetail {
	if len(details) == 0 {
		return details
	}
	result := append([]RequestDetail(nil), details...)
	for i := range result {
		result[i] = sanitizeRequestDetailAPIKeyForOutput(result[i])
	}
	return result
}

func sanitizeSnapshotAPIKeysForOutput(snapshot *StatisticsSnapshot) {
	if snapshot == nil {
		return
	}
	for apiName, apiSnapshot := range snapshot.APIs {
		for modelName, modelSnapshot := range apiSnapshot.Models {
			modelSnapshot.Details = sanitizeRequestDetailsAPIKeysForOutput(modelSnapshot.Details)
			apiSnapshot.Models[modelName] = modelSnapshot
		}
		snapshot.APIs[apiName] = apiSnapshot
	}
}

func sanitizeDashboardSummaryAPIKeysForOutput(summary *DashboardSummary) {
	if summary == nil {
		return
	}
	summary.ClientAPIStats = append([]ClientAPIStat(nil), summary.ClientAPIStats...)
	for i := range summary.ClientAPIStats {
		summary.ClientAPIStats[i].APIKey = maskAPIKey(summary.ClientAPIStats[i].APIKey)
	}
	summary.SourceStats = append([]SourceStat(nil), summary.SourceStats...)
	for i := range summary.SourceStats {
		summary.SourceStats[i].Source = sanitizeSensitiveTextForOutput(summary.SourceStats[i].Source)
		summary.SourceStats[i].Provider = sanitizeSensitiveTextForOutput(summary.SourceStats[i].Provider)
	}
	summary.CredentialStats = append([]CredentialStat(nil), summary.CredentialStats...)
	for i := range summary.CredentialStats {
		summary.CredentialStats[i].AuthIndex = sanitizeSensitiveTextForOutput(summary.CredentialStats[i].AuthIndex)
	}
}

func sanitizeEventsAPIKeysForOutput(result *EventsResult) {
	if result == nil {
		return
	}
	result.Events = sanitizeRequestDetailsAPIKeysForOutput(result.Events)
}

func sanitizeAPIDetailAPIKeysForOutput(result *APIDetailResponse) {
	if result == nil {
		return
	}
	result.RecentEvents = sanitizeRequestDetailsAPIKeysForOutput(result.RecentEvents)
	result.API = sanitizeSensitiveTextForOutput(result.API)
	result.SourceStats = append([]SourceStat(nil), result.SourceStats...)
	for i := range result.SourceStats {
		result.SourceStats[i].Source = sanitizeSensitiveTextForOutput(result.SourceStats[i].Source)
		result.SourceStats[i].Provider = sanitizeSensitiveTextForOutput(result.SourceStats[i].Provider)
	}
	result.ErrorStats = append([]APIDetailErrorStat(nil), result.ErrorStats...)
	for i := range result.ErrorStats {
		result.ErrorStats[i].Failure = sanitizeSensitiveTextForOutput(result.ErrorStats[i].Failure)
	}
}
