package statistics

import (
	"net/http"
)

func init() {
	http.HandleFunc("/monitor/start", httpAPIStartMonitorHandler)
	http.HandleFunc("/monitor/stop", httpAPIStopMonitorHandler)
	http.HandleFunc("/monitor/get", httpAPIGetMonitorHandler)
}

func httpAPIStartMonitorHandler(w http.ResponseWriter, r *http.Request) {
	const paramMonitorAddr = "monitorAddr"

	defer func() {
		httpAPIGetMonitorHandler(w, r)
	}()

	_ = r.ParseForm()
	var monitorAddr string
	monitorAddr = r.FormValue(paramMonitorAddr)

	if monitorAddr == "" || StatisticsModule != nil {
		return
	}
	StatisticsModule = newStatistics(monitorAddr, targetModuleName, targetNodeAddr)
	go StatisticsModule.summaryJob(targetSummaryFunc)
	go StatisticsModule.reportJob()

	return
}

func httpAPIStopMonitorHandler(w http.ResponseWriter, r *http.Request) {
	if StatisticsModule != nil {
		StatisticsModule.CloseStatistics()
		StatisticsModule = nil
		w.Write([]byte("close monitor successfully"))
	} else {
		w.Write([]byte("monitor address is not configured"))
	}

}

func httpAPIGetMonitorHandler(w http.ResponseWriter, r *http.Request) {
	if StatisticsModule == nil {
		_, _ = w.Write([]byte("monitor address is not configured"))
	} else {
		_, _ = w.Write([]byte(StatisticsModule.String()))
	}
}