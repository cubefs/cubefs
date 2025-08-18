package flashgroupmanager

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/gorilla/mux"
)

func (m *FlashGroupManager) startHTTPService(modulename string, cfg *config.Config) {
	router := mux.NewRouter().SkipClean(true)
	m.registerAPIRoutes(router)
	m.registerAPIMiddleware(router)
	exporter.InitWithRouter(modulename, cfg, router, m.port)
	addr := fmt.Sprintf(":%s", m.port)
	if m.bindIp {
		addr = fmt.Sprintf("%s:%s", m.ip, m.port)
	}

	server := &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  5 * time.Minute,
		WriteTimeout: 5 * time.Minute,
	}

	serveAPI := func() {
		if err := server.ListenAndServe(); err != nil {
			log.LogErrorf("serveAPI: serve http server failed: err(%v)", err)
			return
		}
	}
	go serveAPI()
	m.apiServer = server
}

func (m *FlashGroupManager) registerAPIRoutes(router *mux.Router) {
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminGetCluster).
		HandlerFunc(m.getCluster)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminGetNodeInfo).
		HandlerFunc(m.getNodeInfoHandler)
	router.NewRoute().Name(proto.AdminGetIP).
		Methods(http.MethodGet).
		Path(proto.AdminGetIP).
		HandlerFunc(m.getIPAddr)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminChangeMasterLeader).
		HandlerFunc(m.changeMasterLeader)

	// TODO: set by zone in future
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminSetNodeInfo).
		HandlerFunc(m.setNodeInfoHandler)

	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AddRaftNode).
		HandlerFunc(m.addRaftNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.RemoveRaftNode).
		HandlerFunc(m.removeRaftNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.RaftStatus).
		HandlerFunc(m.getRaftStatus)

	// remoteCache config
	router.NewRoute().Methods(http.MethodGet).Path(proto.AdminGetRemoteCacheConfig).HandlerFunc(m.getRemoteCacheConfig)

	// APIs for FlashGroup
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).Path(proto.AdminFlashGroupTurn).HandlerFunc(m.turnFlashGroup)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).Path(proto.AdminFlashGroupCreate).HandlerFunc(m.createFlashGroup)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).Path(proto.AdminFlashGroupSet).HandlerFunc(m.setFlashGroup)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).Path(proto.AdminFlashGroupRemove).HandlerFunc(m.removeFlashGroup)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).Path(proto.AdminFlashGroupNodeAdd).HandlerFunc(m.flashGroupAddFlashNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).Path(proto.AdminFlashGroupNodeRemove).HandlerFunc(m.flashGroupRemoveFlashNode)
	router.NewRoute().Methods(http.MethodGet).Path(proto.AdminFlashGroupGet).HandlerFunc(m.getFlashGroup)
	router.NewRoute().Methods(http.MethodGet).Path(proto.AdminFlashGroupList).HandlerFunc(m.listFlashGroups)
	router.NewRoute().Methods(http.MethodGet).Path(proto.ClientFlashGroups).HandlerFunc(m.clientFlashGroups)

	// APIs for FlashNode
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).Path(proto.FlashNodeAdd).HandlerFunc(m.addFlashNode)
	router.NewRoute().Methods(http.MethodGet).Path(proto.FlashNodeList).HandlerFunc(m.listFlashNodes)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).Path(proto.FlashNodeSet).HandlerFunc(m.setFlashNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).Path(proto.FlashNodeRemove).HandlerFunc(m.removeFlashNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).Path(proto.FlashNodeRemoveAllInactive).HandlerFunc(m.removeAllInactiveFlashNodes)
	router.NewRoute().Methods(http.MethodGet).Path(proto.FlashNodeGet).HandlerFunc(m.getFlashNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).Path(proto.FlashNodeSetReadIOLimits).HandlerFunc(m.setFlashNodeReadIOLimits)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).Path(proto.FlashNodeSetWriteIOLimits).HandlerFunc(m.setFlashNodeWriteIOLimits)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.GetFlashNodeTaskResponse).
		HandlerFunc(m.handleFlashNodeTaskResponse)
}

func (m *FlashGroupManager) registerAPIMiddleware(route *mux.Router) {
	var interceptor mux.MiddlewareFunc = func(next http.Handler) http.Handler {
		return http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				log.LogDebugf("action[interceptor] request, method[%v] path[%v] query[%v]", r.Method, r.URL.Path, r.URL.Query())

				// TODO
				// if m.partition.IsRaftLeader() {
				//	if err := m.cluster.apiLimiter.Wait(r.URL.Path); err != nil {
				//		log.LogWarnf("action[interceptor] too many requests, path[%v]", r.URL.Path)
				//		errMsg := fmt.Sprintf("too many requests for api: %s", html.EscapeString(r.URL.Path))
				//		http.Error(w, errMsg, http.StatusTooManyRequests)
				//		return
				//	}
				// } else {
				//	if m.cluster.apiLimiter.IsFollowerLimiter(r.URL.Path) {
				//		if err := m.cluster.apiLimiter.Wait(r.URL.Path); err != nil {
				//			log.LogWarnf("action[interceptor] too many requests, path[%v]", r.URL.Path)
				//			errMsg := fmt.Sprintf("too many requests for api: %s", html.EscapeString(r.URL.Path))
				//			http.Error(w, errMsg, http.StatusTooManyRequests)
				//			return
				//		}
				//	}
				// }

				log.LogInfof("action[interceptor] request, remote[%v] method[%v] path[%v] query[%v]",
					r.RemoteAddr, r.Method, r.URL.Path, r.URL.Query())
				if mux.CurrentRoute(r).GetName() == proto.AdminGetIP {
					next.ServeHTTP(w, r)
					return
				}

				isFollowerRead := m.isFollowerRead(r)
				if m.partition.IsRaftLeader() || isFollowerRead {
					if m.metaReady || isFollowerRead {
						log.LogDebugf("action[interceptor] request, method[%v] path[%v] query[%v]", r.Method, r.URL.Path, r.URL.Query())
						next.ServeHTTP(w, r)
						return
					}
					log.LogWarnf("action[interceptor] leader meta has not ready")
					http.Error(w, m.leaderInfo.addr, http.StatusBadRequest)
					return
				} else if m.leaderInfo.addr != "" {
					if m.leaderInfo.addr == m.getCurrAddr() {
						log.LogErrorf("action[interceptor] request, self is leader addr, no leader now, method[%v] path[%v] query[%v] status [%v]",
							r.Method, r.URL.Path, r.URL.Query(), m.leaderInfo.addr)
						http.Error(w, "no leader", http.StatusBadRequest)
						return
					}

					m.proxy(w, r)
				} else {
					log.LogErrorf("action[interceptor] no leader,request[%v]", r.URL)
					http.Error(w, "no leader", http.StatusBadRequest)
					return
				}
			})
	}
	route.Use(interceptor)
}

func (m *FlashGroupManager) newReverseProxy() *httputil.ReverseProxy {
	tr := &http.Transport{}
	if m.config != nil {
		tr = proto.GetHttpTransporter(&proto.HttpCfg{
			PoolSize: int(m.config.httpProxyPoolSize),
		})
	}

	return &httputil.ReverseProxy{
		Director: func(request *http.Request) {
			request.URL.Scheme = "http"
			request.URL.Host = m.leaderInfo.addr
		},
		Transport: tr,
	}
}

func (m *FlashGroupManager) proxy(w http.ResponseWriter, r *http.Request) {
	m.reverseProxy.ServeHTTP(w, r)
}
