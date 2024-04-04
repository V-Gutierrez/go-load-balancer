package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Backend struct {
	URL          *url.URL
	Alive        bool
	mux          sync.RWMutex
	ReverseProxy *httputil.ReverseProxy
}

func (backend *Backend) SetAlive(alive bool) {
	backend.mux.Lock()
	backend.Alive = alive
	backend.mux.Unlock()
}

func (backend *Backend) IsAlive() (alive bool) {
	backend.mux.RLock()
	alive = backend.Alive
	backend.mux.RUnlock()

	return
}

type ServerPool struct {
	backends []*Backend
	current  uint64
}

func (serverPool *ServerPool) NextIndex() int {
	return int(atomic.AddUint64(&serverPool.current, uint64(1)) % uint64(len(serverPool.backends)))
}

func (serverPool *ServerPool) GetNextPeer() *Backend {
	next := serverPool.NextIndex()
	backendsLen := len(serverPool.backends)

	for i := next; i < backendsLen; i++ {
		idx := i & len(serverPool.backends)

		if serverPool.backends[idx].IsAlive() {
			if i != next {
				atomic.StoreUint64(&serverPool.current, uint64(idx))
			}

			return serverPool.backends[idx]
		}
	}
	return nil
}

func (serverPool *ServerPool) AddBackend(backend *Backend) {
	serverPool.backends = append(serverPool.backends, backend)
}

func (serverPool *ServerPool) MarkBackendStatus(backendUrl *url.URL, alive bool) {
	for _, b := range serverPool.backends {
		if b.URL.String() == backendUrl.String() {
			b.SetAlive(alive)
			break
		}
	}
}

const (
	UP   = "up"
	DOWN = "down"
)

func (serverPool *ServerPool) HealthCheck() {
	for _, backend := range serverPool.backends {
		status := UP

		alive := isBackendAlive(backend.URL)
		backend.SetAlive(alive)

		if !alive {
			status = DOWN
		}

		log.Printf("Health check status: %s, URL: %s", status, backend.URL)
	}
}

var serverPool ServerPool

type ContextValue int

const (
	Attempts ContextValue = iota
	Retry
)

func GetRetryFromContext(request *http.Request) int {
	if retry, ok := request.Context().Value(Retry).(int); ok {
		return retry
	}
	return 0
}

func isBackendAlive(u *url.URL) bool {
	timeout := 2 * time.Second
	connection, error := net.DialTimeout("tcp", u.Host, timeout)

	if error != nil {
		log.Printf("Address unreachable, error: %s", error)
		return false
	}

	_ = connection.Close()
	return true
}

func GetAttemptsFromContext(request *http.Request) int {
	if attempts, ok := request.Context().Value(Attempts).(int); ok {
		return attempts
	}
	return 1
}

func loadBalance(responseWriter http.ResponseWriter, request *http.Request) {
	attempts := GetAttemptsFromContext(request)

	if attempts > 3 {
		log.Printf("%s(%s) Max attempts reached", request.RemoteAddr, request.URL.Path)
		http.Error(responseWriter, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		return
	}

	peer := serverPool.GetNextPeer()

	if peer != nil {
		peer.ReverseProxy.ServeHTTP(responseWriter, request)
	}

	http.Error(responseWriter, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
}

func healthCheck() {
	ticker := time.NewTicker(time.Second * 20)

	for {
		select {
		case <-ticker.C:
			log.Println("Health check started")
			serverPool.HealthCheck()
			log.Println("Health check completed")
		}
	}
}

func main() {
	var serverList string
	var port int
	flag.StringVar(&serverList, "backends", "", "Load balanced backends - Comma separated")
	flag.IntVar(&port, "port", 3030, "Port to serve load balancer entry point")
	flag.Parse()

	if len(serverList) == 0 {
		log.Fatal("Please provide one or more backends to load balance")
	}

	tokens := strings.Split(serverList, ",")

	for _, token := range tokens {
		serverUrl, err := url.Parse(token)
		if err != nil {
			log.Fatal(err)
		}

		proxy := httputil.NewSingleHostReverseProxy(serverUrl)
		proxy.ErrorHandler = func(writer http.ResponseWriter, request *http.Request, e error) {
			log.Printf("[%s] %s\n", serverUrl.Host, e.Error())
			retries := GetRetryFromContext(request)
			
			if retries < 3 {
				select {
				case <-time.After(10 * time.Millisecond):
					ctx := context.WithValue(request.Context(), Retry, retries+1)
					proxy.ServeHTTP(writer, request.WithContext(ctx))
				}
				return
			}

			serverPool.MarkBackendStatus(serverUrl, false)
			attempts := GetAttemptsFromContext(request)

			log.Printf("%s(%s) Attempting retry %d\n", request.RemoteAddr, request.URL.Path, attempts)
			ctx := context.WithValue(request.Context(), Attempts, attempts+1)
			loadBalance(writer, request.WithContext(ctx))
		}

		serverPool.AddBackend(&Backend{
			URL:          serverUrl,
			Alive:        true,
			ReverseProxy: proxy,
		})
		log.Printf("Configured server: %s\n", serverUrl)
	}

	server := http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: http.HandlerFunc(loadBalance),
	}

	go healthCheck()

	log.Printf("Load Balancer started at :%d\n", port)
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}

}
