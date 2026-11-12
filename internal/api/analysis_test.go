package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nafaz/spot-perp-scanner/internal/config"
)

func TestAnalysisEndpointRequiresPostAndConfiguredService(t *testing.T) {
	server:=New(nil,nil,config.Config{Mode:"MOCK"})
	handler:=server.Handler()

	get:=httptest.NewRecorder()
	handler.ServeHTTP(get,httptest.NewRequest(http.MethodGet,"/api/v1/analysis/scan",nil))
	if get.Code!=http.StatusMethodNotAllowed { t.Fatalf("GET status=%d, want 405",get.Code) }

	post:=httptest.NewRecorder()
	handler.ServeHTTP(post,httptest.NewRequest(http.MethodPost,"/api/v1/analysis/scan",nil))
	if post.Code!=http.StatusServiceUnavailable { t.Fatalf("unconfigured POST status=%d, want 503",post.Code) }
}
