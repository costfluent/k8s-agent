package collector

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	clienttesting "k8s.io/client-go/testing"

	"github.com/costfluent/k8s-agent/internal/config"
)

func TestNewClient_UsesTheConfiguredRateLimit(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	client, err := NewClient(&rest.Config{Host: srv.URL}, &config.Config{KubeAPIQPS: 30, KubeAPIBurst: 60})
	if err != nil {
		t.Fatal(err)
	}
	if qps := client.CoreV1().RESTClient().GetRateLimiter().QPS(); qps != 30 {
		t.Fatalf("QPS = %v, want the configured 30 rather than client-go's default 5", qps)
	}
}

func TestMetadata_OwnerCacheEvictsTheLeastRecentlyUsed(t *testing.T) {
	var objects []runtime.Object
	for _, name := range []string{"a", "b", "c"} {
		objects = append(objects, &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "rs-" + name, OwnerReferences: controllerRef("Deployment", "deploy-"+name)}})
	}
	client := fake.NewClientset(objects...)
	m := NewMetadata(client, &config.Config{OwnerCacheSize: 2}, zap.NewNop())
	gets := func() int {
		n := 0
		for _, a := range client.Actions() {
			if a.GetVerb() == "get" {
				n++
			}
		}
		return n
	}
	for i, step := range []struct {
		owner   string
		fetched bool
	}{
		{"a", true}, {"b", true}, {"a", false}, {"c", true}, {"b", true}, {"a", true}, {"b", false}, {"c", true},
	} {
		before := gets()
		kind, name := m.resolveController(context.Background(), &cachedPod{Namespace: "shop", OwnerKind: "ReplicaSet", OwnerName: "rs-" + step.owner})
		if kind != "Deployment" || name != "deploy-"+step.owner {
			t.Fatalf("step %d: resolved %s/%s", i, kind, name)
		}
		if fetched := gets() > before; fetched != step.fetched {
			t.Fatalf("step %d (%s): fetched = %v, want %v; the cache must keep the two most recently used owners", i, step.owner, fetched, step.fetched)
		}
	}
}

func getsOf(client *fake.Clientset, resource string) int {
	n := 0
	for _, a := range client.Actions() {
		if a.GetVerb() == "get" && a.GetResource().Resource == resource {
			n++
		}
	}
	return n
}

func TestMetadata_RetriesAControllerWhoseLookupFailed(t *testing.T) {
	for _, tc := range []struct {
		ownerKind, ownerName, resource, wantKind, wantName string
		object                                             runtime.Object
	}{
		{"ReplicaSet", "web-7f9", "replicasets", "Deployment", "web",
			&appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "web-7f9", OwnerReferences: controllerRef("Deployment", "web")}}},
		{"Job", "report-2911", "jobs", "CronJob", "report",
			&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "report-2911", OwnerReferences: controllerRef("CronJob", "report")}}},
	} {
		t.Run(tc.ownerKind, func(t *testing.T) {
			client := fake.NewClientset(tc.object)
			failed := false
			client.PrependReactor("get", tc.resource, func(clienttesting.Action) (bool, runtime.Object, error) {
				if failed {
					return false, nil, nil
				}
				failed = true
				return true, nil, apierrors.NewTooManyRequests("throttled", 1)
			})
			m := NewMetadata(client, &config.Config{OwnerCacheSize: 8}, zap.NewNop())
			pod := &cachedPod{Namespace: "shop", OwnerKind: tc.ownerKind, OwnerName: tc.ownerName}

			for i, step := range []struct {
				kind, name string
				gets       int
			}{
				{tc.ownerKind, tc.ownerName, 1}, // the throttled GET falls back to the direct owner
				{tc.wantKind, tc.wantName, 2},   // the fallback was not cached, so the next poll asks again
				{tc.wantKind, tc.wantName, 2},   // the resolved controller is cached
			} {
				kind, name := m.resolveController(context.Background(), pod)
				if kind != step.kind || name != step.name {
					t.Fatalf("call %d: resolved %s/%s, want %s/%s; a failed lookup's fallback must not stick", i+1, kind, name, step.kind, step.name)
				}
				if got := getsOf(client, tc.resource); got != step.gets {
					t.Fatalf("call %d: %d GETs of %s, want %d", i+1, got, tc.resource, step.gets)
				}
			}
		})
	}
}

func TestMetadata_AnUnownedReplicaSetOrJobResolvesToItselfOnce(t *testing.T) {
	client := fake.NewClientset(
		&appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "bare-rs"}},
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "bare-job"}},
	)
	m := NewMetadata(client, &config.Config{OwnerCacheSize: 8}, zap.NewNop())
	for _, tc := range []struct{ kind, name, resource string }{
		{"ReplicaSet", "bare-rs", "replicasets"},
		{"Job", "bare-job", "jobs"},
	} {
		for call := 1; call <= 2; call++ {
			kind, name := m.resolveController(context.Background(), &cachedPod{Namespace: "shop", OwnerKind: tc.kind, OwnerName: tc.name})
			if kind != tc.kind || name != tc.name {
				t.Fatalf("%s call %d: resolved %s/%s, want the owner itself", tc.kind, call, kind, name)
			}
		}
		if got := getsOf(client, tc.resource); got != 1 {
			t.Fatalf("%s: %d GETs across two calls, want 1; an owner with no controller of its own must be cached too", tc.kind, got)
		}
	}
}

func TestMetadata_StartGivesUpWhenTheCachesNeverSync(t *testing.T) {
	client := fake.NewClientset()
	client.PrependReactor("list", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "", errors.New("RBAC: pods is forbidden"))
	})
	m := NewMetadata(client, &config.Config{}, zap.NewNop())
	m.syncTimeout = 200 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- m.Start(ctx) }()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "did not sync within 200ms") || !strings.Contains(err.Error(), "list and watch") {
			t.Fatalf("err = %v, want a sync timeout naming the permission to check", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return when the pod list was forbidden; the agent would hang with /healthz green")
	}
}
