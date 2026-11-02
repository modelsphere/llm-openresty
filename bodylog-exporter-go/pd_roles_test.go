package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

// gauge reads one series WITHOUT creating it. WithLabelValues and
// GetMetricWithLabelValues both instantiate a missing series at 0, so asserting
// through them cannot tell "reported as zero" from "never reported" -- which is
// the single distinction these tests exist to make.
func gauge(t *testing.T, c prometheus.Collector, labels map[string]string) (float64, bool) {
	t.Helper()
	ch := make(chan prometheus.Metric, 128)
	c.Collect(ch)
	close(ch)
	for m := range ch {
		var pb dto.Metric
		if err := m.Write(&pb); err != nil {
			t.Fatalf("write metric: %v", err)
		}
		got := map[string]string{}
		for _, lp := range pb.GetLabel() {
			got[lp.GetName()] = lp.GetValue()
		}
		match := len(got) == len(labels)
		for k, v := range labels {
			if got[k] != v {
				match = false
			}
		}
		if match {
			return pb.GetGauge().GetValue(), true
		}
	}
	return 0, false
}

func lbl(svc, route, role string) map[string]string {
	return map[string]string{"service": svc, "route": route, "role": role}
}

const pdMRJSON = `{"items":[
  {"spec":{"nginx":{"route":"qwen"},"discovery":{"service":"pd/qwen-2p1d"}}}
]}`

// Two prefill slices and one decode slice, as the DisaggregatedSet controller
// creates them. The second prefill slice has no status.readyReplicas at all --
// the field is omitempty, which is how a role with nothing ready actually
// arrives -- and the last item belongs to no DisaggregatedSet.
const pdLWSJSON = `{"items":[
  {"metadata":{"namespace":"pd","name":"qwen-2p1d-0-ab12-prefill",
    "labels":{"disaggregatedset.x-k8s.io/name":"qwen-2p1d","disaggregatedset.x-k8s.io/role":"prefill"}},
   "spec":{"replicas":1},"status":{"replicas":1,"readyReplicas":1}},
  {"metadata":{"namespace":"pd","name":"qwen-2p1d-1-ab12-prefill",
    "labels":{"disaggregatedset.x-k8s.io/name":"qwen-2p1d","disaggregatedset.x-k8s.io/role":"prefill"}},
   "spec":{"replicas":1},"status":{"replicas":1}},
  {"metadata":{"namespace":"pd","name":"qwen-2p1d-0-ab12-decode",
    "labels":{"disaggregatedset.x-k8s.io/name":"qwen-2p1d","disaggregatedset.x-k8s.io/role":"decode"}},
   "spec":{"replicas":1},"status":{"replicas":1,"readyReplicas":1}},
  {"metadata":{"namespace":"pd","name":"someone-elses-lws","labels":{}},
   "spec":{"replicas":9},"status":{"replicas":9,"readyReplicas":9}}
]}`

// pdServer answers the three paths a disaggregated release needs. lwsAnswer is
// written verbatim for the LeaderWorkerSet list unless lwsStatus is non-zero, in
// which case that status is returned instead.
func pdServer(t *testing.T, lwsAnswer string, lwsStatus int, lwsHits *int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/modelroutes"):
			_, _ = w.Write([]byte(pdMRJSON))
		case strings.Contains(r.URL.Path, "endpointslices"):
			// The discovery Service selects the routers: two, one of them not ready.
			_, _ = w.Write([]byte(esJSON("10.2.0.1", "10.2.0.2!")))
		case strings.HasSuffix(r.URL.Path, "/leaderworkersets"):
			if lwsHits != nil {
				*lwsHits++
			}
			if lwsStatus != 0 {
				w.WriteHeader(lwsStatus)
				return
			}
			_, _ = w.Write([]byte(lwsAnswer))
		default:
			// The owner walk for the router's desired count; not what these tests assert.
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

// A disaggregated release reports one series per role, under one service.
func TestPDRolesSplitByRole(t *testing.T) {
	srv := pdServer(t, pdLWSJSON, 0, nil)
	defer srv.Close()
	r := newTestResolver(t, srv)
	r.refresh(context.Background())

	const svc, route = "pd/qwen-2p1d", "qwen"
	for _, c := range []struct {
		role       string
		n, ready   float64
		desired    float64
		hasDesired bool
	}{
		// The discovery Service's own EndpointSlice: two routers, one ready.
		{role: "router", n: 2, ready: 1},
		// Two slices, one ready group each in spec, only the first one actually ready.
		{role: "prefill", n: 2, ready: 1, desired: 2, hasDesired: true},
		{role: "decode", n: 1, ready: 1, desired: 1, hasDesired: true},
	} {
		if got, ok := gauge(t, r.replicas, lbl(svc, route, c.role)); !ok || got != c.n {
			t.Errorf("replicas{role=%q} = %v (present=%v), want %v", c.role, got, ok, c.n)
		}
		if got, ok := gauge(t, r.replicasReady, lbl(svc, route, c.role)); !ok || got != c.ready {
			t.Errorf("replicas_ready{role=%q} = %v (present=%v), want %v", c.role, got, ok, c.ready)
		}
		if c.hasDesired {
			if got, ok := gauge(t, r.replicasDesired, lbl(svc, route, c.role)); !ok || got != c.desired {
				t.Errorf("replicas_desired{role=%q} = %v (present=%v), want %v", c.role, got, ok, c.desired)
			}
		}
	}

	// An LWS that no DisaggregatedSet owns must not become a role of its own.
	if n := testutil.CollectAndCount(r.replicas); n != 3 {
		t.Errorf("replicas series = %d, want 3 (router + prefill + decode)", n)
	}
}

// The point of the per-role series: a role that drains has to read 0, not vanish.
// status.readyReplicas is omitempty, so "nothing ready" arrives as an absent
// field -- reading that as "unknown" would leave the drain invisible.
func TestPDRolesZeroReadyIsReported(t *testing.T) {
	const noneReady = `{"items":[
      {"metadata":{"namespace":"pd","name":"qwen-2p1d-0-ab12-decode",
        "labels":{"disaggregatedset.x-k8s.io/name":"qwen-2p1d","disaggregatedset.x-k8s.io/role":"decode"}},
       "spec":{"replicas":2},"status":{"replicas":2}}
    ]}`
	srv := pdServer(t, noneReady, 0, nil)
	defer srv.Close()
	r := newTestResolver(t, srv)
	r.refresh(context.Background())

	// Present, and present as zero -- the two are different answers to the caller.
	got, ok := gauge(t, r.replicasReady, lbl("pd/qwen-2p1d", "qwen", "decode"))
	if !ok {
		t.Fatalf("replicas_ready{role=\"decode\"} missing; a drained role must report 0, not disappear")
	}
	if got != 0 {
		t.Errorf("replicas_ready{role=\"decode\"} = %v, want 0", got)
	}
	if got, ok := gauge(t, r.replicasDesired, lbl("pd/qwen-2p1d", "qwen", "decode")); !ok || got != 2 {
		t.Errorf("replicas_desired{role=\"decode\"} = %v (present=%v), want 2", got, ok)
	}
}

// A role whose spec.replicas is missing gets no desired series rather than a
// zero, for the same reason the service-level desired gauge is skipped: a 0
// there reads as "scaled to zero".
func TestPDRolesNoDesiredWithoutSpecReplicas(t *testing.T) {
	const noSpec = `{"items":[
      {"metadata":{"namespace":"pd","name":"qwen-2p1d-0-ab12-prefill",
        "labels":{"disaggregatedset.x-k8s.io/name":"qwen-2p1d","disaggregatedset.x-k8s.io/role":"prefill"}},
       "status":{"replicas":1,"readyReplicas":1}}
    ]}`
	srv := pdServer(t, noSpec, 0, nil)
	defer srv.Close()
	r := newTestResolver(t, srv)
	r.refresh(context.Background())

	if got, ok := gauge(t, r.replicasReady, lbl("pd/qwen-2p1d", "qwen", "prefill")); !ok || got != 1 {
		t.Errorf("replicas_ready{role=\"prefill\"} = %v (present=%v), want 1", got, ok)
	}
	if n := testutil.CollectAndCount(r.replicasDesired); n != 0 {
		t.Errorf("replicas_desired series = %d, want 0", n)
	}
}

// No LeaderWorkerSet API, or no permission on it: the per-role series are simply
// absent and everything else still reports. The refusal is remembered, so the
// next refresh does not ask again -- a cluster without disaggregated serving
// must not pay a call per round, nor fill the log.
func TestPDRolesUnavailableIsRememberedAndHarmless(t *testing.T) {
	for _, code := range []int{http.StatusNotFound, http.StatusForbidden} {
		hits := 0
		srv := pdServer(t, "", code, &hits)
		r := newTestResolver(t, srv)
		r.refresh(context.Background())
		r.refresh(context.Background())
		srv.Close()

		if hits != 1 {
			t.Errorf("HTTP %d: leaderworkersets asked %d times, want 1", code, hits)
		}
		if r.pdSkipUntil.Before(time.Now()) {
			t.Errorf("HTTP %d: retry not deferred", code)
		}
		// The service still reports, as an ordinary one: role is "" and the counts
		// are the discovery Service's.
		if got, ok := gauge(t, r.replicas, lbl("pd/qwen-2p1d", "qwen", "")); !ok || got != 2 {
			t.Errorf("HTTP %d: replicas{role=\"\"} = %v (present=%v), want 2", code, got, ok)
		}
		if n := testutil.CollectAndCount(r.replicas); n != 1 {
			t.Errorf("HTTP %d: replicas series = %d, want 1", code, n)
		}
	}
}

// A role whose slices only PARTLY report spec.replicas must publish no desired
// at all. Summing the slices that did report gives a smaller number that still
// looks like an answer, and a caller comparing committed against running reads
// that gap as a scale-down.
func TestPDRolesPartialDesiredIsNotPublished(t *testing.T) {
	const partial = `{"items":[
      {"metadata":{"namespace":"pd","name":"qwen-2p1d-0-ab12-prefill",
        "labels":{"disaggregatedset.x-k8s.io/name":"qwen-2p1d","disaggregatedset.x-k8s.io/role":"prefill"}},
       "spec":{"replicas":1},"status":{"replicas":1,"readyReplicas":1}},
      {"metadata":{"namespace":"pd","name":"qwen-2p1d-1-ab12-prefill",
        "labels":{"disaggregatedset.x-k8s.io/name":"qwen-2p1d","disaggregatedset.x-k8s.io/role":"prefill"}},
       "status":{"replicas":1,"readyReplicas":1}}
    ]}`
	srv := pdServer(t, partial, 0, nil)
	defer srv.Close()
	r := newTestResolver(t, srv)
	r.refresh(context.Background())

	// The counts that do cover every slice are still reported.
	if got, ok := gauge(t, r.replicasReady, lbl("pd/qwen-2p1d", "qwen", "prefill")); !ok || got != 2 {
		t.Errorf("replicas_ready{role=\"prefill\"} = %v (present=%v), want 2", got, ok)
	}
	if got, ok := gauge(t, r.replicasDesired, lbl("pd/qwen-2p1d", "qwen", "prefill")); ok {
		t.Errorf("replicas_desired{role=\"prefill\"} = %v, want no series: one slice of two reported spec.replicas", got)
	}
}

// PD_ROLE_GROUP="" turns the per-role series off in the exporter itself, so a
// chart that disables them does not leave it probing an API it may not be
// allowed to read. No call, no log line, everything else unchanged.
func TestPDRolesEmptyGroupAsksNothing(t *testing.T) {
	hits := 0
	srv := pdServer(t, pdLWSJSON, 0, &hits)
	defer srv.Close()
	r := newTestResolver(t, srv)
	r.pdGroup = ""
	r.refresh(context.Background())

	if hits != 0 {
		t.Errorf("leaderworkersets asked %d times, want 0", hits)
	}
	if got, ok := gauge(t, r.replicas, lbl("pd/qwen-2p1d", "qwen", "")); !ok || got != 2 {
		t.Errorf("replicas{role=\"\"} = %v (present=%v), want 2", got, ok)
	}
	if n := testutil.CollectAndCount(r.replicas); n != 1 {
		t.Errorf("replicas series = %d, want 1", n)
	}
}
