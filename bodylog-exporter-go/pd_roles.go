package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// Engine counts per role for a prefill/decode-disaggregated release.
//
// The route's discovery Service does not select the engines. On a disaggregated
// release it selects the per-slice ROUTERS -- the pods that bind the public port --
// so everything built from its EndpointSlice describes routers and nothing else.
// That series keeps its numbers and is published as role="router"; the engines
// come from here, as role="prefill" and role="decode".
//
// Source: the per-(role, slice) LeaderWorkerSets the DisaggregatedSet controller
// creates. One LWS group is one engine -- a leader plus its workers -- so the LWS
// replica counts answer "how many engines of this role" directly. A pod count
// cannot: at leaderWorkerTemplate.size > 1 a group is several pods, and the role
// would read permanently over its desired count.
//
// Not sourced from the per-role headless Services (<rel>-<role>-hl-s<N>), which
// would otherwise be the obvious choice: they carry publishNotReadyAddresses so
// the router can resolve upstreams while engines are still loading, and the
// EndpointSlice controller then reports every address ready. A ready gauge built
// on them could never fall -- including during the drain it exists to show.
const (
	pdNameLabel = "disaggregatedset.x-k8s.io/name"
	pdRoleLabel = "disaggregatedset.x-k8s.io/role"
	// The role given to the counts derived from the discovery Service itself.
	pdRoleRouter = "router"
	// How long to stop asking after the API server says the LeaderWorkerSet API is
	// not there or not ours to read. Clusters without disaggregated serving are the
	// common case; one line in the log and a slow retry is the right volume for it.
	pdRetryAfter = 30 * time.Minute
)

// pdRoleRep: one role's engine counts, summed over the release's slices.
//
// slices/withSpec exist so desired is published only when it covers the whole
// role. A sum over the slices that happened to carry spec.replicas is a smaller
// number that still looks like an answer, which is worse than no answer: the
// caller comparing committed against running would read the gap as a scale-down.
type pdRoleRep struct {
	n, ready, desired int
	slices, withSpec  int
}

// desiredOK: every slice of this role reported spec.replicas.
func (p *pdRoleRep) desiredOK() bool { return p.slices > 0 && p.withSpec == p.slices }

type lwsList struct {
	Items []lwsItem `json:"items"`
}

type lwsItem struct {
	Metadata struct {
		Namespace string            `json:"namespace"`
		Name      string            `json:"name"`
		Labels    map[string]string `json:"labels"`
	} `json:"metadata"`
	Spec struct {
		Replicas *int32 `json:"replicas"`
	} `json:"spec"`
	Status struct {
		Replicas      *int32 `json:"replicas"`
		ReadyReplicas *int32 `json:"readyReplicas"`
	} `json:"status"`
}

// pdRoles folds every DisaggregatedSet-owned LeaderWorkerSet in the cluster into
//
//	"<namespace>/<disaggregatedset-name>" → role → counts
//
// One list call per refresh, not one per service: a release has an LWS per (role,
// slice), and several routes often resolve to the same release.
//
// Returns nil on any failure, which simply leaves the per-role series off this
// round. The router series and everything else are unaffected -- a disaggregated
// release still reports, it just reports the part the discovery Service can see.
//
// Called only from build, on the single refresh goroutine, so the retry state
// needs no lock.
func (r *podRouteResolver) pdRoles(ctx context.Context) map[string]map[string]*pdRoleRep {
	if r.pdGroup == "" { // PD_ROLE_GROUP="": the per-role series are off, so do not ask
		return nil
	}
	if time.Now().Before(r.pdSkipUntil) {
		return nil
	}
	sel := url.QueryEscape(pdNameLabel) // existence selector: every LWS a DisaggregatedSet owns
	u := fmt.Sprintf("%s/apis/%s/%s/leaderworkersets?labelSelector=%s", r.apiBase, r.pdGroup, r.pdVersion, sel)
	body, err := r.get(ctx, u)
	if err != nil {
		var se *httpStatusError
		// 404: no LeaderWorkerSet CRD. 403: the ClusterRole has not been given list on it.
		// Both are configuration, not a glitch, and both answer the same way every round.
		if errors.As(err, &se) && (se.code == http.StatusNotFound || se.code == http.StatusForbidden) {
			r.pdSkipUntil = time.Now().Add(pdRetryAfter)
			log.Printf("route-enrich: per-role replica series off (%s/%s leaderworkersets → HTTP %d); retrying in %s",
				r.pdGroup, r.pdVersion, se.code, pdRetryAfter)
			return nil
		}
		log.Printf("route-enrich: listing leaderworkersets failed: %v", err)
		return nil
	}
	var lst lwsList
	if err := json.Unmarshal(body, &lst); err != nil {
		log.Printf("route-enrich: decode LeaderWorkerSetList: %v", err)
		return nil
	}

	out := map[string]map[string]*pdRoleRep{}
	for _, it := range lst.Items {
		ds, role := it.Metadata.Labels[pdNameLabel], it.Metadata.Labels[pdRoleLabel]
		if ds == "" || role == "" {
			continue // owned by something that labels differently; not ours to interpret
		}
		key := it.Metadata.Namespace + "/" + ds
		byRole := out[key]
		if byRole == nil {
			byRole = map[string]*pdRoleRep{}
			out[key] = byRole
		}
		rep := byRole[role]
		if rep == nil {
			rep = &pdRoleRep{}
			byRole[role] = rep
		}
		// status.replicas / status.readyReplicas are omitempty, so a role with nothing
		// ready arrives as an absent field. That is a real zero -- the controller writes
		// the status subresource from the start -- and reading it as one is what lets a
		// drain show up. spec.replicas is the committed count and is always set; when it
		// is not, the desired series is skipped for the whole role rather than reported
		// as 0, for the reason given at replicasDesired.
		rep.slices++
		rep.n += int(deref32(it.Status.Replicas))
		rep.ready += int(deref32(it.Status.ReadyReplicas))
		if it.Spec.Replicas != nil {
			rep.desired += int(*it.Spec.Replicas)
			rep.withSpec++
		}
	}
	return out
}

// pdRoleNames: the roles of one release, sorted, so the gauges are written in a
// stable order and the logs read the same way twice.
func pdRoleNames(byRole map[string]*pdRoleRep) []string {
	names := make([]string, 0, len(byRole))
	for n := range byRole {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// pdKey: the map key for a discovery Service. The DisaggregatedSet and the Service
// the route resolves to share a name; "-leader" is stripped for the same reason
// normalizeServiceLabel strips it, so a release that has both shapes still matches.
func pdKey(ns, name string) string {
	return ns + "/" + strings.TrimSuffix(name, "-leader")
}

func deref32(p *int32) int32 {
	if p == nil {
		return 0
	}
	return *p
}
