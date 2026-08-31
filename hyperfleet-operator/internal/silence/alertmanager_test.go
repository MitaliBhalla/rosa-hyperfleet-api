package silence

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestAlertmanagerClientRoundTrip(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	store := map[string]GettableSilence{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v2/silences":
			var out []GettableSilence
			for _, s := range store {
				if s.Status.State == "active" {
					out = append(out, s)
				}
			}
			_ = json.NewEncoder(w).Encode(out)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/silences":
			var body PostableSilence
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			id := "silence-1"
			now := time.Now().UTC()
			store[id] = GettableSilence{
				ID:        id,
				Status:    SilenceStatus{State: "active"},
				UpdatedAt: now,
				Matchers:  body.Matchers,
				StartsAt:  body.StartsAt,
				EndsAt:    body.EndsAt,
				CreatedBy: body.CreatedBy,
				Comment:   body.Comment,
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"silenceID": id})
		case r.Method == http.MethodDelete:
			id := r.URL.Path[len("/api/v2/silence/"):]
			s, ok := store[id]
			if !ok {
				http.NotFound(w, r)
				return
			}
			s.Status.State = "expired"
			store[id] = s
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := NewAlertmanagerClient(server.URL, server.Client())
	identity := ClusterIdentity{Namespace: "cluster-1", Name: "c1"}
	now := time.Now().UTC()
	ctx := context.Background()

	id, err := client.Create(ctx, BuildPostableSilence(identity, ReasonInstalling, now, DefaultTTL))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	silences, err := client.List(ctx, identity)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(silences) != 1 || silences[0].ID != id {
		t.Fatalf("unexpected list result: %+v", silences)
	}

	if err := client.Expire(ctx, id); err != nil {
		t.Fatalf("expire: %v", err)
	}
	silences, err = client.List(ctx, identity)
	if err != nil {
		t.Fatalf("list after expire: %v", err)
	}
	if len(silences) != 0 {
		t.Fatalf("expected no active silences, got %+v", silences)
	}
}
