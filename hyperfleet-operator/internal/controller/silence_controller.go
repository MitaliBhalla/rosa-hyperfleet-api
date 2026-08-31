/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hyperfleetv1alpha1 "github.com/openshift-online/rosa-hyperfleet-api/api/v1alpha1"
	"github.com/openshift-online/rosa-hyperfleet-api/hyperfleet-operator/internal/silence"
)

const silenceAPIRetryDelay = time.Minute

// SilenceReconciler manages Alertmanager silences for cluster lifecycle phases.
type SilenceReconciler struct {
	client.Client
	SilenceClient           silence.Client
	Clock                   func() time.Time
	MaxConcurrentReconciles int
}

// +kubebuilder:rbac:groups=hyperfleet.io,resources=clusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=hyperfleet.io,resources=clusters/status,verbs=get

func (r *SilenceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	if r.SilenceClient == nil {
		return ctrl.Result{}, nil
	}

	var cluster hyperfleetv1alpha1.Cluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	now := time.Now().UTC()
	if r.Clock != nil {
		now = r.Clock().UTC()
	}

	identity := silence.IdentityFromCluster(&cluster)
	intent := silence.IntentForCluster(&cluster)
	if intent == nil && cluster.DeletionTimestamp.IsZero() && cluster.Status.Phase != hyperfleetv1alpha1.ClusterPhaseReady {
		log.V(1).Info("no silence intent for cluster phase", "phase", cluster.Status.Phase, "cluster", cluster.Name)
	}

	existing, err := r.SilenceClient.List(ctx, identity)
	if err != nil {
		log.Error(err, "failed to list silences", "cluster", cluster.Name)
		return ctrl.Result{RequeueAfter: silenceAPIRetryDelay}, nil
	}

	if intent == nil {
		if err := r.expireAll(ctx, existing); err != nil {
			log.Error(err, "failed to expire silences", "cluster", cluster.Name)
			return ctrl.Result{RequeueAfter: silenceAPIRetryDelay}, nil
		}
		return ctrl.Result{}, nil
	}

	var matched *silence.GettableSilence
	for i := range existing {
		s := existing[i]
		if silence.MatchesReason(s, intent.Reason) {
			if matched == nil {
				copy := s
				matched = &copy
				continue
			}
			if err := r.SilenceClient.Expire(ctx, s.ID); err != nil {
				log.Error(err, "failed to expire duplicate silence", "cluster", cluster.Name, "silenceID", s.ID)
				return ctrl.Result{RequeueAfter: silenceAPIRetryDelay}, nil
			}
		}
	}

	for _, s := range existing {
		if matched != nil && s.ID == matched.ID {
			continue
		}
		if !silence.MatchesReason(s, intent.Reason) {
			if err := r.SilenceClient.Expire(ctx, s.ID); err != nil {
				log.Error(err, "failed to expire stale silence", "cluster", cluster.Name, "silenceID", s.ID)
				return ctrl.Result{RequeueAfter: silenceAPIRetryDelay}, nil
			}
		}
	}

	if matched != nil {
		// Observatorium requires silence replacement when renewing; can't update in place like pure Alertmanager.
		if silence.NeedsRenewal(*matched, now) {
			if err := r.SilenceClient.Expire(ctx, matched.ID); err != nil {
				log.Error(err, "failed to expire silence for renewal", "cluster", cluster.Name, "silenceID", matched.ID)
				return ctrl.Result{RequeueAfter: silenceAPIRetryDelay}, nil
			}
			if _, err := r.createSilence(ctx, identity, intent.Reason, now); err != nil {
				log.Error(err, "failed to renew silence", "cluster", cluster.Name)
				return ctrl.Result{RequeueAfter: silenceAPIRetryDelay}, nil
			}
		}
		return ctrl.Result{RequeueAfter: silence.RequeueInterval}, nil
	}

	if _, err := r.createSilence(ctx, identity, intent.Reason, now); err != nil {
		log.Error(err, "failed to create silence", "cluster", cluster.Name)
		return ctrl.Result{RequeueAfter: silenceAPIRetryDelay}, nil
	}
	return ctrl.Result{RequeueAfter: silence.RequeueInterval}, nil
}

func (r *SilenceReconciler) createSilence(ctx context.Context, identity silence.ClusterIdentity, reason silence.Reason, now time.Time) (string, error) {
	postable := silence.BuildPostableSilence(identity, reason, now, silence.DefaultTTL)
	return r.SilenceClient.Create(ctx, postable)
}

func (r *SilenceReconciler) expireAll(ctx context.Context, silences []silence.GettableSilence) error {
	for _, s := range silences {
		if err := r.SilenceClient.Expire(ctx, s.ID); err != nil {
			return err
		}
	}
	return nil
}

func (r *SilenceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&hyperfleetv1alpha1.Cluster{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: r.MaxConcurrentReconciles}).
		Named("cluster-silence").
		Complete(r)
}

var _ reconcile.Reconciler = &SilenceReconciler{}
