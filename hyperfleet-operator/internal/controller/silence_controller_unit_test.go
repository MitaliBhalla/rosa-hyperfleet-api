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
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hyperfleetv1alpha1 "github.com/openshift-online/rosa-hyperfleet-api/api/v1alpha1"
	"github.com/openshift-online/rosa-hyperfleet-api/hyperfleet-operator/internal/silence"
)

func TestSilenceReconcilerProvisioning(t *testing.T) {
	t.Parallel()

	const (
		clusterName = "silence-test-cluster"
		testNS      = "cluster-silence-test-id"
	)

	scheme := runtime.NewScheme()
	if err := hyperfleetv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}

	cluster := &hyperfleetv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: testNS},
		Spec:       hyperfleetv1alpha1.ClusterSpec{DisplayName: clusterName},
		Status:     hyperfleetv1alpha1.ClusterStatus{Phase: hyperfleetv1alpha1.ClusterPhaseProvisioning},
	}

	fakeSilence := silence.NewFakeClient()
	reconciler := &SilenceReconciler{
		Client:        fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).Build(),
		SilenceClient: fakeSilence,
		Clock:         func() time.Time { return time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC) },
	}

	ctx := context.Background()
	if _, err := reconciler.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: clusterName},
	}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	identity := silence.ClusterIdentity{Namespace: testNS, Name: clusterName}
	silences, err := fakeSilence.List(ctx, identity)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(silences) != 1 {
		t.Fatalf("expected 1 silence, got %d", len(silences))
	}
	if !silence.MatchesReason(silences[0], silence.ReasonInstalling) {
		t.Fatalf("unexpected comment: %s", silences[0].Comment)
	}
	if len(silences[0].Matchers) != 3 {
		t.Fatalf("expected install exemption matcher, got %d matchers", len(silences[0].Matchers))
	}
}

func TestSilenceReconcilerReadyExpiresSilence(t *testing.T) {
	t.Parallel()

	const (
		clusterName = "silence-ready-cluster"
		testNS      = "cluster-silence-ready-id"
	)

	scheme := runtime.NewScheme()
	if err := hyperfleetv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}

	cluster := &hyperfleetv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: testNS},
		Spec:       hyperfleetv1alpha1.ClusterSpec{DisplayName: clusterName},
		Status:     hyperfleetv1alpha1.ClusterStatus{Phase: hyperfleetv1alpha1.ClusterPhaseReady},
	}

	fakeSilence := silence.NewFakeClient()
	reconciler := &SilenceReconciler{
		Client:        fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).Build(),
		SilenceClient: fakeSilence,
		Clock:         func() time.Time { return time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC) },
	}

	ctx := context.Background()
	identity := silence.ClusterIdentity{Namespace: testNS, Name: clusterName}
	if _, err := fakeSilence.Create(ctx, silence.BuildPostableSilence(identity, silence.ReasonInstalling, time.Now().UTC(), silence.DefaultTTL)); err != nil {
		t.Fatalf("seed silence: %v", err)
	}

	if _, err := reconciler.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: clusterName},
	}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	silences, err := fakeSilence.List(ctx, identity)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(silences) != 0 {
		t.Fatalf("expected silences removed, got %d", len(silences))
	}
}

func TestSilenceReconcilerDeleting(t *testing.T) {
	t.Parallel()

	const (
		clusterName = "silence-delete-cluster"
		testNS      = "cluster-silence-delete-id"
	)

	scheme := runtime.NewScheme()
	if err := hyperfleetv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}

	now := metav1.Now()
	cluster := &hyperfleetv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:              clusterName,
			Namespace:         testNS,
			DeletionTimestamp: &now,
			Finalizers:        []string{"hyperfleet.io/cluster"},
		},
		Spec:   hyperfleetv1alpha1.ClusterSpec{DisplayName: clusterName},
		Status: hyperfleetv1alpha1.ClusterStatus{Phase: hyperfleetv1alpha1.ClusterPhaseDeleting},
	}

	fakeSilence := silence.NewFakeClient()
	reconciler := &SilenceReconciler{
		Client:        fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).Build(),
		SilenceClient: fakeSilence,
		Clock:         func() time.Time { return time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC) },
	}

	ctx := context.Background()
	if _, err := reconciler.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: clusterName},
	}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	identity := silence.ClusterIdentity{Namespace: testNS, Name: clusterName}
	silences, err := fakeSilence.List(ctx, identity)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(silences) != 1 {
		t.Fatalf("expected 1 silence, got %d", len(silences))
	}
	if !silence.MatchesReason(silences[0], silence.ReasonDeleting) {
		t.Fatalf("unexpected comment: %s", silences[0].Comment)
	}
	if len(silences[0].Matchers) != 2 {
		t.Fatalf("expected no install exemption, got %d matchers", len(silences[0].Matchers))
	}
}
